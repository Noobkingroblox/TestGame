// Online relay for TestGame's friends multiplayer.
//
// Each party is one Durable Object ("room"), named by a 5-letter party code.
// Phones connect over WebSocket: the host to /host (the relay picks a code),
// friends to /join/CODE. The relay never looks inside game messages; it only
// passes them between the host and the friends, like the game's old direct
// connection did, so it works on any Wi-Fi or mobile data.
//
// Binary frames, little-endian:
//   phone -> relay: [int32 target][payload]  (friends can only reach the host;
//                   the host targets one friend, 0 = all, -id = all but id)
//   relay -> phone: [int32 sender][payload]
// (The phones put their own [seq][ack] after that, so they can resend what a
// dropped connection lost; the relay doesn't read it.)
// Text frames are JSON control messages from the relay:
//   {"t":"welcome","id":n,"code":"ABCDE","token":"...","resumed":bool},
//   {"t":"join","id":n}, {"t":"leave","id":n}, {"t":"away","id":n},
//   {"t":"back","id":n}, {"t":"error","msg":"..."}
// and from phones: {"t":"kick","id":n} (host), {"t":"bye"} (leaving on purpose).
// "ping" gets "pong" (keeps the connection alive without waking the room).
//
// Unstable Wi-Fi: when a phone's connection drops without "bye", its seat is
// kept for GRACE_MS. The phone reconnects to /resume/CODE?id=n&token=t and
// gets the same id back; everyone else is told "away" and then "back". Only
// when the time runs out does the party treat them as gone (the host gone
// ends the party).

import { DurableObject } from "cloudflare:workers";

const ALPHABET = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789";
const CODE_LENGTH = 5;
const MAX_PLAYERS = 4;
const HOST = 1;
const GRACE_MS = 60_000;

function randomCode() {
  const bytes = crypto.getRandomValues(new Uint8Array(CODE_LENGTH));
  return Array.from(bytes, (b) => ALPHABET[b % ALPHABET.length]).join("");
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    if (request.headers.get("Upgrade") !== "websocket") {
      return new Response("TestGame relay is running.\n", { status: url.pathname === "/" ? 200 : 426 });
    }
    let code;
    let role;
    if (url.pathname === "/host") {
      code = randomCode();
      role = "host";
    } else {
      const match = url.pathname.match(/^\/(join|resume)\/([A-Za-z0-9]+)$/);
      if (!match) return new Response("Not found", { status: 404 });
      role = match[1];
      code = match[2].toUpperCase().replaceAll("0", "O").replaceAll("1", "I");
    }
    const room = env.ROOMS.get(env.ROOMS.idFromName(code));
    const inner = new URL(request.url);
    inner.searchParams.set("code", code);
    inner.searchParams.set("role", role);
    return room.fetch(new Request(inner, request));
  },
};

export class Room extends DurableObject {
  constructor(ctx, env) {
    super(ctx, env);
    this.ctx.setWebSocketAutoResponse(new WebSocketRequestResponsePair("ping", "pong"));
  }

  idOf(ws) {
    try {
      return ws.deserializeAttachment()?.id ?? 0;
    } catch {
      return 0;
    }
  }

  forget(ws) {
    try {
      ws.serializeAttachment({ id: 0 });
    } catch {}
  }

  // Connected sockets that hold a seat, by player id.
  sockets() {
    const map = new Map();
    for (const ws of this.ctx.getWebSockets()) {
      const id = this.idOf(ws);
      if (id) map.set(id, ws);
    }
    return map;
  }

  // Seats: id -> token, and id -> time their grace runs out (if away).
  async seats() {
    return (await this.ctx.storage.get("seats")) ?? { tokens: {}, away: {} };
  }

  async save(seats) {
    await this.ctx.storage.put("seats", seats);
    const deadlines = Object.values(seats.away);
    if (deadlines.length) await this.ctx.storage.setAlarm(Math.min(...deadlines));
    else await this.ctx.storage.deleteAlarm();
  }

  send(ws, msg) {
    try {
      ws?.send(typeof msg === "string" || msg instanceof Uint8Array ? msg : JSON.stringify(msg));
    } catch {}
  }

  // Tells the host about a friend, or every friend about the host.
  tellOthers(id, msg) {
    const sockets = this.sockets();
    if (id === HOST) {
      for (const [other, ws] of sockets) if (other !== HOST) this.send(ws, msg);
    } else {
      this.send(sockets.get(HOST), msg);
    }
  }

  async fetch(request) {
    const url = new URL(request.url);
    const code = url.searchParams.get("code");
    const role = url.searchParams.get("role");
    const seats = await this.seats();
    const sockets = this.sockets();
    const [client, server] = Object.values(new WebSocketPair());
    this.ctx.acceptWebSocket(server);
    const answer = () => new Response(null, { status: 101, webSocket: client });
    const refuse = (msg) => {
      this.send(server, { t: "error", msg });
      server.close(4000, msg);
      return answer();
    };
    const hostSeat = seats.tokens[HOST];
    let id;
    let resumed = false;
    if (role === "host") {
      if (hostSeat) return refuse("That party code is taken. Try hosting again.");
      id = HOST;
    } else if (role === "resume") {
      id = Number(url.searchParams.get("id"));
      if (!id || seats.tokens[id] !== url.searchParams.get("token")) return refuse("That party has ended.");
      // A stale connection still holding the seat makes way.
      const old = sockets.get(id);
      if (old) {
        this.forget(old);
        try {
          old.close(1000, "replaced");
        } catch {}
      }
      resumed = true;
    } else {
      if (!hostSeat) return refuse("No party with that code. Check it with the host.");
      if (seats.away[HOST]) return refuse("The host is reconnecting. Try again in a moment.");
      if (Object.keys(seats.tokens).length >= MAX_PLAYERS) return refuse("That party is full.");
      do {
        id = 2 + Math.floor(Math.random() * 0x7ffffff0);
      } while (seats.tokens[id]);
    }
    const token = resumed ? seats.tokens[id] : crypto.randomUUID();
    seats.tokens[id] = token;
    const wasAway = Boolean(seats.away[id]);
    delete seats.away[id];
    await this.save(seats);
    server.serializeAttachment({ id });
    this.send(server, { t: "welcome", id, code, token, resumed });
    if (!resumed) {
      if (id !== HOST) this.send(sockets.get(HOST), { t: "join", id });
    } else if (wasAway) {
      this.tellOthers(id, { t: "back", id });
    }
    return answer();
  }

  async webSocketMessage(ws, message) {
    const from = this.idOf(ws);
    if (!from) return;
    if (typeof message === "string") {
      let msg;
      try {
        msg = JSON.parse(message);
      } catch {
        return;
      }
      if (msg.t === "bye") {
        this.forget(ws);
        await this.leave(from);
      } else if (from === HOST && msg.t === "kick" && msg.id !== HOST) {
        const target = this.sockets().get(msg.id);
        if (target) {
          this.forget(target);
          this.send(target, { t: "error", msg: "You were removed from the party." });
          try {
            target.close(4001, "You were removed from the party.");
          } catch {}
        }
        await this.leave(msg.id);
      }
      return;
    }
    const data = new Uint8Array(message);
    if (data.length < 4) return;
    const target = new DataView(data.buffer, data.byteOffset, data.byteLength).getInt32(0, true);
    // Same frame, with the sender written over the target.
    const out = new Uint8Array(data);
    new DataView(out.buffer).setInt32(0, from, true);
    const sockets = this.sockets();
    if (from !== HOST) {
      this.send(sockets.get(HOST), out);
      return;
    }
    for (const [id, socket] of sockets) {
      if (id === HOST) continue;
      if (target > 0 && id !== target) continue;
      if (target < 0 && id === -target) continue;
      this.send(socket, out);
    }
  }

  async webSocketClose(ws) {
    await this.dropped(ws);
  }

  async webSocketError(ws) {
    await this.dropped(ws);
  }

  // A connection dropped without "bye": keep the seat for GRACE_MS.
  async dropped(ws) {
    const id = this.idOf(ws);
    this.forget(ws);
    try {
      ws.close(1000, "bye");
    } catch {}
    if (!id) return;
    const seats = await this.seats();
    if (!seats.tokens[id]) return;
    seats.away[id] = Date.now() + GRACE_MS;
    await this.save(seats);
    this.tellOthers(id, { t: "away", id });
  }

  // Seats whose grace ran out.
  async alarm() {
    const seats = await this.seats();
    const now = Date.now();
    for (const [id, deadline] of Object.entries(seats.away)) {
      if (deadline <= now) await this.leave(Number(id));
    }
  }

  // Someone is gone for good: the host gone ends the party.
  async leave(id) {
    const seats = await this.seats();
    if (!seats.tokens[id]) return;
    delete seats.tokens[id];
    delete seats.away[id];
    if (id === HOST) {
      for (const [other, ws] of this.sockets()) {
        this.forget(ws);
        this.send(ws, { t: "error", msg: "The host left the party." });
        try {
          ws.close(4001, "The host left the party.");
        } catch {}
      }
      await this.ctx.storage.deleteAll();
      await this.ctx.storage.deleteAlarm();
      return;
    }
    await this.save(seats);
    this.send(this.sockets().get(HOST), { t: "leave", id });
  }
}
