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
// Text frames are JSON control messages from the relay:
//   {"t":"welcome","id":n,"code":"ABCDE"}, {"t":"join","id":n},
//   {"t":"leave","id":n}, {"t":"error","msg":"..."}
// and from the host: {"t":"kick","id":n}. "ping" gets "pong" (keeps the
// connection alive without waking the room).

import { DurableObject } from "cloudflare:workers";

const ALPHABET = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789";
const CODE_LENGTH = 5;
const MAX_PLAYERS = 4;
const HOST = 1;

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
      const match = url.pathname.match(/^\/join\/([A-Za-z0-9]+)$/);
      if (!match) return new Response("Not found", { status: 404 });
      code = match[1].toUpperCase().replaceAll("0", "O").replaceAll("1", "I");
      role = "join";
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
    return ws.deserializeAttachment()?.id ?? 0;
  }

  // Sockets in the room that have a player id, by id.
  players() {
    const map = new Map();
    for (const ws of this.ctx.getWebSockets()) {
      const id = this.idOf(ws);
      if (id) map.set(id, ws);
    }
    return map;
  }

  async fetch(request) {
    const url = new URL(request.url);
    const code = url.searchParams.get("code");
    const role = url.searchParams.get("role");
    const players = this.players();
    const [client, server] = Object.values(new WebSocketPair());
    this.ctx.acceptWebSocket(server);
    const refuse = (msg) => {
      server.send(JSON.stringify({ t: "error", msg }));
      server.close(4000, msg);
      return new Response(null, { status: 101, webSocket: client });
    };
    const host = players.get(HOST);
    let id;
    if (role === "host") {
      if (host) return refuse("That party code is taken. Try hosting again.");
      id = HOST;
    } else {
      if (!host) return refuse("No party with that code. Check it with the host.");
      if (players.size >= MAX_PLAYERS) return refuse("That party is full.");
      do {
        id = 2 + Math.floor(Math.random() * 0x7ffffff0);
      } while (players.has(id));
    }
    server.serializeAttachment({ id });
    server.send(JSON.stringify({ t: "welcome", id, code }));
    if (id !== HOST) host.send(JSON.stringify({ t: "join", id }));
    return new Response(null, { status: 101, webSocket: client });
  }

  async webSocketMessage(ws, message) {
    const from = this.idOf(ws);
    if (!from) return;
    const players = this.players();
    if (typeof message === "string") {
      let msg;
      try {
        msg = JSON.parse(message);
      } catch {
        return;
      }
      if (from === HOST && msg.t === "kick") {
        const target = players.get(msg.id);
        if (target) this.drop(target, "You were removed from the party.");
      }
      return;
    }
    const data = new Uint8Array(message);
    if (data.length < 4) return;
    const view = new DataView(data.buffer, data.byteOffset, data.byteLength);
    const target = view.getInt32(0, true);
    // Same frame, with the sender written over the target.
    const out = new Uint8Array(data);
    new DataView(out.buffer).setInt32(0, from, true);
    if (from !== HOST) {
      players.get(HOST)?.send(out);
      return;
    }
    for (const [id, socket] of players) {
      if (id === HOST) continue;
      if (target > 0 && id !== target) continue;
      if (target < 0 && id === -target) continue;
      socket.send(out);
    }
  }

  async webSocketClose(ws) {
    this.left(ws);
  }

  async webSocketError(ws) {
    this.left(ws);
  }

  drop(ws, reason) {
    try {
      ws.send(JSON.stringify({ t: "error", msg: reason }));
      ws.close(4001, reason);
    } catch {}
    this.left(ws);
  }

  // A phone left: if it was the host the party is over, else tell the host.
  left(ws) {
    const id = this.idOf(ws);
    if (!id) return;
    try {
      ws.serializeAttachment({ id: 0 });
    } catch {}
    try {
      ws.close(1000, "bye");
    } catch {}
    const players = this.players();
    if (id === HOST) {
      for (const socket of players.values()) this.drop(socket, "The host left the party.");
    } else {
      players.get(HOST)?.send(JSON.stringify({ t: "leave", id }));
    }
  }
}
