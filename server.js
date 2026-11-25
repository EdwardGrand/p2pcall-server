const { WebSocketServer } = require("ws");
const wss = new WebSocketServer({ port: process.env.PORT || 8080 });
const rooms = new Map();
const users = new Map();

setInterval(() => {
  const now = Date.now();
  for (const [id, r] of rooms) if (r.expiresAt < now || r.peers.size === 0) rooms.delete(id);
}, 30000);

wss.on("connection", (ws) => {
  let currentRoom = null;
  let registeredUserId = null;

  ws.on("message", (raw) => {
    let msg;
    try { msg = JSON.parse(raw.toString()); } catch { return; }
    const { type, roomId, token } = msg;

    if (type === "register") {
      if (!msg.userId) return;
      registeredUserId = String(msg.userId);
      users.set(registeredUserId, ws);
      return;
    }

    if (type === "ring") {
      const target = users.get(String(msg.toUserId || ""));
      if (!target || target.readyState !== 1) {
        if (ws.readyState === 1) {
          ws.send(JSON.stringify({ type: "error", message: "Contact is offline" }));
        }
        return;
      }
      target.send(JSON.stringify({
        type: "incoming_call",
        fromUserId: String(msg.fromUserId || ""),
        fromName: String(msg.fromName || "Caller"),
        roomId: String(roomId || ""),
        token: String(token || "")
      }));
      return;
    }

    if (type === "accept") {
      const caller = users.get(String(msg.toUserId || ""));
      if (caller && caller.readyState === 1) {
        caller.send(JSON.stringify({
          type: "call_accepted",
          roomId: String(roomId || ""),
          token: String(token || "")
        }));
      }
      return;
    }

    if (type === "decline") {
      const caller = users.get(String(msg.toUserId || ""));
      if (caller && caller.readyState === 1) {
        caller.send(JSON.stringify({
          type: "call_declined",
          roomId: String(roomId || "")
        }));
      }
      return;
    }

    if (type === "create") {
      if (!roomId || !token || rooms.has(roomId)) return ws.send(JSON.stringify({ type: "error", message: "Invalid room" }));
      rooms.set(roomId, { token, peers: new Set([ws]), expiresAt: Date.now() + 3600_000 });
      currentRoom = roomId;
      ws.send(JSON.stringify({ type: "created", roomId, token }));
    } else if (type === "join") {
      const r = rooms.get(roomId);
      if (!r || r.token !== token || r.peers.size >= 2) return ws.send(JSON.stringify({ type: "error", message: "Invalid code or room full" }));
      r.peers.add(ws); currentRoom = roomId;
      for (const p of r.peers) if (p !== ws && p.readyState === 1) p.send(JSON.stringify({ type: "peer_joined" }));
    } else if (["offer", "answer", "ice"].includes(type)) {
      const r = rooms.get(roomId);
      if (!r || !r.peers.has(ws)) return;
      for (const p of r.peers) if (p !== ws && p.readyState === 1) p.send(JSON.stringify(msg));
    }
  });

  ws.on("close", () => {
    if (registeredUserId && users.get(registeredUserId) === ws) {
      users.delete(registeredUserId);
    }
    if (!currentRoom) return;
    const r = rooms.get(currentRoom);
    if (!r) return;
    r.peers.delete(ws);
    for (const p of r.peers) if (p.readyState === 1) p.send(JSON.stringify({ type: "peer_left" }));
    if (r.peers.size === 0) rooms.delete(currentRoom);
  });
});

console.log("P2PCall signaling server running on port", process.env.PORT || 8080);
