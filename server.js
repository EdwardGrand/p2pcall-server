const { WebSocketServer } = require("ws");
const wss = new WebSocketServer({ port: process.env.PORT || 8080 });
const rooms = new Map(); // roomId -> { token, peers: Set<ws>, expiresAt }

setInterval(() => {
  const now = Date.now();
  for (const [id, r] of rooms) if (r.expiresAt < now || r.peers.size === 0) rooms.delete(id);
}, 30000);

wss.on("connection", (ws) => {
  let currentRoom = null;
  ws.on("message", (raw) => {
    let msg;
    try { msg = JSON.parse(raw.toString()); } catch { return; }
    const { type, roomId, token } = msg;
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
    if (!currentRoom) return;
    const r = rooms.get(currentRoom);
    if (!r) return;
    r.peers.delete(ws);
    for (const p of r.peers) if (p.readyState === 1) p.send(JSON.stringify({ type: "peer_left" }));
    if (r.peers.size === 0) rooms.delete(currentRoom);
  });
});
console.log("P2PCall signaling server running on port", process.env.PORT || 8080);
