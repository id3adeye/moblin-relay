// Self-hosted Moblin remote-control relay.
//
// Faithful reimplementation of eerimoq/moblin-remote-control-relay's routing,
// minus the Origin check (Node's `ws` does not enforce one), which is what lets
// a browser-based controller served from a different domain connect — the public
// Go relay rejects cross-origin WebSockets with 403.
//
// Roles (matching the upstream naming):
//   bridge   = the controller/assistant (this project's irl-remote browser app).
//              Opens a control lane, then one data lane per streamer connection.
//   streamer = the Moblin iOS app. Opens /streamer/{bridgeId}.
// The relay pairs them by bridgeId and pipes bytes between the streamer and the
// matching bridge data lane.
//
// Endpoints:
//   GET  /                                         health text
//   GET  /stats.json                               connection counts
//   WS   /bridge/control/{bridgeId}                controller control lane
//   WS   /bridge/data/{bridgeId}/{connectionId}    controller data lane
//   WS   /streamer/{bridgeId}                       Moblin app
//   WS   /status/{bridgeId}                         optional status viewers

import http from 'node:http';
import { randomUUID } from 'node:crypto';
import { WebSocketServer } from 'ws';

const PORT = Number(process.env.PORT) || 8080;

/** @type {Map<string, Bridge>} */
const bridges = new Map();
// Bridge: { control, connections: Map<connId, {streamer, data}>, statusSockets: Set }

function closeBridge(bridge) {
  if (bridge.control) safeClose(bridge.control);
  for (const conn of bridge.connections.values()) {
    if (conn.streamer) safeClose(conn.streamer);
    if (conn.data) safeClose(conn.data);
  }
  bridge.connections.clear();
  for (const s of bridge.statusSockets) safeClose(s);
  bridge.statusSockets.clear();
}

function safeClose(ws) {
  try { ws.close(); } catch { /* ignore */ }
}

function safeSend(ws, data, isBinary) {
  if (!ws || ws.readyState !== ws.OPEN) return;
  try { ws.send(data, { binary: isBinary }); } catch { /* ignore */ }
}

function handleControl(ws, bridgeId) {
  const existing = bridges.get(bridgeId);
  if (existing) {
    // A new controller takes over the bridge id; evict the old one.
    safeSend(existing.control, JSON.stringify({ type: 'kicked' }), false);
    closeBridge(existing);
  }
  const bridge = { control: ws, connections: new Map(), statusSockets: new Set() };
  bridges.set(bridgeId, bridge);

  ws.on('message', (data, isBinary) => {
    for (const s of bridge.statusSockets) safeSend(s, data, isBinary);
  });
  ws.on('close', () => {
    if (bridges.get(bridgeId) === bridge) bridges.delete(bridgeId);
    closeBridge(bridge);
  });
}

function handleStreamer(ws, bridgeId) {
  const bridge = bridges.get(bridgeId);
  if (!bridge || !bridge.control) { safeClose(ws); return; }

  const connectionId = randomUUID();
  const conn = { streamer: ws, data: null };
  bridge.connections.set(connectionId, conn);

  safeSend(bridge.control, JSON.stringify({ type: 'connect', data: { connectionId } }), false);

  ws.on('message', (data, isBinary) => safeSend(conn.data, data, isBinary));
  ws.on('close', () => {
    bridge.connections.delete(connectionId);
    if (conn.data) safeClose(conn.data);
  });
}

function handleData(ws, bridgeId, connectionId) {
  const bridge = bridges.get(bridgeId);
  if (!bridge) { safeClose(ws); return; }
  const conn = bridge.connections.get(connectionId);
  if (!conn) { safeClose(ws); return; }

  conn.data = ws;
  ws.on('message', (data, isBinary) => safeSend(conn.streamer, data, isBinary));
  ws.on('close', () => {
    bridge.connections.delete(connectionId);
    if (conn.streamer) safeClose(conn.streamer);
  });
}

function handleStatus(ws, bridgeId) {
  const bridge = bridges.get(bridgeId);
  if (!bridge || !bridge.control) { safeClose(ws); return; }

  if (bridge.statusSockets.size === 0) {
    safeSend(bridge.control, JSON.stringify({ type: 'startStatus' }), false);
  }
  bridge.statusSockets.add(ws);
  ws.on('close', () => {
    bridge.statusSockets.delete(ws);
    if (bridge.statusSockets.size === 0) {
      safeSend(bridge.control, JSON.stringify({ type: 'stopStatus' }), false);
    }
  });
}

const wss = new WebSocketServer({ noServer: true });

const server = http.createServer((req, res) => {
  if (req.url === '/stats.json') {
    let streamers = 0;
    let dataLanes = 0;
    for (const b of bridges.values()) {
      for (const c of b.connections.values()) {
        if (c.streamer) streamers++;
        if (c.data) dataLanes++;
      }
    }
    res.setHeader('content-type', 'application/json');
    res.end(JSON.stringify({ bridges: bridges.size, streamers, dataLanes }));
    return;
  }
  res.setHeader('content-type', 'text/plain');
  res.end('moblin-relay ok\n');
});

server.on('upgrade', (req, socket, head) => {
  let pathname;
  try {
    pathname = new URL(req.url, 'http://localhost').pathname;
  } catch {
    socket.destroy();
    return;
  }
  const parts = pathname.split('/').filter(Boolean);

  let route = null;
  if (parts[0] === 'bridge' && parts[1] === 'control' && parts.length === 3) {
    route = () => handleControl(wsConn, parts[2]);
  } else if (parts[0] === 'bridge' && parts[1] === 'data' && parts.length === 4) {
    route = () => handleData(wsConn, parts[2], parts[3]);
  } else if (parts[0] === 'streamer' && parts.length === 2) {
    route = () => handleStreamer(wsConn, parts[1]);
  } else if (parts[0] === 'status' && parts.length === 2) {
    route = () => handleStatus(wsConn, parts[1]);
  }

  if (!route) {
    socket.destroy();
    return;
  }

  let wsConn;
  wss.handleUpgrade(req, socket, head, (ws) => {
    wsConn = ws;
    ws.isAlive = true;
    ws.on('pong', () => { ws.isAlive = true; });
    route();
  });
});

// WS-level keepalive so reverse proxies / tunnels don't drop idle bridges.
const heartbeat = setInterval(() => {
  for (const ws of wss.clients) {
    if (ws.isAlive === false) { ws.terminate(); continue; }
    ws.isAlive = false;
    try { ws.ping(); } catch { /* ignore */ }
  }
}, 30000);
wss.on('close', () => clearInterval(heartbeat));

server.listen(PORT, () => {
  console.log(`moblin-relay listening on :${PORT}`);
});
