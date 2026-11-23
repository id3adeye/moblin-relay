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
import { pathToFileURL } from 'node:url';
import { WebSocketServer } from 'ws';

const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
// Reject ASCII control characters (0x00-0x1F, 0x7F) while allowing any printable
// free-text bridge ID (UUIDs, custom strings from MoblinPanel, env vars).
// eslint-disable-next-line no-control-regex
const CONTROL_CHAR_RE = /[\x00-\x1f\x7f]/;

export const DEFAULTS = {
  maxPayload: 4 * 1024 * 1024, // 4 MiB (covers base64 JPEG previews, stops 100 MiB deflate bombs)
  maxTotalClients: 1024,
  maxBridges: 256,
  maxConnectionsPerBridge: 8,
  maxStatusPerBridge: 8,
  controlPingMaxBytes: 256,
  statusMaxBufferedBytes: 64 * 1024, // 64 KiB
  dataMaxBufferedBytes: 8 * 1024 * 1024, // 8 MiB
  attachTimeoutMs: 15_000,
  maxPendingMessages: 32,
  maxPendingBytes: 256 * 1024, // 256 KiB
  heartbeatIntervalMs: 15_000,
  headersTimeoutMs: 10_000,
  requestTimeoutMs: 15_000,
  shutdownGraceMs: 5_000,
};

export function isValidBridgeId(id) {
  return typeof id === 'string' && id.length >= 1 && id.length <= 128 && !CONTROL_CHAR_RE.test(id);
}

export function isValidConnectionId(id) {
  return typeof id === 'string' && UUID_RE.test(id);
}

function dataByteLength(data) {
  if (typeof data === 'string') return Buffer.byteLength(data);
  if (Buffer.isBuffer(data) || ArrayBuffer.isView(data)) return data.byteLength;
  if (data instanceof ArrayBuffer) return data.byteLength;
  if (Array.isArray(data)) return data.reduce((sum, buf) => sum + buf.byteLength, 0);
  return 0;
}

function rejectUpgrade(socket, statusCode, message) {
  try {
    if (socket.writable) {
      socket.write(`HTTP/1.1 ${statusCode} ${message}\r\nConnection: close\r\nContent-Length: 0\r\n\r\n`);
    }
  } catch { /* ignore */ }
  socket.destroy();
}

export function createRelayServer(overrides = {}) {
  const opts = { ...DEFAULTS, ...overrides };

  /** @type {Map<string, Bridge>} */
  const bridges = new Map();
  // Bridge: { control, connections: Map<connId, Conn>, statusSockets: Set }
  // Conn: { streamer, data, pending: Array<{data, isBinary}>, pendingBytes, attachTimer }

  function safeClose(ws, code, reason) {
    if (!ws) return;
    try {
      if (code !== undefined) ws.close(code, reason);
      else ws.close();
    } catch { /* ignore */ }
  }

  function safeTerminate(ws) {
    if (!ws) return;
    try { ws.terminate(); } catch { /* ignore */ }
  }

  function clearAttachTimer(conn) {
    if (conn.attachTimer) {
      clearTimeout(conn.attachTimer);
      conn.attachTimer = null;
    }
  }

  function closeConnection(conn, code, reason) {
    clearAttachTimer(conn);
    conn.pending.length = 0;
    conn.pendingBytes = 0;
    if (conn.streamer) safeClose(conn.streamer, code, reason);
    if (conn.data) safeClose(conn.data, code, reason);
  }

  function closeBridge(bridge, code, reason) {
    if (bridge.control) safeClose(bridge.control, code, reason);
    for (const conn of bridge.connections.values()) {
      closeConnection(conn, code, reason);
    }
    bridge.connections.clear();
    for (const s of bridge.statusSockets) safeClose(s, code, reason);
    bridge.statusSockets.clear();
  }

  function safeSend(ws, data, isBinary, { maxBufferedBytes = opts.dataMaxBufferedBytes, dropOnBackpressure = false } = {}) {
    if (!ws || ws.readyState !== ws.OPEN) return false;
    if (ws.bufferedAmount > maxBufferedBytes) {
      if (dropOnBackpressure) return false;
      safeTerminate(ws);
      return false;
    }
    try {
      ws.send(data, { binary: isBinary });
      return true;
    } catch {
      return false;
    }
  }

  function handleControl(ws, bridgeId) {
    const existing = bridges.get(bridgeId);
    if (!existing && bridges.size >= opts.maxBridges) {
      // Never send `rateLimitExceeded` (irl-remote gives up permanently on it).
      safeClose(ws, 1013, 'Bridge limit reached');
      return;
    }
    if (existing) {
      // A new controller takes over the bridge id; evict the old one.
      safeSend(existing.control, JSON.stringify({ type: 'kicked' }), false);
      closeBridge(existing);
    }
    const bridge = { control: ws, connections: new Map(), statusSockets: new Set() };
    bridges.set(bridgeId, bridge);

    ws.on('message', (data, isBinary) => {
      if (!isBinary && dataByteLength(data) <= opts.controlPingMaxBytes) {
        try {
          const msg = JSON.parse(data.toString());
          if (msg && msg.type === 'ping') {
            safeSend(ws, JSON.stringify({ type: 'pong', t: Date.now() }), false);
            return;
          }
        } catch { /* non-JSON control payload: forward as before */ }
      }
      for (const s of bridge.statusSockets) {
        safeSend(s, data, isBinary, {
          maxBufferedBytes: opts.statusMaxBufferedBytes,
          dropOnBackpressure: true,
        });
      }
    });
    ws.on('close', () => {
      if (bridges.get(bridgeId) === bridge) bridges.delete(bridgeId);
      closeBridge(bridge);
    });
  }

  function handleStreamer(ws, bridgeId) {
    const bridge = bridges.get(bridgeId);
    if (!bridge || !bridge.control) { safeClose(ws); return; }
    if (bridge.connections.size >= opts.maxConnectionsPerBridge) {
      safeClose(ws, 1013, 'Connection limit reached');
      return;
    }

    const connectionId = randomUUID();
    const conn = {
      streamer: ws,
      data: null,
      pending: [],
      pendingBytes: 0,
      attachTimer: null,
    };
    bridge.connections.set(connectionId, conn);

    conn.attachTimer = setTimeout(() => {
      conn.attachTimer = null;
      if (bridge.connections.get(connectionId) === conn && !conn.data) {
        bridge.connections.delete(connectionId);
        conn.pending.length = 0;
        conn.pendingBytes = 0;
        safeClose(ws, 1008, 'Data lane attach timeout');
      }
    }, opts.attachTimeoutMs);
    conn.attachTimer.unref?.();

    safeSend(bridge.control, JSON.stringify({ type: 'connect', data: { connectionId } }), false);

    ws.on('message', (data, isBinary) => {
      if (conn.data) {
        safeSend(conn.data, data, isBinary);
        return;
      }
      const size = dataByteLength(data);
      if (conn.pending.length < opts.maxPendingMessages && conn.pendingBytes + size <= opts.maxPendingBytes) {
        conn.pending.push({ data, isBinary });
        conn.pendingBytes += size;
      }
    });
    ws.on('close', () => {
      if (bridge.connections.get(connectionId) !== conn || conn.streamer !== ws) return;
      clearAttachTimer(conn);
      bridge.connections.delete(connectionId);
      conn.pending.length = 0;
      conn.pendingBytes = 0;
      if (conn.data) safeClose(conn.data);
    });
  }

  function handleData(ws, bridgeId, connectionId) {
    const bridge = bridges.get(bridgeId);
    if (!bridge) { safeClose(ws); return; }
    const conn = bridge.connections.get(connectionId);
    if (!conn) { safeClose(ws); return; }
    if (conn.data) {
      safeClose(ws, 1008, 'Data lane already attached');
      return;
    }

    clearAttachTimer(conn);
    conn.data = ws;

    if (conn.pending.length > 0) {
      const queued = conn.pending.splice(0);
      conn.pendingBytes = 0;
      for (const frame of queued) {
        if (!safeSend(ws, frame.data, frame.isBinary)) break;
      }
    }

    ws.on('message', (data, isBinary) => safeSend(conn.streamer, data, isBinary));
    ws.on('close', () => {
      if (bridge.connections.get(connectionId) !== conn || conn.data !== ws) return;
      clearAttachTimer(conn);
      bridge.connections.delete(connectionId);
      if (conn.streamer) safeClose(conn.streamer);
    });
  }

  function handleStatus(ws, bridgeId) {
    const bridge = bridges.get(bridgeId);
    if (!bridge || !bridge.control) { safeClose(ws); return; }
    if (bridge.statusSockets.size >= opts.maxStatusPerBridge) {
      safeClose(ws, 1013, 'Status limit reached');
      return;
    }

    if (bridge.statusSockets.size === 0) {
      safeSend(bridge.control, JSON.stringify({ type: 'startStatus' }), false);
    }
    bridge.statusSockets.add(ws);
    ws.on('close', () => {
      if (!bridge.statusSockets.delete(ws)) return;
      if (bridge.statusSockets.size === 0) {
        safeSend(bridge.control, JSON.stringify({ type: 'stopStatus' }), false);
      }
    });
  }

  const wss = new WebSocketServer({
    noServer: true,
    maxPayload: opts.maxPayload,
    // Compress JSON control/status/data frames for cellular clients. Small 2 KB
    // window keeps per-connection memory low while compressing repeating Moblin
    // status payloads (~5s ticks) by ~70-85% and base64 JPEG previews ~20-25%.
    perMessageDeflate: {
      threshold: 64,
      serverMaxWindowBits: 11,
      zlibDeflateOptions: { memLevel: 4 },
    },
  });

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

  server.headersTimeout = opts.headersTimeoutMs;
  server.requestTimeout = opts.requestTimeoutMs;

  server.on('upgrade', (req, socket, head) => {
    const onSocketError = () => socket.destroy();
    socket.on('error', onSocketError);

    if (wss.clients.size >= opts.maxTotalClients) {
      rejectUpgrade(socket, 503, 'Service Unavailable');
      return;
    }

    let pathname;
    try {
      pathname = new URL(req.url, 'http://localhost').pathname;
    } catch {
      rejectUpgrade(socket, 400, 'Bad Request');
      return;
    }
    const parts = pathname.split('/').filter(Boolean);

    let route = null;
    if (parts[0] === 'bridge' && parts[1] === 'control' && parts.length === 3) {
      if (isValidBridgeId(parts[2])) route = (ws) => handleControl(ws, parts[2]);
    } else if (parts[0] === 'bridge' && parts[1] === 'data' && parts.length === 4) {
      if (isValidBridgeId(parts[2]) && isValidConnectionId(parts[3])) {
        route = (ws) => handleData(ws, parts[2], parts[3]);
      }
    } else if (parts[0] === 'streamer' && parts.length === 2) {
      if (isValidBridgeId(parts[1])) route = (ws) => handleStreamer(ws, parts[1]);
    } else if (parts[0] === 'status' && parts.length === 2) {
      if (isValidBridgeId(parts[1])) route = (ws) => handleStatus(ws, parts[1]);
    }

    if (!route) {
      rejectUpgrade(socket, 400, 'Bad Request');
      return;
    }

    wss.handleUpgrade(req, socket, head, (ws) => {
      socket.removeListener('error', onSocketError);
      ws.on('error', () => safeTerminate(ws));
      ws.isAlive = true;
      ws.on('pong', () => { ws.isAlive = true; });
      ws.on('message', () => { ws.isAlive = true; });
      route(ws);
    });
  });

  // WS-level keepalive so reverse proxies / tunnels don't drop idle bridges
  // and half-open cellular sockets are reaped within 15-30s instead of 60s.
  const heartbeat = setInterval(() => {
    for (const ws of wss.clients) {
      if (ws.isAlive === false) { safeTerminate(ws); continue; }
      ws.isAlive = false;
      try { ws.ping(); } catch { /* ignore */ }
    }
  }, opts.heartbeatIntervalMs);
  heartbeat.unref?.();
  wss.on('close', () => clearInterval(heartbeat));

  let closing = false;
  function close() {
    if (closing) return Promise.resolve();
    closing = true;
    clearInterval(heartbeat);
    for (const bridge of bridges.values()) {
      closeBridge(bridge, 1001, 'Going Away');
    }
    bridges.clear();
    for (const ws of wss.clients) {
      safeClose(ws, 1001, 'Going Away');
    }
    return new Promise((resolve) => {
      server.close(() => resolve());
    });
  }

  return { server, wss, bridges, close };
}

const isMain = process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href;
if (isMain) {
  const PORT = Number(process.env.PORT) || 8080;
  const relay = createRelayServer();

  const shutdown = (signal) => {
    console.log(`moblin-relay received ${signal}, shutting down`);
    const forceExit = setTimeout(() => process.exit(1), DEFAULTS.shutdownGraceMs);
    forceExit.unref?.();
    relay.close().then(() => {
      clearTimeout(forceExit);
      process.exit(0);
    });
  };
  process.once('SIGTERM', () => shutdown('SIGTERM'));
  process.once('SIGINT', () => shutdown('SIGINT'));

  relay.server.listen(PORT, () => {
    console.log(`moblin-relay listening on :${PORT}`);
  });
}
