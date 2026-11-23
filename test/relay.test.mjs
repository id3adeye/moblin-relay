import assert from 'node:assert/strict';
import net from 'node:net';
import { afterEach, describe, it } from 'node:test';
import WebSocket from 'ws';
import { createRelayServer, isValidBridgeId, isValidConnectionId } from '../relay.mjs';

let activeRelay = null;

async function startRelay(opts = {}) {
  const relay = createRelayServer(opts);
  activeRelay = relay;
  await new Promise((resolve) => relay.server.listen(0, '127.0.0.1', resolve));
  const { port } = relay.server.address();
  return { relay, port, wsBase: `ws://127.0.0.1:${port}`, httpBase: `http://127.0.0.1:${port}` };
}

function openWs(url) {
  return new Promise((resolve, reject) => {
    const ws = new WebSocket(url);
    ws.once('open', () => resolve(ws));
    ws.once('error', reject);
  });
}

function nextMessage(ws) {
  return new Promise((resolve) => {
    ws.once('message', (data) => resolve(data.toString()));
  });
}

function waitClose(ws) {
  return new Promise((resolve) => {
    if (ws.readyState === WebSocket.CLOSED) return resolve({ code: ws._closeCode });
    ws.once('close', (code, reason) => resolve({ code, reason: reason.toString() }));
  });
}

afterEach(async () => {
  if (activeRelay) {
    await activeRelay.close();
    activeRelay = null;
  }
});

describe('ID validation', () => {
  it('accepts free-text bridge IDs up to 128 chars and rejects control chars or overly long IDs', () => {
    assert.equal(isValidBridgeId('my-bridge_123'), true);
    assert.equal(isValidBridgeId('a'.repeat(128)), true);
    assert.equal(isValidBridgeId(''), false);
    assert.equal(isValidBridgeId('a'.repeat(129)), false);
    assert.equal(isValidBridgeId('bad\x00id'), false);
    assert.equal(isValidBridgeId('bad\nid'), false);
  });

  it('requires UUID format for connectionId', () => {
    assert.equal(isValidConnectionId('11111111-2222-4333-8444-555555555555'), true);
    assert.equal(isValidConnectionId('not-a-uuid'), false);
  });
});

describe('moblin-relay hardening', () => {
  it('survives malformed WebSocket frames without crashing the server', async () => {
    const { port, httpBase } = await startRelay();

    // Send a valid upgrade then corrupt RSV bits / invalid UTF-8 frame over raw TCP.
    await new Promise((resolve) => {
      const sock = net.connect(port, '127.0.0.1', () => {
        sock.write(
          'GET /bridge/control/test-bridge HTTP/1.1\r\n' +
          'Host: localhost\r\n' +
          'Upgrade: websocket\r\n' +
          'Connection: Upgrade\r\n' +
          'Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n' +
          'Sec-WebSocket-Version: 13\r\n\r\n'
        );
      });
      sock.once('data', () => {
        // Malformed masked frame with invalid RSV1+RSV2+RSV3 bits set.
        sock.write(Buffer.from([0xf1, 0x82, 0x00, 0x00, 0x00, 0x00, 0xff, 0xff]));
        setTimeout(() => { sock.destroy(); resolve(); }, 50);
      });
    });

    const res = await fetch(`${httpBase}/`);
    assert.equal(res.status, 200);
    assert.equal(await res.text(), 'moblin-relay ok\n');
  });

  it('enforces maxPayload and keeps the server alive when exceeded', async () => {
    const { wsBase, httpBase } = await startRelay({ maxPayload: 1024 });
    const control = await openWs(`${wsBase}/bridge/control/small-cap`);
    const closed = waitClose(control);
    control.send('x'.repeat(2048));
    const { code } = await closed;
    assert.equal(code, 1009);

    const res = await fetch(`${httpBase}/`);
    assert.equal(res.status, 200);
  });

  it('flushes early streamer frames on attach and rejects duplicate data-lane attaches without killing the active lane', async () => {
    const { wsBase } = await startRelay();
    const control = await openWs(`${wsBase}/bridge/control/race-test`);
    const connectPromise = nextMessage(control);

    const streamer = await openWs(`${wsBase}/streamer/race-test`);
    const connectMsg = JSON.parse(await connectPromise);
    assert.equal(connectMsg.type, 'connect');
    const { connectionId } = connectMsg.data;

    // Early frame sent before data lane attaches.
    streamer.send('early-frame');

    const data1 = await openWs(`${wsBase}/bridge/data/race-test/${connectionId}`);
    assert.equal(await nextMessage(data1), 'early-frame');

    // Duplicate attach must be rejected with 1008 and must NOT tear down streamer/data1.
    const dup = new WebSocket(`${wsBase}/bridge/data/race-test/${connectionId}`);
    const dupClose = await waitClose(dup);
    assert.equal(dupClose.code, 1008);

    // Original pair still works cleanly.
    const fromData = nextMessage(streamer);
    data1.send('hello-moblin');
    assert.equal(await fromData, 'hello-moblin');

    control.close();
  });

  it('closes an unattached streamer socket after attachTimeoutMs', async () => {
    const { wsBase } = await startRelay({ attachTimeoutMs: 80 });
    const control = await openWs(`${wsBase}/bridge/control/timeout-test`);
    const streamer = await openWs(`${wsBase}/streamer/timeout-test`);
    const { code } = await waitClose(streamer);
    assert.equal(code, 1008);
    control.close();
  });

  it('enforces bridge cap by closing cleanly (never sending rateLimitExceeded)', async () => {
    const { wsBase } = await startRelay({ maxBridges: 1 });
    const b1 = await openWs(`${wsBase}/bridge/control/bridge-1`);
    const b2 = new WebSocket(`${wsBase}/bridge/control/bridge-2`);
    const msgs = [];
    b2.on('message', (d) => msgs.push(d.toString()));
    const { code } = await waitClose(b2);
    assert.equal(code, 1013);
    assert.equal(msgs.length, 0);
    b1.close();
  });

  it('handles control ping and closes all clients with 1001 on graceful shutdown', async () => {
    const { relay, wsBase } = await startRelay();
    const control = await openWs(`${wsBase}/bridge/control/ping-test`);
    const pongPromise = nextMessage(control);
    control.send(JSON.stringify({ type: 'ping' }));
    const pong = JSON.parse(await pongPromise);
    assert.equal(pong.type, 'pong');

    const closed = waitClose(control);
    await relay.close();
    const { code } = await closed;
    assert.equal(code, 1001);
    activeRelay = null;
  });
});
