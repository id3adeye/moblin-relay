# moblin-relay

A self-hosted [Moblin](https://github.com/eerimoq/moblin) remote-control relay.

It pairs a **browser-based controller** (the assistant — e.g. the `irl-remote`
app) with the **Moblin iOS app** (the streamer) and pipes messages between them,
keyed by a shared *bridge ID*.

## Why self-host

The public relay (`moblin.mys-lang.org`) is written in Go and calls
`websocket.Accept(w, r, nil)`, which rejects any WebSocket whose `Origin` host
differs from the server host with **403 Forbidden**. A browser cannot override
its `Origin` header, so a web controller served from your own domain can never
connect to the public relay — only the relay's own same-origin web page can.

This relay is a faithful reimplementation of the upstream routing using Node's
`ws`, which does **not** enforce an Origin check, so your controller connects
from any domain.

## Endpoints

| Method | Path | Role |
| --- | --- | --- |
| WS | `/bridge/control/{bridgeId}` | controller control lane |
| WS | `/bridge/data/{bridgeId}/{connectionId}` | controller data lane |
| WS | `/streamer/{bridgeId}` | Moblin app |
| WS | `/status/{bridgeId}` | optional status viewers |
| GET | `/stats.json` | connection counts |
| GET | `/` | health check |

## Run with Docker

```sh
docker compose up -d --build
```

Published on host port **3005** (container listens on `:8080`). Verify:

```sh
curl http://localhost:3005/         # -> moblin-relay ok
curl http://localhost:3005/stats.json
```

## Run without Docker

```sh
npm install
npm start
```

## TLS / public access

The controller app runs over HTTPS, so it must reach the relay over **`wss://`**
(secure WebSocket). Put the relay behind TLS. Two easy options:

- **Cloudflare Tunnel** (`cloudflared`): point a hostname at
  `http://localhost:3005`. Tunnel terminates TLS; `wss://your-host/...` just works.
- **Reverse proxy** (Caddy / Traefik / nginx): terminate TLS and proxy WebSocket
  upgrades to `localhost:3005`.

Plain `ws://` only works for local/LAN testing where the controller is also
served over `http://`.

## Point the clients at it

Let `RELAY = your-relay-host[/optional-base-path]` (no scheme).

- **irl-remote app** — in the Moblin panel's connection form, set **Relay host**
  to `RELAY`, generate/enter a **Bridge ID**, and set a **Password**.
- **Moblin app** (Settings → Remote control → Streamer) — set the **Assistant
  URL** to `wss://RELAY/streamer/{bridgeId}` and the **password** to match.

The bridge ID and password are shared secrets between the two clients — the relay
never sees the password (auth is end-to-end between controller and streamer).
Treat them like a password; don't show them on stream.
