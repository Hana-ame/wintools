# go-peerserver

Self-hosted PeerJS signaling server + built-in room discovery (Go).

Replaces public cloud signaling (0.peerjs.com) and public MQTT broker: nodes only need to point
`PEERDRIVE_PEERJS_HOST/PORT` (or any PeerJS client configuration) at this server,
discovery uses the built-in HTTP API. PeerJS protocol compatible — any `peerjs` JS client and
[go-peerjs](https://github.com/Hana-ame/go-peerjs) clients can connect directly.

## Usage

```bash
go build -o peerserver ./cmd/peerserver/
./peerserver [-addr :9000] [-key peerjs] [-tokens tok1,tok2] [-tls-cert c.pem -tls-key k.pem]
```

- `-addr` listen address (default `:9000`)
- `-key` PeerJS API key (client must match, prevents unrelated clients from connecting)
- `-tokens` optional: signaling token whitelist (comma-separated). When set, WS connection
  tokens must be in the list, otherwise the upgrade is rejected (prevents arbitrary clients from impersonating nodes to receive signaling)
- `-tls-cert` / `-tls-key`: PEM certificate and private key. **When both are provided, HTTPS/WSS is served**,
  providing only one will error and exit immediately (no silent downgrade — see below)

### When Must TLS (wss) Be Enabled

The public panel (`packages/peerdrive-client/dist/panel.html`, online version runs on GitHub Pages) is
an HTTPS page, browsers will block `ws://` initiated from HTTPS pages as **mixed content** directly, and PeerJS
only manifests as "can't connect" with no prompt at all. So:

- Panel uses `localhost` form of ws signaling: most browsers let it through, it works;
- Panel needs to connect to self-hosted signaling on LAN/public network: **must use wss**.

```bash
./peerserver -addr :9100 -tls-cert cert.pem -tls-key key.pem
# Or don't enable TLS in this process, but put caddy / nginx / Cloudflare Tunnel reverse proxy in front
```

REST endpoints (`/peerjs/id`, `/discover/*`, `/status`) all return `Access-Control-Allow-Origin: *`
and short-circuit OPTIONS preflight — the public panel is on a different origin, cross-origin headers are a hard requirement.

## Endpoints

| Path | Description |
|---|---|
| `GET /peerjs` (WS) | PeerJS compatible signaling (ice/offer/answer/leave/open) |
| `GET /peerjs/id` | ID borrowing rotation + expiry reclamation (H3 queue) |
| `POST /discover/announce` | Node online self-report (peerid → last-seen) |
| `GET /discover/nodes` | Room discovery list (expired nodes removed) |

## Library Usage (Embedding in Your Own Service)

```go
srv := signalserver.NewServer("peerjs", signalserver.WithTokenWhitelist([]string{"tok"}))
srv.Start() // Background sweeper: clean up expired offline queue
mux.HandleFunc("/peerjs", srv.HandleWS)
mux.HandleFunc("/discover/nodes", srv.HandleNodes)
```

## 收紧 CORS（可选，2026-10-04 起）

面板侧 REST 端点默认返回 `Access-Control-Allow-Origin: *`（历史行为，**保持不变**）。
通配的含义是「互联网上任何网站都能读」——实测 `GET /discover/nodes`
因此能被任意网页一行 `fetch()` 列出全网在线节点。

配 `-cors-origin` 改成白名单：命中就回显该 Origin，没命中**不发 Allow-Origin 头**，
浏览器因此读不到。状态码不受影响（拦读靠的是头，不是 403）。

```bash
./peersignal -cors-origin "https://peerdrive.pages.dev,https://peerdrive.moonchan.xyz,null"
```

⚠️ **双击打开的面板 Origin 是 `null`**，要保留这种用法必须把 `null` 显式写进列表，
否则面板的「自动搜索」会在浏览器里静默失败。列表里写 `*` 可回到通配行为。

嵌入用法：`signalserver.NewServer("peerjs", signalserver.WithCORSOrigins([]string{"https://panel.example", "null"}))`

## 运维面需要 token（2026-10-04 起）

`/status` 与 `/discover/nodes?type=` 需要 ops token（`?token=` 或 `Authorization: Bearer`），
且 `/status` 不再返回通配 origin。

面板用到的两种查询（带 `?coll=`、以及不带任何参数的「谁在线」）**保持公开**，
因为公共面板的自动搜索依赖它们。

配了 `-tokens` 白名单时 ops token 必须在名单内；未配时任意非空 token 通过，空 token 一律拒绝。

## Tests

```bash
go test ./... -count=1
```

## Deployment Reference

- systemd + nginx reverse proxy (`wss://` to WS endpoint) see peerdrive main repo AGENTS.md;
- Online instance: `wss://peersignal.moonchan.xyz/peerjs` + `https://peersignal.moonchan.xyz/discover/*`
