# webrtc-proxy 技术报告

**日期**: 2026-08-12
**目的**: 用 WebRTC 直连替代慢速 tunnel 转发，让两个 NAT 后的 HTTP proxy endpoint 点对点互联

## 背景与问题

两端的 Go HTTP proxy 目前经 `*.moonchan.xyz` tunnel 转发流量，链路慢。两端都在 NAT 后（公网不可直达），要求：

1. 免自建服务器：不新增任何 VPS 服务
2. 尽量 P2P 直连：数据不经过中转，延迟/吞吐接近直连
3. 双端都要 Go 实现（不是浏览器场景）

## 方案选型

| 组件 | 选择 | 理由 |
|------|------|------|
| 信令 | PeerJS 公共云 (`0.peerjs.com:443`) | 免费、WSS 加密、已有多年运营；PeerJS 默认配置自带 **免费 TURN** 兜底 |
| 传输 | pion/webrtc v4 DataChannel | 项目已有该依赖；DataChannel = 面向消息的可靠有序通道，天然适配 HTTP 帧 |
| 数据面 | 自定义帧协议多路复用 | 单个 DataChannel 并发承载多个 HTTP 请求流 |

## PeerJS 信令协议逆向要点

浏览器端 PeerJS 协议（`peers/peerjs` 源码确认），Go 端 `pkg/peerjs` 完整实现：

```
1. GET https://0.peerjs.com/peerjs/id?ts=<ts>&version=<v>
   → 返回纯文本 peer id（不带则让服务器分配）
2. WS 连接 wss://0.peerjs.com/peerjs?key=peerjs&id=<id>&token=<token>&version=<v>
3. 服务器发 {"type":"OPEN"} 后才算在线
4. 每 5s 发 {"type":"HEARTBEAT"} 保活
5. 协商消息（JSON，带 dst/src）：
   {"type":"OFFER",    "payload":{"sdp", "type":"data", "connectionId", "label", "reliable", "serialization"}}
   {"type":"ANSWER",   "payload":{"sdp", "type":"data", "connectionId"}}
   {"type":"CANDIDATE","payload":{"candidate":RTCIceCandidate, "type":"data", "connectionId"}}
6. {"type":"ID-TAKEN"} id 被占用；{"type":"LEAVE"} 对端下线
```

关键细节:
- **自定义 id 必须合法**: `^[A-Za-z0-9]+(?:[ _-][A-Za-z0-9]+)*$`，故 peer id 固定为 `wt-<name>`
- **ICE 候选即时转发**（trickle ICE），不用等 gathering 完成
- pion 侧 `ICECandidate.ToJSON()` 输出格式与浏览器 `RTCIceCandidate` 完全一致，可直接互操作

## ICE / TURN 策略

peerjs 默认 ICE servers（与浏览器端一致）：

```
stun:stun.l.google.com:19302                          # 打洞用
turn:eu-0.turn.peerjs.com:3478  /  turn:us-0.turn.peerjs.com:3478
username=peerjs credential=peerjsp                    # 免费 TURN 兜底
```

- **打洞成功**：数据走 host/srflx 候选，P2P 直连，不经任何转发
- **打洞失败**（对称 NAT 等）：自动回落 peerjs TURN relay，仍比 HTTP 层 tunnel 快
- 两端 NAT 后对称 NAT 场景仍有打洞失败可能，TURN 保证可用性

## 数据面帧协议

DataChannel 是消息通道（非字节流），帧格式大端序：

```
[4B len][1B type][4B streamID][payload]
```

| type | 名称 | payload |
|------|------|---------|
| 0x01 | REQ_HEAD | JSON `{method, url, headers, body}` |
| 0x02 | REQ_BODY | 原始字节（16KB 块，对齐 peerjs chunkedMTU）|
| 0x03 | REQ_END | 空 |
| 0x04 | RESP_HEAD | JSON `{status, headers}` |
| 0x05 | RESP_BODY | 原始字节 |
| 0x06 | RESP_END | 空 |
| 0x07/0x08 | PING/PONG | 空（通道保活）|

- **streamID** 由 client 端分配，单 DataChannel 并发承载多个请求流（实测 20 并发 SSE 无问题）
- **流式支持**：响应 body 分块即时发送，SSE/长轮询逐块 flush 到客户端
- 请求体用 `io.Pipe` 流式转发，不上内存全缓冲

## 架构

```
                    PeerJS Cloud (0.peerjs.com)
                    信令: id/token/offer/answer/candidate
                          │
        ┌─────────────────┴──────────────────┐
        │                                    │
  +-------------+  WebRTC DataChannel   +-------------+
  │ serve 端     │◄─────────────────────►│ client 端   │
  │ peer id:    │  ICE 打洞直连          │ peer id:    │
  │ wt-<name>   │  (NAT 打洞失败→TURN)   │ 随机分配    │
  +-------------+                       +-------------+
        │  HTTP 请求                       │  HTTP 请求
        ▼                                  ▼
  本地目标服务                            浏览器/程序访问
  (e.g. :5173 dev server)                http://127.0.0.1:8080
```

- **serve 端**（资源所在侧）：注册固定 id `wt-<name>`，等待连接；收到 REQ 帧 → 转发到本地 target → 流式回 RESP 帧
- **client 端**（访问侧）：随机 id，主动 `Connect("wt-<name>")`，本地起 HTTP server；每个请求分配 streamID 打包发送，响应实时写回
- 连接断开：client 端自动重连（5s 间隔，30s 超时）

## 实测结果（本机回环验证）

| 测试 | 结果 |
|------|------|
| GET / POST + body | 通过，头/体完整 |
| SSE 流式（3 个 tick × 300ms）| 逐块到达，客户端实时收到 |
| 20 并发 slow 请求 | 全部完成 |
| 15 并发普通 GET | 15/15 成功 |
| 5 × 3MB POST 并发 | 字节数精确（目标服务验证 3145728 = 3MB）|
| 断线重连 | client 端自动重连循环 |

**注意**：以上为 localhost 回环验证（host candidate 直连），真实双 NAT 环境的打洞成功率取决于 NAT 类型，peerjs TURN 提供可用性兜底，需两端实际环境验证。

## 代码位置

```
cmd/webrtc-proxy/main.go    # 双角色入口 + 帧协议 + serve/client 业务
pkg/peerjs/peer.go          # PeerJS 信令客户端（socket/心跳/消息路由）
pkg/peerjs/connection.go    # DataConnection（pion PC/DataChannel + TURN 配置）
```

## 使用方法

**目标服务所在端**（serve，如 WSL 里的 dev server :5173）：

```bash
webrtc-proxy -mode serve -name <共享名> -target http://127.0.0.1:5173
# 例如
webrtc-proxy -mode serve -name zen -target http://127.0.0.1:5173
```

**访问端**（client，可跑在任意 NAT 后机器）：

```bash
webrtc-proxy -mode client -name zen -listen 127.0.0.1:8080
# 然后浏览器/curl 访问 http://127.0.0.1:8080/xxx 即到远端的 dev server
```

参数：

| 参数 | 默认 | 说明 |
|------|------|------|
| `-mode` | 必填 | `serve` 或 `client` |
| `-name` | 必填 | 两端共享的通信名，最终 peer id = `wt-<name>`；限字母数字与 `_ -` |
| `-target` | — | serve 端必填，本地目标服务 URL |
| `-listen` | `127.0.0.1:8080` | client 端监听地址 |
| `-debug` | `false` | 打印信令/协商日志 |

发布：`go build ./cmd/webrtc-proxy` 或从 GitHub release 下载 `webrtc-proxy-windows-amd64.exe` 等资产。

## 后续可能改进

- 常规 TCP 隧道模式（不止 HTTP），供 ssh/其他协议使用
- 信令容错：id 冲突自动加后缀重试
- 传输参数调优：DataChannel 大消息分片、Buffer.size 水位背压
- 与浏览器端互连测试（PeerJS 云信令天然兼容浏览器 client）