# WebRTC 简介

## 什么是 WebRTC？

**WebRTC** (Web Real-Time Communication) 是一个开源标准，允许浏览器和原生应用通过 **点对点 (P2P)** 连接进行实时音视频通信和数据传输，无需中间服务器转发媒体流。

它的核心目标：让任意两个设备在浏览器中直接对话。

## 核心技术

### NAT 穿透与 ICE 框架

设备通常位于 NAT（网络地址转换）或防火墙之后，没有公网 IP。WebRTC 使用 **ICE** (Interactive Connectivity Establishment) 框架来发现最优的通信路径：

```
ICE 候选类型:
  host         — 局域网地址（最优先，延迟最低）
  srflx        — 通过 STUN 获取的公网地址（NAT 映射地址）
  relay        — 通过 TURN 中继转发（兜底方案，延迟最高）
```

### STUN / TURN 服务器

| 类型 | 作用 | 适用场景 |
|------|------|---------|
| **STUN** (Session Traversal Utilities for NAT) | 帮助端获取自己的公网 IP 和端口 | 大部分 NAT 类型可穿透 |
| **TURN** (Traversal Using Relays around NAT) | 中继转发媒体流 | 对称 NAT / 防火墙限制严格的场景 |

- STUN 服务器只处理简短的请求/响应，对带宽要求低
- TURN 服务器需要转发实际的媒体数据，对带宽要求高
- 默认 STUN 服务器：`stun:stun.l.google.com:19302`

### SDP (Session Description Protocol)

SDP 是会话描述协议，用于交换通信双方的媒体能力（编码格式、分辨率、端口等）：

```ini
v=0
o=- 123456 2 IN IP4 0.0.0.0
s=-
t=0 0
a=group:BUNDLE 0 1
m=audio 9 UDP/TLS/RTP/SAVPF 111
c=IN IP4 0.0.0.0
a=rtpmap:111 opus/48000/2
```

**重要说明**：SDP **不是** 数据传输协议，它只是通信前的"握手"协商。

### Signaling（信令）

WebRTC 本身 **不定义** 信令协议。信令是通信双方交换 SDP 和 ICE 候选信息的过程，需要由应用层实现，常见方式：

- WebSocket
- HTTP 轮询
- SIP
- 自定义协议

```
[Peer A]                  [信令服务器]                  [Peer B]
   |--- Offer SDP -------->|                            |
   |                        |--- Offer SDP ------------->|
   |                        |<-- Answer SDP ------------|
   |<-- Answer SDP --------|                            |
   |--- ICE Candidate ---->|                            |
   |                        |--- ICE Candidate -------->|
   |<-- ICE Candidate -----|                            |
   |<==================== 直连 P2P ====================>|
```

### DTLS (Datagram TLS)

WebRTC 使用 DTLS 对数据通道进行加密，确保通信安全。所有数据通道通信都是强制加密的。

## 通信建立流程

1. **创建 PeerConnection** — 双方各自创建 PeerConnection 对象
2. **信令交换 SDP** — 通过信令通道交换 Offer/Answer
3. **ICE 候选交换** — 双方收集并通过信令交换 ICE 候选信息
4. **连接检查** — ICE 协议进行连通性检查，选择最佳路径
5. **媒体/数据流** — 连接建立后，开始传输音视频或数据

## 本项目中的实现

本项目的 WebRTC 演示位于 `examples/webrtc-demo`，基于 [pion/webrtc](https://github.com/pion/webrtc)，
信令通过本项目自己的 KV 存储 API（`cmd/api-server` + `pkg/kv`）完成，无需额外信令服务。

```go
// 创建 PeerConnection + DataChannel
pc, err := webrtc.NewPeerConnection(webrtc.Configuration{
    ICEServers: []webrtc.ICEServer{{URLs: []string{"stun:stun.l.google.com:19302"}}},
})
dc, err := pc.CreateDataChannel("chat", &webrtc.DataChannelInit{Ordered: &ordered})

// 发起方 — 创建 Offer
offer, err := pc.CreateOffer(nil)
pc.SetLocalDescription(offer)

// 通过 KV API 交换 SDP（POST 到 /kv/room/:id/offer，GET ?wait=30 阻塞等待）
postJSON(offerPath, sigMessage{Type: "offer", SDP: local})

// 接收方 — 设置远程 Offer，创建 Answer
pc.SetRemoteDescription(...)
answer, err := pc.CreateAnswer(nil)
pc.SetLocalDescription(answer)

// 数据收发
dc.OnMessage(func(msg webrtc.DataChannelMessage) { ... })
dc.Send([]byte("ping from p1"))
```

运行：`p1` 创建房间并等待 `p2` 加入（`examples/webrtc-demo/main.go`，`-mode p1|p2`）。

## 使用场景

| 场景 | 描述 |
|------|------|
| **视频会议** | Zoom/Google Meet 类应用，多人实时音视频 |
| **屏幕共享** | 远程协作、在线教育 |
| **文件传输** | 大文件点对点传输（无需经过服务器） |
| **游戏** | 实时多人游戏的数据同步 |
| **IoT** | 设备间直接通信（结合原生 SDK） |
| **直播** | 低延迟直播的 Web 端推流/拉流 |

## 优缺点

**优点：**
- 点对点直连，降低服务器带宽成本
- 浏览器原生支持，无需安装插件
- DTLS 强制加密，安全可靠
- 自动 NAT 穿透
- 自适应网络质量（前向纠错、码率自适应）

**缺点：**
- 信令服务器仍需自主搭建
- 对称 NAT 环境下依赖 TURN 转发（增加延迟和带宽成本）
- 大规模多人会议仍需 SFU/MCU 架构（非纯 P2P）
- 调试困难（网络环境复杂）

## 参考

- [W3C WebRTC Specification](https://www.w3.org/TR/webrtc/)
- [pion/webrtc](https://github.com/pion/webrtc) — Go 语言 WebRTC 实现
- [MDN WebRTC API](https://developer.mozilla.org/en-US/docs/Web/API/WebRTC_API)
