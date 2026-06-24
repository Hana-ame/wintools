# WebRTC P2P 聊天客户端

## 概述

本示例是一个基于 WebRTC 的点对点（P2P）聊天客户端，演示了如何通过 HTTP 信令服务器交换 SDP 和 ICE 候选，建立两个浏览器/客户端之间的直接 DataChannel 连接。

本示例使用项目中的 `examples/webrtc` 包封装 WebRTC PeerConnection 的底层细节，通过 HTTP REST API 与信令服务器（如 `bwh.moonchan.xyz:8080`）交互来完成 SDP/ICE 的交换。

与直接使用 pion/webrtc 原始 API 相比，本示例展示了：
1. **完整的信令流程**：Room 创建 → Join → SDP 交换 → ICE 候选交换 → 连接建立
2. **信令客户端的实现**：通过 HTTP 与信令服务器交互的 `sigClient` 封装
3. **Trickle ICE 和 Non-Trickle ICE 的双模式支持**：默认使用 Trickle ICE，超时时自动回退
4. **自动重试机制**：连接失败时自动从 Trickle ICE 回退到 Non-Trickle ICE

## 信令协议

本示例与信令服务器之间使用标准的 REST API 进行通信。信令服务器基于 `pkg/signaling`（已归档）中的 KV 存储 + HTTP 路由实现。

### Room 创建（p1）

```http
POST /kv/room
→ {"room_id": "abc123"}
```

创建房间时，服务器在 KV 存储中初始化以下 key：
- `abc123`：房间存在标记
- `abc123/sdp/p1`、`abc123/sdp/p2`：SDP 存储位置
- `abc123/ice/p1`、`abc123/ice/p2`：ICE 候选队列
- `abc123/p1`、`abc123/p2`：Join 时设置

### Join 房间（p1/p2）

```http
POST /kv/room/abc123/join
→ {"peer": "p1"} 或 {"peer": "p2"}
```

第一个加入的返回 `p1`，第二个返回 `p2`，第三个返回 `{"ok":false,"err":"room full"}`。

### SDP 交换

```http
# p1 发送 SDP Offer
POST /kv/room/abc123/sdp?peer=p1
Content-Type: application/json

{"type":"offer","sdp":"v=0\r\no=...\r\n..."}
→ {"ok":true}

# p2 接收 SDP Offer（支持长轮询，等待最多 30 秒）
GET /kv/room/abc123/sdp?peer=p2&wait=30
→ {"type":"offer","sdp":"v=0\r\no=...\r\n..."}

# p2 发送 SDP Answer（流程同上，方向相反）
```

### ICE 候选交换

```http
# p1 发送 ICE 候选（每次收集到一个候选就发送一次）
POST /kv/room/abc123/ice?peer=p1
Content-Type: application/json

{"candidate":"candidate:1 1 UDP 2122252543 ... typ host","sdpMid":"0"}
→ {"ok":true}

# p2 接收 ICE 候选（单条弹出，FIFO 队列）
GET /kv/room/abc123/ice?peer=p2&wait=30
→ {"candidate":"candidate:1 1 UDP 2122252543 ... typ host","sdpMid":"0"}
```

`?all` 参数可以一次性拉取所有候选并清空队列：

```http
GET /kv/room/abc123/ice?peer=p1&all
→ [{"candidate":"...","sdpMid":"0"}, {"candidate":"...","sdpMid":"1"}]
```

## 信令客户端（sigClient）

本示例实现了 `sigClient` 结构体封装所有信令 API 调用：

```go
type sigClient struct {
    Server string  // 信令服务器地址，如 "http://bwh.moonchan.xyz:8080"
    RoomID string  // 房间 ID，p1 创建时生成，p2 从命令行参数传入
    Peer   string  // 分配的角色，"p1" 或 "p2"
}
```

### 方法一览

| 方法 | 功能 | 底层 HTTP 调用 |
|------|------|---------------|
| `createRoom()` | 创建房间 | `POST /kv/room` |
| `join()` | 加入房间 | `POST /kv/room/:id/join` |
| `sendSDP(type, sdp)` | 发送 SDP | `POST /kv/room/:id/sdp?peer=p1|p2` |
| `recvSDP(wait)` | 接收对端 SDP | `GET /kv/room/:id/sdp?peer=p1|p2&wait=N` |
| `sendICE(json)` | 发送 ICE 候选 | `POST /kv/room/:id/ice?peer=p1|p2` |
| `recvICE(wait)` | 接收对端 ICE 候选 | `GET /kv/room/:id/ice?peer=p1|p2&wait=N` |

### 长轮询机制

`recvSDP()` 和 `recvICE()` 使用 HTTP 长轮询（Long Polling）机制。当没有数据可读时，服务器不会立即返回空结果，而是挂起请求，等待数据到来或超时：

```
Client                         Server
  │                              │
  │  GET /.../sdp?peer=p2&wait=30│
  │ ────────────────────────────▶ │ 没有数据，挂起连接
  │                              │  ┌ waiting...
  │                              │  │
  │  (p1 在另一个连接中发送 SDP)   │  │
  │  POST /.../sdp?peer=p1        │  │
  │ ────────────────────────────▶ │  │
  │                              │  │ 数据到达
  │                              │  └ 唤醒
  │  ←── SDP JSON ────────────── │
  │                              │
```

长轮询的优势：
- 避免了轮询的延迟（如果每 1 秒轮询一次，平均延迟 500ms）
- 避免了 WebSocket 的复杂性（只需要 HTTP 即可）
- 服务器超时后返回 `{"ok":false,"err":"timeout"}`，客户端可以重试

## 连接建立流程

### p1 角色（房间创建者）

```
p1 启动
  │
  ├── createRoom()          → POST /kv/room              → 获得 RoomID
  ├── join()                → POST /kv/room/:id/join     → 获得 "p1"
  ├── 打印 RoomID，等待用户按 Enter
  │
  ├── NewPeer(Config{DataChannelLabel: "chat"})
  ├── OnICECandidate(func(c) { sig.sendICE(c) })
  ├── OnData(func(data) { fmt.Println("[recv]", data) })
  │
  ├── CreateOffer()         → 生成 SDP Offer
  │
  ├── [Trickle 模式]
  │   ├── sendSDP("offer", offer)      → 立即发送（候选后续到达）
  │   └── recvSDP(30)                  → 等待 Answer
  │     └── SetRemoteDescription(answer) → 设置远程描述
  │
  ├── [Non-Trickle 模式]
  │   ├── <-GatheringComplete()         → 等待所有候选收集完毕
  │   ├── sendSDP("offer", finalOffer)  → 发送完整 SDP（含所有候选）
  │   └── recvSDP(30)                  → 等待 Answer
  │     └── SetRemoteDescription(answer) → 设置远程描述
  │
  ├── ← 后台 goroutine 持续：
  │      recvICE(30) → AddICECandidate(ice)
  │
  ├── DataChannel Open!
  ├── 每 3 秒通过 dc.Send() 发送一次消息
  └── 接收对端消息 → OnData 回调打印
```

### p2 角色（房间加入者）

```
p2 启动
  │
  ├── join(RoomID)          → POST /kv/room/:id/join     → 获得 "p2"
  │
  ├── NewPeer(Config{})     → 不预创建 DataChannel
  ├── OnICECandidate(func(c) { sig.sendICE(c) })
  ├── OnData(func(data) { fmt.Println("[recv]", data) })
  ├── OnDataChannel(func(dc) { ... })   → 等待 p1 的 DataChannel 到达
  │
  ├── recvSDP(30)           → 等待 Offer
  │   └── SetRemoteDescription(offer) → 设置远程描述
  │
  ├── CreateAnswer()        → 生成 SDP Answer
  │
  ├── [Trickle 模式]
  │   └── sendSDP("answer", answer)    → 立即发送
  │
  ├── [Non-Trickle 模式]
  │   ├── <-GatheringComplete()         → 等待所有候选收集完毕
  │   └── sendSDP("answer", finalAnswer) → 发送完整 SDP
  │
  ├── ← 后台 goroutine 持续：
  │      recvICE(30) → AddICECandidate(ice)
  │
  ├── DataChannel Open!  ← p1 创建的 DataChannel 到达
  ├── 每 3 秒通过 dc.Send() 发送一次消息
  └── 接收对端消息 → OnData 回调打印
```

## SDP 交换的时序细节

### 为什么 SDP 和 ICE 候选的发送顺序很重要？

在 Trickle ICE 中，SDP Offer 和 ICE 候选的发送顺序至关重要：

1. **必须先发送 SDP**：对端需要先通过 `SetRemoteDescription(sdp)` 设置远程描述，获取媒体能力信息（编解码器、传输协议等）。在此之前，ICE 候选是无效的。

2. **SDP 发送后立即发送候选**：对端在收到 SDP 并调用 SetRemoteDescription 后，就可以开始处理 ICE 候选了。如果候选在 SDP 之前到达，对端无法处理（会丢失）。

3. **ICE 候选持续发送**：ICE 候选的收集是渐进的 — 本地地址最先可用（毫秒级），STUN 反射地址次之（几百毫秒），TURN 中继地址最慢（取决于 TURN 服务器的响应时间）。

本示例的处理方式：

```go
// p1: 先发送 SDP
if useTrickle {
    sig.sendSDP("offer", offerSDP)     // 1. 立即发送 SDP
    // 2. ICE 候选通过 OnICECandidate 回调逐步发送
    answerRecv = sig.recvSDP(30)       // 3. 等待 Answer
    peer.SetRemoteDescription(answer)  // 4. 设置远程描述
}

// ICE 候选接收 goroutine 在 SetRemoteDescription 之后开始工作
go func() {
    for {
        c, err := sig.recvICE(30)      // 收到对端候选
        peer.AddICECandidate(c)        // 添加到本地 PeerConnection
    }
}()
```

## Trickle vs Non-Trickle 决策

本示例支持两种 ICE 模式，默认使用 Trickle ICE，但在超时时自动回退到 Non-Trickle：

```go
tryTrickle := true
for attempts := 0; attempts < 2; attempts++ {
    peer, dc, err := connect(tryTrickle)
    if err != nil {
        if tryTrickle {
            tryTrickle = false  // 回退到 Non-Trickle
            continue
        }
        return
    }
}
```

### Trickle ICE 的优势

1. **更快的连接建立**：不需要等待所有候选收集完毕（STUN 反射地址可能需要几百毫秒到几秒）
2. **渐进式连接**：当任何一对候选匹配时立即开始连接尝试，不需要等所有候选就绪
3. **更好的用户体验**：在网络条件复杂时，用户不需要等待十几秒才能开始通信

### Trickle ICE 的劣势

1. **实现更复杂**：需要在 SDP 发送后持续处理候选，需要一个后台接收协程
2. **信令通道压力更大**：每条候选都是一次 HTTP 请求
3. **对信令服务器要求更高**：需要支持长轮询或 WebSocket 来实时推送候选

### Non-Trickle ICE 的优势

1. **实现简单**：SDP 中包含所有候选，一次交换即可
2. **确定性超时**：候选收集完毕的时间是可预测的（通常 3-5 秒）

### Non-Trickle ICE 的劣势

1. **连接建立前等待时间更长**：需要等待 STUN 和 TURN 的响应
2. **在候选丢失时没有补救机会**：如果某条候选在 SDP 传输过程中损坏，连接可能永远无法建立

## 命令行参数

```bash
peer-client --mode p1|p2 [--room ID] [--trickle] [--trickle-timeout N] --server URL
```

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `--mode` | 必填 | `p1` 创建房间并等待 p2 加入；`p2` 加入已有房间 |
| `--server` | `http://bwh.moonchan.xyz:8080` | 信令服务器地址 |
| `--room` | 空 | p2 必填，要加入的房间 ID |
| `--trickle` | `true` | 是否启用 Trickle ICE |
| `--trickle-timeout` | 15 | Trickle ICE 超时秒数，超时后尝试 Non-Trickle 回退 |

### 使用示例

**终端 1（p1）**：
```bash
peer-client --mode p1 --server http://localhost:8080
```

输出：
```
ROOM_ID=abc123 (peer=p1)
Give this room ID to p2, then press Enter to wait...
```

**终端 2（p2）**：
```bash
peer-client --mode p2 --room abc123 --server http://localhost:8080
```

当连接建立后，两个终端都会开始每 3 秒发送一条消息，并显示接收到的消息。

## 容错机制

### 超时回退

本示例的 `connect()` 函数接受一个 `useTrickle` 参数，当 Trickle ICE 连接在 `trickle-timeout` 秒内未建立时，自动使用 Non-Trickle 模式重试：

```go
timeout := time.Duration(*trickleTimeout) * time.Second
if !useTrickle {
    timeout = 30 * time.Second  // Non-Trickle 给予更长的超时
}
select {
case dc := <-dcReady:
    close(stopRecv)
    return peer, dc, nil
case <-time.After(timeout):
    close(stopRecv)
    peer.Close()
    return nil, nil, fmt.Errorf("dc timeout")
}
```

### ICE 候选接收 goroutine

ICE 候选的接收在一个独立的 goroutine 中运行，即使接收失败也不会影响主流程：

```go
go func() {
    for {
        select {
        case <-stopRecv:
            return
        default:
        }
        c, err := sig.recvICE(30)
        if err != nil {
            time.Sleep(1 * time.Second)
            continue
        }
        if err := peer.AddICECandidate(c); err != nil {
            fmt.Printf("add ice: %v\n", err)
        }
    }
}()
```

当连接建立或超时时，`stopRecv` channel 被关闭，goroutine 退出。

### 对比 Tricke 和 Non-Trickle 的超时行为

| 模式 | 超时设置 | 超时原因 |
|------|---------|---------|
| Trickle | `--trickle-timeout`（默认 15s） | 候选传输延迟，STUN 响应慢 |
| Non-Trickle | 固定 30s | SDP 传输延迟，网络中大包传输慢 |

## 安全注意事项

1. **不加密的通信**：本示例使用非加密的 DataChannel（默认未启用 DTLS 加密），在公共网络上通信时数据是明文传输的。生产环境中应启用 DTLS-SRTP 加密。

2. **信令服务器 HTTP 明文**：信令通信使用 HTTP 而非 HTTPS，SDP 和 ICE 候选在传输过程中可能被中间人篡改。生产环境中应使用 HTTPS。

3. **STUN 服务器的选择**：本示例使用 Google 公共 STUN `stun:stun.l.google.com:19302`。STUN 服务器只能获取公网 IP 和端口，不能看到通信内容。但对于隐私要求高的场景，建议使用自建 STUN/TURN。

4. **身份验证**：信令服务器没有任何身份验证机制，任何知道 Room ID 的人都可以加入房间。生产环境中应加入用户认证和房间密码机制。

## 调试技巧

1. **检查 ICE 状态**：`pc.OnICEConnectionStateChange` 注册的回调会打印所有 ICE 状态变化，包括 `checking`、`connected`、`completed`、`failed` 等。

2. **检查 ICE 候选**：查看 `OnICECandidate` 回调中的候选类型（host/prflx/srflx/relay）和优先级，判断哪些候选匹配成功了。

3. **SDP 内容检查**：在 `sendSDP` 和 `recvSDP` 处打印 SDP 内容，检查 `m=application` 段是否存在，以及其中的 ICE 候选列表。

4. **信令日志**：信令服务器的日志会显示每次 API 调用的详细信息和耗时，可以帮助定位问题是在信令层还是 WebRTC 层。

## 代码结构

```
examples/peer-client/
├── readme.md            # 本文档
└── main.go              # 聊天客户端入口
    ├── sigClient         # 信令客户端封装
    │   ├── createRoom    # 创建房间
    │   ├── join          # 加入房间
    │   ├── sendSDP       # 发送 SDP
    │   ├── recvSDP       # 接收 SDP
    │   ├── sendICE       # 发送 ICE 候选
    │   └── recvICE       # 接收 ICE 候选
    ├── connect           # 建立 WebRTC 连接（trickle/non-trickle）
    └── main              # 主流程：信号处理、连接、收发
```
