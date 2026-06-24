# WebRTC Peer/Listener 辅助库

## 概述

本包是基于 [pion/webrtc](https://github.com/pion/webrtc) v4 的 WebRTC PeerConnection 和 DataChannel 封装库。它隐藏了 pion 底层的复杂性，暴露一个简洁、安全的 API，让开发者可以专注于信令交换和数据处理逻辑，而不需要深入了解 SDP 协商、ICE 候选收集和 DTLS 连接建立等底层细节。

pion/webrtc 是一个纯 Go 实现的 WebRTC 规范实现，不需要任何 CGO 或原生库依赖。它实现了 W3C WebRTC API 的子集，支持 STUN/TURN、ICE 候选、DTLS 加密、SCTP 数据通道等核心功能。

本库的核心原则：
1. **不绑定信令传输层**：用户自行管理 SDP/ICE 的交换机制（HTTP/WebSocket/stdin-pipe/自定义）
2. **不限制 ICE 策略**：支持 trickle ICE 和 non-trickle ICE，用户可以自由选择
3. **线程安全**：所有公共方法都是并发安全的
4. **零额外依赖**：除 pion/webrtc v4 外不需要其他外部依赖

## 核心类型

### ConnectionState

`ConnectionState` 枚举了 PeerConnection 的六种可能状态，映射自 pion 的 `PeerConnectionState`：

| 状态 | 值 | 含义 |
|------|-----|------|
| `StateNew` | 0 | 新建，尚未连接 |
| `StateConnecting` | 1 | 正在连接中（ICE 和 DTLS 握手进行中） |
| `StateConnected` | 2 | 已连接，数据通道可用 |
| `StateDisconnected` | 3 | 连接断开（可能自动重连中） |
| `StateFailed` | 4 | 连接失败（不可恢复） |
| `StateClosed` | 5 | 连接已关闭 |

状态的转变方向通常是：

```
New → Connecting → Connected → Disconnected → Failed
                                        ↓
                                     Closed
```

`Disconnected` 状态不一定是终点：某些 WebRTC 实现会在网络短暂中断后自动重连。只有 `Failed` 和 `Closed` 是终态。

### Config

`Config` 是创建 Peer 时的配置项：

```go
type Config struct {
    ICEServers       []string // ICE STUN/TURN 服务器地址列表
    Ordered          bool     // DataChannel 是否保证有序传输
    DataChannelLabel string   // 预创建的 DataChannel 名称
}
```

**ICEServers**：STUN/TURN 服务器列表。如果为空，默认使用 Google 公共 STUN `stun:stun.l.google.com:19302`。对于需要 NAT 穿透的场景，可以配置多个 STUN/TURN 服务器。STUN 服务器仅用于获取公网 IP 和端口，不代理数据流；TURN 服务器在 P2P 直连失败时用于中继数据。

**Ordered**：DataChannel 的传输模式。`true` 表示可靠有序（类似 TCP），`false` 表示可能乱序但延迟更低（类似 UDP）。默认 Go 结构体零值为 `false`，但在本包的 `NewPeer` 中如果未指定 `Ordered` 字段，会使用 `true`（需要编译器零值但逻辑中会特殊处理，参考源代码）。

**DataChannelLabel**：预创建的 DataChannel 名称。如果设置了这个值，`NewPeer` 会立即调用 `pc.CreateDataChannel()` 创建 DataChannel，这样生成的 SDP Offer/Answer 中会包含 `m=application` 段。如果留空，则通过 `OnDataChannel` 事件等待对端主动创建 DataChannel。

### Peer

`Peer` 是本包的核心封装，它持有一个 `*webrtc.PeerConnection` 和一个可选的默认 `*webrtc.DataChannel`。

#### 生命周期

```
NewPeer(cfg)
    │
    ├── 创建 pion PeerConnection
    ├── 注册 ICE 连接状态变化日志
    ├── 注册 PeerConnection 状态变化回调 → 映射为 ConnectionState
    ├── 注册 ICE 候选收集回调 → 序列化为 JSON → 回调 onIceCandidate
    ├── 如果 cfg.DataChannelLabel 非空 → 立即创建 DataChannel
    └── 注册 OnDataChannel 事件（等待对端创建时使用）
    │
    ▼
CreateOffer() / CreateAnswer()
    │
    ├── 创建 SDP Offer/Answer
    ├── 设置为本地描述
    └── 返回 SDP 字符串
    │
    ▼
SetRemoteDescription(sdp)
    │
    ├── 自动检测 SDP 类型（Offer/Answer）
    └── 设置为远程描述，触发 ICE 开始连接
    │
    ▼
OnICECandidate(candidate)  ← ICE 候选到达
OnData(data)              ← 收到数据
OnState(state)            ← 连接状态变化
    │
    ▼
Close()
    └── 关闭 PeerConnection，释放所有资源
```

## 详细功能解析

### 1. ICE 候选管理

ICE（Interactive Connectivity Establishment）是 WebRTC 的 NAT 穿透协议，它尝试多种候选路径（本地地址、STUN 反射地址、TURN 中继地址）来建立 P2P 连接。

#### 候选收集

`Peer` 通过 `pc.OnICECandidate` 注册 ICE 候选回调。当 pion 收集到一个新的候选地址时，会回调这个函数：

```go
pc.OnICECandidate(func(c *webrtc.ICECandidate) {
    if c == nil {
        // nil 表示候选收集完毕（gathering complete）
        close(p.gatheringDone)
        return
    }
    data, _ := json.Marshal(c.ToJSON())
    p.onIceCandidate(string(data))
})
```

关键细节：
- `c == nil` 是一个特殊的结束标记，表示不会再收集到新的候选
- `gatheringDone` channel 在收集完成时关闭，用户可以等待这个 channel 来实现 non-trickle ICE
- 候选被序列化为 JSON 格式，包含 `candidate`、`sdpMid`、`sdpMLineIndex` 等字段

#### 候选暂存（Pending Candidates）

ICE 候选可能在用户注册 `OnICECandidate` 回调之前就已经开始收集了（比如在 `NewPeer` 过程中）。为了避免丢失这些候选，`Peer` 内部有一个暂存机制：

```go
type Peer struct {
    // ...
    pendingCandidates []string  // 注册前暂存的候选
    muPending         sync.Mutex
}
```

当候选到达时：
- 如果 `onIceCandidate` 已经注册，直接回调
- 否则，追加到 `pendingCandidates` 暂存

当用户调用 `OnICECandidate(fn)` 注册回调时：
1. 设置 `p.onIceCandidate = fn`
2. 遍历 `pendingCandidates`，通过 `fn` 逐一发送
3. 清空 `pendingCandidates`

这种设计确保了**在任意时间点注册回调都不会丢失候选**，用户可以自由选择在 `NewPeer` 之前或之后注册 `OnICECandidate`。

#### 候选添加

对端的候选通过 `AddICECandidate(candidate)` 方法添加。这个方法支持两种格式：

```go
func (p *Peer) AddICECandidate(candidate string) error {
    var c webrtc.ICECandidateInit
    if err := json.Unmarshal([]byte(candidate), &c); err != nil {
        // JSON 解析失败，当作原始 candidate 字符串处理
        c.Candidate = candidate
    }
    return p.pc.AddICECandidate(c)
}
```

- JSON 格式：`{"candidate":"candidate:1 1 UDP 2122252543 192.168.1.1 54321 typ host","sdpMid":"0","sdpMLineIndex":0}`
- 纯文本格式：`candidate:1 1 UDP 2122252543 192.168.1.1 54321 typ host`

自动检测使得用户不需要关心对端发送的具体格式。

### 2. SDP 交换

SDP（Session Description Protocol）是 WebRTC 用来描述媒体会话的协议。在 WebRTC 中，Offer/Answer 模型的角色划分如下：

```
Peer A（Offerer）                        Peer B（Answerer）
    │                                          │
    │  CreateOffer()                            │
    │  → 生成包含本地能力描述的 SDP Offer       │
    │                                          │
    │  SetLocalDescription(offer)               │
    │  → 将 Offer 设置为本地描述                │
    │                                          │
    │  ——通过信令通道发送 SDP Offer——           │
    │                                          │
    │                              SetRemoteDescription(offer)
    │                              → 从信令通道获得 Offer，设置为远程描述
    │                                          │
    │                              CreateAnswer()
    │                              → 根据远程 Offer 生成 Answer
    │                                          │
    │                              SetLocalDescription(answer)
    │                              → 将 Answer 设置为本地描述
    │                                          │
    │  ←——通过信令通道接收 SDP Answer——         │
    │                                          │
    │  SetRemoteDescription(answer)             │
    │  → 将 Answer 设置为远程描述               │
    │  → ICE 连接开始                          │
    │                                          │
```

`Peer.CreateOffer()` 和 `Peer.CreateAnswer()` 分别封装了上述流程中调用 `pc.CreateOffer()`、`pc.SetLocalDescription()` 等步骤。

`Peer.SetRemoteDescription(sdp)` 自动检测 SDP 类型：

```go
func (p *Peer) SetRemoteDescription(sdp string) error {
    for _, t := range []webrtc.SDPType{webrtc.SDPTypeOffer, webrtc.SDPTypeAnswer} {
        if err := p.pc.SetRemoteDescription(
            webrtc.SessionDescription{Type: t, SDP: sdp},
        ); err == nil {
            return nil  // 只要一种类型成功就返回
        }
    }
    return fmt.Errorf("webrtc: set remote description: invalid SDP")
}
```

SDP 字符串的格式中自带了类型信息（`a=type:offer` 或 `a=type:answer`）。pion 的 `webrtc.SessionDescription` 解析时会自动验证类型是否匹配实际的 SDP 内容，因此不匹配的类型会返回错误。

### 3. Trickle ICE 支持

Trickle ICE 是一种增强的 ICE 流程，允许 ICE 候选在 SDP 交换完成后逐步到达，而不是等待所有候选收集完毕后再发送 SDP。这大大缩短了连接建立的时间。

#### Trickle ICE 流程

```
Peer A                              Peer B
  │                                   │
  │  收集第一个候选 → 立即发送         │
  │  ───SDP(offer)+候选1──▶           │
  │                                   │  设置 Offer + 添加候选1
  │                                   │  收集第一个候选 → 立即发送
  │  ◀───SDP(answer)+候选1──         │
  │                                   │
  │  设置 Answer + 添加候选1           │
  │                                   │
  │  收集更多候选 → 逐次发送           │
  │  ─────ICE候选2─────▶              │  添加候选2
  │  ─────ICE候选3─────▶              │  添加候选3
  │                                   │
  │  收集更多候选 → 逐次发送           │
  │  ◀────ICE候选2──────             │
  │  添加候选2                        │
  │  ◀────ICE候选3──────             │
  │  添加候选3                        │
  │                                   │
  │  连接建立 ✅                      │
```

#### Non-Trickle ICE 流程

```
Peer A                              Peer B
  │                                   │
  │  等待所有候选收集完毕              │
  │  ←GatheringComplete()─           │
  │                                   │
  │  一次性发送 SDP(包含所有候选)      │
  │  ─────SDP(offer)─────▶            │  设置 Offer（含所有候选）
  │                                   │  等待所有候选收集完毕
  │                                   │  ←GatheringComplete()─
  │                                   │
  │  ◀────SDP(answer)────            │
  │  设置 Answer（含所有候选）          │
  │                                   │
  │  连接建立 ✅                      │
```

#### GatheringComplete

`Peer` 提供了 `GatheringComplete()` 方法，返回一个 channel，当 ICE 候选收集完毕时关闭：

```go
<-peer.GatheringComplete()
finalOffer := peer.LocalDescriptionSDP()
```

这在 non-trickle ICE 场景中使用：Offerer 等待所有候选收集完毕后，将包含全部 ICE 候选的完整 SDP 发送给 Answerer。

### 4. DataChannel

DataChannel 是 WebRTC 提供的一个基于 SCTP（Stream Control Transmission Protocol）的双向数据通道 API。它支持可靠有序和不可靠无序两种传输模式。

#### 创建模式

**预创建模式**（本端主动创建）：

```go
cfg := webrtc.Config{
    DataChannelLabel: "chat",  // 设置标签后立即创建
}
peer, _ := webrtc.NewPeer(cfg)
// 此时 peer.DataChannel() 已经有效
```

预创建模式的 SDP 中包含 `m=application` 段，表示本端希望建立数据通道。对端通过 `OnDataChannel` 事件接收。

**被动接收模式**（等待对端创建）：

```go
cfg := webrtc.Config{
    DataChannelLabel: "",  // 不预创建
}
peer, _ := webrtc.NewPeer(cfg)
peer.OnDataChannel(func(dc *webrtc.DataChannel) {
    // 对端在 SDP 中声明了 DataChannel
})
```

被动接收模式的 SDP 中不包含 `m=application` 段，但如果对端的 SDP 中包含 `m=application`，pion 会自动创建 DataChannel 并通过 `OnDataChannel` 事件通知。

#### 数据收发

```go
// 发送数据
peer.Send([]byte("hello"))

// 接收数据
peer.OnData(func(data []byte) {
    fmt.Printf("收到: %s", string(data))
})
```

`Send()` 使用默认的 DataChannel（预创建或第一个到达的）。如果需要使用多个 DataChannel，可以通过 `OnDataChannel` 回调获得原始的 `*webrtc.DataChannel` 对象，直接使用 pion 的 API。

### 5. 回调机制

#### OnICECandidate

```go
peer.OnICECandidate(func(candidate string) {
    signaling.send(candidate)  // 通过信令通道发送给对端
})
```

ICE 候选回调是必须注册的！如果不注册，本端的候选永远无法到达对端，连接将永远无法建立。这个回调在 `AddICECandidate` 被调用前可能会被多次触发。

#### OnData

```go
peer.OnData(func(data []byte) {
    fmt.Printf("收到消息: %s", string(data))
})
```

数据回调在 DataChannel 收到消息时触发。`data` 是原始的二进制数据。如果发送的是文本，可以直接转换为 `string`。消息是有序且可靠的（如果 `Ordered=true`）。

#### OnState

```go
peer.OnState(func(state webrtc.ConnectionState) {
    fmt.Printf("连接状态: %s", state)
})
```

状态回调在 PeerConnection 状态变化时触发。典型的状态序列：
- 新建：`new`
- 开始连接：`connecting`
- 连接成功：`connected`
- 连接断开（暂时）：`disconnected`
- 连接断开（永久）：`failed`
- 连接关闭：`closed`

#### OnDataChannel

```go
peer.OnDataChannel(func(dc *webrtc.DataChannel) {
    dc.OnOpen(func() {
        dc.Send([]byte("hello"))
    })
    dc.OnMessage(func(msg webrtc.DataChannelMessage) {
        fmt.Printf("收到: %s", string(msg.Data))
    })
})
```

DataChannel 回调在对端创建 DataChannel 并到达本端时触发。这个回调只有在没有预创建 DataChannel 时才有意义。如果已经预创建了，对端的 DataChannel 会通过 SDP 协商匹配到已有的 DataChannel。

## 线程安全

本包中所有公共方法都是并发安全的：

- `Send()`：通过 RLock 获取默认 DataChannel 后调用 `dc.Send()`，pion 的 `Send()` 是并发安全的
- `OnData()`、`OnState()`、`OnDataChannel()`：通过 mutex 保护回调函数指针的读写
- `OnICECandidate()`：通过 mutex 保护回调注册和 pending 候选的 flush
- `AddICECandidate()`：直接调用 pion 的 `AddICECandidate()`，本身是并发安全的
- `Close()`：调用 `pc.Close()`，是并发安全的
- `CreateOffer()`、`CreateAnswer()`、`SetRemoteDescription()`：这些方法调用 pion 的对应方法，pion 内部通过 mutex 保护状态

## 使用示例

### 最基本的 P2P 连接

```go
// Peer A（发起方）
peerA, _ := webrtc.NewPeer(webrtc.Config{DataChannelLabel: "data"})
peerA.OnICECandidate(func(c string) { signaling.Send(c) })
peerA.OnData(func(d []byte) { fmt.Println("A收到:", string(d)) })
offer, _ := peerA.CreateOffer()
signaling.SendOffer(offer)

// 接收到 Answer
signaling.OnAnswer(func(answer string) {
    peerA.SetRemoteDescription(answer)
})

// Peer B（应答方）
peerB, _ := webrtc.NewPeer(webrtc.Config{})
peerB.OnICECandidate(func(c string) { signaling.Send(c) })
peerB.OnData(func(d []byte) { fmt.Println("B收到:", string(d)) })

signaling.OnOffer(func(offer string) {
    peerB.SetRemoteDescription(offer)
    answer, _ := peerB.CreateAnswer()
    signaling.SendAnswer(answer)
})
```

### Trickle ICE 示例

```go
// Peer A
peer.OnICECandidate(func(c string) { signaling.Send(c) })
offer, _ := peer.CreateOffer()
signaling.Send(offer + "|TRICKLE")  // 标记为 trickle

// Peer B 收到 Offer 和候选后
peer.OnICECandidate(func(c string) { signaling.Send(c) })
signaling.OnOffer(func(offer string) {
    peer.SetRemoteDescription(offer)
    answer, _ := peer.CreateAnswer()
    signaling.Send(answer + "|TRICKLE")
})
signaling.OnCandidate(func(c string) {
    peer.AddICECandidate(c)
})
```

### 连接建立后获取对端地址

```go
peer.OnState(func(s webrtc.ConnectionState) {
    if s == webrtc.StateConnected {
        fmt.Println("对端地址:", peer.RemoteAddr())
        fmt.Println("连接状态:", peer.StateInfo())
    }
})
```

## 代码结构

```
examples/webrtc/
├── readme.md                # 本文档
├── webrtc.go                # Peer 封装
│   ├── ConnectionState       # 连接状态枚举
│   ├── Config                # 配置结构
│   ├── Peer                  # 核心结构体
│   ├── NewPeer               # 创建 Peer
│   ├── CreateOffer           # 创建 SDP Offer
│   ├── CreateAnswer          # 创建 SDP Answer
│   ├── SetRemoteDescription  # 设置远程 SDP
│   ├── AddICECandidate       # 添加 ICE 候选
│   ├── OnICECandidate        # 注册候选回调
│   ├── OnData                # 注册数据回调
│   ├── OnState               # 注册状态回调
│   ├── OnDataChannel         # 注册 DC 回调
│   ├── Send                  # 发送数据
│   ├── Close                 # 关闭连接
│   ├── RemoteAddr            # 获取对端地址
│   ├── StateInfo             # 获取状态摘要
│   ├── DataChannel           # 获取默认 DC
│   ├── PeerConnection        # 获取底层 PC
│   └── GatheringComplete     # 等待候选收集完毕
├── listener.go               # Listener 封装
│   ├── Listener              # 多连接监听器
│   ├── NewListener           # 创建 Listener
│   ├── Offer                 # 处理新的 Offer
│   ├── Accept                # 等待新连接
│   └── Candidate             # 处理 ICE 候选
├── webrtc_test.go            # 单元测试
└── real_test.go              # E2E 集成测试
```
