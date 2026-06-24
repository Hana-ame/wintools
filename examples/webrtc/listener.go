// AI: generated with assistance from AI (2026-06-23)
//
// Package webrtc — Listener：多连接 WebRTC 监听器
//
// =====================================================
// 功能概述
// =====================================================
// Listener 管理多个远程 WebRTC 连接。每个连接通过唯一的
// connID 标识和路由。类似 TCP Listener：
//   - Offer(offerSDP) 处理远程 SDP Offer，返回 connID + answerSDP
//   - Accept() 阻塞等待新 DataChannel 建立
//   - Candidate(connID, candidate) 远程 ICE 候选按连接路由
//   - OnCandidate(callback) 本端 ICE 候选通过回调通知用户
//
// 用户负责通过自己的信令通道转发 SDP 和 ICE 候选。
//
// =====================================================
// 典型使用场景
// =====================================================
// 服务端机器运行 Listener，等待多个远程客户端连接。
// 每个客户端建立独立的 PeerConnection + DataChannel。
// 管理端通过 goroutine 持续 Accept() 新连接，每条连接独立处理。
//
// =====================================================
// 典型用法
// =====================================================
//
//	listener := webrtc.NewListener()
//	listener.OnCandidate(func(connID, candidate string) {
//	    signaling.Send(connID, candidate)
//	})
//	go func() {
//	    for {
//	        dc := listener.Accept()
//	        go handleConn(dc)
//	    }
//	}()
//	signaling.OnMessage(func(msg Message) {
//	    switch msg.Type {
//	    case "offer":
//	        connID, answer, _ := listener.Offer(msg.SDP)
//	        signaling.Reply(msg.From, answer)
//	    case "candidate":
//	        listener.Candidate(msg.ConnID, msg.Candidate)
//	    }
//	})
//
// =====================================================
// 数据流
// =====================================================
//  远程 ── SDP Offer ──→ Listener.Offer() ──→ 返回 connID + Answer
//  远程 ── ICE 候选 ──→ Listener.Candidate(connID, candidate)
//  本端 ICE 候选 ──→ OnCandidate(connID, candidate) ──→ 用户转发给远程
//  远程 DataChannel 到达 ──→ Accept() 返回 DataChannel + ConnID
package webrtc

import (
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"sync/atomic"

	"github.com/pion/webrtc/v4"
)

// ============================================================================
// 默认配置
// ============================================================================

// DefaultICEServers 默认 ICE 服务器列表
// Listener.Offer() 未指定自定义配置时使用
// Google 公共 STUN 用于 NAT 类型探测和候选收集
var DefaultICEServers = []string{"stun:stun.l.google.com:19302"}

// iceServersFromCfg 将字符串数组转换为 pion ICEServer 格式
func iceServersFromCfg(urls []string) []webrtc.ICEServer {
	se := make([]webrtc.ICEServer, len(urls))
	for i, u := range urls {
		se[i] = webrtc.ICEServer{URLs: []string{u}}
	}
	return se
}

// ============================================================================
// DataChannel 包装
// ============================================================================

// DataChannel 是对 pion DataChannel 的包装
//
// 嵌入了 *webrtc.DataChannel，所以原始方法（Send、Label 等）可以直接使用
// 额外增加了：
//   - ConnID：标识该通道属于哪个连接
//   - OnData：便捷的消息回调注册
type DataChannel struct {
	*webrtc.DataChannel                       // 嵌入原始 DataChannel
	ConnID               string              // 连接标识，与 Listener.Offer() 返回的 connID 一致

	mu       sync.Mutex // 保护 onDataFn
	onDataFn func([]byte) // 消息回调
}

// OnData 注册该 DataChannel 的消息回调
//
// 当对端通过此通道发送数据时触发
// 与 Listener.Accept() 配合使用:
//
//	dc := listener.Accept()
//	dc.OnData(func(data []byte) {
//	    fmt.Printf("[%s] 收到: %s", dc.ConnID, data)
//	})
func (d *DataChannel) OnData(fn func(data []byte)) {
	d.mu.Lock()
	d.onDataFn = fn
	d.mu.Unlock()
}

// ============================================================================
// 内部连接状态
// ============================================================================

// conn 存储每个连接内部需要的状态
type conn struct {
	pc *webrtc.PeerConnection // 该连接的底层 PeerConnection
	id string                 // 连接 ID
}

// ============================================================================
// Listener 核心结构
// ============================================================================

// Listener 管理多个 WebRTC 连接，按 connID 路由 ICE 候选和数据通道
//
// 工作流程:
//
//                    信令通道（用户自行实现）
//                     ┌────────────────────┐
//                     │                    │
//    ┌──── Offer ─────┼──► Listener.Offer  │
//    │                │         │          │
//    │                │    返回 Answer     │
//    │                │         │          │
//    │                │   Accept() 阻塞等待 │
//    │                │         │          │
//    │                │   返回 DataChannel │
//    │                │         │          │
//    └── Candidate ──┼─► Candidate()      │
//                     │         │          │
//    ┌─────────────   │  路由到对应 conn    │
//    │  OnCandidate───┼──► 回调通知用户     │
//    │                └────────────────────┘
type Listener struct {
	ch          chan *DataChannel                // 新 DataChannel 到达时通知 Accept()
	conns       sync.Map                         // connID → *conn（保存所有已接受的连接）
	onCandidate func(connID, candidate string)    // ICE 候选回调
	seq         atomic.Int64                     // 自增序号，用于生成 connID
}

// NewListener 创建一个新的 Listener
//
// 创建后必须按顺序：
//   1. 设置 OnCandidate 回调
//   2. 调用 Offer() 处理来自远程的 SDP Offer
//   3. 调用 Accept() 等待 DataChannel 建立
//   4. 将远程 ICE Candidate 通过 Candidate() 路由进来
func NewListener() *Listener {
	return &Listener{
		ch: make(chan *DataChannel, 16), // 缓冲 16 个，防止并发过高时阻塞
	}
}

// ============================================================================
// API: 候选回调
// ============================================================================

// OnCandidate 设置 ICE 候选回调
//
// 当任一连接的 PeerConnection 收集到本地 ICE 候选时触发
// 回调参数:
//   - connID:    该候选属于哪个连接
//   - candidate: JSON 编码的 ICE 候选数据
//
// 用户必须在回调中将候选转发给对应的远程对端
// 如果未设置此回调，ICE 候选不会被转发，连接将无法建立
func (l *Listener) OnCandidate(fn func(connID, candidate string)) {
	l.onCandidate = fn
}

// ============================================================================
// API: Offer 处理
// ============================================================================

// Offer 处理远程发来的 SDP Offer
//
// 完整流程:
//   1. 创建新的 PeerConnection
//   2. 注册 ICE 候选回调（路由到 Listener.OnCandidate）
//   3. 设置远程 SDP Offer
//   4. 创建 SDP Answer 并设为本地描述
//   5. 注册 OnDataChannel（对端 DataChannel 到达时 → Accept 通道）
//   6. 返回 connID 和 answerSDP
//
// 参数:
//   offerSDP: 远程对端发来的 SDP Offer 字符串
//
// 返回值:
//   connID:    该连接的全局唯一标识，用于后续 Candidate() 路由
//   answerSDP: 需要发回给远程对端的 SDP Answer 字符串
//   err:       可能错误（SDP 解析失败等）
//
// 注意:
//   - Listener 侧不需要预先创建 DataChannel，由对端创建
//   - 对端创建 DataChannel 后，OnDataChannel 事件自动触发
//   - DataChannel 包装后被送入 Accept() 通道
func (l *Listener) Offer(offerSDP string) (connID string, answerSDP string, err error) {
	// 1. 创建新的 PeerConnection
	pc, err := webrtc.NewPeerConnection(webrtc.Configuration{
		ICEServers: iceServersFromCfg(DefaultICEServers),
	})
	if err != nil {
		return "", "", fmt.Errorf("listener offer: %w", err)
	}

	// 2. 生成唯一连接 ID
	id := fmt.Sprintf("conn-%d", l.seq.Add(1))

	// 3. 注册 ICE 候选收集事件
	//    候选 JSON 化后通过 OnCandidate 回调通知用户
	//    用户在回调中将候选转发给 connID 对应的远程对端
	pc.OnICECandidate(func(c *webrtc.ICECandidate) {
		if c == nil || l.onCandidate == nil {
			return
		}
		data, _ := json.Marshal(c.ToJSON())
		l.onCandidate(id, string(data))
	})

	// 4. 设置远程 SDP Offer
	if err := pc.SetRemoteDescription(webrtc.SessionDescription{
		Type: webrtc.SDPTypeOffer,
		SDP:  offerSDP,
	}); err != nil {
		pc.Close()
		return "", "", fmt.Errorf("listener set remote desc: %w", err)
	}

	// 5. 创建 SDP Answer
	answer, err := pc.CreateAnswer(nil)
	if err != nil {
		pc.Close()
		return "", "", fmt.Errorf("listener create answer: %w", err)
	}
	if err := pc.SetLocalDescription(answer); err != nil {
		pc.Close()
		return "", "", fmt.Errorf("listener set local desc: %w", err)
	}

	// 6. 注册 OnDataChannel 事件
	//    对端 DataChannel 建立后会触发此回调
	//    包装为本地 DataChannel 后送入 Accept() 通道
	pc.OnDataChannel(func(raw *webrtc.DataChannel) {
		wdc := &DataChannel{DataChannel: raw, ConnID: id}
		raw.OnOpen(func() {
			log.Printf("listener: datachannel '%s' (conn=%s) opened", raw.Label(), id)
		})
		raw.OnMessage(func(msg webrtc.DataChannelMessage) {
			wdc.mu.Lock()
			fn := wdc.onDataFn
			wdc.mu.Unlock()
			if fn != nil {
				fn(msg.Data)
			}
		})
		l.ch <- wdc
	})

	// 7. 注册连接状态变化日志
	pc.OnConnectionStateChange(func(s webrtc.PeerConnectionState) {
		log.Printf("listener: conn=%s state=%s", id, s)
	})

	// 8. 保存连接引用，供后续 Candidate() 路由
	l.conns.Store(id, &conn{pc: pc, id: id})

	return id, answer.SDP, nil
}

// ============================================================================
// API: ICE 候选路由
// ============================================================================

// Candidate 将远程 ICE 候选路由到对应的连接
//
// 远程对端通过信令通道发来 ICE 候选时，调用此方法
// 候选根据 connID 被路由到正确的 PeerConnection
//
// 参数:
//   connID:    连接标识（由 Offer() 返回）
//   candidate: JSON 格式或原始格式的 ICE 候选
//
// candidate 支持的格式:
//   JSON:     {"candidate":"candidate:... typ host ...", "sdpMid":"0", ...}
//   原始字符串: "candidate:... typ host ..."
//
// 自动检测格式:
//   - 优先尝试 JSON 解析
//   - JSON 解析失败则当作原始 candidate 字符串
func (l *Listener) Candidate(connID, candidate string) error {
	// 查找到对应的连接
	v, ok := l.conns.Load(connID)
	if !ok {
		return fmt.Errorf("listener: connection %s not found", connID)
	}
	c := v.(*conn)

	// 解析并添加 ICE 候选
	var init webrtc.ICECandidateInit
	if err := json.Unmarshal([]byte(candidate), &init); err != nil {
		// JSON 解析失败，视为原始 candidate 字符串
		init.Candidate = candidate
	}
	return c.pc.AddICECandidate(init)
}

// ============================================================================
// API: 接受连接与关闭
// ============================================================================

// Accept 阻塞等待直到有新的 DataChannel 建立
//
// 行为类似 TCP Accept：
//   - 当对端与 Listener 完成 ICE + DTLS + SCTP 握手后
//   - 对端的 DataChannel 会触发 OnDataChannel 事件
//   - Listener 包装后通过此通道返回
//
// 返回的 DataChannel 对象可以直接用于：
//   - dc.Send(data)   发送数据
//   - dc.OnData(fn)   接收数据
//   - dc.ConnID       获取连接标识
func (l *Listener) Accept() *DataChannel {
	return <-l.ch
}

// Close 关闭所有管理的 WebRTC 连接
//
// 遍历 conns 中所有连接，逐个调用 Close()
// 注意：不会关闭 Accept() 通道，正在 Accept() 的 goroutine 会继续阻塞
func (l *Listener) Close() {
	l.conns.Range(func(_, v interface{}) bool {
		v.(*conn).pc.Close()
		return true
	})
}
