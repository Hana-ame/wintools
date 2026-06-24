// AI: generated with assistance from AI (2026-06-23)
//
// Package webrtc — WebRTC Peer 封装
//
// =====================================================
// 功能概述
// =====================================================
// 本包基于 pion/webrtc v4，提供简洁的 WebRTC PeerConnection
// + DataChannel 封装。核心目标：
//   1. 隐藏 pion 底层复杂性，暴露简单 API
//   2. 不绑定任何信令传输层（用户自行管理 SDP/ICE 交换）
//   3. Trickle ICE 默认启用，候选边收集边转发
//
// =====================================================
// 核心抽象
// =====================================================
// Peer     — 单连接封装，适用于主动拨号方（dialer）
//            CreateOffer → SetRemoteDescription → AddICECandidate
//            OnICECandidate / OnData / OnState 回调
//            Send / Close / RemoteAddr / StateInfo
//
// Listener — 多连接监听器，详见 listener.go
//
// =====================================================
// 使用前提
// =====================================================
//   1. 用户自行实现信令通道（HTTP / WebSocket / 命名管道 / stdin/stdout 等）
//   2. 用户负责在管理端和远程之间转发 SDP Offer/Answer 和 ICE Candidate
//   3. 必须注册 OnICECandidate 回调，否则 ICE 候选无法到达对端，连接失败
//   4. DataChannel 可在 CreateOffer 前预创建（SDP 含 m=application），
//      也可在连接建立后通过 OnDataChannel 事件接收对端创建的通道
//
// =====================================================
// 依赖
// =====================================================
// github.com/pion/webrtc/v4
// go get github.com/pion/webrtc/v4@latest
package webrtc

import (
	"encoding/json"
	"fmt"
	"log"
	"sync"

	"github.com/pion/webrtc/v4"
)

// ============================================================================
// 连接状态枚举
// ============================================================================

// ConnectionState 表示 PeerConnection 的当前状态
// 通过 OnState 回调通知用户
type ConnectionState int

const (
	StateNew         ConnectionState = iota // 新建，尚未连接
	StateConnecting                         // 正在连接中
	StateConnected                          // 已连接，数据通道可用
	StateDisconnected                       // 连接断开（可能重连）
	StateFailed                             // 连接失败（不可恢复）
	StateClosed                             // 连接已关闭
)

// String 返回 ConnectionState 的可读字符串
func (s ConnectionState) String() string {
	switch s {
	case StateNew:
		return "new"
	case StateConnecting:
		return "connecting"
	case StateConnected:
		return "connected"
	case StateDisconnected:
		return "disconnected"
	case StateFailed:
		return "failed"
	case StateClosed:
		return "closed"
	default:
		return "unknown"
	}
}

// stateMap 将 pion 的 PeerConnectionState 映射到本包的 ConnectionState
var stateMap = map[webrtc.PeerConnectionState]ConnectionState{
	webrtc.PeerConnectionStateNew:          StateNew,
	webrtc.PeerConnectionStateConnecting:   StateConnecting,
	webrtc.PeerConnectionStateConnected:    StateConnected,
	webrtc.PeerConnectionStateDisconnected: StateDisconnected,
	webrtc.PeerConnectionStateFailed:       StateFailed,
	webrtc.PeerConnectionStateClosed:       StateClosed,
}

// ============================================================================
// 配置与核心结构
// ============================================================================

// Config 创建 Peer 时的配置项
type Config struct {
	ICEServers       []string // ICE STUN/TURN 服务器地址列表，如 []string{"stun:stun.l.google.com:19302"}
	Ordered          bool     // DataChannel 是否保证有序传输（true=可靠有序，false=可能乱序但延迟更低）
	DataChannelLabel string   // 预创建的 DataChannel 名称；留空则不预创建，等待对端创建
}

// Peer 封装了一个 WebRTC PeerConnection + 默认 DataChannel
//
// 使用方法:
//
//	peer, _ := webrtc.NewPeer(webrtc.Config{
//	    ICEServers: []string{"stun:stun.l.google.com:19302"},
//	    DataChannelLabel: "data",
//	})
//	defer peer.Close()
//
//	// 设置回调
//	peer.OnICECandidate(func(candidate string) {
//	    signaling.Send(candidate)  // 转发给对端
//	})
//	peer.OnData(func(data []byte) {
//	    fmt.Printf("收到: %s", data)
//	})
//	peer.OnState(func(state webrtc.ConnectionState) {
//	    fmt.Printf("状态变化: %s", state)
//	})
//
//	// 发起连接
//	offer, _ := peer.CreateOffer()
//	signaling.Send(offer)          // 发送 SDP Offer 给对端
//	signaling.OnAnswer(func(answer string) {
//	    peer.SetRemoteDescription(answer)
//	})
type Peer struct {
	pc                *webrtc.PeerConnection   // 底层 pion PeerConnection
	dc                *webrtc.DataChannel       // 默认 DataChannel（可选）
	mu                sync.RWMutex              // 保护 onData/onState 回调
	onIceCandidate    func(string)              // ICE 候选回调：收到本端候选时触发
	onData            func([]byte)              // 数据回调：收到对端消息时触发
	onState           func(ConnectionState)     // 状态回调：连接状态变化时触发
	onDataChannel     func(*webrtc.DataChannel) // DataChannel 回调：收到对端创建的 DC 时触发
	pendingCandidates []string                  // 在 OnICECandidate 注册前暂存的 ICE 候选
	muPending         sync.Mutex                // 保护 pendingCandidates
	gatheringDone     chan struct{}             // ICE 候选收集完成时关闭
	gatheredOnce      sync.Once                 // 确保 gatheringDone 只关闭一次
}

// NewPeer 创建一个新的 WebRTC Peer
//
// cfg.ICEServers 为空时默认使用 Google 公共 STUN
// cfg.DataChannelLabel 非空时立即创建 DataChannel（SDP 中会包含 m=application）
// 如果留空，则通过 OnDataChannel 事件等待对端创建
func NewPeer(cfg Config) (*Peer, error) {
	// 默认 STUN 服务器
	iceServers := cfg.ICEServers
	if iceServers == nil {
		iceServers = []string{"stun:stun.l.google.com:19302"}
	}

	// 转换为 pion ICEServer 格式
	se := make([]webrtc.ICEServer, len(iceServers))
	for i, u := range iceServers {
		se[i] = webrtc.ICEServer{URLs: []string{u}}
	}

	// 创建底层 PeerConnection
	pc, err := webrtc.NewPeerConnection(webrtc.Configuration{
		ICEServers: se,
	})
	if err != nil {
		return nil, fmt.Errorf("webrtc: new peer connection: %w", err)
	}

	p := &Peer{pc: pc, gatheringDone: make(chan struct{})}

	// ICE 连接状态日志（调试用）
	pc.OnICEConnectionStateChange(func(state webrtc.ICEConnectionState) {
		log.Printf("ICE state: %s", state)
	})

	// PeerConnection 状态变化 → 映射后回调 onState
	pc.OnConnectionStateChange(func(s webrtc.PeerConnectionState) {
		p.mu.RLock()
		fn := p.onState
		p.mu.RUnlock()
		if fn != nil {
			fn(stateMap[s])
		}
	})

	// ICE 候选收集事件
	//   c == nil 表示候选收集完毕（gathering complete），此时可以发送 end-of-candidates
	//   非空时序列化为 JSON 字符串，通过 onIceCandidate 回调转发给对端
	pc.OnICECandidate(func(c *webrtc.ICECandidate) {
		if c == nil {
			p.gatheredOnce.Do(func() { close(p.gatheringDone) })
			return // 候选收集完成，忽略
		}
		data, err := json.Marshal(c.ToJSON())
		if err != nil {
			log.Printf("marshal ICE candidate: %v", err)
			return
		}
		s := string(data)

		// 如果 onIceCandidate 已注册，直接回调
		// 否则暂存到 pendingCandidates，等注册时一次性 flush
		p.muPending.Lock()
		if p.onIceCandidate != nil {
			p.muPending.Unlock()
			p.onIceCandidate(s)
		} else {
			p.pendingCandidates = append(p.pendingCandidates, s)
			p.muPending.Unlock()
		}
	})

	// 如果指定了 DataChannel 标签名，立即创建 DataChannel
	// 这样 CreateOffer 时 SDP 中就会包含 m=application 段
	if cfg.DataChannelLabel != "" {
		ordered := cfg.Ordered
		dc, err := pc.CreateDataChannel(cfg.DataChannelLabel, &webrtc.DataChannelInit{
			Ordered: &ordered,
		})
		if err != nil {
			pc.Close()
			return nil, fmt.Errorf("webrtc: create data channel: %w", err)
		}
		p.dc = dc
		setupDataChannel(p, dc) // 注册 DataChannel 的 open/message 事件
	}

	// 对端主动创建的 DataChannel（当本端没有预创建时）
	// 收到后同样包装回调
	pc.OnDataChannel(func(dc *webrtc.DataChannel) {
		setupDataChannel(p, dc)
	})

	return p, nil
}

// setupDataChannel 注册 DataChannel 的打开和消息事件
func setupDataChannel(p *Peer, dc *webrtc.DataChannel) {
	// 如果还没有默认 DataChannel，设为第一个到达的
	p.mu.Lock()
	if p.dc == nil {
		p.dc = dc
	}
	fn := p.onDataChannel
	p.mu.Unlock()

	dc.OnOpen(func() {
		log.Printf("data channel '%s' opened", dc.Label())
	})
	dc.OnMessage(func(msg webrtc.DataChannelMessage) {
		p.mu.RLock()
		fn := p.onData
		p.mu.RUnlock()
		if fn != nil {
			fn(msg.Data)
		}
	})

	if fn != nil {
		fn(dc)
	}
}

// ============================================================================
// API: 信令流程 (SDP Offer/Answer)
// ============================================================================

// CreateOffer 创建 SDP Offer 并设置为本地描述
//
// 调用前应确保：
//   - 已注册 OnICECandidate 回调（否则 ICE 候选无法转发）
//   - 如果配置了 DataChannelLabel，Offer 中会包含 DataChannel 的 m=application 段
//
// 返回的 SDP 字符串需要通过用户自己的信令通道发送给对端
func (p *Peer) CreateOffer() (string, error) {
	offer, err := p.pc.CreateOffer(nil)
	if err != nil {
		return "", fmt.Errorf("webrtc: create offer: %w", err)
	}
	if err := p.pc.SetLocalDescription(offer); err != nil {
		return "", fmt.Errorf("webrtc: set local description: %w", err)
	}
	return offer.SDP, nil
}

// CreateAnswer 创建 SDP Answer 并设置为本地描述
//
// 在 SetRemoteDescription 之后调用，返回的 Answer 需要发送给对端
func (p *Peer) CreateAnswer() (string, error) {
	answer, err := p.pc.CreateAnswer(nil)
	if err != nil {
		return "", fmt.Errorf("webrtc: create answer: %w", err)
	}
	if err := p.pc.SetLocalDescription(answer); err != nil {
		return "", fmt.Errorf("webrtc: set local description: %w", err)
	}
	return answer.SDP, nil
}

// SetRemoteDescription 设置对端的 SDP（Offer 或 Answer）
//
// 自动检测 SDP 类型是 Offer 还是 Answer
func (p *Peer) SetRemoteDescription(sdp string) error {
	for _, t := range []webrtc.SDPType{webrtc.SDPTypeOffer, webrtc.SDPTypeAnswer} {
		if err := p.pc.SetRemoteDescription(webrtc.SessionDescription{Type: t, SDP: sdp}); err == nil {
			return nil
		}
	}
	return fmt.Errorf("webrtc: set remote description: invalid SDP")
}

// ============================================================================
// API: ICE 候选处理
// ============================================================================

// AddICECandidate 添加对端的 ICE 候选
//
// candidate 可以是 JSON 格式（{"candidate":"...", "sdpMid":"..."}）
// 也可以是原始 candidate 字符串（"candidate:... typ host ..."）
// 自动检测格式
func (p *Peer) AddICECandidate(candidate string) error {
	var c webrtc.ICECandidateInit
	if err := json.Unmarshal([]byte(candidate), &c); err != nil {
		// JSON 解析失败，当作原始 candidate 字符串处理
		c.Candidate = candidate
	}
	return p.pc.AddICECandidate(c)
}

// OnICECandidate 注册 ICE 候选回调
//
// 注册前收集到的候选（pendingCandidates）会立即通过回调 flush 出去
// 确保用户不会丢失候选
func (p *Peer) OnICECandidate(fn func(candidate string)) {
	p.mu.Lock()
	p.onIceCandidate = fn
	p.mu.Unlock()

	// flush 之前暂存的候选
	p.muPending.Lock()
	for _, c := range p.pendingCandidates {
		fn(c)
	}
	p.pendingCandidates = nil
	p.muPending.Unlock()
}

// ============================================================================
// API: 数据收发与生命周期
// ============================================================================

// OnData 注册数据接收回调
//
// 当 DataChannel 收到对端消息时触发
// data 是原始二进制数据（[]byte），UTF-8 文本可直接转为 string
func (p *Peer) OnData(fn func(data []byte)) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.onData = fn
}

// OnState 注册连接状态变化回调
//
// 状态变化包括：New → Connecting → Connected / Disconnected / Failed / Closed
func (p *Peer) OnState(fn func(state ConnectionState)) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.onState = fn
}

// OnDataChannel 注册 DataChannel 回调
//
// 当对端创建 DataChannel 并到达本端时触发
// 参数 dc 是 pion 的原始 DataChannel，可在 OnOpen 后使用 Send / OnMessage
func (p *Peer) OnDataChannel(fn func(dc *webrtc.DataChannel)) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.onDataChannel = fn
}

// Send 通过默认 DataChannel 发送数据
//
// 如果没有预创建 DataChannel（NewPeer 时 DataChannelLabel 为空），会返回错误
func (p *Peer) Send(data []byte) error {
	p.mu.RLock()
	dc := p.dc
	p.mu.RUnlock()
	if dc == nil {
		return fmt.Errorf("webrtc: no data channel")
	}
	return dc.Send(data)
}

// Close 关闭 PeerConnection，释放资源
//
// 关闭后所有回调都不会再触发
func (p *Peer) Close() error {
	return p.pc.Close()
}

// RemoteAddr 返回对端的 IP 地址和端口
//
// 仅在连接建立后有效，否则返回空字符串
// 格式: "192.168.1.100:5000"
func (p *Peer) RemoteAddr() string {
	sctp := p.pc.SCTP()
	if sctp == nil {
		return ""
	}
	pair, err := sctp.Transport().ICETransport().GetSelectedCandidatePair()
	if err != nil || pair == nil {
		return ""
	}
	return fmt.Sprintf("%s:%d", pair.Remote.Address, pair.Remote.Port)
}

// StateInfo 返回当前 ICE 状态和连接状态摘要
//
// 返回的 map 包含两个字段：
//   - ice_state:        ICE 连接状态字符串
//   - connection_state: PeerConnection 状态字符串
func (p *Peer) StateInfo() map[string]string {
	return map[string]string{
		"ice_state":        p.pc.ICEConnectionState().String(),
		"connection_state": p.pc.ConnectionState().String(),
	}
}

// PeerConnection 返回底层 pion PeerConnection，用于高级用法
//
// 仅在需要直接访问 pion 功能时使用（如自定义统计、传输配置等）
func (p *Peer) PeerConnection() *webrtc.PeerConnection {
	return p.pc
}

// DataChannel 返回默认 DataChannel
//
// 当 NewPeer 时指定了 DataChannelLabel 则立即可用
// 未指定时，在对端创建 DataChannel 并抵达后可用
func (p *Peer) DataChannel() *webrtc.DataChannel {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.dc
}

// GatheringComplete 返回一个 channel，当 ICE 候选收集完成时关闭
//
// 在 CreateOffer / CreateAnswer 后等待此 channel：
//
//	offer, _ := peer.CreateOffer()
//	<-peer.GatheringComplete()
//	finalSDP := peer.LocalDescriptionSDP()
//	sendToRemote(finalSDP)
func (p *Peer) GatheringComplete() <-chan struct{} {
	return p.gatheringDone
}

// LocalDescriptionSDP 返回当前本地描述（包含已收集的 ICE 候选）
//
// 在 GatheringComplete() 后调用，获取含完整候选的 SDP
func (p *Peer) LocalDescriptionSDP() string {
	desc := p.pc.LocalDescription()
	if desc == nil {
		return ""
	}
	return desc.SDP
}
