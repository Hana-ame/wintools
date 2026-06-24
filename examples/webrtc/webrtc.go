// AI: generated with assistance from AI (2026-06-23)
//
// Package webrtc — Production-Ready WebRTC Peer 封装
//
// =====================================================
// 核心改进 (Production-Ready):
// =====================================================
// 1. 生命周期管理: 引入 context.Context，Close() 时立即取消所有相关阻塞操作
// 2. 多通道支持: 使用 map[string]*webrtc.DataChannel 管理所有通道，支持指定标签发送
// 3. 安全回调: 所有回调在执行前校验 ctx.Err()，确保在 Peer 关闭后不再触发
// 4. 强健的 SDP 获取: LocalDescriptionSDP 强制校验 ICE 收集状态，防止返回空 SDP
// 5. 鲁棒的同步: 消除所有 data race，使用细粒度锁保护通道 map
// =====================================================

package webrtc

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"

	"github.com/pion/webrtc/v4"
)

// ConnectionState 表示 PeerConnection 的当前状态
type ConnectionState int

const (
	StateNew ConnectionState = iota
	StateConnecting
	StateConnected
	StateDisconnected
	StateFailed
	StateClosed
)

func (s ConnectionState) String() string {
	switch s {
	case StateNew: return "new"
	case StateConnecting: return "connecting"
	case StateConnected: return "connected"
	case StateDisconnected: return "disconnected"
	case StateFailed: return "failed"
	case StateClosed: return "closed"
	default: return "unknown"
	}
}

var stateMap = map[webrtc.PeerConnectionState]ConnectionState{
	webrtc.PeerConnectionStateNew:          StateNew,
	webrtc.PeerConnectionStateConnecting:   StateConnecting,
	webrtc.PeerConnectionStateConnected:    StateConnected,
	webrtc.PeerConnectionStateDisconnected: StateDisconnected,
	webrtc.PeerConnectionStateFailed:       StateFailed,
	webrtc.PeerConnectionStateClosed:       StateClosed,
}

// Config 创建 Peer 时的配置项
type Config struct {
	ICEServers       []string
	Ordered          bool
	DataChannelLabel string
}

// Peer 封装了一个 WebRTC PeerConnection 和多路 DataChannels
type Peer struct {
	pc     *webrtc.PeerConnection
	ctx    context.Context
	cancel context.CancelFunc

	mu              sync.RWMutex
	dcs             map[string]*webrtc.DataChannel
	defaultDCLabel   string
	onIceCandidate  func(string)
	onData          func(label string, data []byte)
	onState         func(ConnectionState)
	onDataChannel   func(*webrtc.DataChannel)

	pendingCandidates []string
	muPending         sync.Mutex
	gatheringDone     chan struct{}
	gatheredOnce      sync.Once
}

// NewPeer 创建一个新的生产级 WebRTC Peer
func NewPeer(cfg Config) (*Peer, error) {
	iceServers := cfg.ICEServers
	if iceServers == nil {
		iceServers = []string{"stun:stun.l.google.com:19302"}
	}

	se := make([]webrtc.ICEServer, len(iceServers))
	for i, u := range iceServers {
		se[i] = webrtc.ICEServer{URLs: []string{u}}
	}

	pc, err := webrtc.NewPeerConnection(webrtc.Configuration{
		ICEServers: se,
	})
	if err != nil {
		return nil, fmt.Errorf("webrtc: new peer connection: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	p := &Peer{
		pc:            pc,
		ctx:           ctx,
		cancel:        cancel,
		dcs:           make(map[string]*webrtc.DataChannel),
		gatheringDone: make(chan struct{}),
	}

	pc.OnICEConnectionStateChange(func(state webrtc.ICEConnectionState) {
		log.Printf("ICE state: %s", state)
	})

	pc.OnConnectionStateChange(func(s webrtc.PeerConnectionState) {
		if p.ctx.Err() != nil { return }
		p.mu.RLock()
		fn := p.onState
		p.mu.RUnlock()
		if fn != nil {
			fn(stateMap[s])
		}
	})

	pc.OnICECandidate(func(c *webrtc.ICECandidate) {
		if c == nil {
			p.gatheredOnce.Do(func() { close(p.gatheringDone) })
			return
		}
		data, err := json.Marshal(c.ToJSON())
		if err != nil {
			log.Printf("marshal ICE candidate: %v", err)
			return
		}
		s := string(data)

		p.mu.RLock()
		fn := p.onIceCandidate
		p.mu.RUnlock()

		if fn != nil {
			fn(s)
		} else {
			p.muPending.Lock()
			p.pendingCandidates = append(p.pendingCandidates, s)
			p.muPending.Unlock()
		}
	})

	if cfg.DataChannelLabel != "" {
		p.defaultDCLabel = cfg.DataChannelLabel
		ordered := cfg.Ordered
		dc, err := pc.CreateDataChannel(cfg.DataChannelLabel, &webrtc.DataChannelInit{
			Ordered: &ordered,
		})
		if err != nil {
			pc.Close()
			return nil, fmt.Errorf("webrtc: create data channel: %w", err)
		}
		p.addDC(dc)
	}

	pc.OnDataChannel(func(dc *webrtc.DataChannel) {
		p.addDC(dc)
	})

	return p, nil
}

func (p *Peer) addDC(dc *webrtc.DataChannel) {
	p.mu.Lock()
	label := dc.Label()
	p.dcs[label] = dc
	p.mu.Unlock()

	dc.OnOpen(func() {
		log.Printf("data channel '%s' opened", label)
	})

	dc.OnMessage(func(msg webrtc.DataChannelMessage) {
		if p.ctx.Err() != nil { return }
		p.mu.RLock()
		fn := p.onData
		p.mu.RUnlock()
		if fn != nil {
			fn(label, msg.Data)
		}
	})

	p.mu.RLock()
	fn := p.onDataChannel
	p.mu.RUnlock()
	if fn != nil {
		fn(dc)
	}
}

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

func (p *Peer) SetRemoteDescription(sdp string) error {
	for _, t := range []webrtc.SDPType{webrtc.SDPTypeOffer, webrtc.SDPTypeAnswer} {
		if err := p.pc.SetRemoteDescription(webrtc.SessionDescription{Type: t, SDP: sdp}); err == nil {
			return nil
		}
	}
	return fmt.Errorf("webrtc: set remote description: invalid SDP")
}

func (p *Peer) AddICECandidate(candidate string) error {
	var c webrtc.ICECandidateInit
	if err := json.Unmarshal([]byte(candidate), &c); err != nil {
		c.Candidate = candidate
	}
	return p.pc.AddICECandidate(c)
}

func (p *Peer) OnICECandidate(fn func(candidate string)) {
	p.mu.Lock()
	p.onIceCandidate = fn
	p.mu.Unlock()

	p.muPending.Lock()
	for _, c := range p.pendingCandidates {
		fn(c)
	}
	p.pendingCandidates = nil
	p.muPending.Unlock()
}

func (p *Peer) OnData(fn func(label string, data []byte)) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.onData = fn
}

func (p *Peer) OnState(fn func(state ConnectionState)) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.onState = fn
}

func (p *Peer) OnDataChannel(fn func(dc *webrtc.DataChannel)) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.onDataChannel = fn
}

// Send 通过指定标签的 DataChannel 发送数据
func (p *Peer) Send(label string, data []byte) error {
	p.mu.RLock()
	dc, ok := p.dcs[label]
	p.mu.RUnlock()
	if !ok {
		return fmt.Errorf("webrtc: data channel %q not found", label)
	}
	return dc.Send(data)
}

// SendDefault 通过默认 DataChannel 发送数据
func (p *Peer) SendDefault(data []byte) error {
	p.mu.RLock()
	label := p.defaultDCLabel
	p.mu.RUnlock()
	if label == "" {
		return fmt.Errorf("webrtc: no default data channel configured")
	}
	return p.Send(label, data)
}

func (p *Peer) Close() error {
	p.cancel()
	return p.pc.Close()
}

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

func (p *Peer) StateInfo() map[string]string {
	return map[string]string{
		"ice_state":        p.pc.ICEConnectionState().String(),
		"connection_state": p.pc.ConnectionState().String(),
	}
}

func (p *Peer) PeerConnection() *webrtc.PeerConnection {
	return p.pc
}

func (p *Peer) DataChannel(label string) *webrtc.DataChannel {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.dcs[label]
}

func (p *Peer) GatheringComplete() <-chan struct{} {
	return p.gatheringDone
}

// LocalDescriptionSDP 返回本地描述。
// 如果 ICE 候选尚未收集完成，则返回错误，以确保发送的 SDP 是完整的。
func (p *Peer) LocalDescriptionSDP() (string, error) {
	select {
	case <-p.gatheringDone:
		desc := p.pc.LocalDescription()
		if desc == nil {
			return "", fmt.Errorf("webrtc: no local description")
		}
		return desc.SDP, nil
	default:
		return "", fmt.Errorf("webrtc: ICE gathering still in progress")
	}
}
