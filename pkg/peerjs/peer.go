// Package peerjs implements a PeerJS-compatible client in Go:
// signaling over the PeerJS cloud broker (0.peerjs.com) plus WebRTC
// DataChannel transport via pion/webrtc.
package peerjs

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/coder/websocket"
	"github.com/pion/webrtc/v4"

	"github.com/Hana-ame/wintools/pkg/netdial"
)

const (
	cloudHost  = "0.peerjs.com"
	cloudPort  = 443
	key        = "peerjs"
	version    = "1.5.4"
	pingEvery  = 5 * time.Second
	msgTimeout = 30 * time.Second
)

type Config struct {
	Host     string // 0.peerjs.com
	Port     int
	Secure   bool
	Key      string
	ID       string // 留空则向服务器申请
	Token    string // 随机生成即可
	Debug    bool
	PingWait time.Duration
	// ICEHook 可选：定制 PeerConnection 配置（测试注入 loopback 候选、
	// 部署换自建 TURN 等）。在 NewPeerConnection 构造前同步调用。
	ICEHook func(*webrtc.Configuration, *webrtc.SettingEngine)
}

type ServerMessage struct {
	Type    string          `json:"type"`
	Src     string          `json:"src,omitempty"`
	Dst     string          `json:"dst,omitempty"`
	Payload json.RawMessage `json:"payload,omitempty"`
}

type connectionEvent struct {
	conn *DataConnection
}

type Peer struct {
	cfg Config
	ws  *websocket.Conn

	mu       sync.Mutex
	open     bool
	id       string
	conns    map[string]*DataConnection // by connectionId
	onConn   func(*DataConnection)
	msgs     chan ServerMessage
	closeCh  chan struct{}
	once     sync.Once
	deadline time.Duration
}

func NewPeer(cfg Config) *Peer {
	if cfg.Host == "" {
		cfg.Host = cloudHost
	}
	if cfg.Port == 0 {
		cfg.Port = cloudPort
	}
	if cfg.Key == "" {
		cfg.Key = key
	}
	if cfg.Token == "" {
		cfg.Token = randomToken()
	}
	if cfg.PingWait == 0 {
		cfg.PingWait = pingEvery
	}
	p := &Peer{
		cfg:      cfg,
		conns:    make(map[string]*DataConnection),
		msgs:     make(chan ServerMessage, 256),
		closeCh:  make(chan struct{}),
		deadline: msgTimeout,
	}
	return p
}

// ID returns the brokering ID once the peer is open (or the requested one).
func (p *Peer) ID() string { return p.id }

func (p *Peer) Open() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.open
}

// OnConnection registers the callback for inbound DataConnections.
func (p *Peer) OnConnection(cb func(*DataConnection)) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.onConn = cb
}

func (p *Peer) Debugf(format string, a ...any) {
	if p.cfg.Debug {
		log.Printf("[peerjs] "+format, a...)
	}
}

// randomToken 生成 64 位十六进制随机串, 用作 peer token 与 connectionId。
// 用 crypto/rand (密码学安全): math/rand 可预测, connectionId 被猜到
// 可被中间人注入 OFFER/ANSWER 劫持数据连接。
func randomToken() string {
	b := make([]byte, 8)
	// crypto/rand.Read 在 Go 1.24+ 永不返回错误 (失败时 panic)。
	if _, err := rand.Read(b); err != nil {
		panic("crypto/rand unavailable: " + err.Error())
	}
	const hexdig = "0123456789abcdef"
	var sb strings.Builder
	for _, c := range b {
		sb.WriteByte(hexdig[c>>4])
		sb.WriteByte(hexdig[c&0xf])
	}
	return sb.String()
}

// Start dials the signaling server and waits for the OPEN message.
func (p *Peer) Start(ctx context.Context) error {
	if p.cfg.ID == "" {
		id, err := p.retrieveID(ctx)
		if err != nil {
			return err
		}
		p.cfg.ID = id
	}
	p.id = p.cfg.ID

	scheme := "ws"
	if p.cfg.Secure || p.cfg.Port == 443 {
		scheme = "wss"
	}
	host := fmt.Sprintf("%s:%d", p.cfg.Host, p.cfg.Port)
	if (p.cfg.Secure || p.cfg.Port == 443) && p.cfg.Port == 443 {
		host = p.cfg.Host
	}
	u := fmt.Sprintf("%s://%s/peerjs?key=%s&id=%s&token=%s&version=%s",
		scheme, host, p.cfg.Key, p.cfg.ID, p.cfg.Token, version)

	p.Debugf("dialing signaling %s", u)
	ws, _, err := websocket.Dial(ctx, u, netdial.WebsocketDialOptions())
	if err != nil {
		return fmt.Errorf("signal dial: %w", err)
	}
	p.ws = ws
	p.ws.SetReadLimit(8 << 20)

	go p.readLoop()
	go p.heartbeatLoop(ctx)

	select {
	case <-ctx.Done():
		return p.failStart(ctx.Err())
	case <-time.After(p.deadline):
		return p.failStart(fmt.Errorf("signaling OPEN timeout"))
	case m := <-p.msgs:
		if m.Type != "OPEN" {
			if m.Type == "ID-TAKEN" {
				return p.failStart(fmt.Errorf("peer id %q already taken", p.cfg.ID))
			}
			if m.Type == "INVALID-KEY" {
				return p.failStart(fmt.Errorf("invalid key %q", p.cfg.Key))
			}
			if m.Type == "ERROR" {
				return p.failStart(fmt.Errorf("server error: %s", string(m.Payload)))
			}
			return p.failStart(fmt.Errorf("unexpected message %q waiting for OPEN", m.Type))
		}
		p.mu.Lock()
		p.open = true
		p.mu.Unlock()
		p.Debugf("signaling open, id=%s", p.id)
		go p.handleLoop()
		return nil
	}
}

// handleLoop routes signaling messages to connections. It runs after OPEN.
func (p *Peer) handleLoop() {
	for {
		select {
		case <-p.closeCh:
			return
		case m := <-p.msgs:
			if m.Type == "OPEN" {
				continue
			}
			p.handleMessage(m)
		}
	}
}

func (p *Peer) retrieveID(ctx context.Context) (string, error) {
	// scheme 跟随 Secure：retrieveID 旧版写死 https，导致自托管 http
	// 信令（如本地 peerserver）借 ID 直接报 "server gave HTTP response to
	// HTTPS client"。与 Start() 的 ws/wss 选择保持一致。
	scheme := "http"
	if p.cfg.Secure || p.cfg.Port == 443 {
		scheme = "https"
	}
	host := fmt.Sprintf("%s:%d", p.cfg.Host, p.cfg.Port)
	if p.cfg.Port == 443 {
		host = p.cfg.Host
	}
	u := fmt.Sprintf("%s://%s/%s/id?ts=%d&version=%s", scheme, host, p.cfg.Key, time.Now().UnixMilli(), version)
	p.Debugf("retrieving id from %s", u)
	req, err := newGET(ctx, u)
	if err != nil {
		return "", err
	}
	resp, err := doHTTP(req, 15*time.Second)
	if err != nil {
		return "", fmt.Errorf("retrieve id: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return "", fmt.Errorf("retrieve id: status %d", resp.StatusCode)
	}
	b, err := io.ReadAll(io.LimitReader(resp.Body, 1<<16))
	if err != nil {
		return "", fmt.Errorf("retrieve id: read: %w", err)
	}
	id := strings.TrimSpace(string(b))
	if id == "" {
		return "", fmt.Errorf("retrieve id: empty response")
	}
	return id, nil
}

// send writes a message to the signaling server (dst for routed messages).
func (p *Peer) send(t ServerMessage) error {
	if p.ws == nil {
		return fmt.Errorf("signaling not connected")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	b, _ := json.Marshal(t)
	p.Debugf("send %s -> %s", t.Type, t.Dst)
	return p.ws.Write(ctx, websocket.MessageText, b)
}

// failStart 在等待 OPEN 失败时清理信令连接与后台 goroutine:
// 关闭 ws 让 readLoop 退出 (它读错误后走 closeSignalCh, 幂等),
// 关闭 closeCh 让 heartbeatLoop 退出, 避免 ws/goroutine 泄漏。
func (p *Peer) failStart(err error) error {
	if p.ws != nil {
		p.ws.Close(websocket.StatusNormalClosure, "open failed")
	}
	p.closeSignalCh()
	return err
}

// closeSignalCh 幂等关闭 closeCh, readLoop 错误路径与 Close 共用同一个
// sync.Once: 两者并发时绝不能 double close (close 已关闭的 channel 会 panic,
// 这是曾经的真实竞态 — readLoop 的 select-default-close 与 Close 并发触发)。
func (p *Peer) closeSignalCh() {
	p.once.Do(func() { close(p.closeCh) })
}

func (p *Peer) readLoop() {
	for {
		_, data, err := p.ws.Read(context.Background())
		if err != nil {
			p.Debugf("signal read error: %v", err)
			p.closeSignalCh()
			return
		}
		var m ServerMessage
		if err := json.Unmarshal(data, &m); err != nil {
			p.Debugf("bad signal message: %v", err)
			continue
		}
		p.Debugf("recv %s from %s", m.Type, m.Src)
		// 信令消息不能丢 (OFFER/ANSWER/CANDIDATE 丢失 → 连接建不起来且无日志,
		// 原来 select-default 静默丢弃就是这类隐性 bug 的来源): 缓冲满时阻塞
		// 背压 (信令量小, 不会长时间卡住 readLoop), closeCh 关闭时退出。
		select {
		case p.msgs <- m:
		case <-p.closeCh:
			return
		}
	}
}

func (p *Peer) heartbeatLoop(ctx context.Context) {
	t := time.NewTicker(p.cfg.PingWait)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-p.closeCh:
			return
		case <-t.C:
			if err := p.send(ServerMessage{Type: "HEARTBEAT"}); err != nil {
				p.Debugf("heartbeat: %v", err)
			}
		}
	}
}

func (p *Peer) Close() error {
	p.mu.Lock()
	p.open = false
	conns := make([]*DataConnection, 0, len(p.conns))
	for _, c := range p.conns {
		conns = append(conns, c)
	}
	p.mu.Unlock()

	if p.ws != nil {
		p.ws.Close(websocket.StatusNormalClosure, "bye")
	}
	p.closeSignalCh()
	for _, c := range conns {
		c.Close()
	}
	return nil
}

func (p *Peer) newDataConnection(remote, connectionId, label string, serialization string, reliable bool) *DataConnection {
	dc := &DataConnection{
		peer:          p,
		remote:        remote,
		connectionId:  connectionId,
		label:         label,
		serialization: serialization,
		reliable:      reliable,
		closeCh:       make(chan struct{}),
	}
	p.mu.Lock()
	p.conns[connectionId] = dc
	p.mu.Unlock()
	return dc
}

// Connect initiates a data connection to the remote peer.
func (p *Peer) Connect(ctx context.Context, remote string) (*DataConnection, error) {
	if !p.Open() {
		return nil, fmt.Errorf("peer not open")
	}
	connectionId := "dc_" + randomToken()
	dc := p.newDataConnection(remote, connectionId, connectionId, "binary", true)

	pc, err := newPeerConnection(connectionId, &p.cfg)
	if err != nil {
		return nil, err
	}
	dc.pc = pc

	// serialization 必须声明 "raw": 浏览器 peerjs 会沿用 offerer 声明的序列化
	// 方式,默认 "binary" 是 BinaryPack 编码,Go 侧裸字节对不上;raw 模式下
	// string=文本帧 / ArrayBuffer=二进制帧直传,与 SendText/Send 一致。
	label := "media"
	opts := &webrtc.DataChannelInit{Ordered: &dc.reliable}
	rdc, err := pc.CreateDataChannel(label, opts)
	if err != nil {
		return nil, fmt.Errorf("create datachannel: %w", err)
	}
	dc.rdc = rdc
	dc.wireDataChannel()

	pc.OnICECandidate(func(c *webrtc.ICECandidate) {
		if c == nil {
			return
		}
		p.send(ServerMessage{
			Type: "CANDIDATE",
			Dst:  remote,
			Payload: mustJSON(map[string]any{
				"candidate":    c.ToJSON(),
				"type":         "data",
				"connectionId": connectionId,
			}),
		})
	})

	offer, err := pc.CreateOffer(nil)
	if err != nil {
		return nil, fmt.Errorf("create offer: %w", err)
	}
	if err := pc.SetLocalDescription(offer); err != nil {
		return nil, fmt.Errorf("set local description: %w", err)
	}
	p.send(ServerMessage{
		Type: "OFFER",
		Dst:  remote,
		Payload: mustJSON(map[string]any{
			"sdp": map[string]string{
				"type": "offer",
				"sdp":  offer.SDP,
			},
			"type":          "data",
			"connectionId":  connectionId,
			"label":         label,
			"reliable":      true,
			"serialization": "raw",
		}),
	})
	p.Debugf("offer sent to %s conn=%s", remote, connectionId)
	return dc, nil
}

// handleMessage routes a signaling message to the right connection or
// creates one for inbound OFFERs.
func (p *Peer) handleMessage(m ServerMessage) {
	var payload struct {
		ConnectionId  string          `json:"connectionId"`
		SDP           json.RawMessage `json:"sdp"`
		Candidate     json.RawMessage `json:"candidate"`
		Type          string          `json:"type"`
		Label         string          `json:"label"`
		Reliable      *bool           `json:"reliable"`
		Serialization string          `json:"serialization"`
	}
	if len(m.Payload) > 0 {
		if err := json.Unmarshal(m.Payload, &payload); err != nil {
			p.Debugf("bad payload: %v", err)
			return
		}
	}

	p.mu.Lock()
	dc := p.conns[payload.ConnectionId]
	p.mu.Unlock()

	switch m.Type {
	case "OFFER":
		if dc == nil {
			reliable := true
			if payload.Reliable != nil {
				reliable = *payload.Reliable
			}
			serialization := payload.Serialization
			if serialization == "" {
				serialization = "binary"
			}
			dc = p.newDataConnection(m.Src, payload.ConnectionId, payload.Label, serialization, reliable)
			p.mu.Lock()
			cb := p.onConn
			p.mu.Unlock()
			if cb != nil {
				cb(dc)
			}
			dc.onOffer()
		}
		dc.handleOffer(m.Src, payload.SDP)
	case "ANSWER":
		if dc != nil {
			dc.handleAnswer(payload.SDP)
		}
	case "CANDIDATE":
		if dc != nil {
			dc.handleCandidate(payload.Candidate)
		}
	case "LEAVE":
		if dc != nil {
			dc.Close()
		}
	}
}

func mustJSON(v any) json.RawMessage {
	b, _ := json.Marshal(v)
	return b
}

// AddRemoteCandidate 将外部通道（如 HTTP 8080 临时候选通道）接收到的对端 ICE 候选直接注入连接。
func (p *Peer) AddRemoteCandidate(connId string, cand webrtc.ICECandidateInit) {
	p.mu.Lock()
	var dc *DataConnection
	if connId != "" {
		dc = p.conns[connId]
	}
	if dc == nil {
		for _, c := range p.conns {
			dc = c
			break
		}
	}
	p.mu.Unlock()
	if dc != nil {
		b, _ := json.Marshal(cand)
		dc.handleCandidate(b)
	}
}
