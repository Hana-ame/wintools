// Package peerjs implements a PeerJS-compatible client in Go:
// signaling over the PeerJS cloud broker (0.peerjs.com) plus WebRTC
// DataChannel transport via pion/webrtc.
package peerjs

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/rand"
	"strings"
	"sync"
	"time"

	"github.com/coder/websocket"
	"github.com/pion/webrtc/v4"
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

func randomToken() string {
	b := make([]byte, 8)
	rand.Read(b)
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
	ws, _, err := websocket.Dial(ctx, u, nil)
	if err != nil {
		return fmt.Errorf("signal dial: %w", err)
	}
	p.ws = ws
	p.ws.SetReadLimit(8 << 20)

	go p.readLoop()
	go p.heartbeatLoop(ctx)

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(p.deadline):
		return fmt.Errorf("signaling OPEN timeout")
	case m := <-p.msgs:
		if m.Type != "OPEN" {
			if m.Type == "ID-TAKEN" {
				return fmt.Errorf("peer id %q already taken", p.cfg.ID)
			}
			if m.Type == "INVALID-KEY" {
				return fmt.Errorf("invalid key %q", p.cfg.Key)
			}
			if m.Type == "ERROR" {
				return fmt.Errorf("server error: %s", string(m.Payload))
			}
			return fmt.Errorf("unexpected message %q waiting for OPEN", m.Type)
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
	scheme := "https"
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

func (p *Peer) readLoop() {
	for {
		_, data, err := p.ws.Read(context.Background())
		if err != nil {
			p.Debugf("signal read error: %v", err)
			select {
			case <-p.closeCh:
			default:
				close(p.closeCh)
			}
			return
		}
		var m ServerMessage
		if err := json.Unmarshal(data, &m); err != nil {
			p.Debugf("bad signal message: %v", err)
			continue
		}
		p.Debugf("recv %s from %s", m.Type, m.Src)
		select {
		case p.msgs <- m:
		default:
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
	p.once.Do(func() { close(p.closeCh) })
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

	pc, err := newPeerConnection(connectionId)
	if err != nil {
		return nil, err
	}
	dc.pc = pc

	label := "http"
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
			"serialization": "binary",
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
