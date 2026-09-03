package peerjs

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Hana-ame/wintools/pkg/netdial"
	"github.com/pion/ice/v4"
	"github.com/pion/webrtc/v4"
)

// HTTP helpers (kept local to avoid extra deps).
func newGET(ctx context.Context, url string) (*http.Request, error) {
	return http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
}

func doHTTP(req *http.Request, timeout time.Duration) (*http.Response, error) {
	c := netdial.Client(timeout)
	return c.Do(req)
}

var (
	defaultUDPMuxOnce sync.Once
	defaultUDPConn    *net.UDPConn
	defaultUDPMux     ice.UDPMux
)

func getUDPMux() (ice.UDPMux, *net.UDPConn, error) {
	var err error
	defaultUDPMuxOnce.Do(func() {
		defaultUDPConn, err = net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4zero, Port: 0})
		if err == nil {
			defaultUDPMux = webrtc.NewICEUDPMux(nil, defaultUDPConn)
		}
	})
	return defaultUDPMux, defaultUDPConn, err
}

// newPeerConnection 创建 PeerConnection；cfg.ICEHook 非空时允许调用方定制
// Configuration/SettingEngine（测试注入 loopback 候选、部署换 TURN 等）。
func newPeerConnection(connectionId string, cfg *Config) (*webrtc.PeerConnection, error) {
	config := webrtc.Configuration{
		ICEServers: []webrtc.ICEServer{
			{URLs: []string{
				"stun:stun.l.google.com:19302",
				"stun:stun1.l.google.com:19302",
				"stun:stun.cloudflare.com:3478",
			}},
		},
	}
	s := webrtc.SettingEngine{}
	s.SetIncludeLoopbackCandidate(true)
	if mux, _, err := getUDPMux(); err == nil && mux != nil {
		s.SetICEUDPMux(mux)
	}
	if cfg != nil && cfg.ICEHook != nil {
		cfg.ICEHook(&config, &s)
	}
	pc, err := webrtc.NewAPI(webrtc.WithSettingEngine(s)).NewPeerConnection(config)
	if err != nil {
		return nil, fmt.Errorf("new peer connection: %w", err)
	}
	return pc, nil
}

// Message 是一条带类型标记的 DataChannel 消息。
//
// 为什么需要: 浏览器 peerjs 用 serialization:"raw" 时,string 走 SCTP
// 文本消息、ArrayBuffer 走二进制消息;pion 用 IsString 区分,但旧版
// OnMessage(func([]byte)) 把标记丢了 —— 上层无法区分「JSON 控制头」和
// 「文件数据块」。需要区分的消费者用 OnMessageKind。
type Message struct {
	IsText bool
	Data   []byte
}

// bufHighWater SendThrottled 的缓冲高水位 (64KB): 超过则等待 SCTP 排空,
// 防 pion 内部发送缓冲无限膨胀打爆内存 (大文件连续 Send 时必然发生)。
const bufHighWater = 1 << 16

// DataConnection is one WebRTC DataChannel between two peers.
type DataConnection struct {
	peer          *Peer
	remote        string
	connectionId  string
	label         string
	serialization string
	reliable      bool

	pc      *webrtc.PeerConnection
	rdc     *webrtc.DataChannel
	isChild bool

	mu        sync.Mutex
	open      bool
	msgs      chan []byte
	closeCh   chan struct{}
	onMsg     func([]byte)
	onMsgKind func(Message)
	onOpen    func()
	onClose   func()

	iceMu sync.Mutex
	// ICE 候选缓存：远端描述设置前的早到候选（见 handleCandidate）。
	pendingCands  [][]byte
	hasRemoteDesc bool
}

func (dc *DataConnection) Remote() string { return dc.remote }

func (dc *DataConnection) Open() bool {
	dc.mu.Lock()
	defer dc.mu.Unlock()
	return dc.open
}

func (dc *DataConnection) OnMessage(cb func([]byte)) {
	dc.mu.Lock()
	dc.onMsg = cb
	dc.mu.Unlock()
}

// OnMessageKind 注册带文本/二进制标记的消息回调 (见 Message)。
// 与 OnMessage 互斥: 后注册的生效。新代码建议用这个。
func (dc *DataConnection) OnMessageKind(cb func(Message)) {
	dc.mu.Lock()
	dc.onMsgKind = cb
	dc.mu.Unlock()
}

func (dc *DataConnection) OnOpen(cb func()) {
	dc.mu.Lock()
	dc.onOpen = cb
	dc.mu.Unlock()
}

func (dc *DataConnection) OnClose(cb func()) {
	dc.mu.Lock()
	dc.onClose = cb
	dc.mu.Unlock()
}

// onOffer is called on the answering side when the OFFER arrives, before
// SDP processing, to create the PC and wire the DataChannel.
func (dc *DataConnection) onOffer() {
	pc, err := newPeerConnection(dc.connectionId, &dc.peer.cfg)
	if err != nil {
		dc.peer.Debugf("onOffer: %v", err)
		return
	}
	dc.pc = pc

	var firstChannel atomic.Bool
	firstChannel.Store(true)

	pc.OnDataChannel(func(ch *webrtc.DataChannel) {
		if firstChannel.CompareAndSwap(true, false) {
			dc.rdc = ch
			dc.wireDataChannel()
			return
		}

		// 方案 2: 同一 WebRTC PeerConnection 下并发建立多条轻量 DataChannel
		subDC := &DataConnection{
			peer:          dc.peer,
			remote:        dc.remote,
			connectionId:  fmt.Sprintf("%s-%s", dc.connectionId, ch.Label()),
			label:         ch.Label(),
			serialization: dc.serialization,
			reliable:      dc.reliable,
			pc:            dc.pc,
			rdc:           ch,
			isChild:       true,
			msgs:          make(chan []byte, 64),
			closeCh:       make(chan struct{}),
		}
		subDC.wireDataChannel()

		dc.peer.mu.Lock()
		dc.peer.conns[subDC.connectionId] = subDC
		onConn := dc.peer.onConn
		dc.peer.mu.Unlock()

		if onConn != nil {
			onConn(subDC)
		}
	})

	pc.OnICECandidate(func(c *webrtc.ICECandidate) {
		if c == nil {
			return
		}
		dc.peer.send(ServerMessage{
			Type: "CANDIDATE",
			Dst:  dc.remote,
			Payload: mustJSON(map[string]any{
				"candidate":    c.ToJSON(),
				"type":         "data",
				"connectionId": dc.connectionId,
			}),
		})
	})
}

func (dc *DataConnection) wireDataChannel() {
	dc.rdc.OnOpen(func() {
		dc.mu.Lock()
		dc.open = true
		cb := dc.onOpen
		dc.mu.Unlock()
		dc.peer.Debugf("datachannel open conn=%s", dc.connectionId)
		if cb != nil {
			cb()
		}
	})
	dc.rdc.OnMessage(func(msg webrtc.DataChannelMessage) {
		dc.mu.Lock()
		cb, cbKind := dc.onMsg, dc.onMsgKind
		dc.mu.Unlock()
		if cbKind != nil {
			cbKind(Message{IsText: msg.IsString, Data: msg.Data})
			return
		}
		if cb != nil {
			cb(msg.Data)
		}
	})
	dc.rdc.OnClose(func() {
		dc.Close()
	})
}

func (dc *DataConnection) handleOffer(src string, sdp json.RawMessage) {
	var sd struct {
		SDP string `json:"sdp"`
	}
	json.Unmarshal(sdp, &sd)
	offer := webrtc.SessionDescription{Type: webrtc.SDPTypeOffer, SDP: sd.SDP}
	if err := dc.pc.SetRemoteDescription(offer); err != nil {
		dc.peer.Debugf("set remote offer: %v", err)
		return
	}
	dc.markRemoteDesc()
	answer, err := dc.pc.CreateAnswer(nil)
	if err != nil {
		dc.peer.Debugf("create answer: %v", err)
		return
	}
	if err := dc.pc.SetLocalDescription(answer); err != nil {
		dc.peer.Debugf("set local answer: %v", err)
		return
	}
	dc.peer.send(ServerMessage{
		Type: "ANSWER",
		Dst:  src,
		Payload: mustJSON(map[string]any{
			"sdp": map[string]string{
				"type": "answer",
				"sdp":  answer.SDP,
			},
			"type":         "data",
			"connectionId": dc.connectionId,
		}),
	})
	dc.peer.Debugf("answer sent to %s conn=%s", src, dc.connectionId)
}

func (dc *DataConnection) handleAnswer(sdp json.RawMessage) {
	var sd struct {
		SDP string `json:"sdp"`
	}
	json.Unmarshal(sdp, &sd)
	answer := webrtc.SessionDescription{Type: webrtc.SDPTypeAnswer, SDP: sd.SDP}
	if err := dc.pc.SetRemoteDescription(answer); err != nil {
		dc.peer.Debugf("set remote answer: %v", err)
		return
	}
	dc.markRemoteDesc()
}

// handleCandidate 处理远端 ICE 候选。
// 关键：候选可能先于 ANSWER 到达（信令转发无顺序保证），此时远端描述还没
// 设置，直接 AddICECandidate 会报错且候选永久丢失（曾导致连接卡 checking）。
// 与浏览器 peerjs 行为一致：未设置远端描述前先缓存，设置后立即回放。
func (dc *DataConnection) handleCandidate(cand json.RawMessage) {
	dc.iceMu.Lock()
	if !dc.hasRemoteDesc {
		dc.pendingCands = append(dc.pendingCands, append([]byte(nil), cand...))
		dc.iceMu.Unlock()
		return
	}
	dc.iceMu.Unlock()
	dc.addCandidate(cand)
}

func (dc *DataConnection) addCandidate(cand []byte) {
	var c webrtc.ICECandidateInit
	if err := json.Unmarshal(cand, &c); err != nil {
		dc.peer.Debugf("bad candidate: %v", err)
		return
	}
	if c.Candidate != "" {
		go PunchCandidate(c.Candidate)
	}
	if err := dc.pc.AddICECandidate(c); err != nil {
		dc.peer.Debugf("add candidate: %v", err)
	}
}

// PunchCandidate 解析对端 (浏览器) 候选并主动发射出站 UDP STUN Ping 包，
// 强制 NAT / 云端防火墙创建会话映射表项 (Stateful Connection Tracking)，
// 实现出站打洞，使对端后续的数据通道报文能够穿透入站防火墙。
func PunchCandidate(candStr string) {
	fields := strings.Fields(candStr)
	if len(fields) < 8 {
		return
	}
	transport := strings.ToLower(fields[2])
	if transport != "udp" {
		return
	}
	ipStr := fields[4]
	portStr := fields[5]
	if strings.HasSuffix(ipStr, ".local") {
		return // mDNS 地址无法直连
	}
	ip := net.ParseIP(ipStr)
	if ip == nil {
		return
	}
	port, err := strconv.Atoi(portStr)
	if err != nil || port <= 0 || port > 65535 {
		return
	}
	raddr := &net.UDPAddr{IP: ip, Port: port}

	// 关键：必须从 Pion 正在监听的同一块 UDP Socket (udpConn) 发出出站打洞包！
	// 这样 NAT / 云防火墙建立的放行规则才恰好对应 Pion 的监听端口。
	_, uConn, _ := getUDPMux()

	// 构造标准 STUN Binding Request 请求包头 (RFC 5389)
	stunReq := []byte{
		0x00, 0x01, 0x00, 0x00, // Binding Request, length = 0
		0x21, 0x12, 0xa4, 0x42, // Magic Cookie
		0x70, 0x65, 0x65, 0x72, 0x66, 0x73, 0x2d, 0x70, 0x69, 0x6e, 0x67, 0x21, // Transaction ID
	}
	for i := 0; i < 3; i++ {
		if uConn != nil {
			_, _ = uConn.WriteTo(stunReq, raddr)
		} else {
			conn, err := net.DialUDP("udp", nil, raddr)
			if err == nil {
				_, _ = conn.Write(stunReq)
				conn.Close()
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	localPort := 0
	if uConn != nil {
		localPort = uConn.LocalAddr().(*net.UDPAddr).Port
	}
	log.Printf("[peerfs-punch] sent outbound UDP STUN hole-punch ping (local_port=%d) to browser: %s:%d", localPort, ipStr, port)
}

// markRemoteDesc 在 SetRemoteDescription 成功后调用：解锁候选缓存并回放。
func (dc *DataConnection) markRemoteDesc() {
	dc.iceMu.Lock()
	dc.hasRemoteDesc = true
	pending := dc.pendingCands
	dc.pendingCands = nil
	dc.iceMu.Unlock()
	for _, c := range pending {
		dc.addCandidate(c)
	}
}

// Send writes raw bytes on the DataChannel (ordered).
func (dc *DataConnection) Send(data []byte) error {
	if !dc.Open() {
		return fmt.Errorf("dataconnection not open")
	}
	return dc.rdc.Send(data)
}

// SendText 发送文本帧 (SCTP string 消息)。浏览器 peerjs raw 序列化下
// 收到 typeof data === 'string',用于 JSON 控制头;数据块必须用 Send/SendThrottled
// 走二进制帧 —— 发反了对端把控制头当数据块吞掉 (go-peerjs 协议约束)。
func (dc *DataConnection) SendText(s string) error {
	if !dc.Open() {
		return fmt.Errorf("dataconnection not open")
	}
	return dc.rdc.SendText(s)
}

// SendThrottled 发送二进制块并做流控: 发送缓冲超过 bufHighWater 时退避等待
// SCTP 排空后再发,供大文件分块连续发送使用 (裸 Send 会把 pion 发送缓冲撑爆)。
// 轮询而非 OnBufferedAmountLow 回调: pion 该回调是替换式的,并发注册互相覆盖
// 会死等 (go-peerjs README 记录过的坑);轮询 200µs~5ms 开销可忽略且无此风险。
func (dc *DataConnection) SendThrottled(data []byte) error {
	if !dc.Open() {
		return fmt.Errorf("dataconnection not open")
	}
	wait := 200 * time.Microsecond
	for dc.rdc.BufferedAmount() > bufHighWater {
		select {
		case <-dc.closeCh:
			return fmt.Errorf("dataconnection closed")
		case <-time.After(wait):
		}
		if wait < 5*time.Millisecond {
			wait *= 2
		}
	}
	return dc.rdc.Send(data)
}

func (dc *DataConnection) Close() {
	dc.mu.Lock()
	if !dc.open {
		if dc.pc != nil {
			dc.pc.Close()
		}
		dc.mu.Unlock()
		return
	}
	dc.open = false
	cb := dc.onClose
	dc.mu.Unlock()

	if dc.rdc != nil {
		dc.rdc.Close()
	}
	if !dc.isChild && dc.pc != nil {
		dc.pc.Close()
	}
	dc.peer.mu.Lock()
	delete(dc.peer.conns, dc.connectionId)
	dc.peer.mu.Unlock()
	if cb != nil {
		cb()
	}
}
