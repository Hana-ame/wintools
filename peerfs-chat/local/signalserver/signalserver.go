// Package signalserver 自托管 PeerJS 信令服务器（兼容 peerjs-server 协议子集）
// + 内置房间发现（替代公共信令 + 公共 MQTT broker）。
//
// 职责：
//  1. 信令：节点注册（WS + token）、OFFER/ANSWER/CANDIDATE/LEAVE 按 dst 转发、
//     dst 不在线入队（带过期）、心跳保活、ID 分配
//  2. 发现：节点 announce 自己关注的集合，`GET /discover/nodes?coll=` 查询在线节点
//     ——自托管后服务器天然知道所有在线节点，不再需要 MQTT 广播
//
// 协议细节对齐 peers/peerjs-server（src/services/webSocketServer、messageHandler）：
//   - WS URL: /{path}peerjs?key=&id=&token=
//   - 消息 {type, src, dst, payload}，服务端覆盖 src
//   - dst 在线转发；不在线入队（LEAVE/EXPIRE 不入队）
//   - OPEN / ID-TAKEN / ERROR 控制消息
//   - 客户端每 5s 发 HEARTBEAT 保活
package signalserver

import (
	"crypto/rand"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

type Server struct {
	key          string
	path         string
	queueTTL     time.Duration // 离线队列存活时间（OFFER 过期用）
	heartbeatTTL time.Duration // 发现的心跳过期时间
	tokenWhitelist map[string]bool // 允许的信令 token（nil/空 = 不限制）

	mu      sync.Mutex
	clients map[string]*client              // id → 在线连接
	queues  map[string][]queuedMsg          // dst → 待转发消息
	disc    map[string]map[string]time.Time // collection → peerId → lastSeen
	peerColls map[string][]string // peerId -> collections
	peerStats  map[string]*PeerStats  // peerId -> stats
}

// Option 信令服务器配置项。
type Option func(*Server)

// WithTokenWhitelist 设置信令 token 白名单：WS 连接的 token 必须在名单内，
// 否则拒绝升级（"Invalid token provided"）。空名单 = 不限制（默认，兼容
// 公共部署现状）。
// 坑（2026-08-18 代码审阅）：token 原本只是 ID 占用保护——任意客户端可自定
// token 连接，攻击者可注册任意 ID 冒充在线节点收信令（配合其自选 ID 可以
// 对任何节点发起 OFFER 诱导连向攻击者）；白名单让自托管部署只信任已知节点。
// 注意：发现端点（announce/nodes）保持公开——发现的目的就是让任何人找到
// 节点，白名单只约束信令面。
func WithTokenWhitelist(tokens []string) Option {
	return func(s *Server) {
		if len(tokens) == 0 {
			return
		}
		s.tokenWhitelist = make(map[string]bool, len(tokens))
		for _, t := range tokens {
			s.tokenWhitelist[t] = true
		}
	}
}

// 离线队列限制（H3 修复）：每 dst 最多缓存 maxQueuedPerDst 条消息。
// 坑：原实现无上限——dst 永不连接时队列无限增长（每个恶意客户端可对任意随机 ID
// 发 OFFER 把服务器内存打爆）。超限丢最旧（信令消息过期即失效，丢旧比丢新合理）。
const maxQueuedPerDst = 100

// Start 启动后台 sweeper：定期清理已过期队列项与空队列。
// 背景：过期清理原只在 flushQueue（dst 上线）时做，dst 永不连接则过期消息堆积。
func (s *Server) Start() {
	go func() {
		t := time.NewTicker(30 * time.Second)
		defer t.Stop()
		for range t.C {
			s.sweepQueues()
		}
	}()
}

func (s *Server) sweepQueues() {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	for dst, q := range s.queues {
		kept := q[:0]
		for _, qm := range q {
			if qm.expire.After(now) && s.clients[dst] == nil {
				kept = append(kept, qm)
			}
		}
		if len(kept) == 0 {
			delete(s.queues, dst)
		} else {
			s.queues[dst] = kept
		}
	}
}

// client 一条在线信令连接。
type client struct {
	id     string
	token  string
	conn   *websocket.Conn
	sendMu sync.Mutex // gorilla 不允许并发写
	last   time.Time  // 最后心跳
}

// queuedMsg 离线队列条目（入队时带过期时间）。
type queuedMsg struct {
	msg    Message
	expire time.Time
}

// Message 信令消息（与 peerjs 客户端协议一致）。
type Message struct {
	Type    string          `json:"type"`
	Src     string          `json:"src,omitempty"`
	Dst     string          `json:"dst,omitempty"`
	Payload json.RawMessage `json:"payload,omitempty"`
}

// NewServer 创建信令服务器。
func NewServer(key string, opts ...Option) *Server {
	if key == "" {
		key = "peerjs"
	}
	s := &Server{
		key:          key,
		path:         "",
		queueTTL:     30 * time.Second,
		heartbeatTTL: 90 * time.Second,
		clients:      make(map[string]*client),
		queues:       make(map[string][]queuedMsg),
		disc:         make(map[string]map[string]time.Time),
		peerColls:    make(map[string][]string),
		peerStats:     make(map[string]*PeerStats),
	}
	for _, o := range opts {
		o(s)
	}
	return s
}

// HandleID GET /{path}{key}/id → 随机 id（peerjs API 兼容）。
func (s *Server) HandleID(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain")
	fmt.Fprint(w, randomID())
}

// HandleWS 处理信令 WebSocket 升级与消息循环。
// 路径形如 /{path}peerjs?key=&id=&token=（gin 路由挂载时提供 {path}）。
func (s *Server) HandleWS(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	id, token, key := q.Get("id"), q.Get("token"), q.Get("key")
	if id == "" || token == "" || key == "" {
		s.wsError(w, "No id, token, or key supplied to websocket server")
		return
	}
	if key != s.key {
		s.wsError(w, "Invalid key provided")
		return
	}
	// token 白名单：空名单 = 不限制（默认）；非空则 token 必须在名单内。
	// 见 WithTokenWhitelist 的坑说明（任意 token 可冒充节点收信令）。
	if len(s.tokenWhitelist) > 0 && !s.tokenWhitelist[token] {
		s.wsError(w, "Invalid token provided")
		return
	}

	upgrader := websocket.Upgrader{
		CheckOrigin: func(*http.Request) bool { return true }, // 自托管，由调用方配置白名单
	}
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	// 读限制：信令消息很小（SDP/ICE 文案），40KB 足够；防恶意客户端塞超大 payload。
	// 同时设 60s 读超时兜底——readLoop 里靠 HEARTBEAT 刷新，断连客户端不再占资源。
	conn.SetReadLimit(40 << 10)
	_ = conn.SetReadDeadline(time.Now().Add(60 * time.Second))

	s.mu.Lock()
	// ID 占用：token 匹配则复用连接，否则拒绝
	if existing, ok := s.clients[id]; ok {
		if existing.token != token {
			s.mu.Unlock()
			_ = conn.WriteJSON(Message{Type: "ID-TAKEN", Payload: raw(`{"msg":"ID is taken"}`)})
			_ = conn.Close()
			return
		}
		existing.closeConn()
	}
	cl := &client{id: id, token: token, conn: conn, last: time.Now()}
	s.clients[id] = cl
	s.mu.Unlock()

	_ = cl.send(Message{Type: "OPEN"})
	s.flushQueue(cl)

	go s.readLoop(cl)
}

// readLoop 读取客户端消息并路由。
func (s *Server) readLoop(cl *client) {
	defer func() {
		s.removeClient(cl)
		cl.closeConn()
	}()
	for {
		var m Message
		if err := cl.conn.ReadJSON(&m); err != nil {
			return
		}
		m.Src = cl.id // 服务端覆盖 src
		s.mu.Lock()
		cl.last = time.Now()
		// 收到消息即视为活跃，续读超时（配合 HandleWS 的 SetReadDeadline）
		_ = cl.conn.SetReadDeadline(time.Now().Add(60 * time.Second))
		s.mu.Unlock()
		s.route(m)
	}
}

// route 路由消息：dst 在线转发，不在线入队（LEAVE/EXPIRE 除外）。
func (s *Server) route(m Message) {
	s.mu.Lock()
	defer s.mu.Unlock()
	dst := s.clients[m.Dst]
	if dst != nil {
		_ = dst.send(m)
		return
	}
	if m.Type == "LEAVE" || m.Type == "EXPIRE" {
		return
	}
	if m.Dst == "" {
		return
	}
	// 入队：目标上线后补发（OFFER/ANSWER/CANDIDATE）
	// H3：队列无上限 → OOM。超 maxQueuedPerDst 丢最旧。
	q := append(s.queues[m.Dst], queuedMsg{msg: m, expire: time.Now().Add(s.queueTTL)})
	if len(q) > maxQueuedPerDst {
		q = q[len(q)-maxQueuedPerDst:]
	}
	s.queues[m.Dst] = q
}

// flushQueue 客户端上线后补发离线队列（含过期清理）。
func (s *Server) flushQueue(cl *client) {
	s.mu.Lock()
	q := s.queues[cl.id]
	delete(s.queues, cl.id)
	s.mu.Unlock()
	now := time.Now()
	for _, qm := range q {
		if qm.expire.After(now) {
			_ = cl.send(qm.msg)
		}
	}
}

// removeClient 断开清理：通知其他节点 LEAVE、删除发现记录。
func (s *Server) removeClient(cl *client) {
	s.mu.Lock()
	if s.clients[cl.id] != cl {
		s.mu.Unlock()
		return
	}
	delete(s.clients, cl.id)
	leave := Message{Type: "LEAVE", Src: cl.id}
	var victims []*client
	for _, c := range s.clients {
		victims = append(victims, c)
	}
	for coll, peers := range s.disc {
		delete(peers, cl.id)
		if len(peers) == 0 {
			delete(s.disc, coll)
		}
	}
	s.mu.Unlock()
	for _, c := range victims {
		_ = c.send(leave)
	}
}

// HandleAnnounce POST /discover/announce {peerId, collections[]} 节点登记房间。
// 与信令连接解耦（节点可通过任意 HTTP 入口上报），lastSeen 由心跳刷新。
// M15：无界 decode 风险——限制 body 大小（1KB 足够：peerId + 少量 collection hash）
// 与 collection 数量（单节点关注房间数有限）。
func (s *Server) HandleAnnounce(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<10)
	var body struct {
		PeerID      string   `json:"peerId"`
		Collections []string `json:"collections"`
		NodeType      string   `json:"nodeType,omitempty"`
		Uptime        int64    `json:"uptime,omitempty"`
		UploadBytes   int64    `json:"uploadBytes,omitempty"`
		DownloadBytes int64    `json:"downloadBytes,omitempty"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.PeerID == "" {
		http.Error(w, "peerId required", http.StatusBadRequest)
		return
	}
	const maxCollectionsPerAnnounce = 64
	if len(body.Collections) > maxCollectionsPerAnnounce {
		http.Error(w, "too many collections", http.StatusBadRequest)
		return
	}
	now := time.Now()
	s.mu.Lock()
	for _, coll := range body.Collections {
		coll = strings.TrimSpace(coll)
		if coll == "" {
			continue
		}
		peers, ok := s.disc[coll]
		if !ok {
			peers = make(map[string]time.Time)
			s.disc[coll] = peers
		}
		peers[body.PeerID] = now
	}
	// Store peer collections
	// Store peer stats
	if body.NodeType != "" || body.Uptime > 0 || body.UploadBytes > 0 || body.DownloadBytes > 0 {
		s.peerStats[body.PeerID] = &PeerStats{
			NodeType:      body.NodeType,
			Uptime:        body.Uptime,
			UploadBytes:   body.UploadBytes,
			DownloadBytes: body.DownloadBytes,
		}
	}
	if len(body.Collections) > 0 {
		s.peerColls[body.PeerID] = body.Collections
	}
	s.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write([]byte(`{"ok":true}`))
}

// HandleNodes GET /discover/nodes?coll= → 在线节点列表（心跳过期剔除）。
func (s *Server) HandleNodes(w http.ResponseWriter, r *http.Request) {
	coll := r.URL.Query().Get("coll")
	cutoff := time.Now().Add(-s.heartbeatTTL)
	s.mu.Lock()
	var out []NodeInfo
	if coll == "" {
		seen := make(map[string]int64)
		for _, peers := range s.disc {
			for id, last := range peers {
				if last.After(cutoff) {
					if _, ok := seen[id]; !ok {
						seen[id] = last.Unix()
								out = append(out, NodeInfo{PeerID: id, LastSeen: last.Unix(), NodeType: func() string { if s.peerStats[id] != nil { return s.peerStats[id].NodeType }; return "" }(), Collections: s.peerColls[id], Uptime: func() int64 { if s.peerStats[id] != nil { return s.peerStats[id].Uptime }; return 0 }(), UploadBytes: func() int64 { if s.peerStats[id] != nil { return s.peerStats[id].UploadBytes }; return 0 }(), DownloadBytes: func() int64 { if s.peerStats[id] != nil { return s.peerStats[id].DownloadBytes }; return 0 }()})
					}
				}
			}
		}
	} else {
		peers := s.disc[coll]
		for id, last := range peers {
			if last.After(cutoff) {
						out = append(out, NodeInfo{PeerID: id, LastSeen: last.Unix(), NodeType: func() string { if s.peerStats[id] != nil { return s.peerStats[id].NodeType }; return "" }(), Collections: s.peerColls[id], Uptime: func() int64 { if s.peerStats[id] != nil { return s.peerStats[id].Uptime }; return 0 }(), UploadBytes: func() int64 { if s.peerStats[id] != nil { return s.peerStats[id].UploadBytes }; return 0 }(), DownloadBytes: func() int64 { if s.peerStats[id] != nil { return s.peerStats[id].DownloadBytes }; return 0 }()})
			}
		}
	}
	s.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"nodes": out})
}

// NodeInfo 发现响应条目。
type PeerStats struct {
	NodeType      string `json:"nodeType,omitempty"`
	Uptime        int64  `json:"uptime,omitempty"`
	UploadBytes   int64  `json:"uploadBytes,omitempty"`
	DownloadBytes int64  `json:"downloadBytes,omitempty"`
}

type NodeInfo struct {
	PeerID        string    `json:"peerId"`
	LastSeen      int64     `json:"lastSeen"`
	NodeType      string    `json:"nodeType,omitempty"`
	Collections   []string  `json:"collections,omitempty"`
	Uptime        int64     `json:"uptime,omitempty"`
	UploadBytes   int64     `json:"uploadBytes,omitempty"`
	DownloadBytes int64     `json:"downloadBytes,omitempty"`
}

// wsError 升级失败（HTTP 层）。
func (s *Server) wsError(w http.ResponseWriter, msg string) {
	http.Error(w, msg, http.StatusBadRequest)
}

func (cl *client) send(m Message) error {
	cl.sendMu.Lock()
	defer cl.sendMu.Unlock()
	cl.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
	return cl.conn.WriteJSON(m)
}

func (cl *client) closeConn() {
	cl.sendMu.Lock()
	defer cl.sendMu.Unlock()
	_ = cl.conn.Close()
}

func raw(s string) json.RawMessage { return json.RawMessage(s) }

// randomID 生成符合 PeerJS 规则的首尾字母数字 id。
func randomID() string {
	const chars = "abcdefghijklmnopqrstuvwxyz0123456789"
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	for i := range b {
		b[i] = chars[b[i]%byte(len(chars))]
	}
	return string(b)
}
