package peerfs

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"log"
	"net/http"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/pion/webrtc/v4"

	"github.com/Hana-ame/wintools/pkg/netdial"
	"github.com/Hana-ame/wintools/pkg/peerjs"
)

// 帧协议（文本帧 = JSON 控制头，二进制帧 = 数据块；浏览器 peerjs 需以
// serialization:"raw" 连接，string/ArrayBuffer 与 SCTP 文本/二进制一一对应）：
//
//	→ {"type":"hello","token":"..."}                 连接后首帧，必发；超时未发关连接
//	→ {"type":"list","path":"/","reqId"}
//	← {"type":"entries","entries":[...],"reqId"}     文本帧（一次到齐）
//	→ {"type":"read","path":"a.jpg","offset":0,"size":-1,"reqId"}
//	← {"type":"meta","total":N,"reqId"}              文本帧
//	← <N 字节二进制块，chunkSize/块>                  二进制帧 × k（无逐块头）
//	← {"type":"done","reqId"}                        文本帧
//	任何一步出错：← {"type":"err","msg":"...","reqId"}
//
// 接收端按 meta.total 计数收块（DataChannel 可靠有序，无需逐块分帧）；
// 中途 err 帧表示流作废。同一连接同一时刻只处理一个请求（lock-step，
// 由 connState.mu 保证），浏览器端 bridge.js 按序排队。

const (
	chunkSize    = 64 << 10 // 64KB，SCTP 消息安全上限内
	helloTimeout = 10 * time.Second
)

// Header 是文本帧上承载的 JSON 控制头。
type Header struct {
	Type    string  `json:"type"`              // hello|list|read|entries|meta|done|err
	Token   string  `json:"token,omitempty"`   // hello
	Path    string  `json:"path,omitempty"`    // list/read
	Offset  int64   `json:"offset,omitempty"`  // read
	Size    int64   `json:"size,omitempty"`    // read；-1 = 读到文件尾
	Total    int64   `json:"total,omitempty"`   // meta
	FileSize int64   `json:"fileSize,omitempty"`// meta: 完整文件大小
	ReqID    string  `json:"reqId,omitempty"`   // 除 hello 外必带
	Msg      string  `json:"msg,omitempty"`     // err
	Entries  []Entry `json:"entries,omitempty"` // entries
}

// UnmarshalJSON 为 Header 提供默认值：未传 size 时默认 -1（读到文件尾）。
func (h *Header) UnmarshalJSON(b []byte) error {
	type alias Header
	raw := alias{Size: -1}
	if err := json.Unmarshal(b, &raw); err != nil {
		return err
	}
	*h = Header(raw)
	return nil
}

// Entry 是 list 返回的目录项。
type Entry struct {
	Name string `json:"name"`
	Dir  bool   `json:"dir,omitempty"`
	Size int64  `json:"size,omitempty"`
}

// Signaling 描述节点连哪个信令服务器。
type Signaling struct {
	Host   string // 空 = 公共云 0.peerjs.com
	Port   int    // 0 → 443
	Secure bool   // wss
	Key    string // 空 = "peerjs"
}

// Config 是 Node 的配置。
type Config struct {
	PeerID    string    // 浏览器连接的目标 id；空 = 向信令服务器借随机 id
	Root      string    // 文件服务根目录（穿越防护由 http.Dir 保证）
	Token     string    // hello 校验；空 = 不校验（信令白名单足够时可不设）
	Signaling Signaling // 连哪个信令服务器
	Debug     bool
	ConfigURL string    // 中心配置与分发 URL（可拉取配置、集中注册、分发全网节点）
	// ICEHook 可选：透传给 pkg/peerjs（测试注入 loopback、部署换 TURN）。
	ICEHook func(*webrtc.Configuration, *webrtc.SettingEngine)
}

// Node 是一个对外提供文件服务的后端节点。
type Node struct {
	cfg  Config
	root http.Dir
	peer *peerjs.Peer

	hubNodesMu sync.RWMutex
	hubNodes   []map[string]any

	startOnce sync.Once
	startErr  error
}

func New(cfg Config) *Node {
	if cfg.Root == "" {
		cfg.Root = "."
	}
	return &Node{cfg: cfg, root: http.Dir(cfg.Root)}
}

// ID 返回信令注册成功后的 peer id。
func (n *Node) ID() string {
	if n.peer == nil {
		return n.cfg.PeerID
	}
	return n.peer.ID()
}

// GetHubNodes 返回从中心配置拉取到的全网活跃节点列表（分发用）。
func (n *Node) GetHubNodes() []map[string]any {
	n.hubNodesMu.RLock()
	defer n.hubNodesMu.RUnlock()
	if len(n.hubNodes) == 0 {
		return nil
	}
	out := make([]map[string]any, len(n.hubNodes))
	copy(out, n.hubNodes)
	return out
}

func (n *Node) fetchCentralConfig(ctx context.Context) {
	if n.cfg.ConfigURL == "" {
		return
	}
	client := netdial.Client(5 * time.Second)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, n.cfg.ConfigURL, nil)
	if err != nil {
		log.Printf("[peerfs-config] build request failed: %v", err)
		return
	}
	resp, err := client.Do(req)
	if err != nil {
		log.Printf("[peerfs-config] fetch central config from %s failed: %v", n.cfg.ConfigURL, err)
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		log.Printf("[peerfs-config] central config status: %d", resp.StatusCode)
		return
	}

	var data struct {
		PeerID    string           `json:"peerId"`
		Name      string           `json:"name"`
		Token     string           `json:"token"`
		Signaling *Signaling       `json:"signaling"`
		Nodes     []map[string]any `json:"nodes"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
		return
	}
	if n.cfg.PeerID == "" {
		if data.PeerID != "" {
			n.cfg.PeerID = data.PeerID
		} else if data.Name != "" {
			n.cfg.PeerID = "wt-media-" + data.Name
		}
	}
	if n.cfg.Token == "" && data.Token != "" {
		n.cfg.Token = data.Token
	}
	if data.Signaling != nil && n.cfg.Signaling.Host == "" {
		n.cfg.Signaling = *data.Signaling
	}
	if len(data.Nodes) > 0 {
		n.hubNodesMu.Lock()
		n.hubNodes = data.Nodes
		n.hubNodesMu.Unlock()
	}
	log.Printf("[peerfs-config] successfully loaded central config from %s", n.cfg.ConfigURL)
}

func (n *Node) startAnnounceLoop(ctx context.Context) {
	announceURL := n.cfg.ConfigURL
	if strings.HasSuffix(announceURL, "/discover/nodes") {
		announceURL = strings.TrimSuffix(announceURL, "/discover/nodes") + "/discover/announce"
	} else if !strings.Contains(announceURL, "/announce") {
		announceURL = strings.TrimRight(announceURL, "/") + "/discover/announce"
	}
	nodesURL := strings.Replace(announceURL, "/announce", "/nodes", 1)

	client := netdial.Client(5 * time.Second)

	ticker := time.NewTicker(20 * time.Second)
	go func() {
		defer ticker.Stop()
		n.doAnnounce(ctx, client, announceURL, nodesURL)
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				n.doAnnounce(ctx, client, announceURL, nodesURL)
			}
		}
	}()
}

func (n *Node) doAnnounce(ctx context.Context, client *http.Client, announceURL, nodesURL string) {
	bodyData := map[string]any{
		"id":            n.ID(),
		"peerId":        n.ID(),
		"name":          n.cfg.PeerID,
		"root":          n.cfg.Root,
		"collections":   []string{"media", "default"},
		"tokenRequired": n.cfg.Token != "",
		"signaling":     n.cfg.Signaling,
		"lastSeen":      time.Now().Unix(),
	}
	b, _ := json.Marshal(bodyData)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, announceURL, bytes.NewReader(b))
	if err == nil {
		req.Header.Set("Content-Type", "application/json")
		if resp, err := client.Do(req); err == nil {
			_ = resp.Body.Close()
		}
	}

	queryURL := nodesURL
	if !strings.Contains(queryURL, "?") {
		queryURL += "?coll=media"
	}
	nreq, err := http.NewRequestWithContext(ctx, http.MethodGet, queryURL, nil)
	if err == nil {
		if resp, err := client.Do(nreq); err == nil {
			defer resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				var res struct {
					Nodes []map[string]any `json:"nodes"`
				}
				if err := json.NewDecoder(resp.Body).Decode(&res); err == nil && len(res.Nodes) > 0 {
					n.hubNodesMu.Lock()
					n.hubNodes = res.Nodes
					n.hubNodesMu.Unlock()
				}
			}
		}
	}
}

// InjectCandidate 允许通过外部通道（如 HTTP 8080 临时候选通道）直接向正在建立的连接注入对端候选。
func (n *Node) InjectCandidate(connId string, cand webrtc.ICECandidateInit) {
	if n.peer != nil {
		n.peer.AddRemoteCandidate(connId, cand)
	}
}

// Start 注册信令并开始接受浏览器的 DataChannel 连接。
func (n *Node) Start(ctx context.Context) error {
	n.startOnce.Do(func() { n.startErr = n.start(ctx) })
	return n.startErr
}

func (n *Node) start(ctx context.Context) error {
	n.fetchCentralConfig(ctx)

	sig := n.cfg.Signaling
	n.peer = peerjs.NewPeer(peerjs.Config{
		ID:      n.cfg.PeerID,
		Host:    sig.Host,
		Port:    sig.Port,
		Secure:  sig.Secure,
		Key:     sig.Key,
		Debug:   n.cfg.Debug,
		ICEHook: n.cfg.ICEHook,
	})
	// OnConnection 必须在 Start 前注册：Start 返回即可能收到 OFFER。
	n.peer.OnConnection(n.onConn)
	if err := n.peer.Start(ctx); err != nil {
		return fmt.Errorf("peerfs: signaling start: %w", err)
	}

	if n.cfg.ConfigURL != "" {
		n.startAnnounceLoop(ctx)
	}

	return nil
}

// Close 关闭信令与全部连接。
func (n *Node) Close() error {
	if n.peer != nil {
		return n.peer.Close()
	}
	return nil
}

// connState 是一条入站 DataConnection 的会话状态。
type connState struct {
	n      *Node
	remote string
	authed bool
}

// frameWriter 是 handler 对数据连接的最小依赖（便于测试注入 fake）。
type frameWriter interface {
	SendText(string) error
	SendThrottled([]byte) error
	Close()
}

func (n *Node) onConn(dc *peerjs.DataConnection) {
	remote := dc.Remote()
	st := &connState{n: n, remote: remote}
	log.Printf("[peerfs] incoming connection from %s", remote)

	// hello 门禁：超时未通过认证直接关连接（防任意网页猜到 ID 就能读文件）。
	t := time.AfterFunc(helloTimeout, func() {
		if !st.authed {
			log.Printf("[peerfs] %s handshake timeout, closing", remote)
			dc.Close()
		}
	})
	dc.OnClose(func() {
		t.Stop()
		log.Printf("[peerfs] connection closed: %s", remote)
	})
	dc.OnMessageKind(func(m peerjs.Message) { st.onMessage(dc, m) })
}

func (st *connState) onMessage(dc frameWriter, m peerjs.Message) {
	// 入站只允许文本帧控制头；二进制帧是服务端 → 浏览器方向的产物。
	if !m.IsText {
		return
	}
	var h Header
	if err := json.Unmarshal(m.Data, &h); err != nil {
		st.replyErr(dc, "", "bad header: "+err.Error())
		return
	}
	if !st.authed {
		if h.Type != "hello" {
			log.Printf("[peerfs] %s rejected: non-hello first frame", st.remote)
			dc.Close()
			return
		}
		if st.n.cfg.Token != "" && h.Token != st.n.cfg.Token {
			st.replyErr(dc, "", "bad token")
			time.AfterFunc(100*time.Millisecond, dc.Close) // 给 err 帧留发送时间
			return
		}
		st.authed = true
		log.Printf("[peerfs] %s handshake authenticated", st.remote)
		return
	}
	switch h.Type {
	case "ping":
		_ = dc.SendText(`{"type":"pong"}`)
	case "pong":
		// 保活确认，无需额外操作
	case "list":
		st.handleList(dc, h)
	case "read":
		st.handleRead(dc, h)
	default:
		st.replyErr(dc, h.ReqID, "unknown type "+h.Type)
	}
}

func (st *connState) handleList(dc frameWriter, h Header) {
	name := path.Clean("/" + h.Path)
	f, err := st.n.root.Open(name)
	if err != nil {
		st.replyErr(dc, h.ReqID, err.Error())
		return
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil || !fi.IsDir() {
		st.replyErr(dc, h.ReqID, "not a directory")
		return
	}
	rdf, ok := f.(interface {
		ReadDir(int) ([]fs.DirEntry, error)
	})
	if !ok {
		st.replyErr(dc, h.ReqID, "cannot readdir")
		return
	}
	des, err := rdf.ReadDir(-1)
	if err != nil {
		st.replyErr(dc, h.ReqID, err.Error())
		return
	}
	entries := make([]Entry, 0, len(des))
	for _, de := range des {
		e := Entry{Name: de.Name(), Dir: de.IsDir()}
		if info, err := de.Info(); err == nil && !de.IsDir() {
			e.Size = info.Size()
		}
		entries = append(entries, e)
	}
	b, _ := json.Marshal(Header{Type: "entries", ReqID: h.ReqID, Entries: entries})
	log.Printf("[peerfs] %s: list %s -> %d entries", st.remote, name, len(entries))
	dc.SendText(string(b))
}

func (st *connState) handleRead(dc frameWriter, h Header) {
	name := path.Clean("/" + h.Path)
	f, err := st.n.root.Open(name)
	if err != nil {
		st.replyErr(dc, h.ReqID, err.Error())
		return
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		st.replyErr(dc, h.ReqID, err.Error())
		return
	}
	if fi.IsDir() {
		st.replyErr(dc, h.ReqID, "is a directory")
		return
	}
	rs, ok := f.(io.ReadSeeker)
	if !ok {
		st.replyErr(dc, h.ReqID, "file is not seekable")
		return
	}
	size := fi.Size()
	if h.Offset < 0 {
		// 支持负数 offset 读取文件末尾 N 字节（如非 faststart 视频读取末尾 moov box）
		h.Offset = size + h.Offset
		if h.Offset < 0 {
			h.Offset = 0
		}
	} else if h.Offset > size {
		st.replyErr(dc, h.ReqID, "offset beyond end of file")
		return
	}
	if _, err := rs.Seek(h.Offset, io.SeekStart); err != nil {
		st.replyErr(dc, h.ReqID, "seek: "+err.Error())
		return
	}
	total := size - h.Offset
	if h.Size >= 0 && h.Size < total {
		total = h.Size
	}
	log.Printf("[peerfs] %s: read %s (offset=%d, size=%d, total=%d, fileSize=%d)", st.remote, name, h.Offset, h.Size, total, size)
	// meta 先行：浏览器按 total 计数收块，收满等 done 帧。
	headB, _ := json.Marshal(Header{Type: "meta", ReqID: h.ReqID, Total: total, FileSize: size})
	if err := dc.SendText(string(headB)); err != nil {
		return
	}
	remaining := total
	buf := make([]byte, chunkSize)
	for remaining > 0 {
		want := int64(chunkSize)
		if remaining < want {
			want = remaining
		}
		nr, err := io.ReadFull(rs, buf[:want])
		if nr > 0 {
			if serr := dc.SendThrottled(buf[:nr]); serr != nil {
				return // 对端断开，err 帧也发不出了
			}
			remaining -= int64(nr)
		}
		if err != nil {
			// 磁盘中途出错：已发的块作废，显式 err 让浏览器中止等待。
			st.replyErr(dc, h.ReqID, "read: "+err.Error())
			return
		}
	}
	doneB, _ := json.Marshal(Header{Type: "done", ReqID: h.ReqID})
	dc.SendText(string(doneB))
	log.Printf("[peerfs] %s: read %s done", st.remote, name)
}

func (st *connState) replyErr(dc frameWriter, reqID, msg string) {
	log.Printf("[peerfs] %s: error (reqId=%s): %s", st.remote, reqID, msg)
	b, _ := json.Marshal(Header{Type: "err", ReqID: reqID, Msg: msg})
	if err := dc.SendText(string(b)); err != nil {
		log.Printf("[peerfs] %s: send err frame failed: %v", st.remote, err)
	}
}
