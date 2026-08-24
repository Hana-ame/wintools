//go:build integration

// 端到端集成测试：真信令（go-peerserver 挂 httptest）+ 真 WebRTC DataChannel，
// Go 节点（peerfs）↔ Go 客户端（pkg/peerjs，raw 序列化）走完整帧协议。
//
// 运行：go test -tags integration ./pkg/peerfs/ -count=1 -v
package peerfs

import (
	"bytes"
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/pion/webrtc/v4"

	"github.com/Hana-ame/wintools/pkg/peerjs"
)

// localICE 让双方只在 eth0 上收发并宣告真实 IP（本机互通）。
// 注意不能用 lo：pion/ice 天生跳过 loopback 接口，一个候选都不产；
// 也不能只 SetNAT1To1IPs：它改的是宣告地址，socket 仍绑在默认网卡 IP 上。
func localICE(c *webrtc.Configuration, s *webrtc.SettingEngine) {
	c.ICEServers = nil
	s.SetInterfaceFilter(func(name string) bool { return name == "eth0" })
	ifs, _ := net.Interfaces()
	for _, i := range ifs {
		if i.Name != "eth0" {
			continue
		}
		addrs, _ := i.Addrs()
		for _, a := range addrs {
			if ipn, ok := a.(*net.IPNet); ok && ipn.IP.To4() != nil {
				s.SetNAT1To1IPs([]string{ipn.IP.String()}, webrtc.ICECandidateTypeHost)
			}
		}
	}
}

type clientSession struct {
	t       *testing.T
	dc      *peerjs.DataConnection
	mu      sync.Mutex
	headers chan Header
	chunks  bytes.Buffer
	opened  chan struct{}
}

func newClientSession(t *testing.T, p *peerjs.Peer, peerID string) *clientSession {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	dc, err := p.Connect(ctx, peerID)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	cs := &clientSession{
		t:       t,
		dc:      dc,
		headers: make(chan Header, 16),
		opened:  make(chan struct{}, 1),
	}
	dc.OnOpen(func() { cs.opened <- struct{}{} })
	dc.OnMessageKind(func(m peerjs.Message) {
		if m.IsText {
			var h Header
			if err := json.Unmarshal(m.Data, &h); err != nil {
				t.Errorf("bad header: %v", err)
				return
			}
			cs.headers <- h
			return
		}
		cs.mu.Lock()
		cs.chunks.Write(m.Data)
		cs.mu.Unlock()
	})
	select {
	case <-cs.opened:
	case <-time.After(20 * time.Second):
		t.Fatal("datachannel open timeout")
	}
	return cs
}

func (cs *clientSession) send(h Header) {
	b, _ := json.Marshal(h)
	if err := cs.dc.SendText(string(b)); err != nil {
		cs.t.Fatalf("send: %v", err)
	}
}

// expectHeader 取下一个控制头。
func (cs *clientSession) expectHeader(what string) Header {
	select {
	case h := <-cs.headers:
		return h
	case <-time.After(10 * time.Second):
		cs.t.Fatalf("%s: no header within timeout", what)
		return Header{}
	}
}

// expectDone 等到本请求的终结帧（跳过中间帧如 meta）。
func (cs *clientSession) expectDone(what string) {
	for {
		h := cs.expectHeader(what)
		switch h.Type {
		case "done":
			return
		case "err":
			cs.t.Fatalf("%s: err frame: %s", what, h.Msg)
		}
	}
}

func TestIntegrationPeerFSEndToEnd(t *testing.T) {
	dir := fixtureDir(t)

	// 1. 内嵌信令挂 httptest（复用 MountSignaling —— 与 cmd 同一路径）
	mux := http.NewServeMux()
	MountSignaling(mux, "peerjs", nil)
	srv := httptest.NewServer(mux)
	defer srv.Close()
	parts := strings.Split(srv.Listener.Addr().String(), ":")
	host := parts[0]
	port, _ := strconv.Atoi(parts[1])
	if host == "" {
		host = "127.0.0.1"
	}

	// 2. 节点（answerer，带 token 校验）
	node := New(Config{
		PeerID:    "wt-itest-node",
		Root:      dir,
		Token:     "tok123",
		Signaling: Signaling{Host: host, Port: port, Key: "peerjs"},
		ICEHook:   localICE,
	})
	if err := node.Start(context.Background()); err != nil {
		t.Fatalf("node start: %v", err)
	}
	defer node.Close()

	// 3. 客户端（offerer）
	cli := peerjs.NewPeer(peerjs.Config{
		Host:    host,
		Port:    port,
		Key:     "peerjs",
		ICEHook: localICE,
	})
	if err := cli.Start(context.Background()); err != nil {
		t.Fatalf("client start: %v", err)
	}
	defer cli.Close()

	cs := newClientSession(t, cli, "wt-itest-node")

	// hello：错 token → err 帧 + 关连接；重连后对 token
	cs.send(Header{Type: "hello", Token: "nope"})
	h := cs.expectHeader("bad-token")
	if h.Type != "err" || h.Msg != "bad token" {
		t.Fatalf("want err 'bad token', got %+v", h)
	}
	cs = newClientSession(t, cli, "wt-itest-node")
	cs.send(Header{Type: "hello", Token: "tok123"})
	time.Sleep(300 * time.Millisecond) // 认证无回执

	// list
	cs.send(Header{Type: "list", Path: "/", ReqID: "L1"})
	h = cs.expectHeader("list-entries")
	if h.Type != "entries" || len(h.Entries) == 0 {
		t.Fatalf("want entries, got %+v", h)
	}

	// read 大文件（70KB 跨块）
	cs.send(Header{Type: "read", Path: "/img/a.png", Size: -1, ReqID: "R1"})
	h = cs.expectHeader("meta")
	if h.Type != "meta" || h.Total != 70000 {
		t.Fatalf("want meta total=70000, got %+v", h)
	}
	cs.expectDone("big read")
	cs.mu.Lock()
	got := cs.chunks.Len()
	cs.mu.Unlock()
	if got != 70000 {
		t.Fatalf("received %d bytes, want 70000", got)
	}

	// range 读（同一连接继续：lock-step 第二个请求）
	cs.send(Header{Type: "read", Path: "/hello.txt", Offset: 6, Size: 5, ReqID: "R2"})
	h = cs.expectHeader("range-meta")
	if h.Total != 5 {
		t.Fatalf("range meta.total=%d want 5", h.Total)
	}
	cs.expectDone("range read")
	cs.mu.Lock()
	rangeData := string(cs.chunks.Bytes()[70000:])
	cs.mu.Unlock()
	if rangeData != "peerf" {
		t.Fatalf("range data=%q want peerf", rangeData)
	}
}
