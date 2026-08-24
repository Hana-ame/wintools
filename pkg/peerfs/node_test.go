package peerfs

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/Hana-ame/wintools/pkg/peerjs"
)

// fakeConn 收集 frameWriter 输出，按序还原帧序列。
type fakeConn struct {
	mu     sync.Mutex
	frames []frame
	closed bool
}

type frame struct {
	text string
	bin  []byte
}

func (f *fakeConn) SendText(s string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.frames = append(f.frames, frame{text: s})
	return nil
}

func (f *fakeConn) SendThrottled(b []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.frames = append(f.frames, frame{bin: append([]byte(nil), b...)})
	return nil
}

func (f *fakeConn) Close() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closed = true
}

// headers 返回全部文本帧解析出的控制头。
func (f *fakeConn) headers() []Header {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []Header
	for _, fr := range f.frames {
		if fr.text != "" {
			var h Header
			json.Unmarshal([]byte(fr.text), &h)
			out = append(out, h)
		}
	}
	return out
}

func (f *fakeConn) binary() []byte {
	f.mu.Lock()
	defer f.mu.Unlock()
	var buf bytes.Buffer
	for _, fr := range f.frames {
		buf.Write(fr.bin)
	}
	return buf.Bytes()
}

func newTestNode(t *testing.T, root string) *Node {
	t.Helper()
	return New(Config{Root: root})
}

func fixtureDir(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	write := func(name, content string) {
		p := filepath.Join(dir, name)
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	write("hello.txt", "hello peerfs")                   // 12B
	write("img/a.png", strings.Repeat("PNGDATA", 10000)) // 70KB，跨多个 64KB 块边界
	if err := os.MkdirAll(filepath.Join(dir, "emptydir"), 0o755); err != nil {
		t.Fatal(err)
	}
	return dir
}

// feed 把控制头喂给 onMessage 并等待同步段（认证/err/派发）完成。
// list/read 实际处理在 goroutine 里，调用方需要等结果时直接调 handle* 同步方法。
func feed(t *testing.T, st *connState, fc *fakeConn, h Header) {
	t.Helper()
	b, _ := json.Marshal(h)
	st.onMessage(fc, peerjs.Message{IsText: true, Data: b})
}

func TestHelloGate(t *testing.T) {
	n := newTestNode(t, fixtureDir(t))
	n.cfg.Token = "secret"
	st := &connState{n: n}
	fc := &fakeConn{}

	feed(t, st, fc, Header{Type: "list", Path: "/", ReqID: "r1"})
	// 未认证非 hello 帧：静默关连接（不回帧，不泄露信息）
	if !fc.closed {
		t.Fatal("pre-auth request must close connection")
	}
	if hs := fc.headers(); len(hs) != 0 {
		t.Fatalf("pre-auth request must be silent, got %+v", hs)
	}

	feed(t, st, fc, Header{Type: "hello", Token: "wrong"})
	hs := fc.headers()
	if len(hs) != 1 || hs[0].Msg != "bad token" {
		t.Fatalf("wrong token must get err 'bad token', got %+v", hs)
	}
	if !fc.closed {
		t.Fatal("wrong token should close connection")
	}

	// 正确 token：无 token 配置的节点 hello 即通过；带 token 配置需匹配
	st2 := &connState{n: n}
	feed(t, st2, fc, Header{Type: "hello", Token: "secret"})
	if !st2.authed {
		t.Fatal("correct token should authenticate")
	}
}

func TestListAndEntries(t *testing.T) {
	n := newTestNode(t, fixtureDir(t))
	st := &connState{n: n, authed: true}
	fc := &fakeConn{}

	st.handleList(fc, Header{Type: "list", Path: "/", ReqID: "r"})
	hs := fc.headers()
	if len(hs) != 1 || hs[0].Type != "entries" || hs[0].ReqID != "r" {
		t.Fatalf("want single entries frame, got %+v", hs)
	}
	found := map[string]Entry{}
	for _, e := range hs[0].Entries {
		found[e.Name] = e
	}
	for _, want := range []string{"hello.txt", "img", "emptydir"} {
		if _, ok := found[want]; !ok {
			t.Fatalf("entries missing %q: %+v", want, hs[0].Entries)
		}
	}
	if found["hello.txt"].Size != 12 {
		t.Fatalf("hello.txt size=%d want 12", found["hello.txt"].Size)
	}
	if !found["img"].Dir || !found["emptydir"].Dir {
		t.Fatalf("dirs not flagged: %+v", found)
	}

	// 子目录 + 路径清洗（前导斜杠可有可无）
	fc2 := &fakeConn{}
	st.handleList(fc2, Header{Type: "list", Path: "img/", ReqID: "s"})
	if hs := fc2.headers(); len(hs) != 1 || len(hs[0].Entries) != 1 || hs[0].Entries[0].Name != "a.png" {
		t.Fatalf("subdir list broken: %+v", hs)
	}
}

func TestReadRangeAndChunks(t *testing.T) {
	n := newTestNode(t, fixtureDir(t))
	st := &connState{n: n, authed: true}

	// 大文件跨块：a.png 70000 字节 → meta.total=70000，二进制总量一致
	fc := &fakeConn{}
	st.handleRead(fc, Header{Type: "read", Path: "/img/a.png", Size: -1, ReqID: "big"})
	hs := fc.headers()
	if len(hs) != 2 || hs[0].Type != "meta" || hs[1].Type != "done" {
		t.Fatalf("want meta+done frames, got %+v", hs)
	}
	const want = 70000
	if hs[0].Total != want {
		t.Fatalf("meta.total=%d want %d", hs[0].Total, want)
	}
	got := fc.binary()
	if int64(len(got)) != want {
		t.Fatalf("binary size=%d want %d", len(got), want)
	}
	// 内容一致性：首尾都是 PNGDATA 开头
	if string(got[:7]) != "PNGDATA" || string(got[len(got)-7:]) != "PNGDATA" {
		t.Fatal("chunked content corrupted at boundary")
	}

	// offset+size 范围读："hello peerfs"[6:11] = "peerf"
	fc2 := &fakeConn{}
	st.handleRead(fc2, Header{Type: "read", Path: "/hello.txt", Offset: 6, Size: 5, ReqID: "rng"})
	if got := string(fc2.binary()); got != "peerf" {
		t.Fatalf("range read=%q want peerf", got)
	}
	if hs := fc2.headers(); hs[0].Total != 5 || hs[1].Type != "done" {
		t.Fatalf("range meta/done wrong: %+v", hs)
	}

	// offset 越界 → err
	fc3 := &fakeConn{}
	st.handleRead(fc3, Header{Type: "read", Path: "/hello.txt", Offset: 9999, ReqID: "oob"})
	if hs := fc3.headers(); len(hs) != 1 || hs[0].Type != "err" {
		t.Fatalf("offset beyond EOF must err, got %+v", hs)
	}

	// 目录不可读
	fc4 := &fakeConn{}
	st.handleRead(fc4, Header{Type: "read", Path: "/img", ReqID: "dir"})
	if hs := fc4.headers(); len(hs) != 1 || hs[0].Msg != "is a directory" {
		t.Fatalf("directory read must err 'is a directory', got %+v", hs)
	}

	// 空文件语义：size=-1 → total=0，直接 done 无二进制
	fc5 := &fakeConn{}
	st.handleRead(fc5, Header{Type: "read", Path: "/emptydir/../hello.txt", Offset: 12, ReqID: "zero"})
	if hs := fc5.headers(); len(hs) != 2 || hs[0].Total != 0 || hs[1].Type != "done" || len(fc5.binary()) != 0 {
		t.Fatalf("EOF read should be total=0 + done, got %+v bin=%d", hs, len(fc5.binary()))
	}
}

func TestTraversalRejected(t *testing.T) {
	n := newTestNode(t, t.TempDir()) // 根外另有系统目录
	st := &connState{n: n, authed: true}
	fc := &fakeConn{}
	st.handleList(fc, Header{Type: "list", Path: "/../../etc", ReqID: "x"})
	hs := fc.headers()
	if len(hs) != 1 {
		t.Fatalf("want single reply frame, got %+v", hs)
	}
	if hs[0].Type == "entries" && len(hs[0].Entries) > 0 {
		t.Fatalf("traversal listed outside root: %+v", hs[0].Entries)
	}
}

func TestConsoleHandlers(t *testing.T) {
	n := New(Config{
		PeerID:    "wt-media-test",
		Root:      fixtureDir(t),
		Signaling: Signaling{Host: "sig.example.com", Port: 9000, Secure: true, Key: "k1"},
	})
	mux := http.NewServeMux()
	n.MountConsole(mux)

	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, httptest.NewRequest("GET", "/__peerfs/", nil))
	body := rr.Body.String()
	if !strings.Contains(body, "window.__PEERFS__ = ") || strings.Contains(body, "__PEERFS_CONFIG__") {
		t.Fatal("console page must have config injected")
	}
	if !strings.Contains(body, `"peerId":"wt-media-test"`) || !strings.Contains(body, `"host":"sig.example.com"`) {
		t.Fatal("config JSON not injected correctly")
	}

	rr = httptest.NewRecorder()
	mux.ServeHTTP(rr, httptest.NewRequest("GET", "/__peerfs/config.json", nil))
	if !strings.Contains(rr.Body.String(), `"port":9000`) {
		t.Fatalf("config.json missing signaling port: %s", rr.Body.String())
	}

	rr = httptest.NewRecorder()
	mux.ServeHTTP(rr, httptest.NewRequest("GET", "/__peerfs/bridge.js", nil))
	if !strings.Contains(rr.Body.String(), "PeerFS.prototype.connect") {
		t.Fatal("bridge.js not served")
	}

	rr = httptest.NewRecorder()
	mux.ServeHTTP(rr, httptest.NewRequest("GET", "/__peerfs/nope.js", nil))
	if rr.Code != http.StatusNotFound {
		t.Fatalf("unknown asset must 404, got %d", rr.Code)
	}
}
