// peerfs-proxy: ECH proxy node for peerfs-chat.
// Fetches media from twimg (or any URL) via ECH domain fronting,
// and serves it over WebRTC DataChannel to the browser.
//
// Usage:
//
//	go run ./cmd/peerfs-proxy -id twimg-proxy -shost 127.0.0.1 -sport 8000
//	go run ./cmd/peerfs-proxy -id twimg-proxy -shost 127.0.0.1 -sport 8000 -token SECRET
package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"strings"
	"sync/atomic"
	"time"

	cloudflare_ech "github.com/Hana-ame/wintools/pkg/ech"
	"github.com/Hana-ame/wintools/pkg/netdial"
	"github.com/Hana-ame/wintools/pkg/peerjs"
)

const (
	chunkSize    = 64 << 10
	helloTimeout = 10 * time.Second
)

type Header struct {
	Type     string  `json:"type"`
	Path     string  `json:"path,omitempty"`
	Offset   int64   `json:"offset,omitempty"`
	Size     int64   `json:"size,omitempty"`
	Total    int64   `json:"total,omitempty"`
	ReqID    string  `json:"reqId,omitempty"`
	StreamID uint32  `json:"streamId,omitempty"`
	Msg      string  `json:"msg,omitempty"`
	Token    string  `json:"token,omitempty"`
	Entries  []Entry `json:"entries,omitempty"`
}

type Entry struct {
	Name string `json:"name"`
	Dir  bool   `json:"dir,omitempty"`
	Size int64  `json:"size,omitempty"`
}

type proxyNode struct {
	peer          *peerjs.Peer
	collections   []string
	discoverURL   string
	token         string
	startTime     time.Time
	uploadBytes   atomic.Int64
	downloadBytes atomic.Int64
}

type connState struct {
	node      *proxyNode
	dc        *peerjs.DataConnection
	streamSeq atomic.Uint32
	authed    atomic.Bool
}

func main() {
	id := flag.String("id", "twimg-proxy", "our peer id")
	collections := flag.String("collections", "proxy,twitter,images", "comma separated collections")
	sigHost := flag.String("shost", "127.0.0.1", "signaling host")
	sigPort := flag.Int("sport", 8000, "signaling port")
	token := flag.String("token", "", "data channel auth token; empty = no auth")
	debug := flag.Bool("debug", false, "debug logging")
	flag.Parse()

	colls := strings.Split(*collections, ",")
	clean := make([]string, 0, len(colls))
	for _, c := range colls {
		if c = strings.TrimSpace(c); c != "" {
			clean = append(clean, c)
		}
	}

	ctx := context.Background()

	if err := cloudflare_ech.InitDefault(); err != nil {
		log.Printf("[proxy] ECH init: %v (will retry on each request)", err)
	} else {
		log.Printf("[proxy] ECH client initialized")
	}

	p := &proxyNode{
		collections: clean,
		token:       *token,
		startTime:   time.Now(),
	}

	peerCfg := peerjs.Config{
		ID:    *id,
		Host:  *sigHost,
		Port:  *sigPort,
		Debug: *debug,
	}
	p.peer = peerjs.NewPeer(peerCfg)
	p.peer.OnConnection(p.onConn)
	if err := p.peer.Start(ctx); err != nil {
		log.Fatalf("[proxy] peer start: %v", err)
	}
	log.Printf("[proxy] registered as %q, collections: %v", p.peer.ID(), clean)

	p.discoverURL = fmt.Sprintf("http://%s:%d", *sigHost, *sigPort)
	p.announce()

	go func() {
		t := time.NewTicker(60 * time.Second)
		defer t.Stop()
		for range t.C {
			p.announce()
		}
	}()

	select {}
}

func (p *proxyNode) announce() {
	body, _ := json.Marshal(map[string]any{
		"peerId":        p.peer.ID(),
		"uptime":        int64(time.Since(p.startTime).Seconds()),
		"uploadBytes":   p.uploadBytes.Load(),
		"downloadBytes": p.downloadBytes.Load(),
		"nodeType":      "twimg",
		"collections":   p.collections,
	})
	client := netdial.Client(15 * time.Second)
	resp, err := client.Post(p.discoverURL+"/discover/announce",
		"application/json", bytes.NewReader(body))
	if err != nil {
		log.Printf("[proxy] announce failed: %v", err)
		return
	}
	resp.Body.Close()
}

func (p *proxyNode) onConn(dc *peerjs.DataConnection) {
	st := &connState{node: p, dc: dc}
	// no token configured -> open access, skip hello gate entirely
	st.authed.Store(p.token == "")

	var helloTimer *time.Timer
	if p.token != "" {
		helloTimer = time.AfterFunc(helloTimeout, func() {
			if !st.authed.Load() {
				log.Printf("[proxy] hello timeout for %s", dc.Remote())
				dc.Close()
			}
		})
	}

	dc.OnOpen(func() {
		log.Printf("[proxy] data channel open from %s", dc.Remote())
	})
	dc.OnMessageKind(func(m peerjs.Message) {
		if !m.IsText {
			return
		}
		st.node.downloadBytes.Add(int64(len(m.Data)))
		if err := st.handleControl(m.Data); err != nil {
			log.Printf("[proxy] control from %s: %v", dc.Remote(), err)
		}
	})
	dc.OnClose(func() {
		log.Printf("[proxy] data channel closed")
		if helloTimer != nil {
			helloTimer.Stop()
		}
	})
}

func (st *connState) handleControl(data []byte) error {
	var h Header
	if err := json.Unmarshal(data, &h); err != nil {
		return err
	}

	// hello frame
	if h.Type == "hello" {
		if st.authed.Load() {
			return nil // already open, ignore
		}
		if st.node.token != "" && h.Token != st.node.token {
			_ = st.sendHeader(Header{Type: "err", Msg: "bad token"})
			time.AfterFunc(100*time.Millisecond, st.dc.Close)
			return nil
		}
		st.authed.Store(true)
		return nil
	}

	// non-hello: must be authed
	if !st.authed.Load() {
		st.dc.Close()
		return nil
	}

	switch h.Type {
	case "list":
		return st.handleList(h)
	case "read":
		go st.handleRead(h)
		return nil
	default:
		return st.sendHeader(Header{Type: "err", ReqID: h.ReqID,
			Msg: "unknown type: " + h.Type})
	}
}

func (st *connState) sendHeader(h Header) error {
	b, _ := json.Marshal(h)
	st.node.uploadBytes.Add(int64(len(b)))
	return st.dc.SendText(string(b))
}

func (st *connState) sendBinaryChunk(streamID uint32, data []byte) error {
	frame := make([]byte, 4+len(data))
	binary.BigEndian.PutUint32(frame[:4], streamID)
	copy(frame[4:], data)
	st.node.uploadBytes.Add(int64(len(data)))
	return st.dc.SendThrottled(frame)
}

func (st *connState) handleList(h Header) error {
	path := strings.TrimPrefix(h.Path, "/")
	switch {
	case path == "", path == "twimg":
		entries := []Entry{
			{Name: "twimg/", Dir: true, Size: 0},
		}
		return st.sendHeader(Header{Type: "entries", ReqID: h.ReqID, Entries: entries})
	case strings.HasPrefix(path, "twimg/"):
		return st.sendHeader(Header{Type: "entries", ReqID: h.ReqID, Entries: nil})
	default:
		return st.sendHeader(Header{Type: "err", ReqID: h.ReqID, Msg: "unknown path"})
	}
}

func (st *connState) handleRead(h Header) {
	path := strings.TrimPrefix(h.Path, "/")

	if !strings.HasPrefix(path, "twimg/") {
		_ = st.sendHeader(Header{Type: "err", ReqID: h.ReqID, Msg: "only twimg/ is allowed"})
		return
	}

	fetchURL := "https://video-cf.twimg.com/" + strings.TrimPrefix(path, "twimg/")

	target, err := url.Parse(fetchURL)
	if err != nil {
		_ = st.sendHeader(Header{Type: "err", ReqID: h.ReqID, Msg: "bad url: " + err.Error()})
		return
	}

	req, err := http.NewRequest("GET", fetchURL, nil)
	if err != nil {
		_ = st.sendHeader(Header{Type: "err", ReqID: h.ReqID, Msg: err.Error()})
		return
	}
	req.Host = target.Host
	req.Header.Set("User-Agent", "Mozilla/5.0")
	req.Header.Set("Referer", "https://twitter.com/")

	if h.Offset > 0 || h.Size > 0 {
		rangeHeader := fmt.Sprintf("bytes=%d", h.Offset)
		if h.Size > 0 {
			rangeHeader += fmt.Sprintf("-%d", h.Offset+h.Size-1)
		}
		req.Header.Set("Range", rangeHeader)
	}

	resp, err := cloudflare_ech.Do(req)
	if err != nil {
		_ = st.sendHeader(Header{Type: "err", ReqID: h.ReqID, Msg: "fetch: " + err.Error()})
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK &&
		resp.StatusCode != http.StatusPartialContent {
		_ = st.sendHeader(Header{Type: "err", ReqID: h.ReqID,
			Msg: fmt.Sprintf("HTTP %d", resp.StatusCode)})
		return
	}

	streamID := st.streamSeq.Add(1)
	total := resp.ContentLength

	if err := st.sendHeader(Header{Type: "meta", ReqID: h.ReqID,
		Total: total, StreamID: streamID}); err != nil {
		return
	}

	buf := make([]byte, chunkSize)
	for {
		nr, err := resp.Body.Read(buf)
		if nr > 0 {
			if serr := st.sendBinaryChunk(streamID, buf[:nr]); serr != nil {
				return
			}
		}
		if err != nil {
			if err == io.EOF {
				break
			}
			_ = st.sendHeader(Header{Type: "err", ReqID: h.ReqID,
				Msg: "read: " + err.Error()})
			return
		}
	}

	_ = st.sendHeader(Header{Type: "done", ReqID: h.ReqID})
}
