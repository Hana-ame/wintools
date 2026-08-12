// webrtc-proxy — HTTP proxy over a PeerJS-signaled WebRTC DataChannel.
//
// Two modes, one binary:
//
//	serve   — registers a fixed peer id (wt-<name>), receives DataConnections
//	          and forwards HTTP requests it gets over the channel to a local
//	          HTTP target.
//	client  — gets a random peer id, connects to wt-<name>, serves a local
//	          HTTP port and tunnels every request over the DataChannel.
//
// DataChannel frame format (big-endian):
//
//	[4B len][1B type][4B streamID][payload]
//	type 0x01 REQ_HEAD  payload=JSON {method,url,headers,body}
//	type 0x02 REQ_BODY  payload=bytes
//	type 0x03 REQ_END   payload=empty
//	type 0x04 RESP_HEAD payload=JSON {status,headers}
//	type 0x05 RESP_BODY payload=bytes
//	type 0x06 RESP_END  payload=empty
//	type 0x07 PING      payload=empty
//	type 0x08 PONG      payload=empty
package main

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Hana-ame/wintools/pkg/peerjs"
)

const (
	frameREQHEAD  = 0x01
	frameREQBODY  = 0x02
	frameREQEND   = 0x03
	frameRESPHEAD = 0x04
	frameRESPBODY = 0x05
	frameRESPEND  = 0x06
	framePING     = 0x07
	framePONG     = 0x08

	maxFrame = 1 << 20
	chunk    = 16 << 10 // chunkedMTU-equivalent, safe for browser peers
)

func marshalFrame(typ byte, streamID uint32, payload []byte) []byte {
	buf := make([]byte, 9+len(payload))
	binary.BigEndian.PutUint32(buf[:4], uint32(len(payload)))
	buf[4] = typ
	binary.BigEndian.PutUint32(buf[5:9], streamID)
	copy(buf[9:], payload)
	return buf
}

type frame struct {
	typ      byte
	streamID uint32
	payload  []byte
}

func parseFrame(b []byte) (*frame, error) {
	if len(b) < 9 {
		return nil, fmt.Errorf("short frame: %d bytes", len(b))
	}
	l := binary.BigEndian.Uint32(b[:4])
	if int(l) != len(b)-9 {
		return nil, fmt.Errorf("frame len mismatch: head %d got %d", l, len(b)-9)
	}
	return &frame{typ: b[4], streamID: binary.BigEndian.Uint32(b[5:9]), payload: b[9:]}, nil
}

type reqHead struct {
	Method  string            `json:"method"`
	URL     string            `json:"url"`
	Headers map[string]string `json:"headers"`
	Body    bool              `json:"body"`
}

type respHead struct {
	Status  int               `json:"status"`
	Headers map[string]string `json:"headers"`
}

// ---- serve side ------------------------------------------------------------

type serve struct {
	target  string
	client  *http.Client
	mu      sync.Mutex
	streams map[uint32]*pipeStream
}

type pipeStream struct {
	pw *io.PipeWriter
}

func newServe(target string) *serve {
	return &serve{
		target:  target,
		client:  &http.Client{},
		streams: make(map[uint32]*pipeStream),
	}
}

func (s *serve) onMessage(dc *peerjs.DataConnection, f *frame) {
	switch f.typ {
	case framePING:
		dc.Send(marshalFrame(framePONG, f.streamID, nil))
	case frameREQHEAD:
		var h reqHead
		if err := json.Unmarshal(f.payload, &h); err != nil {
			log.Printf("bad req head: %v", err)
			return
		}
		pr, pw := io.Pipe()
		s.mu.Lock()
		s.streams[f.streamID] = &pipeStream{pw: pw}
		s.mu.Unlock()
		go s.forward(dc, f.streamID, h, pr, pw)
	case frameREQBODY:
		s.mu.Lock()
		st := s.streams[f.streamID]
		s.mu.Unlock()
		if st != nil && len(f.payload) > 0 {
			st.pw.Write(f.payload)
		}
	case frameREQEND:
		s.mu.Lock()
		st := s.streams[f.streamID]
		s.mu.Unlock()
		if st != nil {
			st.pw.Close()
		}
	}
}

func (s *serve) forward(dc *peerjs.DataConnection, id uint32, h reqHead, pr *io.PipeReader, pw *io.PipeWriter) {
	defer pr.Close()

	var body io.Reader
	if h.Body {
		body = pr
	}
	req, err := http.NewRequest(h.Method, s.target+h.URL, body)
	if err != nil {
		log.Printf("build request: %v", err)
		s.respond(dc, id, http.StatusBadGateway, map[string]string{"Content-Type": "text/plain"}, []byte(err.Error()))
		s.clear(id)
		return
	}
	for k, v := range h.Headers {
		req.Header.Set(k, v)
	}
	if h.Body {
		req.ContentLength = -1 // streamed, keep chunked
	}

	resp, err := s.client.Do(req)
	if err != nil {
		log.Printf("forward %s %s: %v", h.Method, h.URL, err)
		s.respond(dc, id, http.StatusBadGateway, map[string]string{"Content-Type": "text/plain"}, []byte(err.Error()))
		s.clear(id)
		return
	}
	defer resp.Body.Close()

	hdrs := map[string]string{}
	for k, vs := range resp.Header {
		hdrs[k] = strings.Join(vs, ",")
	}
	b, _ := json.Marshal(respHead{Status: resp.StatusCode, Headers: hdrs})
	dc.Send(marshalFrame(frameRESPHEAD, id, b))

	buf := make([]byte, chunk)
	for {
		n, err := resp.Body.Read(buf)
		if n > 0 {
			dc.Send(marshalFrame(frameRESPBODY, id, buf[:n]))
		}
		if err != nil {
			break
		}
	}
	dc.Send(marshalFrame(frameRESPEND, id, nil))
	s.clear(id)
	log.Printf("served %s %s -> %d", h.Method, h.URL, resp.StatusCode)
}

func (s *serve) respond(dc *peerjs.DataConnection, id uint32, status int, hdrs map[string]string, body []byte) {
	b, _ := json.Marshal(respHead{Status: status, Headers: hdrs})
	dc.Send(marshalFrame(frameRESPHEAD, id, b))
	dc.Send(marshalFrame(frameRESPBODY, id, body))
	if _, ok := hdrs["Content-Type"]; !ok {
		hdrs["Content-Type"] = "text/plain"
	}
	dc.Send(marshalFrame(frameRESPEND, id, nil))
}

func (s *serve) clear(id uint32) {
	s.mu.Lock()
	delete(s.streams, id)
	s.mu.Unlock()
}

// ---- client side -----------------------------------------------------------

type stream struct {
	headCh chan *respHead
	bodyCh chan []byte
	done   chan struct{}
	once   sync.Once
}

func (st *stream) finish() { st.once.Do(func() { close(st.done) }) }

type client struct {
	mu      sync.Mutex
	streams map[uint32]*stream
	nextID  uint32
	conn    *peerjs.DataConnection
}

func newClient() *client {
	return &client{streams: make(map[uint32]*stream)}
}

func (c *client) setConn(dc *peerjs.DataConnection) {
	c.mu.Lock()
	c.conn = dc
	c.mu.Unlock()
}

func (c *client) curConn() *peerjs.DataConnection {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.conn
}

func (c *client) handler(w http.ResponseWriter, r *http.Request) {
	dc := c.curConn()
	if dc == nil {
		http.Error(w, "no peer connection", http.StatusBadGateway)
		return
	}
	id := atomic.AddUint32(&c.nextID, 1)
	st := &stream{
		headCh: make(chan *respHead, 1),
		bodyCh: make(chan []byte, 64),
		done:   make(chan struct{}),
	}
	c.mu.Lock()
	c.streams[id] = st
	c.mu.Unlock()
	defer func() {
		c.mu.Lock()
		delete(c.streams, id)
		c.mu.Unlock()
		st.finish()
	}()

	hdrs := map[string]string{}
	for k, vs := range r.Header {
		hdrs[k] = strings.Join(vs, ",")
	}
	hasBody := r.Body != nil && r.ContentLength != 0
	b, _ := json.Marshal(reqHead{
		Method:  r.Method,
		URL:     r.URL.RequestURI(),
		Headers: hdrs,
		Body:    hasBody,
	})
	if err := dc.Send(marshalFrame(frameREQHEAD, id, b)); err != nil {
		http.Error(w, "peer send failed", http.StatusBadGateway)
		return
	}
	if hasBody {
		buf := make([]byte, chunk)
		for {
			n, err := r.Body.Read(buf)
			if n > 0 {
				if err := dc.Send(marshalFrame(frameREQBODY, id, buf[:n])); err != nil {
					return
				}
			}
			if err != nil {
				break
			}
		}
	}
	dc.Send(marshalFrame(frameREQEND, id, nil))

	// wait for response head
	select {
	case head := <-st.headCh:
		for k, v := range head.Headers {
			w.Header().Set(k, v)
		}
		w.WriteHeader(head.Status)
	case <-st.done:
		return
	case <-time.After(2 * time.Minute):
		return
	}

	flusher, _ := w.(http.Flusher)
	for {
		// drain any pending body before checking terminal state
		select {
		case body := <-st.bodyCh:
			w.Write(body)
			if flusher != nil {
				flusher.Flush()
			}
			continue
		default:
		}
		select {
		case body := <-st.bodyCh:
			w.Write(body)
			if flusher != nil {
				flusher.Flush()
			}
		case <-st.done:
			// final drain, then exit
			select {
			case body := <-st.bodyCh:
				w.Write(body)
			default:
			}
			return
		case <-time.After(5 * time.Minute):
			return
		}
	}
}

func (c *client) onMessage(dc *peerjs.DataConnection, f *frame) {
	c.mu.Lock()
	st := c.streams[f.streamID]
	c.mu.Unlock()
	if st == nil {
		return
	}
	switch f.typ {
	case frameRESPHEAD:
		var h respHead
		if err := json.Unmarshal(f.payload, &h); err != nil {
			log.Printf("bad resp head: %v", err)
			return
		}
		select {
		case st.headCh <- &h:
		default:
		}
	case frameRESPBODY:
		if len(f.payload) > 0 {
			select {
			case st.bodyCh <- f.payload:
			default:
			}
		}
	case frameRESPEND:
		st.finish()
	case framePING:
		dc.Send(marshalFrame(framePONG, f.streamID, nil))
	}
}

// ---- main ------------------------------------------------------------------

func main() {
	var (
		mode   = flag.String("mode", "", "serve | client")
		name   = flag.String("name", "", "shared peer name (both ends)")
		listen = flag.String("listen", "127.0.0.1:8080", "client listen address")
		target = flag.String("target", "", "serve upstream target, e.g. http://127.0.0.1:5173")
		debug  = flag.Bool("debug", false, "verbose signaling logs")
	)
	flag.Parse()

	if *mode == "" || *name == "" {
		fmt.Fprintln(os.Stderr, "usage: webrtc-proxy -mode serve|client -name <name> [-listen addr] [-target url]")
		flag.PrintDefaults()
		os.Exit(2)
	}
	if !validName(*name) {
		fmt.Fprintf(os.Stderr, "bad name %q: only [A-Za-z0-9] and [ _-] separators\n", *name)
		os.Exit(2)
	}

	peerID := "wt-" + *name
	ctx := context.Background()

	var peer *peerjs.Peer
	if *mode == "serve" {
		if *target == "" {
			fmt.Fprintln(os.Stderr, "-target required in serve mode")
			os.Exit(2)
		}
		peer = peerjs.NewPeer(peerjs.Config{ID: peerID, Debug: *debug})
		if err := peer.Start(ctx); err != nil {
			log.Fatalf("peer start: %v", err)
		}
		log.Printf("serve peer open: %s", peer.ID())
		s := newServe(*target)
		peer.OnConnection(func(dc *peerjs.DataConnection) {
			log.Printf("serve: connection from %s conn=%s", dc.Remote(), "")
			dc.OnClose(func() { log.Printf("serve: peer %s disconnected", dc.Remote()) })
			dc.OnMessage(func(data []byte) {
				f, err := parseFrame(data)
				if err != nil {
					log.Printf("bad frame: %v", err)
					return
				}
				s.onMessage(dc, f)
			})
		})
	} else {
		// random id, dialing side
		peer = peerjs.NewPeer(peerjs.Config{Debug: *debug})
		if err := peer.Start(ctx); err != nil {
			log.Fatalf("peer start: %v", err)
		}
		log.Printf("client peer open: %s", peer.ID())

		c := newClient()
		go func() {
			for {
				connectCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
				dc, err := peer.Connect(connectCtx, peerID)
				cancel()
				if err != nil {
					log.Printf("connect retry: %v", err)
					time.Sleep(5 * time.Second)
					continue
				}
				opened := make(chan struct{})
				dc.OnOpen(func() {
					c.setConn(dc)
					log.Printf("client: data channel open to %s", dc.Remote())
					close(opened)
				})
				dc.OnClose(func() {
					log.Printf("client: data channel closed, reconnecting")
					c.setConn(nil)
				})
				dc.OnMessage(func(data []byte) {
					f, err := parseFrame(data)
					if err != nil {
						log.Printf("bad frame: %v", err)
						return
					}
					c.onMessage(dc, f)
				})
				select {
				case <-opened:
					return
				case <-time.After(30 * time.Second):
					dc.Close()
				}
			}
		}()
		srv := &http.Server{Addr: *listen, Handler: http.HandlerFunc(c.handler)}
		log.Printf("client: listening on http://%s", *listen)
		if err := srv.ListenAndServe(); err != nil {
			log.Fatalf("listen: %v", err)
		}
	}

	// keep running
	select {}
}

func validName(name string) bool {
	if name == "" {
		return false
	}
	for i := 0; i < len(name); i++ {
		ch := name[i]
		ok := ch >= 'a' && ch <= 'z' || ch >= 'A' && ch <= 'Z' || ch >= '0' && ch <= '9'
		if !ok && i > 0 && i < len(name)-1 && (ch == ' ' || ch == '_' || ch == '-') {
			ok = true
		}
		if !ok {
			return false
		}
	}
	return true
}
