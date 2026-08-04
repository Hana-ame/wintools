// Zen Proxy in Go — forwards /chat/completions to opencode.ai's free zen endpoint.
// Replaces zen_proxy.py: same failover/cooldown/stall semantics, but Go's
// stream handling removes the Python headaches (blocking close, generator races,
// uncancellable daemon threads).
package main

import (
	"bytes"
	"context"
	"encoding/json"

	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"runtime"
	"strings"
	"sync"
	"time"
)

const (
	zenHost          = "opencode.ai"
	zenPath          = "/zen/v1"
	zenAPIKey        = "public"
	freeLimitErr     = "FreeUsageLimitError"
	connectTimeout   = 10 * time.Second
	stallTimeout     = 30 * time.Second
	toolStallTimeout = 180 * time.Second
	cleanupInterval  = 5 * time.Minute
	maxReqsPerClient = 200
	banWindow        = 600 * time.Second
	idleConnTimeout  = 90 * time.Second
	maxIdleConns     = 64
)

// chunkMsg is one unit produced by the upstream reader goroutine.
type chunkMsg struct {
	data []byte
	eof  bool
	err  error
}

// server holds the shared state for one zen_proxy instance.
type server struct {
	zenURL   string
	serverID string

	clientV4 *http.Client
	clientV6 *http.Client

	mu         sync.Mutex
	cooldownV4 time.Time
	cooldownV6 time.Time

	ban *banList
}

func (s *server) cooldownFam(fam string) time.Time {
	s.mu.Lock()
	defer s.mu.Unlock()
	if fam == "v6" {
		return s.cooldownV6
	}
	return s.cooldownV4
}

func (s *server) setCooldown(fam string, until time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if fam == "v6" {
		s.cooldownV6 = until
	} else {
		s.cooldownV4 = until
	}
}

func (s *server) inCooldown(fam string, now time.Time) bool {
	return s.cooldownFam(fam).After(now)
}

// ---- SSE helpers -----------------------------------------------------------

func hasRealSSE(buf []byte) bool {
	return bytes.Contains(buf, []byte("data:")) || bytes.Contains(buf, []byte("event:"))
}

// nextSSEEvent pops one complete SSE event (trailing \n\n removed) from buf.
// Returns (ev, rest, ok). ok=false when no complete event is buffered.
func nextSSEEvent(buf []byte) ([]byte, []byte, bool) {
	if i := bytes.Index(buf, []byte("\r\n\r\n")); i != -1 {
		if j := bytes.Index(buf, []byte("\n\n")); j == -1 || i < j {
			return buf[:i], buf[i+4:], true
		}
	}
	if i := bytes.Index(buf, []byte("\n\n")); i != -1 {
		return buf[:i], buf[i+2:], true
	}
	return nil, buf, false
}

// eventHasToolCall is true when the SSE event carries a real tool_call delta.
func eventHasToolCall(ev []byte) bool {
	for _, line := range bytes.Split(ev, []byte("\n")) {
		if !bytes.HasPrefix(line, []byte("data: ")) {
			continue
		}
		var obj struct {
			Choices []struct {
				Delta struct {
					ToolCalls []json.RawMessage `json:"tool_calls"`
				} `json:"delta"`
			} `json:"choices"`
		}
		if err := json.Unmarshal(line[len("data: "):], &obj); err != nil {
			continue
		}
		for _, ch := range obj.Choices {
			if len(ch.Delta.ToolCalls) > 0 {
				return true
			}
		}
	}
	return false
}

func stallFor(sawTool bool) time.Duration {
	if sawTool {
		return toolStallTimeout
	}
	return stallTimeout
}

// ---- upstream reader goroutine --------------------------------------------

// startReader drains resp.Body in a single goroutine, sending chunks on ch.
// It is closed (EOF) or an error sentinel is emitted; the goroutine always
// terminates once resp.Body is closed (which unblocks Read).
func startReader(body io.Reader) chan chunkMsg {
	ch := make(chan chunkMsg, 32)
	go func() {
		buf := make([]byte, 16384)
		for {
			n, err := body.Read(buf)
			if n > 0 {
				cp := make([]byte, n)
				copy(cp, buf[:n])
				ch <- chunkMsg{data: cp}
			}
			if err != nil {
				if err == io.EOF {
					ch <- chunkMsg{eof: true}
				} else {
					ch <- chunkMsg{err: err}
				}
				close(ch)
				return
			}
		}
	}()
	return ch
}

// forwardStream relays an SSE stream from upstream to the client with
// keep-alive filtering, stall detection and tool-call protection.
// Returns true on success.
func (s *server) forwardStream(w http.ResponseWriter, resp *http.Response, fam string) bool {
	ch := startReader(resp.Body)
	t0 := time.Now()

	// Pre-read: wait up to stallTimeout for real SSE data; else give up.
	deadline := time.Now().Add(stallTimeout)
	pre := []byte{}
	hasReal := false
	for !hasReal && time.Now().Before(deadline) {
		remain := time.Until(deadline)
		timer := time.NewTimer(remain)
		select {
		case m, ok := <-ch:
			timer.Stop()
			if !ok || m.eof {
				resp.Body.Close()
				return false
			}
			if m.err != nil {
				log.Printf("%s pre-read error: %v", fam, m.err)
				resp.Body.Close()
				return false
			}
			pre = append(pre, m.data...)
			hasReal = hasRealSSE(pre)
		case <-timer.C:
		}
	}
	if !hasReal {
		log.Printf("%s no real data in %.0fs, try other stack", fam, stallTimeout.Seconds())
		resp.Body.Close()
		return false
	}

	log.Printf("%s first real data +%.2fs", fam, time.Since(t0).Seconds())

	flusher, _ := w.(http.Flusher)
	h := w.Header()
	h.Set("Content-Type", "text/event-stream")
	h.Set("Cache-Control", "no-cache")
	h.Set("Connection", "keep-alive")
	h.Set("Access-Control-Allow-Origin", "*")
	h.Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
	h.Set("Access-Control-Allow-Headers", "Content-Type, Authorization")
	w.WriteHeader(200)
	if flusher != nil {
		flusher.Flush()
	}

	buf := pre
	sawTool := false
	lastReal := time.Now()
	timer := time.NewTimer(stallFor(false))
	defer timer.Stop()
	total := len(pre)
	fail := false

loop:
	for {
		// Drain complete events from buf.
		for {
			ev, rest, ok := nextSSEEvent(buf)
			if !ok {
				buf = rest
				break
			}
			buf = rest
			if !hasRealSSE(ev) {
				continue
			}
			if eventHasToolCall(ev) {
				sawTool = true
			}
			lastReal = time.Now()
			total += len(ev) + 2
			if _, err := w.Write(append(ev, '\n', '\n')); err != nil {
				log.Printf("client disconnected mid-stream (%d bytes, %s)", total, time.Since(t0).Round(time.Millisecond))
				fail = true
				break loop
			}
			if flusher != nil {
				flusher.Flush()
			}
			timer.Reset(stallFor(sawTool))
		}

		if len(buf) > 0 {
			// leftover partial event; wait for more bytes
		}

		stall := stallFor(sawTool)
		select {
		case m, ok := <-ch:
			if !ok || m.eof {
				break loop
			}
			if m.err != nil {
				log.Printf("%s mid-stream error: %v", fam, m.err)
				break loop
			}
			buf = append(buf, m.data...)
		case <-timer.C:
			if time.Since(lastReal) >= stall {
				log.Printf("%s stream stalled (no real data %s, saw_tool=%v), closing", fam, stall.Round(time.Second), sawTool)
				if !sawTool {
					msg := "data: {\"error\": {\"message\": \"upstream stalled\", \"type\": \"UpstreamStall\"}}\n\ndata: [DONE]\n\n"
					w.Write([]byte(msg))
					if flusher != nil {
						flusher.Flush()
					}
				}
				resp.Body.Close()
				break loop
			}
			timer.Reset(stall - time.Since(lastReal))
		}
	}

	if !fail {
		log.Printf("%s stream done (%d bytes, ttft=%.2fs, total=%.2fs)", fam, total, time.Since(t0).Seconds(), time.Since(t0).Seconds())
	}
	resp.Body.Close()
	return !fail
}

// ---- request handling ------------------------------------------------------

func (s *server) handleProxy(w http.ResponseWriter, r *http.Request, method string) {
	clientIP := clientIPOf(r)
	log.Printf("-> %s %s from %s", method, r.URL.Path, clientIP)

	if s.ban != nil && s.ban.isBanned(clientIP) {
		log.Printf("banned")
		writeJSON(w, 429, map[string]any{"error": "Banned"})
		return
	}
	if s.ban != nil {
		s.ban.incr(clientIP)
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		log.Printf("read body error: %v", err)
		writeJSON(w, 400, map[string]any{"error": "Bad Request"})
		return
	}

	isStream := false
	bodyStr := string(body)
	var payload map[string]any
	if len(body) > 0 {
		if err := json.Unmarshal(body, &payload); err != nil {
			log.Printf("invalid JSON")
			writeJSON(w, 400, map[string]any{"error": "Invalid JSON"})
			return
		}
		model, _ := payload["model"].(string)
		if model == "" {
			model = "deepseek-v4-flash-free"
		}
		if model == "deepseek-v4-flash" {
			model = "deepseek-v4-flash-free"
			log.Printf("model deepseek-v4-flash -> deepseek-v4-flash-free")
		}
		payload["model"] = model
		if mt, ok := payload["max_tokens"].(float64); !ok || mt > 65536 {
			payload["max_tokens"] = 65536
		}
		if tp, ok := payload["top_p"].(float64); !ok || tp <= 0 || tp > 1.0 {
			payload["top_p"] = 1.0
		}
		if temp, ok := payload["temperature"].(float64); !ok || temp < 0 || temp > 2.0 {
			payload["temperature"] = 1.0
		}
		isStream, _ = payload["stream"].(bool)
		re, _ := json.Marshal(payload)
		bodyStr = string(re)
		log.Printf("body: model=%v stream=%v max_tokens=%v messages=%d tools=%d",
			payload["model"], isStream, payload["max_tokens"], len(messagesOf(payload)), len(toolsOf(payload)))
	}

	limitErr := any(nil)
	lastStatus := 0
	var lastErrBody any

	for _, fam := range []string{"v6", "v4"} {
		if s.inCooldown(fam, time.Now()) {
			continue
		}
		client := s.clientV6
		if fam == "v4" {
			client = s.clientV4
		}

		var req *http.Request
		path := "/chat/completions"
		if len(body) == 0 {
			path = "/models"
		}
		req, err = http.NewRequest(method, s.zenURL+path, strings.NewReader(bodyStr))
		if err != nil {
			continue
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer "+zenAPIKey)

		t0 := time.Now()
		resp, err := client.Do(req)
		if err != nil {
			log.Printf("%s upstream error: %v", fam, err)
			lastStatus = 502
			lastErrBody = map[string]any{"error": map[string]any{"message": err.Error(), "type": "UpstreamError"}}
			continue
		}
		tt := time.Since(t0)
		log.Printf("%s upstream %d in %.2fs", fam, resp.StatusCode, tt.Seconds())

		if resp.StatusCode != 200 {
			data, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			var obj map[string]any
			_ = json.Unmarshal(data, &obj)
			if e, ok := obj["error"].(map[string]any); ok && e["type"] == freeLimitErr {
				log.Printf("%s FreeUsageLimitError", fam)
				s.setCooldown(fam, nextUTCMidnight())
				continue
			}
			lastStatus = resp.StatusCode
			if len(obj) > 0 {
				lastErrBody = obj
			} else {
				lastErrBody = map[string]any{"error": map[string]any{"message": string(data)}}
			}
			log.Printf("%s upstream %d error: %v", fam, resp.StatusCode, lastErrBody)
			continue
		}

		if !isStream {
			data, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			h := w.Header()
			h.Set("Content-Type", "application/json")
			setCORS(h)
			w.WriteHeader(200)
			w.Write(data)
			log.Printf("non-stream done")
			return
		}

		// Streaming.
		if s.forwardStream(w, resp, fam) {
			return
		}
		resp.Body.Close()
	}

	if limitErr != nil {
		writeJSON(w, 429, limitErr)
		return
	}
	if s.inCooldown("v4", time.Now()) && s.inCooldown("v6", time.Now()) {
		writeJSON(w, 429, map[string]any{"error": map[string]any{
			"message": "All IPs have reached the daily free usage limit", "type": freeLimitErr}})
		return
	}
	if lastErrBody != nil {
		writeJSON(w, lastStatus, lastErrBody)
		return
	}
	writeJSON(w, 503, map[string]any{"error": map[string]any{
		"message": "All upstream IPs exhausted or unavailable", "type": "UpstreamError"}})
}

// ---- small helpers ---------------------------------------------------------

func messagesOf(p map[string]any) []any {
	m, _ := p["messages"].([]any)
	return m
}

func toolsOf(p map[string]any) []any {
	t, _ := p["tools"].([]any)
	return t
}

func nextUTCMidnight() time.Time {
	now := time.Now().UTC()
	next := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC).AddDate(0, 0, 1)
	return next
}

func setCORS(h http.Header) {
	h.Set("Access-Control-Allow-Origin", "*")
	h.Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
	h.Set("Access-Control-Allow-Headers", "Content-Type, Authorization")
	h.Set("Access-Control-Max-Age", "86400")
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	h := w.Header()
	h.Set("Content-Type", "application/json")
	setCORS(h)
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

func clientIPOf(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

// ---- ban list --------------------------------------------------------------

type banList struct {
	mu     sync.Mutex
	counts map[string][2]int64 // key -> [count, windowStart]
	banned map[string]time.Time
	max    int
	window time.Duration
	banLen time.Duration
}

func newBanList(max int, window, banLen time.Duration) *banList {
	b := &banList{
		counts: map[string][2]int64{},
		banned: map[string]time.Time{},
		max:    max,
		window: window,
		banLen: banLen,
	}
	go b.cleanupLoop()
	return b
}

func (b *banList) cleanupLoop() {
	for {
		time.Sleep(cleanupInterval)
		now := time.Now()
		b.mu.Lock()
		for k, until := range b.banned {
			if now.After(until) {
				delete(b.banned, k)
			}
		}
		for k, c := range b.counts {
			if time.Now().Sub(time.Unix(c[1], 0)) > b.window {
				delete(b.counts, k)
			}
		}
		b.mu.Unlock()
	}
}

func (b *banList) incr(key string) {
	now := time.Now()
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, banned := b.banned[key]; banned {
		return
	}
	c, ok := b.counts[key]
	if !ok || now.Sub(time.Unix(c[1], 0)) > b.window {
		c = [2]int64{0, now.Unix()}
	}
	c[0]++
	if c[0] > int64(b.max) {
		b.banned[key] = now.Add(b.banLen)
		delete(b.counts, key)
		return
	}
	b.counts[key] = c
}

func (b *banList) isBanned(key string) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	until, banned := b.banned[key]
	if !banned {
		return false
	}
	if time.Now().After(until) {
		delete(b.banned, key)
		return false
	}
	return true
}

func (b *banList) count() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.banned)
}

// ---- HTTP server wiring ----------------------------------------------------

func (s *server) handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/chat/completions", func(w http.ResponseWriter, r *http.Request) {
		s.handleProxy(w, r, r.Method)
	})
	mux.HandleFunc("/v1/chat/completions", func(w http.ResponseWriter, r *http.Request) {
		s.handleProxy(w, r, r.Method)
	})
	mux.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) {
		now := time.Now()
		v4 := int(s.cooldownFam("v4").Sub(now).Seconds())
		v6 := int(s.cooldownFam("v6").Sub(now).Seconds())
		if v4 < 0 {
			v4 = 0
		}
		if v6 < 0 {
			v6 = 0
		}
		writeJSON(w, 200, map[string]any{
			"path":              r.URL.Path,
			"server":            s.serverID,
			"status":            "ok",
			"v4_cooldown_sec":   v4,
			"v6_cooldown_sec":   v6,
			"banned_ips":        s.ban.count(),
			"active_goroutines": runtime.NumGoroutine(),
		})
		})
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "OPTIONS" {
			h := w.Header()
			setCORS(h)
			w.WriteHeader(204)
			return
		}
		http.NotFound(w, r)
	})
	return mux
}

func makeClient(family string) *http.Client {
	dialer := &net.Dialer{Timeout: connectTimeout, KeepAlive: 30 * time.Second}
	tr := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			return dialer.DialContext(ctx, family, addr)
		},
		ResponseHeaderTimeout: connectTimeout,
		IdleConnTimeout:       idleConnTimeout,
		MaxIdleConns:          maxIdleConns,
		MaxIdleConnsPerHost:   maxIdleConns,
		TLSHandshakeTimeout:   connectTimeout,
		ForceAttemptHTTP2:     true,
	}
	return &http.Client{Transport: tr}
}

func main() {
	// Positional args mirror the python service: [addr] [port] [cert] [key] [timeout] [server_id]
	addr := "0.0.0.0"
	port := 8443
	cert := ""
	key := ""
	serverID := "unknown"

	if len(os.Args) > 1 {
		addr = os.Args[1]
	}
	if len(os.Args) > 2 {
		fmt.Sscanf(os.Args[2], "%d", &port)
	}
	if len(os.Args) > 3 {
		cert = os.Args[3]
	}
	if len(os.Args) > 4 {
		key = os.Args[4]
	}
	if len(os.Args) > 5 {
		// timeout (5th arg) is fixed by constants; accept and ignore.
	}
	if len(os.Args) > 6 {
		serverID = os.Args[6]
	}

	s := &server{
		zenURL:   "https://" + zenHost + zenPath,
		serverID: serverID,
		clientV4: makeClient("tcp4"),
		clientV6: makeClient("tcp6"),
		ban:      newBanList(maxReqsPerClient, banWindow, banWindow),
	}

	proto := "http"
	listen := fmt.Sprintf("%s:%d", addr, port)
	srv := &http.Server{
		Addr:              listen,
		Handler:           s.handler(),
		ReadHeaderTimeout: 15 * time.Second,
	}

	log.Printf("Zen proxy on %s://%s (timeout=%s)", proto, listen, stallTimeout)
	var err error
	if cert != "" && key != "" {
		proto = "https"
		log.Printf("Zen proxy on https://%s (timeout=%s)", listen, stallTimeout)
		err = srv.ListenAndServeTLS(cert, key)
	} else {
		err = srv.ListenAndServe()
	}
	if err != nil {
		log.Fatal(err)
	}
}
