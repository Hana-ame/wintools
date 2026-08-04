// Local Proxy in Go — pure forwarder. Ollama-compatible (11434) endpoint that
// simply relays to opencode.ai/zen/v1 (dual-stack v6/v4 failover), nothing else.
package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
)

const (
	zenHost          = "opencode.ai"
	zenPath          = "/zen/v1"
	freeLimitErr     = "FreeUsageLimitError"
	connectTimeout   = 10 * time.Second
	stallTimeout     = 30 * time.Second
	toolStallTimeout = 180 * time.Second
)

func nextUTCMidnight() time.Time {
	now := time.Now().UTC()
	return time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC).AddDate(0, 0, 1)
}

// ---- SSE helpers -----------------------------------------------------------

func hasRealSSE(buf []byte) bool {
	return bytes.Contains(buf, []byte("data:")) || bytes.Contains(buf, []byte("event:"))
}

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

type chunkMsg struct {
	data []byte
	eof  bool
	err  error
}

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

// ---- proxy core ------------------------------------------------------------

type proxy struct {
	mu         sync.Mutex
	cooldownV4 time.Time
	cooldownV6 time.Time

	clientV4 *http.Client
	clientV6 *http.Client

	mode  string // "", "v4", or "v6"
	v4URL string
	v6URL string
}

// resolveOnce resolves host via public DNS, bypassing the system resolver
// (Termux may point at an unreachable ::1:53).
func resolveOnce(host string) (v4, v6 string) {
	for _, dns := range []string{"1.1.1.1:53", "8.8.8.8:53", "223.5.5.5:53", "114.114.114.114:53"} {
		r := &net.Resolver{
			PreferGo: true,
			Dial: func(ctx context.Context, network, address string) (net.Conn, error) {
				d := net.Dialer{Timeout: 5 * time.Second}
				return d.DialContext(ctx, "udp", dns)
			},
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		addrs, err := r.LookupIPAddr(ctx, host)
		cancel()
		if err != nil || len(addrs) == 0 {
			continue
		}
		for _, a := range addrs {
			if a.IP.To4() != nil {
				if v4 == "" {
					v4 = a.IP.String()
				}
			} else if a.IP.To16() != nil {
				if v6 == "" {
					v6 = a.IP.String()
				}
			}
		}
		log.Printf("resolved %s via %s -> v4=%s v6=%s", host, dns, v4, v6)
		return v4, v6
	}
	log.Printf("resolve %s failed on all public DNS", host)
	return "", ""
}

func (p *proxy) inCooldown(fam string, now time.Time) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if fam == "v6" {
		return p.cooldownV6.After(now)
	}
	return p.cooldownV4.After(now)
}

func (p *proxy) setCooldownUntil(fam string, t time.Time) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if fam == "v6" {
		p.cooldownV6 = t
	} else {
		p.cooldownV4 = t
	}
}

func (p *proxy) handle(w http.ResponseWriter, r *http.Request, method string) {
	log.Printf("-> %s %s", method, r.URL.Path)
	body, err := io.ReadAll(r.Body)
	if err != nil {
		writeJSON(w, 400, map[string]any{"error": "Bad Request"})
		return
	}

	isStream := false
	var payload map[string]any
	bodyStr := string(body)
	if len(body) > 0 {
		if err := json.Unmarshal(body, &payload); err != nil {
			writeJSON(w, 400, map[string]any{"error": "Invalid JSON"})
			return
		}
		model, _ := payload["model"].(string)
		if model == "" {
			model = "deepseek-v4-flash-free"
		}
		payload["model"] = model
		if mt, ok := payload["max_tokens"].(float64); !ok || mt > 65536 {
			payload["max_tokens"] = 65536
		}
		isStream, _ = payload["stream"].(bool)
		re, _ := json.Marshal(payload)
		bodyStr = string(re)
	}

	lastStatus := 0
	var lastErrBody any
	limitErr := any(nil)

	stacks := []string{"v6", "v4"}
	switch p.mode {
	case "v4":
		stacks = []string{"v4"}
	case "v6":
		stacks = []string{"v6"}
	}
	for _, fam := range stacks {
		if p.inCooldown(fam, time.Now()) {
			continue
		}
		client := p.clientV6
		if fam == "v4" {
			client = p.clientV4
		}
		path := "/chat/completions"
		if len(body) == 0 {
			path = "/models"
		}
		base := p.v6URL
		if fam == "v4" {
			base = p.v4URL
		}
		req, err := http.NewRequest(method, base+zenPath+path, strings.NewReader(bodyStr))
		if err != nil {
			continue
		}
		req.Host = zenHost // keep Host header = domain even though we dial the IP
		req.Header.Set("Content-Type", "application/json")
		if a := r.Header.Get("Authorization"); a != "" {
			req.Header.Set("Authorization", a)
		}

		t0 := time.Now()
		resp, err := client.Do(req)
		if err != nil {
			log.Printf("%s upstream error: %v", fam, err)
			lastStatus = 502
			lastErrBody = map[string]any{"error": map[string]any{"message": err.Error(), "type": "UpstreamError"}}
			continue
		}
		log.Printf("%s upstream %d in %.2fs", fam, resp.StatusCode, time.Since(t0).Seconds())

		if resp.StatusCode != 200 {
			data, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			var obj map[string]any
			_ = json.Unmarshal(data, &obj)
			if e, ok := obj["error"].(map[string]any); ok && e["type"] == freeLimitErr {
				log.Printf("%s FreeUsageLimitError", fam)
				p.setCooldownUntil(fam, nextUTCMidnight())
				continue
			}
			lastStatus = resp.StatusCode
			if len(obj) > 0 {
				lastErrBody = obj
			} else {
				lastErrBody = map[string]any{"error": map[string]any{"message": string(data)}}
			}
			continue
		}

		if !isStream {
			data, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			w.Header().Set("Content-Type", "application/json")
			setCORS(w.Header())
			w.WriteHeader(200)
			w.Write(data)
			return
		}

		if p.forwardStream(w, resp, fam) {
			return
		}
		resp.Body.Close()
	}

	if limitErr != nil {
		writeJSON(w, 429, limitErr)
		return
	}
	if p.inCooldown("v4", time.Now()) && p.inCooldown("v6", time.Now()) {
		writeJSON(w, 429, map[string]any{"error": map[string]any{"message": "All IPs reached daily free usage limit", "type": freeLimitErr}})
		return
	}
	if lastErrBody != nil {
		writeJSON(w, lastStatus, lastErrBody)
		return
	}
	writeJSON(w, 503, map[string]any{"error": map[string]any{"message": "All upstream IPs exhausted or unavailable", "type": "UpstreamError"}})
}

// forwardStream relays the SSE stream with keep-alive filtering and stall
// detection (30s; 180s during a tool call). Returns true on success.
func (p *proxy) forwardStream(w http.ResponseWriter, resp *http.Response, fam string) bool {
	ch := startReader(resp.Body)
	t0 := time.Now()

	deadline := time.Now().Add(stallTimeout)
	pre := []byte{}
	hasReal := false
	for !hasReal && time.Now().Before(deadline) {
		remain := time.Until(deadline)
		timer := time.NewTimer(remain)
		select {
		case m, ok := <-ch:
			timer.Stop()
			if !ok || m.eof || m.err != nil {
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
	setCORS(h)
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

	for {
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
				log.Printf("client disconnected mid-stream")
				resp.Body.Close()
				return false
			}
			if flusher != nil {
				flusher.Flush()
			}
			timer.Reset(stallFor(sawTool))
		}

		stall := stallFor(sawTool)
		select {
		case m, ok := <-ch:
			if !ok || m.eof {
				resp.Body.Close()
				log.Printf("%s stream done (%d bytes, %.2fs)", fam, total, time.Since(t0).Seconds())
				return true
			}
			if m.err != nil {
				log.Printf("%s mid-stream error: %v", fam, m.err)
				resp.Body.Close()
				return false
			}
			buf = append(buf, m.data...)
		case <-timer.C:
			if time.Since(lastReal) >= stall {
				log.Printf("%s stream stalled (no real data %s, saw_tool=%v), closing", fam, stall.Round(time.Second), sawTool)
				if !sawTool {
					w.Write([]byte("data: {\"error\": {\"message\": \"upstream stalled\", \"type\": \"UpstreamStall\"}}\n\ndata: [DONE]\n\n"))
					if flusher != nil {
						flusher.Flush()
					}
				}
				resp.Body.Close()
				return false
			}
			timer.Reset(stall - time.Since(lastReal))
		}
	}
}

// ---- server wiring ---------------------------------------------------------

func setCORS(h http.Header) {
	h.Set("Access-Control-Allow-Origin", "*")
	h.Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, PATCH, OPTIONS, HEAD")
	h.Set("Access-Control-Allow-Headers", "*")
	h.Set("Access-Control-Expose-Headers", "*")
	h.Set("Access-Control-Max-Age", "86400")
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	h := w.Header()
	h.Set("Content-Type", "application/json")
	setCORS(h)
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

func makeClient(endpoint, sniHost string) *http.Client {
	tr := &http.Transport{
		// Dial the resolved IP directly (no per-request DNS). Skip cert verify
		// like curl -k (IP dial makes some chains fail even with SNI).
		TLSClientConfig: &tls.Config{ServerName: sniHost, InsecureSkipVerify: true},
		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			d := &net.Dialer{Timeout: connectTimeout, KeepAlive: 30 * time.Second}
			return d.DialContext(ctx, "tcp", endpoint)
		},
		ResponseHeaderTimeout: connectTimeout,
		IdleConnTimeout:       90 * time.Second,
		MaxIdleConnsPerHost:   8,
		TLSHandshakeTimeout:   connectTimeout,
		ForceAttemptHTTP2:     true,
	}
	return &http.Client{Transport: tr}
}

func main() {
	host := "127.0.0.1"
	port := 11434
	mode := ""
	if len(os.Args) > 1 {
		host = os.Args[1]
	}
	if len(os.Args) > 2 {
		fmt.Sscanf(os.Args[2], "%d", &port)
	}
	if len(os.Args) > 3 {
		mode = os.Args[3]
	}
	if mode != "v4" && mode != "v6" {
		mode = ""
	}

	v4, v6 := resolveOnce(zenHost)
	if mode == "v4" && v4 == "" || mode == "v6" && v6 == "" {
		log.Fatalf("no %s address for %s", mode, zenHost)
	}
	p := &proxy{
		clientV4: makeClient(net.JoinHostPort(v4, "443"), zenHost),
		clientV6: makeClient(net.JoinHostPort(v6, "443"), zenHost),
		mode:     mode,
		v4URL:    "https://" + v4,
		v6URL:    "https://[" + v6 + "]",
	}
	if v4 == "" && v6 == "" {
		log.Fatalf("could not resolve %s via public DNS", zenHost)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/v1/chat/completions", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "OPTIONS" {
			writeJSON(w, 204, map[string]any{})
			return
		}
		p.handle(w, r, r.Method)
	})
	mux.HandleFunc("/chat/completions", func(w http.ResponseWriter, r *http.Request) {
		p.handle(w, r, r.Method)
	})
	mux.HandleFunc("/v1/models", func(w http.ResponseWriter, r *http.Request) {
		p.handle(w, r, "GET")
	})
	mux.HandleFunc("/models", func(w http.ResponseWriter, r *http.Request) {
		p.handle(w, r, "GET")
	})
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "OPTIONS" {
			writeJSON(w, 204, map[string]any{})
			return
		}
		w.Header().Set("Content-Type", "text/plain")
		w.WriteHeader(200)
		w.Write([]byte("Local proxy running\n"))
	})

	srv := &http.Server{
		Addr:              fmt.Sprintf("%s:%d", host, port),
		Handler:           mux,
		ReadHeaderTimeout: 15 * time.Second,
	}
	log.Printf("Local proxy on http://%s:%d/v1/chat/completions (forward -> opencode.ai)", host, port)
	if mode != "" {
		log.Printf("IP stack forced: %s", mode)
	}
	if err := srv.ListenAndServe(); err != nil {
		log.Fatal(err)
	}
}
