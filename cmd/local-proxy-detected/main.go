// Local Proxy (detected) — Ollama-compatible (11434) endpoint relaying to
// opencode.ai/zen/v1 with dual-stack v6/v4 failover AND active stream detection:
// pre-read wait for first token, token-cadence stall detection (10s/token),
// 180s tool-call thinking window, keep-alive filtering, UpstreamStall / length
// terminators. The vanilla twin (cmd/local-proxy) relays with zero detection.
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

	"github.com/Hana-ame/wintools/pkg/proxyheaders"
)

const (
	zenHost          = "opencode.ai"
	zenPath          = "/zen/v1"
	freeLimitErr     = "FreeUsageLimitError"
	connectTimeout   = 10 * time.Second
	stallTimeout     = 30 * time.Second
	tokenGapTimeout  = 10 * time.Second
	toolStallTimeout = 180 * time.Second

	// maxRequestBody 限制请求体大小，防止恶意超大 body 耗尽内存。
	maxRequestBody = 10 << 20

	// 计费价格（每百万 token），与 opencode.json 中 zen-multi 一致。
	priceInputM     = 1.0
	priceOutputM    = 2.0
	priceCacheReadM = 0.02
	priceCacheWriteM = 0.0
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

// stallFor 返回「两个真实 SSE 事件之间允许的最大间隔」：
// 首 token 之后按 token 节奏收敛到 tokenGapTimeout；工具流保留更长思考窗口。
func stallFor(sawTool bool) time.Duration {
	if sawTool {
		return toolStallTimeout
	}
	return tokenGapTimeout
}

type chunkMsg struct {
	data []byte
	eof  bool
	err  error
}

// ---- usage stats ------------------------------------------------------------

// usage 记录响应体里的 token 用量。非流式响应整体解析；
// 流式响应每个 SSE 事件里可能带 usage（通常只在最后一个事件）。
type usage struct {
	PromptTokens     int64 `json:"prompt_tokens"`
	CompletionTokens int64 `json:"completion_tokens"`
	ReasoningTokens  int64 `json:"reasoning_tokens"`
	Cache            struct {
		Read  int64 `json:"read"`
		Write int64 `json:"write"`
	} `json:"cache"`
}

type modelStats struct {
	Requests   int64   `json:"requests"`
	Input      int64   `json:"input"`
	Output     int64   `json:"output"`
	Reasoning  int64   `json:"reasoning"`
	CacheRead  int64   `json:"cache_read"`
	CacheWrite int64   `json:"cache_write"`
	Cost       float64 `json:"est_cost"`
}

type stats struct {
	mu     sync.Mutex
	models map[string]*modelStats
}

func newStats() *stats {
	return &stats{models: make(map[string]*modelStats)}
}

func (s *stats) add(model string, req bool, u *usage) {
	s.mu.Lock()
	defer s.mu.Unlock()
	m := s.models[model]
	if m == nil {
		m = &modelStats{}
		s.models[model] = m
	}
	if req {
		m.Requests++
	}
	if u == nil {
		return
	}
	m.Input += u.PromptTokens
	m.Output += u.CompletionTokens
	m.Reasoning += u.ReasoningTokens
	m.CacheRead += u.Cache.Read
	m.CacheWrite += u.Cache.Write
	m.Cost += estCost(u)
}

func (s *stats) snapshot() map[string]*modelStats {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[string]*modelStats, len(s.models))
	for k, v := range s.models {
		c := *v
		out[k] = &c
	}
	return out
}

func estCost(u *usage) float64 {
	return float64(u.Cache.Read)/1e6*priceCacheReadM +
		float64(u.PromptTokens)/1e6*priceInputM +
		float64(u.CompletionTokens+u.ReasoningTokens)/1e6*priceOutputM
}

func parseUsage(data []byte) *usage {
	var obj struct {
		Usage *usage `json:"usage"`
	}
	if err := json.Unmarshal(data, &obj); err != nil || obj.Usage == nil {
		return nil
	}
	return obj.Usage
}

// parseSSEUsage 从单个 SSE 事件（"data: {...}" 行）里抽取 usage。
func parseSSEUsage(ev []byte) *usage {
	for _, line := range bytes.Split(ev, []byte("\n")) {
		if !bytes.HasPrefix(line, []byte("data: ")) {
			continue
		}
		if u := parseUsage(line[len("data: "):]); u != nil {
			return u
		}
	}
	return nil
}

func startReader(body io.Reader, done <-chan struct{}) chan chunkMsg {
	ch := make(chan chunkMsg, 32)
	go func() {
		buf := make([]byte, 16384)
		for {
			n, err := body.Read(buf)
			if n > 0 {
				cp := make([]byte, n)
				copy(cp, buf[:n])
				select {
				case ch <- chunkMsg{data: cp}:
				case <-done:
					return
				}
			}
			if err != nil {
				if err == io.EOF {
					select {
					case ch <- chunkMsg{eof: true}:
					case <-done:
						return
					}
				} else {
					select {
					case ch <- chunkMsg{err: err}:
					case <-done:
						return
					}
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

	// detect=true 时启用流检测（预读/stall/DONE 兜底）；false 时纯透传（vanilla 模式）。
	detect bool

	stats *stats
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
	body, err := io.ReadAll(io.LimitReader(r.Body, maxRequestBody+1))
	if err != nil {
		writeJSON(w, 400, map[string]any{"error": "Bad Request"})
		return
	}
	if len(body) > maxRequestBody {
		writeJSON(w, http.StatusRequestEntityTooLarge, map[string]any{"error": "Request body too large"})
		return
	}

	isStream := false
	model := "deepseek-v4-flash-free"
	var payload map[string]any
	bodyStr := string(body)
	if len(body) > 0 {
		if err := json.Unmarshal(body, &payload); err != nil {
			writeJSON(w, 400, map[string]any{"error": "Invalid JSON"})
			return
		}
		if m, _ := payload["model"].(string); m != "" {
			model = m
		}
		payload["model"] = model
		if mt, ok := payload["max_tokens"].(float64); !ok || mt > 131072 {
			payload["max_tokens"] = 131072
		}
		isStream, _ = payload["stream"].(bool)
		re, _ := json.Marshal(payload)
		bodyStr = string(re)
	}

	lastStatus := 0
	var lastErrBody any
	var lastHeaders http.Header

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
		proxyheaders.ForwardRequestHeaders(req.Header, r.Header)
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
			lastHeaders = http.Header{}
			proxyheaders.ForwardResponseHeaders(lastHeaders, resp.Header)
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
			p.stats.add(model, true, parseUsage(data))
			h := w.Header()
			proxyheaders.ForwardResponseHeaders(h, resp.Header)
			h.Set("Content-Type", "application/json")
			setCORS(h)
			w.WriteHeader(200)
			w.Write(data)
			return
		}

		ok, committed := p.forwardStream(w, resp, fam, model)
		if ok || committed {
			return
		}
		resp.Body.Close()
	}

	if p.inCooldown("v4", time.Now()) && p.inCooldown("v6", time.Now()) {
		writeJSON(w, 429, map[string]any{"error": map[string]any{"message": "All IPs reached daily free usage limit", "type": freeLimitErr}})
		return
	}
	if lastErrBody != nil {
		proxyheaders.MergeHeaders(w.Header(), lastHeaders)
		writeJSON(w, lastStatus, lastErrBody)
		return
	}
	writeJSON(w, 503, map[string]any{"error": map[string]any{"message": "All upstream IPs exhausted or unavailable", "type": "UpstreamError"}})
}

// forwardStream relays the SSE stream. detect=true 时启用预读/stall/DONE 兜底；
// detect=false（vanilla 模式）时纯透传，所有事件原样转发。
// Returns (ok, committed) on success;
// committed 表示响应头已提交，调用方此后不能再 failover（否则会双写响应）。
func (p *proxy) forwardStream(w http.ResponseWriter, resp *http.Response, fam, model string) (bool, bool) {
	if !p.detect {
		return p.forwardPassthrough(w, resp, fam, model)
	}
	done := make(chan struct{})
	defer close(done)
	ch := startReader(resp.Body, done)
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
				return false, false
			}
			pre = append(pre, m.data...)
			hasReal = hasRealSSE(pre)
		case <-timer.C:
		}
	}
	if !hasReal {
		log.Printf("%s no real data in %.0fs, try other stack", fam, stallTimeout.Seconds())
		resp.Body.Close()
		return false, false
	}
	log.Printf("%s first real data +%.2fs", fam, time.Since(t0).Seconds())

	flusher, _ := w.(http.Flusher)
	h := w.Header()
	proxyheaders.ForwardResponseHeaders(h, resp.Header)
	h.Set("Content-Type", "text/event-stream")
	h.Set("Cache-Control", "no-cache")
	setCORS(h)
	w.WriteHeader(200)
	if flusher != nil {
		flusher.Flush()
	}

	p.stats.add(model, true, nil)

	buf := pre
	sawTool := false
	toolDone := false
	doneSent := false
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
			if u := parseSSEUsage(ev); u != nil {
				p.stats.add(model, false, u)
			}
			if eventHasToolCall(ev) {
				sawTool = true
			}
			if fr := eventFinishReason(ev); fr != nil && *fr == "tool_calls" {
				toolDone = true // 工具调用自然收尾
			}
			lastReal = time.Now()
			total += len(ev) + 2
			if _, err := w.Write(append(ev, '\n', '\n')); err != nil {
				log.Printf("client disconnected mid-stream")
				resp.Body.Close()
				return false, true
			}
			if flusher != nil {
				flusher.Flush()
			}
			timer.Reset(stallFor(sawTool))
		}

		if bytes.Contains(buf, []byte("data: [DONE]")) {
			doneSent = true
		}

		stall := stallFor(sawTool)
		select {
		case m, ok := <-ch:
			if !ok || m.eof {
				// EOF：若此前从未发过 [DONE]，补发终止哨兵收尾，客户端才能
				// 区分「完成」与「截断」（上游可能正常 close 但没发 [DONE]）。
				if !doneSent {
					if sawTool && !toolDone {
						w.Write(lengthChunk(model))
					} else {
						w.Write([]byte("data: [DONE]\n\n"))
					}
					if flusher != nil {
						flusher.Flush()
					}
					doneSent = true
				}
				resp.Body.Close()
				log.Printf("%s stream done (%d bytes, %.2fs)", fam, total, time.Since(t0).Seconds())
				return true, true
			}
			if m.err != nil {
				log.Printf("%s mid-stream error: %v", fam, m.err)
				// 上游硬断流：与 stall 同等待遇，补发终止哨兵再断开，
				// 否则客户端收到截断流（无 [DONE]），无法区分「完成」与「截断」。
				if !doneSent {
					if !sawTool {
						msgB, _ := json.Marshal(m.err.Error())
						w.Write([]byte(fmt.Sprintf("data: {\"error\": {\"message\": %s, \"type\": \"UpstreamError\"}}\n\ndata: [DONE]\n\n", msgB)))
					} else if !toolDone {
						w.Write(lengthChunk(model))
					} else {
						w.Write([]byte("data: [DONE]\n\n"))
					}
					if flusher != nil {
						flusher.Flush()
					}
					doneSent = true
				}
				resp.Body.Close()
				return false, true
			}
			buf = append(buf, m.data...)
		case <-timer.C:
			if time.Since(lastReal) >= stall {
				log.Printf("%s stream stalled (no real data %s, saw_tool=%v), closing", fam, stall.Round(time.Second), sawTool)
				if !doneSent {
					if !sawTool {
						w.Write([]byte("data: {\"error\": {\"message\": \"upstream stalled\", \"type\": \"UpstreamStall\"}}\n\ndata: [DONE]\n\n"))
					} else {
						// 工具流：补发终止事件再断开，客户端能区分「完成」与「截断」
						w.Write(lengthChunk(model))
					}
					if flusher != nil {
						flusher.Flush()
					}
					doneSent = true
				}
				resp.Body.Close()
				return false, true
			}
			timer.Reset(stall - time.Since(lastReal))
		}
	}
}

// forwardPassthrough 纯透传模式：不做预读 / stall 检测 / DONE 兜底，
// 上游事件原样转发，仅统计请求数（usage 若有也记录）。
func (p *proxy) forwardPassthrough(w http.ResponseWriter, resp *http.Response, fam, model string) (bool, bool) {
	done := make(chan struct{})
	defer close(done)
	ch := startReader(resp.Body, done)
	t0 := time.Now()

	flusher, _ := w.(http.Flusher)
	h := w.Header()
	proxyheaders.ForwardResponseHeaders(h, resp.Header)
	h.Set("Content-Type", "text/event-stream")
	h.Set("Cache-Control", "no-cache")
	setCORS(h)
	w.WriteHeader(200)
	if flusher != nil {
		flusher.Flush()
	}

	p.stats.add(model, true, nil)

	total := 0
	buf := []byte{}
	for {
		select {
		case m, ok := <-ch:
			if !ok || m.eof {
				resp.Body.Close()
				log.Printf("%s stream done (%d bytes, %.2fs)", fam, total, time.Since(t0).Seconds())
				return true, true
			}
			if m.err != nil {
				log.Printf("%s upstream error: %v", fam, m.err)
				resp.Body.Close()
				return true, true
			}
			buf = append(buf, m.data...)
			total += len(m.data)
			// 逐事件转发，顺带解析 usage 统计
			for {
				ev, rest, ok := nextSSEEvent(buf)
				if !ok {
					buf = rest
					break
				}
				buf = rest
				if u := parseSSEUsage(ev); u != nil {
					p.stats.add(model, false, u)
				}
				total += len(ev) + 2
				if _, err := w.Write(append(ev, '\n', '\n')); err != nil {
					log.Printf("client disconnected mid-stream")
					resp.Body.Close()
					return false, true
				}
			}
			if flusher != nil {
				flusher.Flush()
			}
		}
	}
}

// eventFinishReason returns the first non-nil finish_reason in the event.
func eventFinishReason(ev []byte) *string {
	for _, line := range bytes.Split(ev, []byte("\n")) {
		if !bytes.HasPrefix(line, []byte("data: ")) {
			continue
		}
		payload := line[len("data: "):]
		if bytes.Equal(payload, []byte("[DONE]")) {
			continue
		}
		var obj struct {
			Choices []struct {
				FinishReason *string `json:"finish_reason"`
			} `json:"choices"`
		}
		if err := json.Unmarshal(payload, &obj); err != nil {
			continue
		}
		for _, ch := range obj.Choices {
			if ch.FinishReason != nil {
				return ch.FinishReason
			}
		}
	}
	return nil
}

// lengthChunk 工具流 stall 截断时补发的终止事件：finish_reason=length + [DONE]。
func lengthChunk(model string) []byte {
	return []byte(fmt.Sprintf("data: {\"id\":\"stall\",\"object\":\"chat.completion.chunk\",\"created\":0,\"model\":\"%s\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"length\"}]}\n\ndata: [DONE]\n\n", model))
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
	port := 11434
	mode := ""  // "", "v4", or "v6"
	detect := true // 默认 detected 模式；传 "vanilla" 纯透传
	if len(os.Args) > 1 {
		// host 参数保留兼容；双栈监听固定 0.0.0.0 / [::]
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
	if len(os.Args) > 4 {
		switch os.Args[4] {
		case "vanilla":
			detect = false
		}
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
		detect:   detect,
		stats:    newStats(),
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
	mux.HandleFunc("/stats", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "OPTIONS" {
			writeJSON(w, 204, map[string]any{})
			return
		}
		writeJSON(w, 200, map[string]any{"models": p.stats.snapshot()})
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
		Handler:           mux,
		ReadHeaderTimeout: 15 * time.Second,
	}

	// 双栈监听：IPv4 (0.0.0.0) + IPv6 ([::])，LAN 设备两种协议族都能连上。
	ln4, err := net.Listen("tcp4", "0.0.0.0:"+fmt.Sprint(port))
	if err != nil {
		log.Fatalf("listen tcp4: %v", err)
	}
	ln6, err := net.Listen("tcp6", "[::]:"+fmt.Sprint(port))
	if err != nil {
		ln4.Close()
		log.Fatalf("listen tcp6: %v", err)
	}
	log.Printf("Local proxy on 0.0.0.0:%d & [::]:%d/v1/chat/completions (forward -> opencode.ai)", port, port)
	if mode != "" {
		log.Printf("IP stack forced: %s", mode)
	}
	if !detect {
		log.Printf("mode: vanilla (passthrough, no stall detection)")
	}
	if err := srv.Serve(ln4); err != nil {
		log.Fatal(err)
	}
	go srv.Serve(ln6)
}
