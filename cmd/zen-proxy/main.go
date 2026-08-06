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

	"github.com/Hana-ame/wintools/pkg/proxyheaders"
)

const (
	zenHost          = "opencode.ai"
	zenPath          = "/zen/v1"
	zenAPIKey        = "public"
	freeLimitErr     = "FreeUsageLimitError"
	connectTimeout   = 10 * time.Second
	stallTimeout     = 10 * time.Second
	tokenGapTimeout  = 10 * time.Second
	toolStallTimeout = 180 * time.Second
	cleanupInterval  = 5 * time.Minute
	maxReqsPerClient = 200
	banWindow        = 600 * time.Second
	idleConnTimeout  = 90 * time.Second
	maxIdleConns     = 64

	// FreeUsageLimitError 连续失败阈值与冷却策略：
	// 第 1 次短冷却 1 分钟，第 2 次 5 分钟，达到阈值（3 次）后才锁到午夜。
	// opencode.ai 的该错误可能是瞬时限流，不能一上来就锁死一整天。
	limFailThreshold = 3
	limFailCooldown1 = 1 * time.Minute
	limFailCooldown2 = 5 * time.Minute

	// maxRequestBody 限制请求体大小，防止恶意超大 body 耗尽内存。
	maxRequestBody = 10 << 20
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
	limFailsV4 int // 连续 FreeUsageLimitError 次数，成功时清零
	limFailsV6 int

	statsMu sync.Mutex
	stats   map[string]*famStats // 按协议栈（v4/v6）的用量统计
	started time.Time

	ban *banList
}

// famStats 记录单个协议栈（v4/v6）的用量统计，进程存活期间累计。
type famStats struct {
	mu        sync.Mutex
	Reqs      int64 // 尝试连接上游的次数
	OK        int64 // 上游返回 200 的次数
	Streams   int64 // 已提交的流式响应次数
	Bytes     int64 // 转发给客户端的 SSE 字节数
	ToolCalls int64 // 转发的 tool_call delta 次数
	Stalls    int64 // stall 超时中断次数
	Errs      int64 // 传输错误 / 非 200 / 预读失败次数
	FreeLimit int64 // FreeUsageLimitError 命中次数
}

func (a *famStats) add(reqs, ok, streams, bytes, tools, stalls, errs, freeLimit int64) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.Reqs += reqs
	a.OK += ok
	a.Streams += streams
	a.Bytes += bytes
	a.ToolCalls += tools
	a.Stalls += stalls
	a.Errs += errs
	a.FreeLimit += freeLimit
}

func (a *famStats) snapshot() map[string]int64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	return map[string]int64{
		"reqs":       a.Reqs,
		"ok":         a.OK,
		"streams":    a.Streams,
		"bytes":      a.Bytes,
		"tool_calls": a.ToolCalls,
		"stalls":     a.Stalls,
		"errs":       a.Errs,
		"free_limit": a.FreeLimit,
	}
}

func (s *server) statsFor(fam string) *famStats {
	s.statsMu.Lock()
	defer s.statsMu.Unlock()
	if st, ok := s.stats[fam]; ok {
		return st
	}
	st := &famStats{}
	s.stats[fam] = st
	return st
}

// statsSnapshot 汇总 v4/v6 及 total，供 /status 输出。
func (s *server) statsSnapshot() map[string]any {
	s.statsMu.Lock()
	defer s.statsMu.Unlock()
	per := make(map[string]any, len(s.stats))
	var tot map[string]int64
	for fam, st := range s.stats {
		snap := st.snapshot()
		per[fam] = snap
		if tot == nil {
			tot = make(map[string]int64, len(snap))
		}
		for k, v := range snap {
			tot[k] += v
		}
	}
	return map[string]any{
		"since": s.started.UTC().Format(time.RFC3339),
		"total": tot,
		"per":   per,
	}
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

// onLimitErr 记录一次 FreeUsageLimitError，返回按连续失败次数升级的冷却时长：
// 第 1 次 limFailCooldown1，第 2 次 limFailCooldown2，达到 limFailThreshold
// 后才锁到午夜（此时可视为真·配额耗尽）。
func (s *server) onLimitErr(fam string) time.Duration {
	s.mu.Lock()
	defer s.mu.Unlock()
	if fam == "v6" {
		s.limFailsV6++
		switch s.limFailsV6 {
		case 1:
			return limFailCooldown1
		case 2:
			return limFailCooldown2
		default:
			return time.Until(nextUTCMidnight())
		}
	}
	s.limFailsV4++
	switch s.limFailsV4 {
	case 1:
		return limFailCooldown1
	case 2:
		return limFailCooldown2
	default:
		return time.Until(nextUTCMidnight())
	}
}

// clearLimFails 在源成功响应后清零连续失败计数。
func (s *server) clearLimFails(fam string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if fam == "v6" {
		s.limFailsV6 = 0
	} else {
		s.limFailsV4 = 0
	}
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

// eventFinishReason returns the first non-nil finish_reason in the event.
func eventFinishReason(ev []byte) *string {
	for _, line := range bytes.Split(ev, []byte("\n")) {
		if !bytes.HasPrefix(line, []byte("data: ")) {
			continue
		}
		var obj struct {
			Choices []struct {
				FinishReason *string `json:"finish_reason"`
			} `json:"choices"`
		}
		if err := json.Unmarshal(line[len("data: "):], &obj); err != nil {
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

// eventHasContent reports whether the event actually advances the stream:
// a non-empty content delta, a tool_call delta, a non-null finish_reason,
// or the [DONE] sentinel. Empty deltas / heartbeat events do not count,
// so a stream that only emits heartbeats still trips the stall detector.
func eventHasContent(ev []byte) bool {
	for _, line := range bytes.Split(ev, []byte("\n")) {
		if !bytes.HasPrefix(line, []byte("data: ")) {
			continue
		}
		payload := line[len("data: "):]
		if bytes.Equal(payload, []byte("[DONE]")) {
			return true
		}
		var obj struct {
			Choices []struct {
				Delta struct {
					Content   *string           `json:"content"`
					ToolCalls []json.RawMessage `json:"tool_calls"`
					Reasoning *string           `json:"reasoning_content"`
				} `json:"delta"`
				FinishReason *string `json:"finish_reason"`
			} `json:"choices"`
		}
		if err := json.Unmarshal(payload, &obj); err != nil {
			continue
		}
		for _, ch := range obj.Choices {
			if ch.Delta.Content != nil && *ch.Delta.Content != "" {
				return true
			}
			if len(ch.Delta.ToolCalls) > 0 {
				return true
			}
			if ch.Delta.Reasoning != nil && *ch.Delta.Reasoning != "" {
				return true
			}
			if ch.FinishReason != nil {
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

// ---- upstream reader goroutine --------------------------------------------

// startReader drains resp.Body in a single goroutine, sending chunks on ch.
// It is closed (EOF) or an error sentinel is emitted; the goroutine always
// terminates once resp.Body is closed (which unblocks Read).
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

// forwardStream relays an SSE stream from upstream to the client with
// keep-alive filtering, stall detection and tool-call protection.
// Returns true on success.
func (s *server) forwardStream(w http.ResponseWriter, resp *http.Response, fam, model string, st *famStats) bool {
	done := make(chan struct{})
	defer close(done)
	ch := startReader(resp.Body, done)
	t0 := time.Now()

	total := 0
	fwTools := int64(0)
	stalled := false
	served := false
	defer func() {
		if served {
			var addStalls int64
			if stalled {
				addStalls = 1
			}
			// 已提交的流式响应：记 ok/streams/bytes/tools/stalls
			st.add(0, 1, 1, int64(total), fwTools, addStalls, 0, 0)
		} else {
			// 上游 200 但预读失败（无真实数据 / 立即 EOF）：视为上游异常
			st.add(0, 0, 0, 0, 0, 0, 1, 0)
		}
	}()

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
	proxyheaders.ForwardResponseHeaders(h, resp.Header)
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
	served = true

	buf := pre
	sawTool := false
	toolDone := false
	doneSent := false
	lastReal := time.Now()
	timer := time.NewTimer(stallFor(false))
	defer timer.Stop()
	total = len(pre)
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
				fwTools++
			}
			if fr := eventFinishReason(ev); fr != nil && *fr == "tool_calls" {
				toolDone = true // 工具调用自然收尾
			}
			if bytes.Contains(ev, []byte("data: [DONE]")) {
				toolDone = true
				doneSent = true // 已转发终止哨兵，后续 EOF 不再补
			}
			if eventHasContent(ev) {
				// 只有内容推进事件才刷新 stall 时钟：
				// 空 delta/心跳事件照常转发，但不算推进，否则上游持续心跳
				// 会把 stall 检测喂饱，导致「卡住但既不注入也不断开」。
				lastReal = time.Now()
				timer.Reset(stallFor(sawTool))
			}
			total += len(ev) + 2
			if _, err := w.Write(append(ev, '\n', '\n')); err != nil {
				log.Printf("client disconnected mid-stream (%d bytes, %s)", total, time.Since(t0).Round(time.Millisecond))
				fail = true
				break loop
			}
			if flusher != nil {
				flusher.Flush()
			}
		}

		if len(buf) > 0 {
			// leftover partial event; wait for more bytes
		}

		stall := stallFor(sawTool)
		select {
		case m, ok := <-ch:
			if !ok || m.eof {
				// EOF：若此前从未发过 [DONE]，补发终止哨兵收尾，
				// 这样当上游正常结束时（即使没有 [DONE]）客户端也能区分完成/截断。
				if !doneSent {
					if sawTool && !toolDone {
						// 工具调用被截断：补 finish_reason=length + [DONE]
						w.Write(lengthChunk(model))
					} else {
						w.Write([]byte("data: [DONE]\n\n"))
					}
					if flusher != nil {
						flusher.Flush()
					}
					doneSent = true
				}
				break loop
			}
			if m.err != nil {
				log.Printf("%s mid-stream error: %v", fam, m.err)
				// 上游硬断流：与 stall 同等对待，补发终止哨兵再断开，
				// 否则客户端收到截断流（无 [DONE]），无法区分「完成」与「截断」。
				if !doneSent {
					if !sawTool {
						msgB, _ := json.Marshal(m.err.Error())
						msg := fmt.Sprintf("data: {\"error\": {\"message\": %s, \"type\": \"UpstreamError\"}}\n\ndata: [DONE]\n\n", msgB)
						w.Write([]byte(msg))
					} else if !toolDone {
						// 工具流：补发终止事件再断开，客户端能区分「完成」与「截断」
						w.Write(lengthChunk(model))
					} else {
						w.Write([]byte("data: [DONE]\n\n"))
					}
					if flusher != nil {
						flusher.Flush()
					}
					doneSent = true
				}
				break loop
			}
			buf = append(buf, m.data...)
		case <-timer.C:
			if time.Since(lastReal) >= stall {
				log.Printf("%s stream stalled (no real data %s, saw_tool=%v), closing", fam, stall.Round(time.Second), sawTool)
				stalled = true
				if !sawTool {
					msg := "data: {\"error\": {\"message\": \"upstream stalled\", \"type\": \"UpstreamStall\"}}\n\ndata: [DONE]\n\n"
					w.Write([]byte(msg))
				} else {
					// 工具流：补发终止事件再断开，客户端能区分「完成」与「截断」
					w.Write(lengthChunk(model))
				}
				if flusher != nil {
					flusher.Flush()
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

// lengthChunk 工具流 stall 截断时补发的终止事件：finish_reason=length + [DONE]。
func lengthChunk(model string) []byte {
	return []byte(fmt.Sprintf("data: {\"id\":\"stall\",\"object\":\"chat.completion.chunk\",\"created\":0,\"model\":\"%s\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"length\"}]}\n\ndata: [DONE]\n\n", model))
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

	body, err := io.ReadAll(io.LimitReader(r.Body, maxRequestBody+1))
	if err != nil {
		log.Printf("read body error: %v", err)
		writeJSON(w, 400, map[string]any{"error": "Bad Request"})
		return
	}
	if len(body) > maxRequestBody {
		writeJSON(w, http.StatusRequestEntityTooLarge, map[string]any{"error": "Request body too large"})
		return
	}

	isStream := false
	model := "deepseek-v4-flash-free"
	bodyStr := string(body)
	var payload map[string]any
	if len(body) > 0 {
		if err := json.Unmarshal(body, &payload); err != nil {
			log.Printf("invalid JSON")
			writeJSON(w, 400, map[string]any{"error": "Invalid JSON"})
			return
		}
		if m, _ := payload["model"].(string); m != "" {
			model = m
		}
		if model == "deepseek-v4-flash" {
			model = "deepseek-v4-flash-free"
			log.Printf("model deepseek-v4-flash -> deepseek-v4-flash-free")
		}
		payload["model"] = model
		if mt, ok := payload["max_tokens"].(float64); !ok || mt > 131072 {
			payload["max_tokens"] = 131072
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

	lastStatus := 0
	var lastErrBody any
	var lastHeaders http.Header

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
		proxyheaders.ForwardRequestHeaders(req.Header, r.Header)
		if a := r.Header.Get("Authorization"); a != "" {
			req.Header.Set("Authorization", a)
		} else {
			req.Header.Set("Authorization", "Bearer "+zenAPIKey)
		}

		t0 := time.Now()
		st := s.statsFor(fam)
		st.add(1, 0, 0, 0, 0, 0, 0, 0)
		resp, err := client.Do(req)
		if err != nil {
			log.Printf("%s upstream error: %v", fam, err)
			st.add(0, 0, 0, 0, 0, 0, 1, 0)
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
				d := s.onLimitErr(fam)
				log.Printf("%s FreeUsageLimitError (lim_fails, cooldown=%s)", fam, d.Round(time.Second))
				st.add(0, 0, 0, 0, 0, 0, 1, 1)
				s.setCooldown(fam, time.Now().Add(d))
				continue
			}
			st.add(0, 0, 0, 0, 0, 0, 1, 0)
			lastStatus = resp.StatusCode
			lastHeaders = http.Header{}
			proxyheaders.ForwardResponseHeaders(lastHeaders, resp.Header)
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
			s.clearLimFails(fam)
			st.add(0, 1, 0, int64(len(data)), 0, 0, 0, 0)
			h := w.Header()
			proxyheaders.ForwardResponseHeaders(h, resp.Header)
			h.Set("Content-Type", "application/json")
			setCORS(h)
			w.WriteHeader(200)
			w.Write(data)
			log.Printf("non-stream done")
			return
		}

		// Streaming.
		if s.forwardStream(w, resp, fam, model, st) {
			s.clearLimFails(fam)
			return
		}
		resp.Body.Close()
	}

	if s.inCooldown("v4", time.Now()) && s.inCooldown("v6", time.Now()) {
		writeJSON(w, 429, map[string]any{"error": map[string]any{
			"message": "All IPs have reached the daily free usage limit", "type": freeLimitErr}})
		return
	}
	if lastErrBody != nil {
		proxyheaders.MergeHeaders(w.Header(), lastHeaders)
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
		banned := 0
		if s.ban != nil {
			banned = s.ban.count()
		}
		writeJSON(w, 200, map[string]any{
			"path":              r.URL.Path,
			"server":            s.serverID,
			"status":            "ok",
			"v4_cooldown_sec":   v4,
			"v6_cooldown_sec":   v6,
			"banned_ips":        banned,
			"active_goroutines": runtime.NumGoroutine(),
			"stats":             s.statsSnapshot(),
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
		stats:    map[string]*famStats{},
		started:  time.Now(),
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
