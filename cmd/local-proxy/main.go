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
	"runtime"
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
	stallTimeout     = 10 * time.Second
	tokenGapTimeout  = 10 * time.Second
	toolStallTimeout = 180 * time.Second

	// FreeUsageLimitError 连续失败阈值与冷却策略：
	// 第 1 次短冷却 1 分钟，第 2 次 5 分钟，达到阈值（3 次）后才锁到午夜。
	limFailThreshold = 3
	limFailCooldown1 = 1 * time.Minute
	limFailCooldown2 = 5 * time.Minute

	// maxRequestBody 限制请求体大小，防止恶意超大 body 耗尽内存。
	maxRequestBody = 10 << 20
)

func nextUTCMidnight() time.Time {
	now := time.Now().UTC()
	return time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC).AddDate(0, 0, 1)
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

// eventUsage extracts the usage object from an SSE event, if present.
// Streams carry it in the final chunk (usage:null everywhere else).
// Returns zero values when absent.
func eventUsage(ev []byte) (inTok, outTok, cacheHit, cacheMiss int64) {
	for _, line := range bytes.Split(ev, []byte("\n")) {
		if !bytes.HasPrefix(line, []byte("data: ")) {
			continue
		}
		payload := line[len("data: "):]
		if bytes.Equal(payload, []byte("[DONE]")) {
			continue
		}
		var obj struct {
			Usage *struct {
				Prompt    int64 `json:"prompt_tokens"`
				Completed int64 `json:"completion_tokens"`
				CacheHit  int64 `json:"prompt_cache_hit_tokens"`
				CacheMiss int64 `json:"prompt_cache_miss_tokens"`
			} `json:"usage"`
		}
		if err := json.Unmarshal(payload, &obj); err != nil {
			continue
		}
		if obj.Usage != nil && obj.Usage.Prompt > 0 {
			return obj.Usage.Prompt, obj.Usage.Completed, obj.Usage.CacheHit, obj.Usage.CacheMiss
		}
	}
	return 0, 0, 0, 0
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

// lengthChunk 工具流 stall 截断时补发的终止事件：finish_reason=length + [DONE]。
func lengthChunk(model string) []byte {
	return []byte(fmt.Sprintf("data: {\"id\":\"stall\",\"object\":\"chat.completion.chunk\",\"created\":0,\"model\":\"%s\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"length\"}]}\n\ndata: [DONE]\n\n", model))
}

type chunkMsg struct {
	data []byte
	eof  bool
	err  error
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

	// token 累计（仅流式响应：从末尾 usage chunk 解析）
	InTok     int64
	OutTok    int64
	CacheHit  int64
	CacheMiss int64
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

// addUsage 累加一次流式响应的 token 用量。
func (a *famStats) addUsage(inTok, outTok, cacheHit, cacheMiss int64) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.InTok += inTok
	a.OutTok += outTok
	a.CacheHit += cacheHit
	a.CacheMiss += cacheMiss
}

func (a *famStats) snapshot() map[string]int64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	return map[string]int64{
		"reqs":              a.Reqs,
		"ok":                a.OK,
		"streams":           a.Streams,
		"bytes":             a.Bytes,
		"tool_calls":        a.ToolCalls,
		"stalls":            a.Stalls,
		"errs":              a.Errs,
		"free_limit":        a.FreeLimit,
		"in_tokens":         a.InTok,
		"out_tokens":        a.OutTok,
		"cache_hit_tokens":  a.CacheHit,
		"cache_miss_tokens": a.CacheMiss,
	}
}

// statsSnapshot 汇总 v4/v6 及 total，供 /status 输出。
func (p *proxy) statsSnapshot() map[string]any {
	p.statsMu.Lock()
	defer p.statsMu.Unlock()
	per := make(map[string]any, len(p.stats))
	var tot map[string]int64
	for fam, st := range p.stats {
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
		"since": p.started.UTC().Format(time.RFC3339),
		"total": tot,
		"per":   per,
	}
}

// onLimitErr 记录一次 FreeUsageLimitError，返回按连续失败次数升级的冷却时长：
// 第 1 次 limFailCooldown1，第 2 次 limFailCooldown2，达到 limFailThreshold
// 后才锁到午夜（此时可视为真·配额耗尽）。
func (p *proxy) onLimitErr(fam string) time.Duration {
	p.mu.Lock()
	defer p.mu.Unlock()
	if fam == "v6" {
		p.limFailsV6++
		switch p.limFailsV6 {
		case 1:
			return limFailCooldown1
		case 2:
			return limFailCooldown2
		default:
			return time.Until(nextUTCMidnight())
		}
	}
	p.limFailsV4++
	switch p.limFailsV4 {
	case 1:
		return limFailCooldown1
	case 2:
		return limFailCooldown2
	default:
		return time.Until(nextUTCMidnight())
	}
}

// clearLimFails 在源成功响应后清零连续失败计数。
func (p *proxy) clearLimFails(fam string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if fam == "v6" {
		p.limFailsV6 = 0
	} else {
		p.limFailsV4 = 0
	}
}

type proxy struct {
	mu         sync.Mutex
	cooldownV4 time.Time
	cooldownV6 time.Time
	limFailsV4 int // 连续 FreeUsageLimitError 次数，成功时清零
	limFailsV6 int

	clientV4 *http.Client
	clientV6 *http.Client

	statsMu sync.Mutex
	stats   map[string]*famStats // 按协议栈（v4/v6）的用量统计
	started time.Time

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

func (p *proxy) cooldownFam(fam string) time.Time {
	p.mu.Lock()
	defer p.mu.Unlock()
	if fam == "v6" {
		return p.cooldownV6
	}
	return p.cooldownV4
}

func (p *proxy) inCooldown(fam string, now time.Time) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if fam == "v6" {
		return p.cooldownV6.After(now)
	}
	return p.cooldownV4.After(now)
}

func (p *proxy) statsFor(fam string) *famStats {
	p.statsMu.Lock()
	defer p.statsMu.Unlock()
	if st, ok := p.stats[fam]; ok {
		return st
	}
	st := &famStats{}
	p.stats[fam] = st
	return st
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
		if model == "deepseek-v4-flash" {
			model = "deepseek-v4-flash-free"
			log.Printf("model deepseek-v4-flash -> deepseek-v4-flash-free")
		}
		payload["model"] = model
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
		st := p.statsFor(fam)
		st.add(1, 0, 0, 0, 0, 0, 0, 0)
		resp, err := client.Do(req)
		if err != nil {
			log.Printf("%s upstream error: %v", fam, err)
			st.add(0, 0, 0, 0, 0, 0, 1, 0)
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
				d := p.onLimitErr(fam)
				log.Printf("%s FreeUsageLimitError (lim_fails, cooldown=%s)", fam, d.Round(time.Second))
				st.add(0, 0, 0, 0, 0, 0, 1, 1)
				p.setCooldownUntil(fam, time.Now().Add(d))
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
			continue
		}

		if !isStream {
			data, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			p.clearLimFails(fam)
			st.add(0, 1, 0, int64(len(data)), 0, 0, 0, 0)
			h := w.Header()
			proxyheaders.ForwardResponseHeaders(h, resp.Header)
			h.Set("Content-Type", "application/json")
			setCORS(h)
			w.WriteHeader(200)
			w.Write(data)
			return
		}

		// Streaming.
		ok, committed := p.forwardStream(w, resp, fam, model, st)
		if ok || committed {
			if ok {
				p.clearLimFails(fam)
			}
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

// forwardStream relays an SSE stream from upstream to the client with
// keep-alive filtering, stall detection and tool-call protection.
// Returns (ok, committed); ok=true on success, committed=true when the
// response headers have been committed (caller must not failover after that).
func (p *proxy) forwardStream(w http.ResponseWriter, resp *http.Response, fam, model string, st *famStats) (bool, bool) {
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
				return false, false
			}
			if m.err != nil {
				log.Printf("%s pre-read error: %v", fam, m.err)
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
	h.Set("Connection", "keep-alive")
	setCORS(h)
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
			if in, out, hit, miss := eventUsage(ev); in > 0 {
				st.addUsage(in, out, hit, miss)
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
	return !fail, true
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
		stats:    map[string]*famStats{},
		started:  time.Now(),
	}
	if v4 == "" && v6 == "" {
		log.Fatalf("could not resolve %s via public DNS", zenHost)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) {
		now := time.Now()
		v4 := int(p.cooldownFam("v4").Sub(now).Seconds())
		v6 := int(p.cooldownFam("v6").Sub(now).Seconds())
		if v4 < 0 {
			v4 = 0
		}
		if v6 < 0 {
			v6 = 0
		}
		writeJSON(w, 200, map[string]any{
			"path":              r.URL.Path,
			"mode":              p.mode,
			"status":            "ok",
			"v4_cooldown_sec":   v4,
			"v6_cooldown_sec":   v6,
			"active_goroutines": runtime.NumGoroutine(),
			"stats":             p.statsSnapshot(),
		})
	})
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

	log.Printf("Local proxy on http://%s:%d/v1/chat/completions (forward -> opencode.ai)", host, port)
	if mode != "" {
		log.Printf("IP stack forced: %s", mode)
	}

	// listenDual 同时监听 IPv4 (0.0.0.0) 与 IPv6 ([::]) 双栈，LAN 设备两种
	// 协议族都能连上。各自跑同一个 mux。
	listenDual := func() (net.Listener, net.Listener, error) {
		ln4, err := net.Listen("tcp4", "0.0.0.0:"+fmt.Sprint(port))
		if err != nil {
			return nil, nil, err
		}
		ln6, err := net.Listen("tcp6", "[::]:"+fmt.Sprint(port))
		if err != nil {
			ln4.Close()
			return nil, nil, err
		}
		return ln4, ln6, nil
	}

	srv := &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: 15 * time.Second,
	}

	ln4, ln6, err := listenDual()
	if err != nil {
		log.Fatalf("listen dual-stack: %v", err)
	}
	log.Printf("listening on tcp4 0.0.0.0:%d and tcp6 [::]:%d (dual-stack)", port, port)

	errc := make(chan error, 2)
	go func() { errc <- srv.Serve(ln4) }()
	go func() { errc <- srv.Serve(ln6) }()
	if err := <-errc; err != nil {
		log.Fatal(err)
	}
}
