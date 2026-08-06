// Zen Multi in Go — local multi-source aggregator that forwards to the
// Go zen_proxies (bwh/vps/cloudcone) with failover, cooldown and the
// "inf loop" tool_call injection. Replaces zen_multi.py.
package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Hana-ame/wintools/pkg/proxyheaders"
)

const (
	baseModel    = "deepseek-v4-flash-free"
	modelPrefix  = "deepseek-v4-flash"
	infModel     = "deepseek-v4-flash-inf"
	infTool      = "bash"
	infToolArg   = `{"command": "echo 请继续完善当前项目，补充文档，与设计目标对齐；如果没有需要继续做的工作了，请执行 sleep 1800"}`
	infIdleArg   = `{"command": "echo 继续"}`
	freeLimitErr = "FreeUsageLimitError"

	connectTimeout   = 10 * time.Second
	headerTimeout    = 30 * time.Second
	stallTimeout     = 10 * time.Second
	tokenGapTimeout  = 10 * time.Second
	toolStallTimeout = 180 * time.Second
	cooldownShort    = 60 * time.Second
	maxRetries       = 3
	idleConnTimeout  = 90 * time.Second

	// FreeUsageLimitError 连续失败阈值与冷却策略：
	// 第 1 次短冷却 1 分钟，第 2 次 5 分钟，达到阈值（3 次）后才锁到午夜。
	// opencode.ai 的该错误可能是瞬时限流，不能一上来就锁死一整天。
	limFailThreshold = 3
	limFailCooldown1 = 1 * time.Minute
	limFailCooldown2 = 5 * time.Minute

	// maxRequestBody 限制请求体大小，防止恶意超大 body 耗尽内存。
	maxRequestBody = 10 << 20
)

type upstream struct {
	name string
	base string

	mu            sync.Mutex
	cooldownUntil time.Time
	lastErr       string
	reqs          int64
	limFails      int // 连续 FreeUsageLimitError 次数，成功时清零

	// token 累计（仅流式响应：从末尾 usage chunk 解析）
	inTok     int64
	outTok    int64
	cacheHit  int64
	cacheMiss int64

	client *http.Client
}

func (u *upstream) inCooldown(now time.Time) bool {
	u.mu.Lock()
	defer u.mu.Unlock()
	return u.cooldownUntil.After(now)
}

func (u *upstream) setCooldown(d time.Duration) {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.cooldownUntil = time.Now().Add(d)
}

func (u *upstream) setCooldownUntil(t time.Time) {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.cooldownUntil = t
}

// setErr 记录最近一次错误。所有读写都持 u.mu，避免与 /status 的读取竞争。
func (u *upstream) setErr(e string) {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.lastErr = e
}

// onLimitErr 记录一次 FreeUsageLimitError，返回按连续失败次数升级的冷却时长：
// 第 1 次 limFailCooldown1，第 2 次 limFailCooldown2，达到 limFailThreshold
// 后才锁到午夜（此时可视为真·配额耗尽）。
func (u *upstream) onLimitErr() time.Duration {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.limFails++
	switch u.limFails {
	case 1:
		return limFailCooldown1
	case 2:
		return limFailCooldown2
	default:
		return time.Until(nextUTCMidnight())
	}
}

// clearLimFails 在源成功响应后清零连续失败计数。
func (u *upstream) clearLimFails() {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.limFails = 0
}

func (u *upstream) incrReqs() {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.reqs++
}

// ---- SSE helpers (shared with zen_proxy.go) --------------------------------

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
func eventUsage(ev []byte) *usageStats {
	for _, line := range bytes.Split(ev, []byte("\n")) {
		if !bytes.HasPrefix(line, []byte("data: ")) {
			continue
		}
		payload := line[len("data: "):]
		if bytes.Equal(payload, []byte("[DONE]")) {
			continue
		}
		var obj struct {
			Usage *usageStats `json:"usage"`
		}
		if err := json.Unmarshal(payload, &obj); err != nil {
			continue
		}
		if obj.Usage != nil && obj.Usage.Prompt > 0 {
			return obj.Usage
		}
	}
	return nil
}

// usageStats mirrors the OpenAI-style usage block.
type usageStats struct {
	Prompt    int64 `json:"prompt_tokens"`
	Completed int64 `json:"completion_tokens"`
	Total     int64 `json:"total_tokens"`
	CacheHit  int64 `json:"prompt_cache_hit_tokens"`
	CacheMiss int64 `json:"prompt_cache_miss_tokens"`
}

// addUsage accumulates per-source token totals.
func (u *upstream) addUsage(us *usageStats) {
	if us == nil {
		return
	}
	u.mu.Lock()
	defer u.mu.Unlock()
	u.inTok += us.Prompt
	u.outTok += us.Completed
	u.cacheHit += us.CacheHit
	u.cacheMiss += us.CacheMiss
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

// neutralizeFinish returns the event with finish_reason cleared to null.
func neutralizeFinish(ev []byte) []byte {
	for _, line := range bytes.Split(ev, []byte("\n")) {
		if !bytes.HasPrefix(line, []byte("data: ")) {
			continue
		}
		var obj map[string]any
		if err := json.Unmarshal(line[len("data: "):], &obj); err != nil {
			return ev
		}
		found := false
		if choices, ok := obj["choices"].([]any); ok {
			for _, c := range choices {
				cm, ok := c.(map[string]any)
				if !ok {
					continue
				}
				if fr, exists := cm["finish_reason"]; exists && fr != nil {
					found = true
				}
				cm["finish_reason"] = nil
			}
		}
		if !found {
			return ev
		}
		re, _ := json.Marshal(obj)
		return append([]byte("data: "), re...)
	}
	return ev
}

func eventHasError(ev []byte) bool {
	for _, line := range bytes.Split(ev, []byte("\n")) {
		if !bytes.HasPrefix(line, []byte("data: ")) {
			continue
		}
		var obj map[string]any
		if err := json.Unmarshal(line[len("data: "):], &obj); err != nil {
			continue
		}
		if _, ok := obj["error"]; ok {
			return true
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

// ---- inf injection ---------------------------------------------------------

var (
	infMu    sync.Mutex
	infCount int64
)

func infInject() []byte {
	infMu.Lock()
	infCount++
	n := infCount
	infMu.Unlock()
	callID := fmt.Sprintf("call_inf_%d", n)
	toolEvt := fmt.Sprintf(
		`{"id":"inf","object":"chat.completion.chunk","created":0,"model":"%s","choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"id":"%s","type":"function","function":{"name":"%s","arguments":%s}}]},"finish_reason":null}]}`,
		baseModel, callID, infTool, jsonString(infToolArg))
	finishEvt := fmt.Sprintf(
		`{"id":"inf","object":"chat.completion.chunk","created":0,"model":"%s","choices":[{"index":0,"delta":{},"finish_reason":"tool_calls"}]}`,
		baseModel)
	log.Printf("injecting tool_call #%d", n)
	return []byte("data: " + toolEvt + "\n\ndata: " + finishEvt + "\n\n")
}

func idleInject() []byte {
	inject := fmt.Sprintf(
		`{"id":"inf","object":"chat.completion.chunk","created":0,"model":"%s","choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"id":"call_inf_idle","type":"function","function":{"name":"%s","arguments":%s}}]},"finish_reason":null}]}`,
		baseModel, infTool, jsonString(infIdleArg))
	finishEvt := fmt.Sprintf(
		`{"id":"inf","object":"chat.completion.chunk","created":0,"model":"%s","choices":[{"index":0,"delta":{},"finish_reason":"tool_calls"}]}`,
		baseModel)
	return []byte("data: " + inject + "\n\ndata: " + finishEvt + "\n\ndata: [DONE]\n\n")
}

// finishToolCall seals a truncated tool call with finish_reason=tool_calls,
// so the client executes the (possibly partial) tool call and the loop continues.
func finishToolCall() []byte {
	finishEvt := fmt.Sprintf(
		`{"id":"inf","object":"chat.completion.chunk","created":0,"model":"%s","choices":[{"index":0,"delta":{},"finish_reason":"tool_calls"}]}`,
		baseModel)
	return []byte("data: " + finishEvt + "\n\ndata: [DONE]\n\n")
}

func jsonString(s string) string {
	re, _ := json.Marshal(s)
	return string(re)
}

// lengthChunk 工具流 stall 截断时补发的终止事件：finish_reason=length + [DONE]。
// 与注入事件一致使用 baseModel。
func lengthChunk() []byte {
	return []byte(fmt.Sprintf("data: {\"id\":\"stall\",\"object\":\"chat.completion.chunk\",\"created\":0,\"model\":\"%s\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"length\"}]}\n\ndata: [DONE]\n\n", baseModel))
}

// ---- model resolution ------------------------------------------------------

func resolveModel(model string) (forced string, resolved string) {
	if model == infModel {
		return "", baseModel
	}
	if strings.HasPrefix(model, modelPrefix+"-") {
		src := model[len(modelPrefix)+1:]
		for _, u := range upList {
			if u.name == src {
				return src, baseModel
			}
		}
	}
	return "", model
}

var upList = []*upstream{
	{name: "bwh", base: "https://bwh.moonchan.xyz:8443"},
	{name: "vps", base: "https://vps.moonchan.xyz:8443"},
	{name: "cloudcone", base: "https://c.moonchan.xyz:8443"},
}

func sourceModels() []map[string]any {
	models := []map[string]any{
		{"id": baseModel, "name": "DeepSeek V4 Flash (auto)"},
		{"id": infModel, "name": "DeepSeek V4 Flash (inf loop)"},
	}
	for _, u := range upList {
		models = append(models, map[string]any{"id": modelPrefix + "-" + u.name, "name": "DeepSeek V4 Flash (" + u.name + ")"})
	}
	return models
}

// ---- request handling ------------------------------------------------------

// server carries per-instance state (currently stateless).
type server struct{}

// reqSeq 为每个请求生成递增序号，贯穿所有日志，方便并发请求下的追踪。
var reqSeq atomic.Uint64

func (s *server) handleProxy(w http.ResponseWriter, r *http.Request) {
	reqID := reqSeq.Add(1)
	log.Printf("req=%d -> %s %s", reqID, r.Method, r.URL.Path)
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
	var payload map[string]any
	bodyStr := string(body)
	var forced string
	canInject := false
	recoverable := false
	if len(body) > 0 {
		if err := json.Unmarshal(body, &payload); err != nil {
			writeJSON(w, 400, map[string]any{"error": "Invalid JSON"})
			return
		}
		model, _ := payload["model"].(string)
		isInf := model == infModel
		toolsN := len(toolsOf(payload))
		// canInject: inf 模型完整续命（EOF 时注入 infInject 继续循环）。
		// recoverable: 只要带 tools，流中断时就注入 idleInject（echo 继续）让客户端恢复工具循环。
		canInject = isInf && toolsN > 0
		recoverable = toolsN > 0
		forced, model = resolveModel(model)
		payload["model"] = model
		if mt, ok := payload["max_tokens"].(float64); !ok || mt > 131072 {
			payload["max_tokens"] = 131072
		}
		isStream, _ = payload["stream"].(bool)
		re, _ := json.Marshal(payload)
		bodyStr = string(re)
		log.Printf("req=%d model=%v src=%v stream=%v tools=%d can_inject=%v recoverable=%v", reqID, payload["model"], forced, isStream, len(toolsOf(payload)), canInject, recoverable)
	}

	lastErrBody := any(nil)
	lastStatus := 0
	limitErr := any(nil)
	lastHeaders := http.Header{}

	for attempt := 0; attempt < maxRetries; attempt++ {
		skipped := []string{}
		for _, u := range order(forced) {
			if u.inCooldown(time.Now()) {
				skipped = append(skipped, u.name)
				continue
			}
			ok := s.tryUpstream(w, u, r.Method, bodyStr, r.Header, len(body) > 0, isStream, canInject, recoverable, &lastErrBody, &lastStatus, &limitErr, &lastHeaders, reqID)
			if ok {
				return
			}
		}
		if len(skipped) > 0 {
			log.Printf("req=%d attempt=%d skipped(cooldown)=%v", reqID, attempt+1, skipped)
		}
	}

	if limitErr != nil {
		log.Printf("req=%d all sources failed; last limit err -> 429", reqID)
		writeJSON(w, 429, limitErr)
		return
	}
	if lastErrBody != nil {
		log.Printf("req=%d all sources failed; last_status=%d -> %d", reqID, lastStatus, lastStatus)
		proxyheaders.MergeHeaders(w.Header(), lastHeaders)
		writeJSON(w, lastStatus, lastErrBody)
		return
	}
	log.Printf("req=%d all sources failed/exhausted -> 503", reqID)
	writeJSON(w, 503, map[string]any{"error": map[string]any{"message": "All upstream sources exhausted or unavailable", "type": "UpstreamError"}})
}

// tryUpstream forwards to one source. Returns true if fully handled.
func (s *server) tryUpstream(w http.ResponseWriter, u *upstream, method, bodyStr string, head http.Header, hasBody, isStream, canInject, recoverable bool, lastErrBody *any, lastStatus *int, limitErr *any, lastHeaders *http.Header, reqID uint64) bool {
	path := "/chat/completions"
	if !hasBody {
		path = "/v1/models"
	}
	var req *http.Request
	var err error
	req, err = http.NewRequest(method, u.base+path, strings.NewReader(bodyStr))
	if err != nil {
		log.Printf("req=%d %s: new request failed: %v", reqID, u.name, err)
		return false
	}
	req.Header.Set("Content-Type", "application/json")
	proxyheaders.ForwardRequestHeaders(req.Header, head)
	if a := head.Get("Authorization"); a != "" {
		req.Header.Set("Authorization", a)
	}

	log.Printf("req=%d %s: connecting...", reqID, u.name)
	resp, err := u.client.Do(req)
	if err != nil {
		log.Printf("req=%d %s: connect failed: %T %v", reqID, u.name, err, err)
		u.setCooldown(cooldownShort)
		u.setErr(err.Error())
		*lastStatus = 502
		*lastErrBody = map[string]any{"error": map[string]any{"message": err.Error(), "type": "UpstreamError"}}
		return false
	}
	log.Printf("req=%d %s: connected status=%d", reqID, u.name, resp.StatusCode)

	if resp.StatusCode != 200 {
		data, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		var obj map[string]any
		_ = json.Unmarshal(data, &obj)
		if e, ok := obj["error"].(map[string]any); ok && e["type"] == freeLimitErr {
			d := u.onLimitErr()
			log.Printf("req=%d %s: FreeUsageLimitError (lim_fails=%d, cooldown=%s, msg=%v)", reqID, u.name, u.limFails, d.Round(time.Second), e["message"])
			u.setCooldown(d)
			u.setErr(freeLimitErr)
			*limitErr = obj
			return false
		}
		log.Printf("req=%d %s: HTTP %d -> try next source (msg=%v)", reqID, u.name, resp.StatusCode, obj["error"])
		u.setErr(fmt.Sprintf("HTTP %d", resp.StatusCode))
		*lastStatus = resp.StatusCode
		*lastHeaders = http.Header{}
		proxyheaders.ForwardResponseHeaders(*lastHeaders, resp.Header)
		if len(obj) > 0 {
			*lastErrBody = obj
		} else {
			*lastErrBody = map[string]any{"error": map[string]any{"message": string(data)}}
		}
		return false
	}

	u.incrReqs()
	u.setErr("")
	u.clearLimFails()

	if !isStream {
		data, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		h := w.Header()
		proxyheaders.ForwardResponseHeaders(h, resp.Header)
		h.Set("Content-Type", "application/json")
		h.Set("Access-Control-Allow-Origin", "*")
		w.WriteHeader(200)
		w.Write(data)
		log.Printf("req=%d %s: done (non-stream)", reqID, u.name)
		return true
	}

	ok, committed := s.forwardStream(w, u, resp, canInject, recoverable, reqID)
	// committed=true 表示响应头已提交，此时不能再 failover（会双写响应）。
	return ok || committed
}

// forwardStream relays the SSE stream with keep-alive filtering, stall
// detection, tool-call protection and inf-loop injection.
// 返回 (ok, committed)：committed 表示响应头已提交给客户端。
// 一旦 committed，调用方禁止再 failover 到其他源（否则会双写响应）。
func (s *server) forwardStream(w http.ResponseWriter, u *upstream, resp *http.Response, canInject, recoverable bool, reqID uint64) (bool, bool) {
	name := u.name
	done := make(chan struct{})
	defer close(done)
	ch := startReader(resp.Body, done)
	t0 := time.Now()

	// Pre-read.
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
				if m.err != nil {
					log.Printf("req=%d %s: pre-read error: %v", reqID, name, m.err)
				}
				u.setCooldown(cooldownShort)
				u.setErr("pre-read error")
				resp.Body.Close()
				return false, false
			}
			pre = append(pre, m.data...)
			hasReal = hasRealSSE(pre)
		case <-timer.C:
		}
	}
	if !hasReal {
		log.Printf("req=%d %s: no real data in %.0fs, try next source", reqID, name, stallTimeout.Seconds())
		u.setCooldown(cooldownShort)
		u.setErr("first token stall")
		resp.Body.Close()
		return false, false
	}
	log.Printf("req=%d %s: first token t=%.2fs", reqID, name, time.Since(t0).Seconds())

	// Verify no error in pre-read events.
	{
		eb := pre
		for {
			ev, rest, ok := nextSSEEvent(eb)
			if !ok {
				break
			}
			eb = rest
			if eventHasError(ev) {
				log.Printf("req=%d %s: SSE error event, try next source", reqID, name)
				u.setCooldown(cooldownShort)
				u.setErr("SSE error")
				resp.Body.Close()
				return false, false
			}
		}
	}

	flusher, _ := w.(http.Flusher)
	h := w.Header()
	proxyheaders.ForwardResponseHeaders(h, resp.Header)
	h.Set("Content-Type", "text/event-stream")
	h.Set("Cache-Control", "no-cache")
	h.Set("Connection", "keep-alive")
	h.Set("Access-Control-Allow-Origin", "*")
	w.WriteHeader(200)
	if flusher != nil {
		flusher.Flush()
	}

	buf := pre
	sawTool := false
	toolClosed := false
	doneSent := false
	injected := false
	lastReal := time.Now()
	timer := time.NewTimer(stallFor(false))
	defer timer.Stop()
	total := len(pre)
	write := func(b []byte) error {
		total += len(b)
		_, err := w.Write(b)
		if err == nil && flusher != nil {
			flusher.Flush()
		}
		return err
	}

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
			u.addUsage(eventUsage(ev))
			if eventHasToolCall(ev) {
				sawTool = true
			}
			if eventHasError(ev) {
				// 上游 error 事件（如 zen-proxy 的 UpstreamStall/UpstreamError）：
				// 不转发给客户端（否则客户端看到空流完成），而是注入「echo 继续」
				// 让工具循环恢复；无 tools 的请求则丢弃并等 EOF 兜底 [DONE]。
				if recoverable && !sawTool && !injected {
					log.Printf("req=%d %s: mid-stream error event -> inject idle (saw_tool=%v)", reqID, name, sawTool)
					if err := write(idleInject()); err != nil {
						log.Printf("req=%d client disconnected", reqID)
					}
					injected = true
					doneSent = true
					break loop
				}
				log.Printf("req=%d %s: mid-stream error event dropped (recoverable=%v)", reqID, name, recoverable)
				continue
			}
			if fr := eventFinishReason(ev); fr != nil && *fr == "tool_calls" {
				toolClosed = true // 工具调用自然收尾（finish_reason=tool_calls）
			}
			if bytes.Contains(ev, []byte("data: [DONE]")) {
				toolClosed = true // 上游正常 [DONE]
				doneSent = true   // 终止哨兵已转发，后续 EOF 不再重复补
			}
			if eventHasContent(ev) {
				// 只有内容推进事件才刷新 stall 时钟：
				// 空 delta/心跳事件照常转发，但不算推进，否则上游持续心跳
				// 会把 stall 检测喂饱，导致「卡住但既不注入也不断开」。
				lastReal = time.Now()
				timer.Reset(stallFor(sawTool))
			}

			if canInject && !sawTool && !injected {
				if fr := eventFinishReason(ev); fr != nil && *fr != "tool_calls" {
					log.Printf("req=%d %s: finish_reason=%q -> neutralize + inject", reqID, name, *fr)
					if err := write(neutralizeFinish(ev)); err != nil {
						log.Printf("req=%d client disconnected", reqID)
						break loop
					}
					if err := write([]byte("\n\n")); err != nil {
						break loop
					}
					if err := write(infInject()); err != nil {
						break loop
					}
					injected = true
					continue
				}
				if bytes.Contains(ev, []byte("data: [DONE]")) {
					log.Printf("req=%d %s: upstream [DONE] -> inject before EOF", reqID, name)
					if err := write(infInject()); err != nil {
						break loop
					}
					injected = true
				}
			}

			if err := write(append(ev, '\n', '\n')); err != nil {
				log.Printf("req=%d client disconnected", reqID)
				break loop
			}
		}

		select {
		case m, ok := <-ch:
			if !ok || m.eof {
				// EOF 出口收敛：优先注入；否则 seal 残缺工具调用；
				// 剩余的「非注入」路径（canInject=false / 已收尾但上游没发 [DONE]）
				// 一律兜底补发 [DONE]，避免客户端收到截断流。
				if canInject && !sawTool && !injected {
					log.Printf("req=%d %s: EOF without tool_call -> inject (saw_tool=%v injected=%v)", reqID, name, sawTool, injected)
					if err := write(infInject()); err != nil {
						break loop
					}
					if err := write([]byte("data: [DONE]\n\n")); err != nil {
						break loop
					}
					injected = true
					doneSent = true
				} else if sawTool && !toolClosed {
					// 工具调用被上游截断（未等到 finish_reason=tool_calls/[DONE]）：
					// seal 收尾，客户端把残缺 tool_call 视为完成并执行，循环得以继续。
					log.Printf("req=%d %s: EOF, tool_call truncated -> seal (saw_tool=%v tool_closed=%v injected=%v)", reqID, name, sawTool, toolClosed, injected)
					if err := write(finishToolCall()); err != nil {
						break loop
					}
					doneSent = true
				} else if !doneSent {
					log.Printf("req=%d %s: EOF, no fatal, sealing [DONE] (saw_tool=%v tool_closed=%v injected=%v)", reqID, name, sawTool, toolClosed, injected)
					if err := write([]byte("data: [DONE]\n\n")); err != nil {
						break loop
					}
					doneSent = true
				}
				break loop
			}
			if m.err != nil {
				log.Printf("req=%d %s: upstream stream error: %v", reqID, name, m.err)
				u.setCooldown(cooldownShort)
				u.setErr("stream error")
				// 硬断流与 EOF 同等对待：未产出 tool_call 时注入续流 tool_call，
				// 否则客户端收到截断流而不会 echo 继续。
				if canInject && !sawTool && !injected {
					log.Printf("req=%d %s: stream error without tool_call -> inject (saw_tool=%v injected=%v)", reqID, name, sawTool, injected)
					if err := write(infInject()); err == nil {
						if err := write([]byte("data: [DONE]\n\n")); err == nil {
							injected = true
							doneSent = true
						}
					}
				} else if sawTool && !toolClosed {
					log.Printf("req=%d %s: tool_call truncated by error -> seal (saw_tool=%v tool_closed=%v injected=%v)", reqID, name, sawTool, toolClosed, injected)
					if err := write(finishToolCall()); err == nil {
						doneSent = true
					}
				} else if !doneSent {
					log.Printf("req=%d %s: stream error, sealing [DONE] (saw_tool=%v tool_closed=%v injected=%v)", reqID, name, sawTool, toolClosed, injected)
					if err := write([]byte("data: [DONE]\n\n")); err == nil {
						doneSent = true
					}
				}
				break loop
			}
			buf = append(buf, m.data...)
		case <-timer.C:
			// stall
			stall := stallFor(sawTool)
			if time.Since(lastReal) >= stall {
				log.Printf("req=%d %s: idle %s (no real data, saw_tool=%v tool_closed=%v injected=%v)", reqID, name, stall.Round(time.Second), sawTool, toolClosed, injected)
				if (canInject || recoverable) && !sawTool && !injected {
					write(idleInject())
					injected = true
					log.Printf("req=%d SUCCESS (idle-timeout inject)", reqID)
					resp.Body.Close()
					return true, true
				}
				if sawTool && !toolClosed {
					// 工具流截断：补发终止事件再断开，客户端能区分「完成」与「截断」
					write(lengthChunk())
					doneSent = true
				} else if injected {
					// 已注入 tool_call（finish_reason=tool_calls），上游 [DONE] 未到就 stall，
					// 只补 [DONE]，避免客户端认为流被截断
					write([]byte("data: [DONE]\n\n"))
					doneSent = true
				} else if !doneSent {
					// 非注入请求（canInject=false）stall：兜底补 [DONE]，避免截断流
					log.Printf("req=%d %s: idle, sealing [DONE]", reqID, name)
					write([]byte("data: [DONE]\n\n"))
					doneSent = true
				}
				log.Printf("req=%d FAIL (idle, no inject possible)", reqID)
				resp.Body.Close()
				return false, true
			}
			timer.Reset(stall - time.Since(lastReal))
		}
	}

	resp.Body.Close()
	log.Printf("req=%d %s: done fwd=%dB saw_tool=%v tool_closed=%v injected=%v (%.2fs)", reqID, name, total, sawTool, toolClosed, injected, time.Since(t0).Seconds())
	log.Printf("req=%d SUCCESS", reqID)
	return true, true
}

// order returns the source list, shuffled with `forced` first.
func order(forced string) []*upstream {
	perm := rand.Perm(len(upList))
	out := make([]*upstream, 0, len(upList))
	var forcedU *upstream
	for _, i := range perm {
		u := upList[i]
		if u.name == forced {
			forcedU = u
			continue
		}
		out = append(out, u)
	}
	if forcedU != nil {
		return append([]*upstream{forcedU}, out...)
	}
	return out
}

func toolsOf(p map[string]any) []any {
	t, _ := p["tools"].([]any)
	return t
}

func nextUTCMidnight() time.Time {
	now := time.Now().UTC()
	return time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC).AddDate(0, 0, 1)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	h := w.Header()
	h.Set("Content-Type", "application/json")
	h.Set("Access-Control-Allow-Origin", "*")
	h.Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
	h.Set("Access-Control-Allow-Headers", "Content-Type, Authorization")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

// ---- HTTP server -----------------------------------------------------------

func (s *server) handlerMulti() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/chat/completions", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "OPTIONS" {
			writeJSON(w, 204, map[string]any{})
			return
		}
		s.handleProxy(w, r)
	})
	mux.HandleFunc("/chat/completions", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "OPTIONS" {
			writeJSON(w, 204, map[string]any{})
			return
		}
		s.handleProxy(w, r)
	})
	mux.HandleFunc("/v1/models", func(w http.ResponseWriter, r *http.Request) {
		data := make([]map[string]any, 0)
		for _, m := range sourceModels() {
			data = append(data, map[string]any{"id": m["id"], "object": "model", "created": 0, "owned_by": "zen"})
		}
		writeJSON(w, 200, map[string]any{"object": "list", "data": data})
	})
	mux.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) {
		now := time.Now()
		srcs := map[string]any{}
		for _, u := range upList {
			u.mu.Lock()
			cd := u.cooldownUntil.Sub(now).Seconds()
			reqs := u.reqs
			lastErr := u.lastErr
			inTok, outTok := u.inTok, u.outTok
			cacheHit, cacheMiss := u.cacheHit, u.cacheMiss
			u.mu.Unlock()
			if cd < 0 {
				cd = 0
			}
			srcs[u.name] = map[string]any{"cooldown_sec": int(cd), "reqs": reqs, "last_err": lastErr,
				"in_tokens": inTok, "out_tokens": outTok,
				"cache_hit_tokens": cacheHit, "cache_miss_tokens": cacheMiss}
		}
		writeJSON(w, 200, map[string]any{"status": "ok", "sources": srcs})
	})
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "OPTIONS" {
			writeJSON(w, 204, map[string]any{})
			return
		}
		w.Header().Set("Content-Type", "text/plain")
		w.WriteHeader(200)
		w.Write([]byte("Zen multi proxy running\n"))
	})
	return mux
}

func main() {
	host := "127.0.0.1"
	port := 8443
	if len(os.Args) > 1 {
		host = os.Args[1]
	}
	if len(os.Args) > 2 {
		fmt.Sscanf(os.Args[2], "%d", &port)
	}

	for _, u := range upList {
		dialer := &net.Dialer{Timeout: connectTimeout, KeepAlive: 30 * time.Second}
		tr := &http.Transport{
			DialContext:           dialer.DialContext,
			ResponseHeaderTimeout: headerTimeout,
			IdleConnTimeout:       idleConnTimeout,
			MaxIdleConnsPerHost:   2,
			ForceAttemptHTTP2:     true,
		}
		u.client = &http.Client{Transport: tr}
	}

	s := &server{}
	srv := &http.Server{
		Addr:              fmt.Sprintf("%s:%d", host, port),
		Handler:           s.handlerMulti(),
		ReadHeaderTimeout: 15 * time.Second,
	}
	names := make([]string, 0, len(upList))
	for _, u := range upList {
		names = append(names, u.name)
	}
	log.Printf("Zen multi proxy sources: %s", strings.Join(names, ", "))
	log.Printf("Listening on %s:%d", host, port)
	if err := srv.ListenAndServe(); err != nil {
		log.Fatal(err)
	}
}
