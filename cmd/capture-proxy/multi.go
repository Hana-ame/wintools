// Zen Multi in Go — local multi-source aggregator that forwards to the
// Go zen_proxies (bwh/vps/cloudcone) with failover, cooldown and the
// Replaces zen_multi.py.
package main

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Hana-ame/wintools/pkg/proxyheaders"
)

// ===== 默认源列表 (暂时硬编码, 发布后按实际部署改这里) =====
// 格式 "源名=https://主机:端口"。三台 VPS × v4/v6; 每台拆两个 provider
// 实例 (--mode v4 / --mode v6) 后端口会不同, 改这里即可。
var defaultSources = []string{
	"cloudcone-v4=https://cloudcone.moonchan.xyz:8443",
	"cloudcone-v6=https://cloudcone.moonchan.xyz:8443",
	"bwh-v4=https://bwh.moonchan.xyz:8443",
	"bwh-v6=https://bwh.moonchan.xyz:8443",
	"vps-v4=https://vps.moonchan.xyz:8443",
	"vps-v6=https://vps.moonchan.xyz:8443",
}

const (
	baseModel   = "deepseek-v4-flash-free"
	modelPrefix = "deepseek-v4-flash"

	// preReadTimeout 预读首 token 的上限: multi 角色保持 10s (与 zen-proxy 一致,
	// 快速 failover), local 角色为 30s。
	preReadTimeout = 10 * time.Second

	headerTimeout = 30 * time.Second
	cooldownShort = 60 * time.Second
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

// setMode 通过 capture_proxy 的 /mode API 切换该源的模式 (v4|v6|auto)。
func (u *upstream) setMode(mode string) error {
	if u.client == nil {
		return fmt.Errorf("no http client")
	}
	body := strings.NewReader(`{"mode":"` + mode + `"}`)
	req, err := http.NewRequest("POST", u.base+"/mode", body)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := u.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		data, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return fmt.Errorf("status %d: %s", resp.StatusCode, strings.TrimSpace(string(data)))
	}
	return nil
}

// ---- SSE helpers (shared with zen_proxy.go) --------------------------------

// eventFinishReason returns the first non-nil finish_reason in the event.

// eventUsage extracts the usage object from an SSE event, if present.
// Streams carry it in the final chunk (usage:null everywhere else).

// eventUsage extracts the usage object from an SSE event, if present.
// Streams carry it in the final chunk (usage:null everywhere else).
func eventUsage(ev []byte) *openaiUsage {
	for _, line := range bytes.Split(ev, []byte("\n")) {
		if !bytes.HasPrefix(line, []byte("data: ")) {
			continue
		}
		payload := line[len("data: "):]
		if bytes.Equal(payload, []byte("[DONE]")) {
			continue
		}
		var obj struct {
			Usage *openaiUsage `json:"usage"`
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

// openaiUsage mirrors the OpenAI-style usage block.
type openaiUsage struct {
	Prompt    int64 `json:"prompt_tokens"`
	Completed int64 `json:"completion_tokens"`
	Total     int64 `json:"total_tokens"`
	CacheHit  int64 `json:"prompt_cache_hit_tokens"`
	CacheMiss int64 `json:"prompt_cache_miss_tokens"`
}

// addUsage accumulates per-source token totals.
func (u *upstream) addUsage(us *openaiUsage) {
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

// stallFor 返回「两个真实 SSE 事件之间允许的最大间隔」：
// 首 token 之后按 token 节奏收敛到 tokenGapTimeout；工具流保留更长思考窗口。

// finishToolCall seals a truncated tool call with finish_reason=tool_calls,
// so the client executes the (possibly partial) tool call and the loop continues.

// lengthChunk 工具流 stall 截断时补发的终止事件：finish_reason=length + [DONE]。
// 与注入事件一致使用 baseModel。

// ---- model resolution ------------------------------------------------------

func resolveModel(model string) (forced string, resolved string) {
	if strings.HasPrefix(model, modelPrefix+"-") {
		src := model[len(modelPrefix)+1:]
		for _, u := range upList {
			if u.name == src {
				return src, baseModel
			}
		}
		return src, model
	}
	return "", model
}

// upList 聚合多个 capture_proxy 实例 (本地 :8000 等), 每个实例可独立
// v4/v6/auto 模式。默认一个本地 capture_proxy; 可用 --source name=base 追加。
var upList = []*upstream{}

func sourceModels() []map[string]any {
	models := []map[string]any{}
	for _, u := range upList {
		models = append(models, map[string]any{"id": modelPrefix + "-" + u.name, "name": "DeepSeek V4 Flash (" + u.name + ")"})
	}
	return models
}

// ---- request handling ------------------------------------------------------

// server carries per-instance state (currently stateless).

// multiCORS zen-multi 原版受限 CORS (与 local 角色的全放行版不同)。
func multiCORS(h http.Header) {
	h.Set("Access-Control-Allow-Origin", "*")
	h.Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
	h.Set("Access-Control-Allow-Headers", "Content-Type, Authorization")
}

func multiWriteJSON(w http.ResponseWriter, status int, v any) {
	h := w.Header()
	h.Set("Content-Type", "application/json")
	multiCORS(h)
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

type multiServer struct{}

// reqSeq 为每个请求生成递增序号，贯穿所有日志，方便并发请求下的追踪。
var reqSeq atomic.Uint64

func (s *multiServer) handleProxy(w http.ResponseWriter, r *http.Request) {
	reqID := reqSeq.Add(1)
	log.Printf("req=%d -> %s %s", reqID, r.Method, r.URL.Path)
	body, err := readBody(r)
	if err != nil {
		status := 400
		msg := "Bad Request"
		if err == errTooLarge {
			status = http.StatusRequestEntityTooLarge
			msg = "Request body too large"
		}
		multiWriteJSON(w, status, map[string]any{"error": msg})
		return
	}

	isStream := false
	var payload map[string]any
	bodyStr := string(body)
	var forced string
	if len(body) > 0 {
		if err := json.Unmarshal(body, &payload); err != nil {
			multiWriteJSON(w, 400, map[string]any{"error": "Invalid JSON"})
			return
		}
		model, _ := payload["model"].(string)
		forced, model = resolveModel(model)
		payload["model"] = model
		if mt, ok := payload["max_tokens"].(float64); !ok || mt > 131072 {
			payload["max_tokens"] = 131072
		}
		isStream, _ = payload["stream"].(bool)
		re, _ := json.Marshal(payload)
		bodyStr = string(re)
		log.Printf("req=%d model=%v src=%v stream=%v max_tokens=%v tools=%d", reqID, payload["model"], forced, isStream, payload["max_tokens"], len(toolsOf(payload)))
	}

	// 不 failover: 一次请求只打一个源。模型必须带源 (deepseek-v4-flash-<name>), 不带源 400。
	lastErrBody := any(nil)
	lastStatus := 0
	limitErr := any(nil)
	lastHeaders := http.Header{}

	var u *upstream
	if forced == "" {
		log.Printf("req=%d model without source -> 400", reqID)
		multiWriteJSON(w, 400, map[string]any{"error": map[string]any{"message": "model must specify a source: " + modelPrefix + "-<name>", "type": "InvalidModel"}})
		return
	}
	u = nil
	for _, cand := range upList {
		if cand.name == forced {
			u = cand
			break
		}
	}
	if u == nil {
		log.Printf("req=%d unknown source %q -> 404", reqID, forced)
		multiWriteJSON(w, 404, map[string]any{"error": map[string]any{"message": "unknown source: " + forced, "type": "UpstreamError"}})
		return
	}

	if u.inCooldown(time.Now()) {
		log.Printf("req=%d %s: in cooldown -> 429", reqID, u.name)
		multiWriteJSON(w, 429, map[string]any{"error": map[string]any{"message": u.name + " in cooldown", "type": freeLimitErr}})
		return
	}

	ok := s.tryUpstream(w, u, r.Method, bodyStr, r.Header, len(body) > 0, isStream, &lastErrBody, &lastStatus, &limitErr, &lastHeaders, reqID)
	if ok {
		return
	}
	if limitErr != nil {
		log.Printf("req=%d %s: FreeUsageLimitError -> 429", reqID, u.name)
		multiWriteJSON(w, 429, limitErr)
		return
	}
	if lastErrBody != nil {
		proxyheaders.MergeHeaders(w.Header(), lastHeaders)
		multiWriteJSON(w, lastStatus, lastErrBody)
		return
	}
	log.Printf("req=%d %s: upstream unavailable -> 503", reqID, u.name)
	multiWriteJSON(w, 503, map[string]any{"error": map[string]any{"message": u.name + " unavailable", "type": "UpstreamError"}})
}

// tryUpstream forwards to one source. Returns true if fully handled.
func (s *multiServer) tryUpstream(w http.ResponseWriter, u *upstream, method, bodyStr string, head http.Header, hasBody, isStream bool, lastErrBody *any, lastStatus *int, limitErr *any, lastHeaders *http.Header, reqID uint64) bool {
	path := "/chat/completions"
	if !hasBody {
		path = "/v1/models"
	}
	// 源名带 -v4/-v6 后缀: 转发时带 stack 参数, 让 provider 绕过 mode 直接走指定栈。
	stack := ""
	if strings.HasSuffix(u.name, "-v4") {
		stack = "v4"
	} else if strings.HasSuffix(u.name, "-v6") {
		stack = "v6"
	}
	if stack != "" {
		path += "?stack=" + stack
	}
	var req *http.Request
	var err error
	// 请求体 gzip 压缩后转发（zen-proxy 会解压），压缩失败则按原文发送。
	if hasBody {
		var buf bytes.Buffer
		zw := gzip.NewWriter(&buf)
		if _, werr := zw.Write([]byte(bodyStr)); werr == nil && zw.Close() == nil {
			bodyStr = buf.String()
			req, err = http.NewRequest(method, u.base+path, strings.NewReader(bodyStr))
			if err != nil {
				log.Printf("req=%d %s: new request failed: %v", reqID, u.name, err)
				return false
			}
			req.Header.Set("Content-Encoding", "gzip")
		} else {
			req, err = http.NewRequest(method, u.base+path, strings.NewReader(bodyStr))
			if err != nil {
				log.Printf("req=%d %s: new request failed: %v", reqID, u.name, err)
				return false
			}
		}
	} else {
		req, err = http.NewRequest(method, u.base+path, nil)
		if err != nil {
			log.Printf("req=%d %s: new request failed: %v", reqID, u.name, err)
			return false
		}
	}
	req.Header.Set("Content-Type", "application/json")
	proxyheaders.ForwardRequestHeaders(req.Header, head)
	if a := head.Get("Authorization"); a != "" {
		req.Header.Set("Authorization", a)
	}

	log.Printf("req=%d %s: connecting... (gzip_body=%v)", reqID, u.name, req.Header.Get("Content-Encoding") == "gzip")
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

	ok, committed := s.forwardStream(w, u, resp, reqID)
	// committed=true 表示响应头已提交，此时不能再 failover（会双写响应）。
	return ok || committed
}

// forwardStream relays the SSE stream with keep-alive filtering, stall
// detection and tool-call protection.
// 返回 (ok, committed)：committed 表示响应头已提交给客户端。
// 一旦 committed，调用方禁止再 failover 到其他源（否则会双写响应）。
func (s *multiServer) forwardStream(w http.ResponseWriter, u *upstream, resp *http.Response, reqID uint64) (bool, bool) {
	name := u.name
	done := make(chan struct{})
	defer close(done)
	ch := startReader(resp.Body, done)
	t0 := time.Now()

	// Pre-read.
	deadline := time.Now().Add(preReadTimeout)
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
		log.Printf("req=%d %s: no real data in %.0fs, try next source", reqID, name, preReadTimeout.Seconds())
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
	lastReal := time.Now()
	timer := time.NewTimer(stallFor(false, tokenGapTimeout))
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
				log.Printf("req=%d %s: mid-stream error event dropped", reqID, name)
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
				timer.Reset(stallFor(sawTool, tokenGapTimeout))
			}

			if err := write(append(ev, '\n', '\n')); err != nil {
				log.Printf("req=%d client disconnected", reqID)
				break loop
			}
		}

		select {
		case m, ok := <-ch:
			if !ok || m.eof {
				if sawTool && !toolClosed {
					// 工具调用被上游截断（未等到 finish_reason=tool_calls/[DONE]）：
					// seal 收尾，客户端把残缺 tool_call 视为完成并执行，循环得以继续。
					log.Printf("req=%d %s: EOF, tool_call truncated -> seal (saw_tool=%v tool_closed=%v)", reqID, name, sawTool, toolClosed)
					if err := write(finishToolCall(baseModel)); err != nil {
						break loop
					}
					doneSent = true
				} else if !doneSent {
					log.Printf("req=%d %s: EOF, no fatal, sealing [DONE] (saw_tool=%v tool_closed=%v)", reqID, name, sawTool, toolClosed)
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
				if sawTool && !toolClosed {
					log.Printf("req=%d %s: tool_call truncated by error -> seal (saw_tool=%v tool_closed=%v)", reqID, name, sawTool, toolClosed)
					if err := write(finishToolCall(baseModel)); err == nil {
						doneSent = true
					}
				} else if !doneSent {
					log.Printf("req=%d %s: stream error, sealing [DONE] (saw_tool=%v tool_closed=%v)", reqID, name, sawTool, toolClosed)
					if err := write([]byte("data: [DONE]\n\n")); err == nil {
						doneSent = true
					}
				}
				break loop
			}
			buf = append(buf, m.data...)
		case <-timer.C:
			// stall
			stall := stallFor(sawTool, tokenGapTimeout)
			if time.Since(lastReal) >= stall {
				log.Printf("req=%d %s: idle %s (no real data, saw_tool=%v tool_closed=%v)", reqID, name, stall.Round(time.Second), sawTool, toolClosed)
				if sawTool && !toolClosed {
					write(lengthChunk(baseModel))
					doneSent = true
				} else if !doneSent {
					log.Printf("req=%d %s: idle, sealing [DONE]", reqID, name)
					write([]byte("data: [DONE]\n\n"))
					doneSent = true
				}
				log.Printf("req=%d FAIL (idle)", reqID)
				resp.Body.Close()
				return false, true
			}
			timer.Reset(stall - time.Since(lastReal))
		}
	}

	resp.Body.Close()
	log.Printf("req=%d %s: done fwd=%dB saw_tool=%v tool_closed=%v (%.2fs)", reqID, name, total, sawTool, toolClosed, time.Since(t0).Seconds())
	log.Printf("req=%d SUCCESS", reqID)
	return true, true
}

func (s *multiServer) handlerMulti() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/chat/completions", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "OPTIONS" {
			multiWriteJSON(w, 204, map[string]any{})
			return
		}
		s.handleProxy(w, r)
	})
	mux.HandleFunc("/chat/completions", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "OPTIONS" {
			multiWriteJSON(w, 204, map[string]any{})
			return
		}
		s.handleProxy(w, r)
	})
	mux.HandleFunc("/v1/models", func(w http.ResponseWriter, r *http.Request) {
		data := make([]map[string]any, 0)
		for _, m := range sourceModels() {
			data = append(data, map[string]any{"id": m["id"], "object": "model", "created": 0, "owned_by": "zen"})
		}
		multiWriteJSON(w, 200, map[string]any{"object": "list", "data": data})
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
				"cache_hit_tokens": cacheHit, "cache_miss_tokens": cacheMiss,
				"base": u.base}
		}
		multiWriteJSON(w, 200, map[string]any{"status": "ok", "sources": srcs})
	})
	// 模式控制: 聚合多个 capture_proxy, 一个开关同步到所有源。
	//   POST /mode?mode=v4|v6|auto   切换所有源的模式
	//   POST /mode  body {"mode":"v6"}
	//   GET  /mode                   返回当前各源模式 + 上游 cooldown
	mux.HandleFunc("/mode", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "OPTIONS" {
			multiWriteJSON(w, 204, map[string]any{})
			return
		}
		if r.Method == "POST" || r.Method == "PUT" || r.Method == "PATCH" {
			m := r.URL.Query().Get("mode")
			if m == "" {
				if b, err := io.ReadAll(io.LimitReader(r.Body, 4096)); err == nil && len(b) > 0 {
					var v struct {
						Mode string `json:"mode"`
					}
					if json.Unmarshal(b, &v) == nil {
						m = v.Mode
					}
				}
			}
			if m == "" {
				multiWriteJSON(w, 400, map[string]any{"error": "mode required (v4|v6|auto)"})
				return
			}
			if m != "v4" && m != "v6" && m != "auto" {
				multiWriteJSON(w, 400, map[string]any{"error": "mode must be v4|v6|auto"})
				return
			}
			fail := 0
			for _, u := range upList {
				if err := u.setMode(m); err != nil {
					log.Printf("%s setMode %s: %v", u.name, m, err)
					fail++
				} else {
					log.Printf("%s mode -> %s", u.name, m)
				}
			}
			if fail > 0 && fail == len(upList) {
				multiWriteJSON(w, 502, map[string]any{"error": "all sources failed to switch mode"})
				return
			}
		}
		now := time.Now()
		srcs := map[string]any{}
		for _, u := range upList {
			u.mu.Lock()
			cd := u.cooldownUntil.Sub(now).Seconds()
			u.mu.Unlock()
			if cd < 0 {
				cd = 0
			}
			srcs[u.name] = map[string]any{"base": u.base, "cooldown_sec": int(cd)}
		}
		multiWriteJSON(w, 200, map[string]any{"status": "ok", "mode": "aggregated", "sources": srcs})
	})
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "OPTIONS" {
			multiWriteJSON(w, 204, map[string]any{})
			return
		}
		w.Header().Set("Content-Type", "text/plain")
		w.WriteHeader(200)
		w.Write([]byte("Zen multi proxy running\n"))
	})
	return mux
}

func runMulti(args []string) {
	// --flags 而非 argv
	fs := flag.NewFlagSet("multi", flag.ExitOnError)
	listen := fs.String("listen", "127.0.0.1:8443", "监听地址")
	fs.Parse(args)

	for _, src := range defaultSources {
		name, base, ok := strings.Cut(src, "=")
		if !ok {
			log.Fatalf("invalid --source %q: want name=base", src)
		}
		name = strings.TrimSpace(name)
		base = strings.TrimRight(strings.TrimSpace(base), "/")
		if name == "" || base == "" {
			log.Fatalf("invalid --source %q: name and base required", src)
		}
		upList = append(upList, &upstream{name: name, base: base})
	}

	host, port, err := net.SplitHostPort(*listen)
	if err != nil {
		log.Fatalf("invalid --listen %q: %v", *listen, err)
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

	s := &multiServer{}
	srv := &http.Server{
		Addr:              net.JoinHostPort(host, port),
		Handler:           s.handlerMulti(),
		ReadHeaderTimeout: 15 * time.Second,
	}
	names := make([]string, 0, len(upList))
	for _, u := range upList {
		names = append(names, u.name+"@"+u.base)
	}
	log.Printf("Zen multi proxy sources: %s", strings.Join(names, ", "))
	log.Printf("Listening on %s", *listen)
	if err := srv.ListenAndServe(); err != nil {
		log.Fatal(err)
	}
}
