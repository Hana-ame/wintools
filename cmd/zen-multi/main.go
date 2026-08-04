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
	"time"
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
	stallTimeout     = 30 * time.Second
	toolStallTimeout = 180 * time.Second
	cooldownShort    = 60 * time.Second
	maxRetries       = 3
	idleConnTimeout  = 90 * time.Second
)

type upstream struct {
	name string
	base string

	mu            sync.Mutex
	cooldownUntil time.Time
	lastErr       string
	reqs          int64

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
	return []byte("data: " + inject + "\n\ndata: [DONE]\n\n")
}

func jsonString(s string) string {
	re, _ := json.Marshal(s)
	return string(re)
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

func (s *server) handleProxy(w http.ResponseWriter, r *http.Request) {
	log.Printf("-> %s %s", r.Method, r.URL.Path)
	body, err := io.ReadAll(r.Body)
	if err != nil {
		writeJSON(w, 400, map[string]any{"error": "Bad Request"})
		return
	}

	isStream := false
	var payload map[string]any
	bodyStr := string(body)
	var forced string
	canInject := false
	if len(body) > 0 {
		if err := json.Unmarshal(body, &payload); err != nil {
			writeJSON(w, 400, map[string]any{"error": "Invalid JSON"})
			return
		}
		model, _ := payload["model"].(string)
		isInf := model == infModel
		canInject = isInf && len(toolsOf(payload)) > 0
		forced, model = resolveModel(model)
		payload["model"] = model
		if mt, ok := payload["max_tokens"].(float64); !ok || mt > 65536 {
			payload["max_tokens"] = 65536
		}
		isStream, _ = payload["stream"].(bool)
		re, _ := json.Marshal(payload)
		bodyStr = string(re)
		log.Printf("req model=%v src=%v stream=%v tools=%d can_inject=%v", payload["model"], forced, isStream, len(toolsOf(payload)), canInject)
	}

	lastErrBody := any(nil)
	lastStatus := 0
	limitErr := any(nil)

	for attempt := 0; attempt < maxRetries; attempt++ {
		for _, u := range order(forced) {
			if u.inCooldown(time.Now()) {
				continue
			}
			ok := s.tryUpstream(w, u, r.Method, bodyStr, len(body) > 0, isStream, canInject, &lastErrBody, &lastStatus, &limitErr)
			if ok {
				return
			}
		}
	}

	if limitErr != nil {
		writeJSON(w, 429, limitErr)
		return
	}
	if lastErrBody != nil {
		writeJSON(w, lastStatus, lastErrBody)
		return
	}
	writeJSON(w, 503, map[string]any{"error": map[string]any{"message": "All upstream sources exhausted or unavailable", "type": "UpstreamError"}})
}

// tryUpstream forwards to one source. Returns true if fully handled.
func (s *server) tryUpstream(w http.ResponseWriter, u *upstream, method, bodyStr string, hasBody, isStream, canInject bool, lastErrBody *any, lastStatus *int, limitErr *any) bool {
	path := "/chat/completions"
	if !hasBody {
		path = "/v1/models"
	}
	var req *http.Request
	var err error
	req, err = http.NewRequest(method, u.base+path, strings.NewReader(bodyStr))
	if err != nil {
		return false
	}
	req.Header.Set("Content-Type", "application/json")

	log.Printf("%s: connecting...", u.name)
	resp, err := u.client.Do(req)
	if err != nil {
		log.Printf("%s: %T", u.name, err)
		u.setCooldown(cooldownShort)
		u.lastErr = err.Error()
		*lastStatus = 502
		*lastErrBody = map[string]any{"error": map[string]any{"message": err.Error(), "type": "UpstreamError"}}
		return false
	}
	log.Printf("%s: connected status=%d", u.name, resp.StatusCode)

	if resp.StatusCode != 200 {
		data, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		var obj map[string]any
		_ = json.Unmarshal(data, &obj)
		if e, ok := obj["error"].(map[string]any); ok && e["type"] == freeLimitErr {
			log.Printf("%s: FreeUsageLimitError (cooldown to midnight)", u.name)
			u.setCooldownUntil(nextUTCMidnight())
			u.lastErr = freeLimitErr
			*limitErr = obj
			return false
		}
		log.Printf("%s: HTTP %d -> try next source", u.name, resp.StatusCode)
		u.lastErr = fmt.Sprintf("HTTP %d", resp.StatusCode)
		*lastStatus = resp.StatusCode
		if len(obj) > 0 {
			*lastErrBody = obj
		} else {
			*lastErrBody = map[string]any{"error": map[string]any{"message": string(data)}}
		}
		return false
	}

	u.reqs++
	u.lastErr = ""

	if !isStream {
		data, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		h := w.Header()
		h.Set("Content-Type", "application/json")
		h.Set("Access-Control-Allow-Origin", "*")
		w.WriteHeader(200)
		w.Write(data)
		log.Printf("%s: done (non-stream)", u.name)
		return true
	}

	return s.forwardStream(w, u, resp, canInject)
}

// forwardStream relays the SSE stream with keep-alive filtering, stall
// detection, tool-call protection and inf-loop injection.
func (s *server) forwardStream(w http.ResponseWriter, u *upstream, resp *http.Response, canInject bool) bool {
	name := u.name
	ch := startReader(resp.Body)
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
					log.Printf("%s: pre-read error: %v", name, m.err)
				}
				u.setCooldown(cooldownShort)
				u.lastErr = "pre-read error"
				resp.Body.Close()
				return false
			}
			pre = append(pre, m.data...)
			hasReal = hasRealSSE(pre)
		case <-timer.C:
		}
	}
	if !hasReal {
		log.Printf("%s: no real data in %.0fs, try next source", name, stallTimeout.Seconds())
		u.setCooldown(cooldownShort)
		u.lastErr = "first token stall"
		resp.Body.Close()
		return false
	}
	log.Printf("%s: first token t=%.2fs", name, time.Since(t0).Seconds())

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
				log.Printf("%s: SSE error event, try next source", name)
				u.setCooldown(cooldownShort)
				u.lastErr = "SSE error"
				resp.Body.Close()
				return false
			}
		}
	}

	flusher, _ := w.(http.Flusher)
	h := w.Header()
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
			if eventHasToolCall(ev) {
				sawTool = true
			}
			if eventHasError(ev) {
				log.Printf("%s: mid-stream error event dropped", name)
				continue
			}
			lastReal = time.Now()
			timer.Reset(stallFor(sawTool))

			if canInject && !sawTool && !injected {
				if fr := eventFinishReason(ev); fr != nil && *fr != "tool_calls" {
					if err := write(append([]byte("data: "), neutralizeFinish(ev)...)); err != nil {
						log.Printf("client disconnected")
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
					if err := write(infInject()); err != nil {
						break loop
					}
					injected = true
				}
			}

			if err := write(append(ev, '\n', '\n')); err != nil {
				log.Printf("client disconnected")
				break loop
			}
		}

		select {
		case m, ok := <-ch:
			if !ok || m.eof {
				// EOF: inject if the stream ended without any tool call.
				if canInject && !sawTool && !injected {
					log.Printf("%s: stream ended without tool_call, injecting at EOF", name)
					if err := write(infInject()); err != nil {
						break loop
					}
					injected = true
				}
				break loop
			}
			if m.err != nil {
				log.Printf("%s: unexpected %v", name, m.err)
				u.setCooldown(cooldownShort)
				u.lastErr = "stream error"
				break loop
			}
			buf = append(buf, m.data...)
		case <-timer.C:
			// stall
			stall := stallFor(sawTool)
			if time.Since(lastReal) >= stall {
				log.Printf("%s: stream idle %s (no real data, saw_tool=%v)", name, stall.Round(time.Second), sawTool)
				if canInject && !sawTool && !injected {
					write(idleInject())
					injected = true
					log.Printf("SUCCESS (idle-timeout inject)")
					resp.Body.Close()
					return true
				}
				log.Printf("FAIL")
				resp.Body.Close()
				return false
			}
			timer.Reset(stall - time.Since(lastReal))
		}
	}

	resp.Body.Close()
	log.Printf("%s: done (source=%s) fwd=%dB", name, name, total)
	log.Printf("SUCCESS")
	return true
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
			u.mu.Unlock()
			if cd < 0 {
				cd = 0
			}
			srcs[u.name] = map[string]any{"cooldown_sec": int(cd), "reqs": u.reqs, "last_err": u.lastErr}
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
