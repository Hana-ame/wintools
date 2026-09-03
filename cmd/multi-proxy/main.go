// multi-proxy — 多源聚合转发器 (极简版, 由旧 zen-multi 恢复重写)。
//
// 只做一件事: 把 /chat/completions 请求按顺序尝试转发到多个 capture-proxy
// 源 (源都是 auto 模式, 协议族 failover 由源自身负责), 连接失败/非 200/
// 流式首块超时才换下一个源。旧 zen-multi 的 SSE 解析/注入/stall 检测/
// 模型源名路由 (deepseek-v4-flash-<name>) /mode 同步全部删除——
// 纯转发, capture-proxy 自己处理这些。
//
// Fallback 语义 (用户明确要求): 按 --source 顺序使用, 先用第一个;
// 该源出现 exceed (429 或 FreeUsageLimitError / daily free usage limit)
// 后设冷却到当日 UTC 午夜 (与 capture-proxy 每日 UTC 午夜重置一致,
// 即"用尽即停, 当天不再碰它"), 后续请求自动换第二个;
// 全部源 exceed/不可用后返回 429 (exceeded), 不注入、不透传具体错误。
//
// 用法:
//
//	multi-proxy --listen 127.0.0.1:8000 \
//	  --source vps=https://vps.moonchan.xyz:8443 \
//	  --source cloudcone=https://cloudcone.moonchan.xyz:8443
//
// API:
//
//	POST /chat/completions, /v1/chat/completions  多源 failover 转发
//	GET  /v1/models                               转发第一个健康源
//	GET  /status                                  各源状态聚合
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
	"os"
	"strings"
	"sync"
	"time"

	"github.com/Hana-ame/wintools/pkg/proxyheaders"
)

const (
	connectTimeout  = 15 * time.Second
	headerTimeout   = 30 * time.Second
	idleConnTimeout = 90 * time.Second

	// cooldownShort 连接级失败的冷却: 挂掉的源 60s 内不再尝试,
	// 避免每个请求都撞一遍超时 (旧 zen-multi 同款)。
	cooldownShort = 60 * time.Second
	// preReadTimeout 流式响应首块预读上限: 超过即视为该源不可用,
	// 响应头还没提交, 可以安全换源。
	preReadTimeout = 10 * time.Second
	// maxBody 客户端请求体上限, 防恶意大请求拖垮转发。
	maxBody = 64 << 20
)

// nextUTCMidnight 下一个 UTC 午夜: 与 capture-proxy 的每日统计重置时刻一致,
// exceed 冷却持续到那时, 到期后源自动恢复可用。
func nextUTCMidnight() time.Time {
	now := time.Now().UTC()
	return time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC).AddDate(0, 0, 1)
}

// isExceed 判断上游响应是否表示"配额用尽" (需换源且当天不再用)。
// 匹配 capture-proxy 的 429 响应形态 (local.go: 全栈耗尽统一 429,
// error.type=FreeUsageLimitError, 或 Banned; 以及 body 里的 daily free
// usage limit 文案)。
func isExceed(status int, body []byte) bool {
	if status == http.StatusTooManyRequests {
		return true
	}
	for _, needle := range []string{"FreeUsageLimitError", "daily free usage limit", "exceeded", "Banned"} {
		if bytes.Contains(body, []byte(needle)) {
			return true
		}
	}
	return false
}

// upstream 一个 capture-proxy 源。
type upstream struct {
	name string
	base string

	mu            sync.Mutex
	cooldownUntil time.Time
	lastErr       string
	reqs          int64
	exceeded      bool
	// 最近一次失败的上游响应 (状态/错误体/响应头), 供 handleProxy
	// 全源失败时聚合出最贴近上游的错误响应。
	lastStatus     int
	lastErrBody    any
	lastErrHeaders http.Header

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

func (u *upstream) setErr(e string) {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.lastErr = e
}

func (u *upstream) incrReqs() {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.reqs++
}

var upList []*upstream

// ---- handlers ---------------------------------------------------------------

type server struct{}

func cors(h http.Header) {
	h.Set("Access-Control-Allow-Origin", "*")
	h.Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
	h.Set("Access-Control-Allow-Headers", "Content-Type, Authorization")
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	h := w.Header()
	h.Set("Content-Type", "application/json")
	cors(h)
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

func (s *server) handleProxy(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(io.LimitReader(r.Body, maxBody+1))
	if err != nil {
		writeJSON(w, 400, map[string]any{"error": "read body failed"})
		return
	}
	if len(body) > maxBody {
		writeJSON(w, http.StatusRequestEntityTooLarge, map[string]any{"error": "request body too large"})
		return
	}
	// 只读 stream 字段决定预读策略, 不改动请求体。
	isStream := false
	if len(body) > 0 {
		var payload map[string]any
		if err := json.Unmarshal(body, &payload); err != nil {
			writeJSON(w, 400, map[string]any{"error": "invalid JSON"})
			return
		}
		isStream, _ = payload["stream"].(bool)
	}

	// 顺序尝试; 冷却中的源直接跳过, 全源不可用时按最后一个错误响应。
	// fallback 语义: 遇 exceed (429/FreeUsageLimitError) 的源冷却到当日
	// UTC 午夜, 后续请求不再碰它, 自动换下一个; 全部 exceed/不可用回 429。
	lastStatus := http.StatusBadGateway
	var lastBody any = map[string]any{"error": map[string]any{"message": "all sources unavailable", "type": "UpstreamError"}}
	var lastHeaders http.Header
	tried := 0
	for _, u := range upList {
		if u.inCooldown(time.Now()) {
			continue
		}
		tried++
		if s.tryUpstream(w, u, r, body, isStream) {
			return
		}
		u.mu.Lock()
		lastStatus, lastBody, lastHeaders = u.lastStatus, u.lastErrBody, u.lastErrHeaders
		u.mu.Unlock()
	}
	if tried == 0 {
		// 全部在冷却: 大概率是配额用尽/临时故障, 回 429 而不是 502。
		// 至少一个源 exceed 过就明确报 exceeded (用户要求)。
		anyExceeded := false
		for _, u := range upList {
			u.mu.Lock()
			e := u.exceeded
			u.mu.Unlock()
			if e {
				anyExceeded = true
				break
			}
		}
		if anyExceeded {
			log.Printf("all sources exceeded -> 429")
			writeJSON(w, http.StatusTooManyRequests, map[string]any{"error": map[string]any{"message": "all sources exceeded daily free usage limit", "type": "FreeUsageLimitError"}})
			return
		}
		log.Printf("all sources in cooldown -> 429")
		writeJSON(w, http.StatusTooManyRequests, map[string]any{"error": map[string]any{"message": "all sources in cooldown", "type": "UpstreamError"}})
		return
	}
	proxyheaders.MergeHeaders(w.Header(), lastHeaders)
	writeJSON(w, lastStatus, lastBody)
}

// tryUpstream 转发到单个源。返回 true 表示响应已写给客户端。
// 失败时在 u 上记录最近一次状态, 供上层聚合错误响应。
func (s *server) tryUpstream(w http.ResponseWriter, u *upstream, r *http.Request, body []byte, isStream bool) bool {
	path := "/chat/completions"
	if len(body) == 0 {
		path = "/v1/models"
	}
	// 请求体 gzip 压缩后转发 (capture-proxy 支持解压, 省带宽);
	// 压缩失败则按原文发送。
	req, err := buildReq(u.base+path, r.Method, body, r.Header)
	if err != nil {
		u.setErr(err.Error())
		return false
	}

	log.Printf("%s: connecting... (gzip=%v)", u.name, req.Header.Get("Content-Encoding") == "gzip")
	resp, err := u.client.Do(req)
	if err != nil {
		log.Printf("%s: connect failed: %v", u.name, err)
		u.setCooldown(cooldownShort)
		u.setErr(err.Error())
		return false
	}
	if resp.StatusCode != http.StatusOK {
		data, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		resp.Body.Close()
		var obj any
		if err := json.Unmarshal(data, &obj); err != nil {
			obj = map[string]any{"error": map[string]any{"message": string(data)}}
		}
		u.setErr(fmt.Sprintf("HTTP %d", resp.StatusCode))
		u.mu.Lock()
		u.lastStatus, u.lastErrBody, u.lastErrHeaders = resp.StatusCode, obj, resp.Header
		// exceed (429 / FreeUsageLimitError): 该源配额用尽, 冷却到当日
		// UTC 午夜 (capture-proxy 每日重置时刻), 当天不再尝试它,
		// 请求自动落到下一个源。非 exceed 的业务错误不设冷却,
		// 避免误锁正常服务的源。
		if isExceed(resp.StatusCode, data) {
			u.exceeded = true
			u.cooldownUntil = nextUTCMidnight()
			log.Printf("%s: EXCEEDED HTTP %d -> cooldown until %s (UTC), next source", u.name, resp.StatusCode, u.cooldownUntil.UTC().Format("15:04"))
		} else {
			u.exceeded = false
		}
		u.mu.Unlock()
		log.Printf("%s: HTTP %d -> next source", u.name, resp.StatusCode)
		return false
	}

	u.incrReqs()
	u.setErr("")
	u.mu.Lock()
	u.exceeded = false
	u.mu.Unlock()

	if !isStream {
		data, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		h := w.Header()
		proxyheaders.ForwardResponseHeaders(h, resp.Header)
		h.Set("Content-Type", "application/json")
		cors(h)
		w.WriteHeader(http.StatusOK)
		w.Write(data)
		return true
	}

	// 流式: 预读首块 (最多 preReadTimeout)。拿到数据才提交响应头并
	// io.Copy 转发; 预读失败/超时则响应头未提交, 可安全换源。
	// 预读 goroutine 用缓冲 channel 且超时后 Close Body, 不会泄漏。
	type readRes struct {
		buf []byte
		err error
	}
	ch := make(chan readRes, 1)
	go func() {
		buf := make([]byte, 4096)
		n, err := resp.Body.Read(buf)
		ch <- readRes{buf[:n], err}
	}()
	timer := time.NewTimer(preReadTimeout)
	defer timer.Stop()
	select {
	case res := <-ch:
		if len(res.buf) == 0 {
			log.Printf("%s: stream closed before first chunk -> next source", u.name)
			resp.Body.Close()
			u.setCooldown(cooldownShort)
			u.setErr("first chunk stall")
			return false
		}
		h := w.Header()
		proxyheaders.ForwardResponseHeaders(h, resp.Header)
		cors(h)
		w.WriteHeader(http.StatusOK)
		if flusher, ok := w.(http.Flusher); ok {
			flusher.Flush()
		}
		// 转发首块 (预读 goroutine 读到的), 再从 resp.Body 接续转发剩余流。
		// goroutine 用的是裸 Body.Read 无缓冲包装, io.Copy 能无缝续读。
		n1 := len(res.buf)
		if _, err := w.Write(res.buf); err != nil {
			log.Printf("%s: client write failed: %v", u.name, err)
		}
		if flusher, ok := w.(http.Flusher); ok {
			flusher.Flush()
		}
		n2, err := io.Copy(w, resp.Body)
		resp.Body.Close()
		log.Printf("%s: done stream fwd=%dB err=%v", u.name, n1+int(n2), err)
		return true
	case <-timer.C:
		log.Printf("%s: first chunk timeout %.0fs -> next source", u.name, preReadTimeout.Seconds())
		resp.Body.Close()
		u.setCooldown(cooldownShort)
		u.setErr("first chunk timeout")
		return false
	}
}

// buildReq 构造转发请求, 请求体按 gzip 压缩 (失败回退原文)。
func buildReq(url, method string, body []byte, head http.Header) (*http.Request, error) {
	var rd io.Reader
	var enc string
	if len(body) > 0 {
		var buf bytes.Buffer
		zw := gzip.NewWriter(&buf)
		if _, err := zw.Write(body); err == nil && zw.Close() == nil {
			rd = &buf
			enc = "gzip"
		} else {
			rd = bytes.NewReader(body)
		}
	}
	req, err := http.NewRequest(method, url, rd)
	if err != nil {
		return nil, err
	}
	if enc != "" {
		req.Header.Set("Content-Encoding", enc)
	}
	req.Header.Set("Content-Type", "application/json")
	proxyheaders.ForwardRequestHeaders(req.Header, head)
	if a := head.Get("Authorization"); a != "" {
		req.Header.Set("Authorization", a)
	}
	return req, nil
}

// ---- admin endpoints ----------------------------------------------------------

func (s *server) handler() http.Handler {
	mux := http.NewServeMux()
	chat := func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "OPTIONS" {
			writeJSON(w, 204, map[string]any{})
			return
		}
		s.handleProxy(w, r)
	}
	mux.HandleFunc("/chat/completions", chat)
	mux.HandleFunc("/v1/chat/completions", chat)
	mux.HandleFunc("/v1/models", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "OPTIONS" {
			writeJSON(w, 204, map[string]any{})
			return
		}
		// 模型列表转发第一个非冷却源, 全部冷却时返回空列表。
		for _, u := range upList {
			if u.inCooldown(time.Now()) {
				continue
			}
			resp, err := u.client.Get(u.base + "/v1/models")
			if err != nil {
				continue
			}
			defer resp.Body.Close()
			data, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
			h := w.Header()
			proxyheaders.ForwardResponseHeaders(h, resp.Header)
			cors(h)
			w.WriteHeader(resp.StatusCode)
			w.Write(data)
			return
		}
		writeJSON(w, 200, map[string]any{"object": "list", "data": []any{}})
	})
	mux.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "OPTIONS" {
			writeJSON(w, 204, map[string]any{})
			return
		}
		now := time.Now()
		srcs := map[string]any{}
		for _, u := range upList {
			u.mu.Lock()
			cd := u.cooldownUntil.Sub(now).Seconds()
			lastErr := u.lastErr
			reqs := u.reqs
			exceeded := u.exceeded
			u.mu.Unlock()
			if cd < 0 {
				cd = 0
			}
			srcs[u.name] = map[string]any{
				"base": u.base, "cooldown_sec": int(cd),
				"reqs": reqs, "last_err": lastErr, "exceeded": exceeded,
			}
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
		w.Write([]byte("multi-proxy running\n"))
	})
	return mux
}

// multiFlag 支持可重复的 --source flag。
type multiFlag []string

func (m *multiFlag) String() string { return strings.Join(*m, ",") }

func (m *multiFlag) Set(v string) error {
	*m = append(*m, v)
	return nil
}

func main() {
	var sourceFlags multiFlag
	fs := flag.NewFlagSet("multi-proxy", flag.ExitOnError)
	listen := fs.String("listen", "127.0.0.1:8443", "监听地址")
	fs.Var(&sourceFlags, "source", "源 name=base, 可重复指定")
	fs.Parse(os.Args[1:])

	// 未指定 --source 时默认转发本地 capture-proxy (auto 模式)。
	sources := sourceFlags
	if len(sources) == 0 {
		sources = []string{"local=http://127.0.0.1:8000"}
	}
	for _, src := range sources {
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
	if len(upList) == 0 {
		log.Fatal("no sources")
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
		Addr:              *listen,
		Handler:           s.handler(),
		ReadHeaderTimeout: 15 * time.Second,
	}
	names := make([]string, 0, len(upList))
	for _, u := range upList {
		names = append(names, u.name+"@"+u.base)
	}
	log.Printf("multi-proxy sources: %s", strings.Join(names, ", "))
	log.Printf("Listening on %s", *listen)
	if err := srv.ListenAndServe(); err != nil {
		log.Fatal(err)
	}
}
