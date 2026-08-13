// opencode-proxy — CORS proxy for the OpenCode Go API.
//
// Exposes a single endpoint POST /chat/completion and forwards it to the
// real OpenCode Go endpoint (https://opencode.ai/zen/go/v1/chat/completions)
// so the upstream URL and API key stay hidden from the browser.
//
// CORS is only granted to https://aichat.moonchan.xyz.
package main

import (
	"encoding/json"
	"flag"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/Hana-ame/wintools/pkg/netdial"
	"golang.org/x/time/rate"
)

const (
	allowedOrigin = "https://aichat.moonchan.xyz"
	upstream      = "https://opencode.ai/zen/go/v1/chat/completions"
	envKey        = "OPENCODE_GO_API_KEY"
	allowedModel  = "deepseek-v4-flash"

	rateLimit   = rate.Limit(10.0 / 60.0) // 每 IP 每分钟 10 次
	rateBurst   = 10
	rateCleanup = 10 * time.Minute

	dayLimit = 10000 // 全局每日配额
)

// ---- 全局每日配额 ----

var (
	dayMu    sync.Mutex
	dayDate  string
	dayCount int
)

// ---- 请求统计 (/status) ----

var (
	statsMu        sync.Mutex
	statsTotal     int64 // 收到校验通过的 POST 总数
	statsOK        int64 // 上游 2xx
	statsErr       int64 // 上游非 2xx
	statsUpstream  int64 // 上游连接失败 (502)
	statsRejected  int64 // 认证/限流/配额/model 拒绝
	statsStart     = time.Now()
	dayOK          int64
	dayUpstreamErr int64
)

// allowDay 检查并占用一次全局每日配额,跨天自动重置。
func allowDay() bool {
	dayMu.Lock()
	defer dayMu.Unlock()
	today := time.Now().Format("2006-01-02")
	if today != dayDate {
		dayDate = today
		dayCount = 0
	}
	if dayCount >= dayLimit {
		return false
	}
	dayCount++
	return true
}

func remainingDay() int {
	dayMu.Lock()
	defer dayMu.Unlock()
	if n := dayLimit - dayCount; n > 0 {
		return n
	}
	return 0
}

var client = netdial.Client(10 * time.Minute)

func apiKey() string {
	return os.Getenv(envKey)
}

// ---- 每 IP 限流 ----

type ipLimiter struct {
	lim    *rate.Limiter
	seenAt time.Time
}

var (
	limitersMu sync.Mutex
	limiters   = map[string]*ipLimiter{}
)

func limiterFor(ip string) *rate.Limiter {
	limitersMu.Lock()
	defer limitersMu.Unlock()
	e, ok := limiters[ip]
	if !ok {
		e = &ipLimiter{
			lim:    rate.NewLimiter(rateLimit, rateBurst),
			seenAt: time.Now(),
		}
		limiters[ip] = e
	}
	e.seenAt = time.Now()
	return e.lim
}

func cleanupLimiters() {
	for {
		time.Sleep(rateCleanup)
		cutoff := time.Now().Add(-rateCleanup)
		limitersMu.Lock()
		for ip, e := range limiters {
			if e.seenAt.Before(cutoff) {
				delete(limiters, ip)
			}
		}
		limitersMu.Unlock()
	}
}

// clientIP 取真实客户端 IP:CF 隧道后优先 CF-Connecting-IP,再退到 XFF/RemoteAddr。
func clientIP(r *http.Request) string {
	if ip := r.Header.Get("CF-Connecting-IP"); ip != "" {
		return ip
	}
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		if i := strings.IndexByte(xff, ','); i >= 0 {
			xff = xff[:i]
		}
		return strings.TrimSpace(xff)
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

func setCORS(w http.ResponseWriter, origin string) {
	h := w.Header()
	h.Set("Access-Control-Allow-Origin", origin)
	h.Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
	h.Set("Access-Control-Allow-Headers", "Authorization, Content-Type")
	h.Set("Access-Control-Max-Age", "86400")
	h.Set("Vary", "Origin")
}

func handle(w http.ResponseWriter, r *http.Request) {
	origin := r.Header.Get("Origin")
	allowed := origin == allowedOrigin
	setCORS(w, allowedOrigin)
	reject := func() {
		statsMu.Lock()
		statsRejected++
		statsMu.Unlock()
	}

	if r.Method == http.MethodOptions {
		if !allowed {
			reject()
			w.WriteHeader(http.StatusForbidden)
			return
		}
		w.WriteHeader(http.StatusNoContent)
		return
	}

	if r.Method != http.MethodPost {
		w.Header().Set("Allow", "POST, OPTIONS")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	if !allowed {
		reject()
		http.Error(w, "origin not allowed", http.StatusForbidden)
		return
	}

	// Referer 必须存在且来自 aichat.moonchan.xyz,拒绝空 Referer。
	if ref := r.Header.Get("Referer"); ref == "" || !strings.HasPrefix(ref, allowedOrigin) {
		reject()
		http.Error(w, "referer not allowed", http.StatusForbidden)
		return
	}

	if !limiterFor(clientIP(r)).Allow() {
		reject()
		w.Header().Set("Retry-After", "60")
		http.Error(w, "rate limit exceeded: 10 requests per minute", http.StatusTooManyRequests)
		return
	}

	// 全局每日配额 (1000 条/天)。
	if !allowDay() {
		reject()
		w.Header().Set("Retry-After", "3600")
		http.Error(w, "daily limit exceeded", http.StatusTooManyRequests)
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if len(body) == 0 {
		http.Error(w, "empty body", http.StatusBadRequest)
		return
	}

	// 只允许指定的 model。
	var reqBody struct {
		Model string `json:"model"`
	}
	if err := json.Unmarshal(body, &reqBody); err != nil {
		http.Error(w, "invalid JSON body", http.StatusBadRequest)
		return
	}
	if reqBody.Model != allowedModel {
		statsMu.Lock()
		statsRejected++
		statsMu.Unlock()
		http.Error(w, "model not allowed", http.StatusForbidden)
		return
	}

	statsMu.Lock()
	statsTotal++
	statsMu.Unlock()

	req, err := http.NewRequest(http.MethodPost, upstream, strings.NewReader(string(body)))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	req.Header.Set("Authorization", "Bearer "+apiKey())
	if ct := r.Header.Get("Content-Type"); ct != "" {
		req.Header.Set("Content-Type", ct)
	} else {
		req.Header.Set("Content-Type", "application/json")
	}
	req.Header.Set("Accept", r.Header.Get("Accept"))

	resp, err := client.Do(req)
	if err != nil {
		log.Printf("upstream error: %v", err)
		statsMu.Lock()
		statsUpstream++
		statsMu.Unlock()
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		statsMu.Lock()
		statsOK++
		dayOK++
		statsMu.Unlock()
	} else {
		statsMu.Lock()
		statsErr++
		dayUpstreamErr++
		statsMu.Unlock()
	}

	w.Header().Set("Content-Type", resp.Header.Get("Content-Type"))
	w.WriteHeader(resp.StatusCode)
	io.Copy(w, resp.Body)
}

// statusHandler 只读状态端点: 配额使用、请求统计、活跃 IP 数。
// 不需要 Origin/Referer 校验 (无敏感信息, 不暴露 key)。
func statusHandler(w http.ResponseWriter, r *http.Request) {
	statsMu.Lock()
	dayCountNow := dayCount
	dayOKNow := dayOK
	dayErrNow := dayUpstreamErr
	sTotal, sOK, sErr, sUp, sRej := statsTotal, statsOK, statsErr, statsUpstream, statsRejected
	statsMu.Unlock()

	limitersMu.Lock()
	activeIPs := len(limiters)
	limitersMu.Unlock()

	dayMu.Lock()
	dayDateNow := dayDate
	dayMu.Unlock()

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"status": "ok",
		"uptime_sec": int(time.Since(statsStart).Seconds()),
		"upstream": upstream,
		"daily": map[string]any{
			"date":          dayDateNow,
			"used":          dayCountNow,
			"limit":         dayLimit,
			"remaining":     remainingDay(),
			"ok":            dayOKNow,
			"upstream_errs": dayErrNow,
		},
		"requests": map[string]int64{
			"total":       sTotal,
			"ok":          sOK,
			"upstream_err": sUp,
			"upstream_4xx_5xx": sErr,
			"rejected":    sRej,
		},
		"active_ips": activeIPs,
	})
}

func main() {
	addr := flag.String("listen", ":8080", "listen address")
	flag.Parse()

	if apiKey() == "" {
		log.Fatalf("missing %s environment variable", envKey)
	}

	http.HandleFunc("/chat/completion", handle)
	http.HandleFunc("/chat/completions", handle)
	http.HandleFunc("/status", statusHandler)
	go cleanupLimiters()
	log.Printf("opencode-proxy listening on %s -> %s (CORS: %s, rate limit: 10/min per IP, daily: %d)", *addr, upstream, allowedOrigin, dayLimit)
	log.Fatal(http.ListenAndServe(*addr, nil))
}
