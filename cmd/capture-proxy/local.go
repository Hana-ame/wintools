// Local Proxy — Ollama-compatible endpoint relaying to opencode.ai/zen/v1 with
// dual-stack v6/v4 failover, active stream detection, capture, and mode switch.
//
// 合并自 cmd/local-proxy-detected 与 scripts/capture_proxy.go (两者核心转发
// 逻辑一致): local-proxy-detected 的 usage 计费统计 + vanilla 透传开关 +
// 双栈监听, capture_proxy 的抓包 / /mode API / 伪装 opencode client / gzip
// 请求体 / TLS 监听。
//
// 设计目标:
//   - 转发到 opencode.ai/zen/v1 (v6/v4 failover、FreeUsageLimitError cooldown);
//   - 流检测: 预读等待首 token、token 节奏 stall 检测、180s 工具调用思考窗口、
//     keep-alive 过滤、EOF/DONE 兜底封流、意外中断注入 "echo 继续" 恢复工具循环;
//   - 抓包: 每个请求 (method/URL/全部 header/body) 存到 --out 目录;
//   - 伪装 opencode client: 透传客户端 header, 缺失时补 opencode UA /
//     x-opencode-* / X-Session-Id, 使上游 (Cloudflare) 视作真实 opencode 客户端;
//   - gzip 请求体: Content-Encoding: gzip 时先解压再转发;
//   - 模式开关: v4 / v6 (单栈), 运行中可经 /mode API 切换;
//   - usage 计费统计: 按模型累计 token/cost, /stats 端点查询;
//   - vanilla 纯透传 (--detect=false): 不做流检测, 仅转发 + 统计。
//
// 用法:
//
//	capture-proxy -role provider --listen 127.0.0.1:8000 --mode v4 [--out dir]
//	capture-proxy -role local --mode v4 --out captured          # 强制 v4
//	capture-proxy -role local --cert fullchain.cer --key key    # HTTPS 监听
//	capture-proxy -role local --detect=false                    # vanilla 纯透传
//
// 模式 API:
//
//	GET  /mode            -> {"mode":"v4","v4_cooldown_sec":..,"v6_cooldown_sec":..}
//	POST /mode            body {"mode":"v6"}  或 query ?mode=v6  (v4|v6)
//	GET  /status          -> 同 /mode + 按协议族累计统计
//	GET  /stats           -> 按模型 usage 计费统计
package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/tls"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httptrace"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Hana-ame/wintools/pkg/proxyheaders"
)

const (
	zenHost       = "opencode.ai"
	zenPath       = "/zen/v1"
	freeLimitErr  = "FreeUsageLimitError"
	defaultModel  = "deepseek-v4-flash-free"
	defaultAPIKey = "public"
	opencodeUA    = "opencode/1.18.16 ai-sdk/provider-utils/4.0.23 runtime/bun/1.3.14"

	connectTimeout   = 10 * time.Second
	stallTimeout     = 30 * time.Second
	tokenGapTimeout  = 10 * time.Second
	toolStallTimeout = 180 * time.Second
	thinkGrace       = 45 * time.Second

	maxRequestBody = 10 << 20

	// 意外中断时注入的「echo 继续」tool_call, 让客户端恢复工具循环而不是报错。
	infTool    = "bash"
	infIdleArg = `{"command": "echo 继续"}`

	// FreeUsageLimitError 连续失败阈值与冷却策略: 第 1 次短冷却 1 分钟, 第 2 次
	// 5 分钟, 达到阈值 (3 次) 后才锁到午夜 (瞬时限流不锁死一整天)。
	limFailThreshold = 3
	limFailCooldown1 = 1 * time.Minute
	limFailCooldown2 = 5 * time.Minute

	idleConnTimeout  = 90 * time.Second
	cleanupInterval  = 5 * time.Minute
	maxReqsPerClient = 200
	banWindow        = 600 * time.Second
	banLen           = 600 * time.Second

	// 计费价格 (每百万 token), 与 zen-multi 的 usage 一致。
	priceInputM      = 1.0
	priceOutputM     = 2.0
	priceCacheReadM  = 0.02
	priceCacheWriteM = 0.0
)

func nextUTCMidnight() time.Time {
	now := time.Now().UTC()
	return time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC).AddDate(0, 0, 1)
}

// ---- SSE helpers (与 zen-proxy 共享语义) -----------------------------------

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

// eventHasContent 报告事件是否真正推进流 (非空 content / tool_call / finish_reason /
// [DONE])。空 delta / 心跳事件不算推进, 避免上游持续心跳喂饱 stall 检测。
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

// stallFor 返回「两个真实 SSE 事件之间允许的最大间隔」。首 token 后按 token 节奏
// 收敛; 工具流保留更长思考窗口; 普通流给 gap 宽限 (local 角色用 thinkGrace
// 避免推理模型思考停顿被误杀; remote/multi 角色用 tokenGapTimeout)。
func stallFor(sawTool bool, gap time.Duration) time.Duration {
	if sawTool {
		return toolStallTimeout
	}
	return gap
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

// ---- usage 计费统计 (来自 local-proxy-detected) ------------------------------

// usage 记录响应体里的 token 用量。非流式响应整体解析;
// 流式响应每个 SSE 事件里可能带 usage (通常只在最后一个事件)。
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

type usageStats struct {
	mu     sync.Mutex
	models map[string]*modelStats
}

func newUsageStats() *usageStats {
	return &usageStats{models: make(map[string]*modelStats)}
}

func (s *usageStats) add(model string, req bool, u *usage) {
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

func (s *usageStats) snapshot() map[string]*modelStats {
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

// parseSSEUsage 从单个 SSE 事件 ("data: {...}" 行) 里抽取 usage。
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

// ---- 意外中断注入 ------------------------------------------------------------

func jsonString(s string) string {
	re, _ := json.Marshal(s)
	return string(re)
}

// toolInject 构造一个 tool_call 事件流 (delta 带 arguments JSON 字符串),
// 以 finish_reason=tool_calls + [DONE] 收尾。
func toolInject(model, callID, arg string) []byte {
	argJSON, _ := json.Marshal(arg)
	toolEvt := fmt.Sprintf(
		`{"id":"idle","object":"chat.completion.chunk","created":0,"model":"%s","choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"id":"%s","type":"function","function":{"name":"%s","arguments":%s}}]},"finish_reason":null}]}`,
		model, callID, infTool, argJSON)
	finishEvt := fmt.Sprintf(
		`{"id":"idle","object":"chat.completion.chunk","created":0,"model":"%s","choices":[{"index":0,"delta":{},"finish_reason":"tool_calls"}]}`,
		model)
	return []byte("data: " + toolEvt + "\n\ndata: " + finishEvt + "\n\ndata: [DONE]\n\n")
}

// idleInject 构造「echo 继续」tool_call: 客户端收到后执行 bash echo 继续, 工具循环恢复。
func idleInject(model string) []byte {
	return toolInject(model, "call_idle", infIdleArg)
}

// ---- 抓包 (capture) ----------------------------------------------------------

type capture struct {
	mu  sync.Mutex
	seq atomic.Uint64
	dir string
}

func (c *capture) save(r *http.Request, body []byte) {
	if c == nil || c.dir == "" {
		return
	}
	n := c.seq.Add(1)
	name := filepath.Join(c.dir, fmt.Sprintf("%06d_%s_%s.txt",
		n, time.Now().Format("20060102-150405"), sanitize(r.URL.Path)))

	var sb strings.Builder
	fmt.Fprintf(&sb, "%s %s %s\n", r.Method, r.URL.String(), r.Proto)
	fmt.Fprintf(&sb, "remote=%s host=%s\n", r.RemoteAddr, r.Host)
	var keys []string
	for k := range r.Header {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		for _, v := range r.Header[k] {
			fmt.Fprintf(&sb, "%s: %s\n", k, v)
		}
	}
	fmt.Fprintf(&sb, "\n--- BODY (%d bytes) ---\n", len(body))
	sb.Write(body)

	if err := os.WriteFile(name, []byte(sb.String()), 0o644); err != nil {
		log.Printf("capture write %s: %v", name, err)
	}
	log.Printf("captured[%d] %s %s %dB -> %s", n, r.Method, r.URL.Path, len(body), name)
}

func sanitize(p string) string {
	p = strings.ReplaceAll(p, "/", "_")
	p = strings.ReplaceAll(p, "?", "_")
	if len(p) > 60 {
		p = p[:60]
	}
	if p == "" || p == "_" {
		return "root"
	}
	return strings.Trim(p, "_")
}

// ---- proxy 核心 --------------------------------------------------------------

type proxy struct {
	mu         sync.Mutex
	mode       string // v4 | v6
	cooldownV4 time.Time
	cooldownV6 time.Time
	limFailsV4 int // 连续 FreeUsageLimitError 次数，成功时清零
	limFailsV6 int

	ban *banList

	clientV4   *http.Client
	clientV6   *http.Client
	clientV4H1 *http.Client
	clientV6H1 *http.Client

	v4URL string
	v6URL string

	// detect=true 时启用流检测 (预读/stall/DONE 兜底); false 时纯透传 (vanilla 模式)。
	detect bool

	famStats map[string]*famStat
	start    time.Time
	usage    *usageStats
}

type famStat struct {
	mu   sync.Mutex
	reqs int64
	ok   int64
	errs int64
	free int64
}

func (f *famStat) add(reqs, ok, errs, free int64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.reqs += reqs
	f.ok += ok
	f.errs += errs
	f.free += free
}

func (p *proxy) statsFor(fam string) *famStat {
	p.mu.Lock()
	defer p.mu.Unlock()
	s := p.famStats[fam]
	if s == nil {
		s = &famStat{}
		p.famStats[fam] = s
	}
	return s
}

func (p *proxy) currentMode() string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.mode
}

func (p *proxy) setMode(m string) bool {
	switch m {
	case "v4", "v6", "auto":
		p.mu.Lock()
		p.mode = m
		p.mu.Unlock()
		return true
	}
	return false
}

// stacks 返回按当前模式确定的尝试顺序(单栈, 无 auto)。
// stacks 返回尝试顺序: 默认先 v6 再 v4; override 为请求级指定栈 (绕过 mode)。
func (p *proxy) stacks(override string) []string {
	switch override {
	case "v4":
		return []string{"v4"}
	case "v6":
		return []string{"v6"}
	}
	switch p.currentMode() {
	case "v4":
		return []string{"v4"}
	case "v6":
		return []string{"v6"}
	default: // auto: 先 v6 再 v4
		return []string{"v6", "v4"}
	}
}

// resolveOnce 用公共 DNS 解析宿主 (本机 resolver 可能指向不可达的 ::1:53)。
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

// onLimitErr 记录一次 FreeUsageLimitError, 返回按连续失败次数升级的冷却时长:
// 第 1 次 limFailCooldown1, 第 2 次 limFailCooldown2, 达到 limFailThreshold
// 后才锁到午夜 (此时可视为真·配额耗尽)。
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

func (p *proxy) cooldownSec(fam string, now time.Time) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	var t time.Time
	if fam == "v6" {
		t = p.cooldownV6
	} else {
		t = p.cooldownV4
	}
	d := t.Sub(now).Seconds()
	if d < 0 {
		return 0
	}
	return int(d)
}

// impersonate 让上游看到「真实 opencode 客户端」特征: 透传客户端 header, 缺失则补齐。
func (p *proxy) impersonate(req *http.Request, client http.Header) {
	req.Header.Set("Content-Type", "application/json")
	proxyheaders.ForwardRequestHeaders(req.Header, client)
	if a := client.Get("Authorization"); a != "" {
		req.Header.Set("Authorization", a)
	} else {
		req.Header.Set("Authorization", "Bearer "+defaultAPIKey)
	}
	// 伪装 opencode client: 除非客户端本来就是真实 opencode (UA 以 opencode 开头),
	// 否则强制覆盖为 opencode UA。curl / Go 等默认 UA 会被上游 Cloudflare 挑战 hang。
	if ua := client.Get("User-Agent"); !strings.HasPrefix(strings.ToLower(ua), "opencode") {
		req.Header.Set("User-Agent", opencodeUA)
	}
	if req.Header.Get("X-Session-Id") == "" {
		req.Header.Set("X-Session-Id", "ses_proxy")
	}
	if req.Header.Get("X-Session-Affinity") == "" {
		req.Header.Set("X-Session-Affinity", "ses_proxy")
	}
	if req.Header.Get("x-opencode-session") == "" {
		req.Header.Set("x-opencode-session", "ses_proxy")
	}
	if req.Header.Get("x-opencode-request") == "" {
		req.Header.Set("x-opencode-request", "user_proxy")
	}
	if req.Header.Get("x-opencode-client") == "" {
		req.Header.Set("x-opencode-client", "cli")
	}
}

// readBody 读取请求体并处理 gzip: Content-Encoding: gzip 时解压后再解析。
func readBody(r *http.Request) ([]byte, error) {
	body, err := io.ReadAll(io.LimitReader(r.Body, maxRequestBody+1))
	if err != nil {
		return nil, err
	}
	if len(body) > maxRequestBody {
		return nil, errTooLarge
	}
	if strings.EqualFold(r.Header.Get("Content-Encoding"), "gzip") {
		zr, err := gzip.NewReader(bytes.NewReader(body))
		if err != nil {
			return nil, fmt.Errorf("invalid gzip body: %w", err)
		}
		defer zr.Close()
		out, err := io.ReadAll(io.LimitReader(zr, maxRequestBody+1))
		if err != nil {
			return nil, fmt.Errorf("gzip read: %w", err)
		}
		if len(out) > maxRequestBody {
			return nil, errTooLarge
		}
		log.Printf("gunzip request body: %d -> %d bytes", len(body), len(out))
		return out, nil
	}
	return body, nil
}

var errTooLarge = fmt.Errorf("request body too large")

func (p *proxy) handle(w http.ResponseWriter, r *http.Request, method string, cap *capture) {
	clientIP := clientIPOf(r)
	stackOverride := r.URL.Query().Get("stack")
	if stackOverride != "" && stackOverride != "v4" && stackOverride != "v6" {
		writeJSON(w, 400, map[string]any{"error": "stack must be v4|v6"})
		return
	}
	log.Printf("-> %s %s from %s", method, r.URL.Path, clientIP)
	if p.ban != nil && p.ban.isBanned(clientIP) {
		log.Printf("banned")
		writeJSON(w, 429, map[string]any{"error": "Banned"})
		return
	}
	if p.ban != nil {
		p.ban.incr(clientIP)
	}
	body, err := readBody(r)
	if err != nil {
		status := 400
		msg := "Bad Request"
		if err == errTooLarge {
			status = http.StatusRequestEntityTooLarge
			msg = "Request body too large"
		}
		writeJSON(w, status, map[string]any{"error": msg})
		return
	}
	if cap != nil {
		cap.save(r, body)
	}

	isStream := false
	model := defaultModel
	recoverable := false
	bodyStr := string(body)
	var payload map[string]any
	if len(body) > 0 {
		if err := json.Unmarshal(body, &payload); err != nil {
			writeJSON(w, 400, map[string]any{"error": "Invalid JSON"})
			return
		}
		if m, _ := payload["model"].(string); m != "" {
			model = m
		}
		if model == "deepseek-v4-flash" {
			model = defaultModel
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
		// 请求带 tools: stall/断流时可注入 idle tool_call 恢复工具循环。
		if tools, _ := payload["tools"].([]any); len(tools) > 0 {
			recoverable = true
		}
		re, _ := json.Marshal(payload)
		bodyStr = string(re)
	}

	lastStatus := 0
	var lastErrBody any
	var lastHeaders http.Header

	for _, fam := range p.stacks(stackOverride) {
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
		req.Host = zenHost // Host header 保持域名, 即使 dial 的是 IP
		p.impersonate(req, r.Header)

		t0 := time.Now()
		st := p.statsFor(fam)
		st.add(1, 0, 0, 0)
		var tDial, tTLS, tWrote time.Time
		var proto string
		trace := &httptrace.ClientTrace{
			ConnectDone:      func(network, addr string, err error) { tDial = time.Now() },
			TLSHandshakeDone: func(cs tls.ConnectionState, err error) { tTLS = time.Now() },
			WroteRequest:     func(info httptrace.WroteRequestInfo) { tWrote = time.Now() },
			GotConn:          func(info httptrace.GotConnInfo) { proto = "reused" },
		}
		resp, err := doWithTrace(client, req, trace)
		if err != nil {
			if strings.Contains(err.Error(), "timeout awaiting response headers") || strings.Contains(err.Error(), "http2") {
				// http/2 挂起/超时: 用 http/1.1 重试一次 (有些路径 http2 被干扰)
				log.Printf("%s %s failed: %v (dial=%s tls=%s wrote=%s) -> retry h1", fam, base, err,
					ms(tDial, t0), ms(tTLS, t0), ms(tWrote, t0))
				h1c := p.clientV6H1
				if fam == "v4" {
					h1c = p.clientV4H1
				}
				// 必须重建请求: 原 req 的 Body 已被第一次尝试耗尽/关闭,
				// 直接复用会报 "ContentLength=X with Body length 0"。
				retryReq, rerr := http.NewRequest(method, base+zenPath+path, strings.NewReader(bodyStr))
				if rerr != nil {
					err = rerr
				} else {
					retryReq.Host = zenHost
					retryReq.Header = req.Header.Clone()
					t0 = time.Now()
					tDial, tTLS, tWrote = time.Time{}, time.Time{}, time.Time{}
					resp, err = doWithTrace(h1c, retryReq, trace)
					if err == nil {
						proto += " (h1 retry)"
					}
				}
			}
			if err != nil {
				log.Printf("%s upstream error: %v (dial=%s tls=%s wrote=%s)", fam, err,
					ms(tDial, t0), ms(tTLS, t0), ms(tWrote, t0))
				st.add(0, 0, 1, 0)
				lastStatus = 502
				lastErrBody = map[string]any{"error": map[string]any{"message": err.Error(), "type": "UpstreamError"}}
				continue
			}
		}
		if resp.Proto != "" {
			proto = resp.Proto
		}
		log.Printf("%s upstream %d in %.2fs proto=%s (dial=%s tls=%s wrote=%s)", fam, resp.StatusCode, time.Since(t0).Seconds(), proto,
			ms(tDial, t0), ms(tTLS, t0), ms(tWrote, t0))

		if resp.StatusCode != 200 {
			data, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			var obj map[string]any
			_ = json.Unmarshal(data, &obj)
			if e, ok := obj["error"].(map[string]any); ok && e["type"] == freeLimitErr {
				d := p.onLimitErr(fam)
				log.Printf("%s FreeUsageLimitError (lim_fails, cooldown=%s)", fam, d.Round(time.Second))
				st.add(0, 0, 1, 1)
				p.setCooldownUntil(fam, time.Now().Add(d))
				continue
			}
			st.add(0, 0, 1, 0)
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

		st.add(0, 1, 0, 0)

		if !isStream {
			data, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			p.clearLimFails(fam)
			p.usage.add(model, true, parseUsage(data))
			h := w.Header()
			proxyheaders.ForwardResponseHeaders(h, resp.Header)
			h.Set("Content-Type", "application/json")
			setCORS(h)
			w.WriteHeader(200)
			w.Write(data)
			log.Printf("non-stream done")
			return
		}

		ok, committed := p.forwardStream(w, resp, fam, model, recoverable)
		if ok || committed {
			p.clearLimFails(fam)
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

// forwardStream 转发 SSE 流, 带预读/stall/工具流保护/意外中断注入。
// 返回 (ok, committed): committed 表示响应头已提交, 调用方禁止再 failover。
func (p *proxy) forwardStream(w http.ResponseWriter, resp *http.Response, fam, model string, recoverable bool) (bool, bool) {
	done := make(chan struct{})
	defer close(done)
	ch := startReader(resp.Body, done)
	t0 := time.Now()

	// 预读: 等待首个真实 SSE 事件, 上限 stallTimeout; 无数据则尝试下一栈。
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

	// 预读内容中若已有错误事件, 尝试下一栈。
	{
		eb := pre
		for {
			ev, rest, ok := nextSSEEvent(eb)
			if !ok {
				break
			}
			eb = rest
			if eventHasError(ev) {
				log.Printf("%s SSE error in pre-read, try other stack", fam)
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
	setCORS(h)
	w.WriteHeader(200)
	if flusher != nil {
		flusher.Flush()
	}

	p.usage.add(model, true, nil)

	buf := pre
	sawTool := false
	finished := false
	doneSent := false
	injected := false
	lastReal := time.Now()
	timer := time.NewTimer(stallFor(false, thinkGrace))
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
		// 排空 buf 中的完整事件。
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
				p.usage.add(model, false, u)
			}
			if eventHasToolCall(ev) {
				sawTool = true
			}
			if eventHasError(ev) {
				// 上游 error 事件: 流非正常结束。允许 toolcall (recoverable)
				// 就注入「echo 继续」恢复工具循环; 不允许则丢弃事件不转发。
				if !finished && recoverable && !injected {
					log.Printf("%s mid-stream error event -> inject idle (recoverable)", fam)
					if err := write(idleInject(model)); err != nil {
						break loop
					}
					injected = true
					doneSent = true
					break loop
				}
				log.Printf("%s mid-stream error event dropped (recoverable=%v)", fam, recoverable)
				continue
			}
			if fr := eventFinishReason(ev); fr != nil {
				finished = true // stop / tool_calls / length 任一都算正常收尾
			}
			if bytes.Contains(ev, []byte("data: [DONE]")) {
				finished = true
				doneSent = true
			}
			if eventHasContent(ev) {
				lastReal = time.Now()
				timer.Reset(stallFor(sawTool, thinkGrace))
			}
			if err := write(append(ev, '\n', '\n')); err != nil {
				log.Printf("client disconnected mid-stream")
				resp.Body.Close()
				return false, true
			}
		}

		select {
		case m, ok := <-ch:
			if !ok || m.eof {
				// EOF 出口: 非正常结束 (没 finish_reason/[DONE]) 且允许
				// toolcall → 注入「echo 继续」; 不允许 → 跳过注入, 兜底
				// 补 [DONE] 收尾 (客户端区分「完成」与「截断」)。
				if !finished && recoverable && !injected {
					log.Printf("%s EOF without finish -> inject idle (recoverable)", fam)
					if err := write(idleInject(model)); err != nil {
						break loop
					}
					injected = true
					doneSent = true
				}
				if !doneSent {
					log.Printf("%s EOF, sealing [DONE] (finished=%v injected=%v)", fam, finished, injected)
					if err := write([]byte("data: [DONE]\n\n")); err != nil {
						break loop
					}
					doneSent = true
				}
				break loop
			}
			if m.err != nil {
				log.Printf("%s mid-stream error: %v", fam, m.err)
				if !finished && recoverable && !injected {
					log.Printf("%s stream error without finish -> inject idle (recoverable)", fam)
					if err := write(idleInject(model)); err == nil {
						injected = true
						doneSent = true
					}
				}
				if !doneSent {
					if err := write([]byte("data: [DONE]\n\n")); err == nil {
						doneSent = true
					}
				}
				resp.Body.Close()
				return false, true
			}
			buf = append(buf, m.data...)
		case <-timer.C:
			stall := stallFor(sawTool, thinkGrace)
			if time.Since(lastReal) >= stall {
				log.Printf("%s stream stalled (no real data %s, saw_tool=%v), closing", fam, stall.Round(time.Second), sawTool)
				if !finished && recoverable && !injected {
					log.Printf("%s stall without finish -> inject idle (recoverable)", fam)
					write(idleInject(model))
					injected = true
					doneSent = true
					resp.Body.Close()
					return true, true
				}
				if !doneSent {
					write([]byte("data: [DONE]\n\n"))
					doneSent = true
				}
				resp.Body.Close()
				return false, true
			}
			timer.Reset(stall - time.Since(lastReal))
		}
	}

	resp.Body.Close()
	log.Printf("%s stream done (%d bytes, %.2fs) saw_tool=%v injected=%v", fam, total, time.Since(t0).Seconds(), sawTool, injected)
	return true, true
}

// forwardPassthrough 纯透传 (vanilla) 模式: 不做预读 / stall 检测 / DONE 兜底,
// 上游事件原样转发, 仅统计请求数与 usage。
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

	p.usage.add(model, true, nil)

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
			// 逐事件转发, 顺带解析 usage 统计
			for {
				ev, rest, ok := nextSSEEvent(buf)
				if !ok {
					buf = rest
					break
				}
				buf = rest
				if u := parseSSEUsage(ev); u != nil {
					p.usage.add(model, false, u)
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

func messagesOf(p map[string]any) []any {
	m, _ := p["messages"].([]any)
	return m
}

func toolsOf(p map[string]any) []any {
	t, _ := p["tools"].([]any)
	return t
}

func clientIPOf(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

// ---- ban list (来自 zen-proxy) ----------------------------------------------

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

// ---- server wiring ----------------------------------------------------------

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

func doWithTrace(client *http.Client, req *http.Request, trace *httptrace.ClientTrace) (*http.Response, error) {
	req2 := req.Clone(req.Context())
	req2 = req2.WithContext(httptrace.WithClientTrace(req2.Context(), trace))
	return client.Do(req2)
}

func ms(t, t0 time.Time) time.Duration {
	if t.IsZero() {
		return 0
	}
	return t.Sub(t0).Round(time.Millisecond)
}

func makeClient(endpoint, sniHost string, forceH2 bool) *http.Client {
	tlsCfg := &tls.Config{ServerName: sniHost, InsecureSkipVerify: true}
	if !forceH2 {
		tlsCfg.NextProtos = []string{"http/1.1"}
	}
	tr := &http.Transport{
		TLSClientConfig: tlsCfg,
		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			d := &net.Dialer{Timeout: connectTimeout, KeepAlive: 30 * time.Second}
			return d.DialContext(ctx, "tcp", endpoint)
		},
		ResponseHeaderTimeout: 60 * time.Second,
		IdleConnTimeout:       90 * time.Second,
		MaxIdleConnsPerHost:   8,
		TLSHandshakeTimeout:   connectTimeout,
		ForceAttemptHTTP2:     forceH2,
	}
	return &http.Client{Transport: tr}
}

func runProvider(args []string) {
	fs := flag.NewFlagSet("provider", flag.ExitOnError)
	listen := fs.String("listen", "127.0.0.1:8000", "监听地址")
	mode := fs.String("mode", "auto", "转发模式: auto(先 v6 再 v4) | v4 | v6")
	outDir := fs.String("out", "", "抓包输出目录 (默认不抓包)")
	cert := fs.String("cert", "", "TLS 证书文件 (提供后以 HTTPS 监听)")
	key := fs.String("key", "", "TLS 私钥文件")
	detect := fs.Bool("detect", true, "流检测 (true=detected, false=vanilla 纯透传)")
	ban := fs.Bool("ban", false, "按 IP 限流 (200 req/10min, zen-proxy 行为)")
	fs.Parse(args)

	if *mode != "v4" && *mode != "v6" {
		*mode = "auto"
	}

	v4, v6 := resolveOnce(zenHost)
	if *mode == "v4" && v4 == "" || *mode == "v6" && v6 == "" {
		log.Fatalf("no %s address for %s", *mode, zenHost)
	}

	p := &proxy{
		mode:       *mode,
		clientV4:   makeClient(net.JoinHostPort(v4, "443"), zenHost, true),
		clientV6:   makeClient(net.JoinHostPort(v6, "443"), zenHost, true),
		clientV4H1: makeClient(net.JoinHostPort(v4, "443"), zenHost, false),
		clientV6H1: makeClient(net.JoinHostPort(v6, "443"), zenHost, false),
		v4URL:      "https://" + v4,
		v6URL:      "https://[" + v6 + "]",
		detect:     *detect,
		famStats:   map[string]*famStat{},
		start:      time.Now(),
		usage:      newUsageStats(),
	}
	if *ban {
		p.ban = newBanList(maxReqsPerClient, banWindow, banLen)
	}
	if v4 == "" && v6 == "" {
		log.Fatalf("could not resolve %s via public DNS", zenHost)
	}

	var cap *capture
	if *outDir != "" {
		if err := os.MkdirAll(*outDir, 0o755); err != nil {
			log.Fatalf("mkdir %s: %v", *outDir, err)
		}
		cap = &capture{dir: *outDir}
	}

	mux := http.NewServeMux()
	chat := func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "OPTIONS" {
			writeJSON(w, 204, map[string]any{})
			return
		}
		p.handle(w, r, r.Method, cap)
	}
	models := func(w http.ResponseWriter, r *http.Request) {
		p.handle(w, r, "GET", cap)
	}
	mux.HandleFunc("/zen/v1/chat/completions", chat)
	mux.HandleFunc("/v1/chat/completions", chat)
	mux.HandleFunc("/chat/completions", chat)
	mux.HandleFunc("/zen/v1/models", models)
	mux.HandleFunc("/v1/models", models)
	mux.HandleFunc("/models", models)

	// 模式控制 API
	writeMode := func(w http.ResponseWriter) {
		now := time.Now()
		writeJSON(w, 200, map[string]any{
			"mode":            p.currentMode(),
			"v4_cooldown_sec": p.cooldownSec("v4", now),
			"v6_cooldown_sec": p.cooldownSec("v6", now),
		})
	}
	mux.HandleFunc("/mode", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "OPTIONS" {
			writeJSON(w, 204, map[string]any{})
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
				writeJSON(w, 400, map[string]any{"error": "mode required (v4|v6|auto)"})
				return
			}
			if !p.setMode(m) {
				writeJSON(w, 400, map[string]any{"error": "mode must be v4|v6|auto"})
				return
			}
			log.Printf("mode switched to %s", m)
		}
		writeMode(w)
	})
	mux.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "OPTIONS" {
			writeJSON(w, 204, map[string]any{})
			return
		}
		now := time.Now()
		stats := map[string]any{}
		// 必须在 p.mu 下遍历 famStats: statsFor 也在 p.mu 下增删该 map,
		// 无锁遍历与写并发会触发 "concurrent map iteration and map write"
		// fatal panic, 整个进程崩溃 (发现背景: 代码审阅)。
		p.mu.Lock()
		for fam, s := range p.famStats {
			s.mu.Lock()
			stats[fam] = map[string]any{"reqs": s.reqs, "ok": s.ok, "errs": s.errs, "free_limit": s.free}
			s.mu.Unlock()
		}
		p.mu.Unlock()
		writeJSON(w, 200, map[string]any{
			"status":          "ok",
			"mode":            p.currentMode(),
			"v4_cooldown_sec": p.cooldownSec("v4", now),
			"v6_cooldown_sec": p.cooldownSec("v6", now),
			"upstream":        "https://" + zenHost + zenPath,
			"uptime_sec":      int(time.Since(p.start).Seconds()),
			"stats":           stats,
			"models":          p.usage.snapshot(),
		})
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
		Addr:              *listen,
		Handler:           mux,
		ReadHeaderTimeout: 15 * time.Second,
	}

	log.Printf("Local proxy on %s (mode=%s, forward -> https://%s%s, capture=%v, detect=%v)",
		*listen, *mode, zenHost, zenPath, cap != nil, *detect)
	var err error
	if *cert != "" || *key != "" {
		if *cert == "" || *key == "" {
			log.Fatal("https mode requires both --cert and --key")
		}
		log.Printf("TLS enabled with %s / %s", *cert, *key)
		err = srv.ListenAndServeTLS(*cert, *key)
	} else {
		err = srv.ListenAndServe()
	}
	if err != nil {
		log.Fatal(err)
	}
}
