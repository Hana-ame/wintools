// Package echproxy 提供 ECH 域前置反向代理的核心组件。
// 包括上游配置加载、TLS 证书下载和代理 HTTP handler。
package echproxy

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	cloudflare_ech "github.com/Hana-ame/wintools/pkg/ech"
	"github.com/Hana-ame/wintools/pkg/netdial"
	"github.com/andybalholm/brotli"
	"github.com/gin-gonic/gin"
	"github.com/klauspost/compress/zstd"
)

// WildcardRule 通配上游规则:
// 请求域名 = Prefix + <sub> + EntrySuffix 时, 转发到 <sub> + UpstreamSuffix。
// 例: iwara-  + filesq + .l.moonchan.xyz → filesq.iwara.tv
type WildcardRule struct {
	Prefix         string `json:"prefix,omitempty"`          // 入口前缀, 如 "iwara-"
	EntrySuffix    string `json:"entry_suffix,omitempty"`    // 入口后缀, 如 ".l.moonchan.xyz"
	UpstreamSuffix string `json:"upstream_suffix,omitempty"` // 上游后缀, 如 ".iwara.tv"
	Referer        string `json:"referer,omitempty"`         // 固定上游 Referer (防盗链)
}

// UpstreamConfig 表示一条上游转发规则。
// Mode 为 "" / "ech" 时走 ECH 域前置（要求目标在 Cloudflare 后面），
// 为 "sni" 时走 SNI 伪装直连（DoH 解析真实 IP + 假 SNI + Host 路由，
// 用于不在 Cloudflare 后面、仅被 SNI 阻断的站点）。
// Rewrites 非空时启用响应域名替换：key 为真实域名，value 为代理入口域名。
// 替换应用到响应头（Location/Refresh）与文本 body（html/js/json/xml），
// 使页面内所有指向真实域名的绝对 URL 都改走代理入口，形成闭环。
type UpstreamConfig struct {
	Host     string            `json:"host"`
	Referer  string            `json:"referer,omitempty"`
	Mode     string            `json:"mode,omitempty"`
	Rewrites map[string]string `json:"rewrites,omitempty"`
	Wildcard *WildcardRule     `json:"wildcard,omitempty"`
}

// UpstreamMap 按请求域名索引的上游配置集合。
type UpstreamMap map[string]UpstreamConfig

// Config 是上游配置文件的完整结构：证书 URL + 上游路由规则。
// 证书位置直接写死在此配置里（cert_path/key_path 为可访问的 URL），
// 证书续期后只需更新该文件，无需改代码。
type Config struct {
	CertPath  string      `json:"cert_path"`
	KeyPath   string      `json:"key_path"`
	Upstreams UpstreamMap `json:"upstreams"`
}

// FetchBytes 从 URL 拉取内容到内存（不落盘），请求失败或状态非 200 时报错。
func FetchBytes(rawURL string) ([]byte, error) {
	client := netdial.Client(30 * time.Second)
	resp, err := client.Get(rawURL)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status: %s", resp.Status)
	}

	return io.ReadAll(io.LimitReader(resp.Body, 64<<20))
}

// LoadConfig 从远程 URL 加载上游配置 JSON（证书 URL + 路由规则）。
func LoadConfig(rawURL string) (*Config, error) {
	client := netdial.Client(30 * time.Second)
	resp, err := client.Get(rawURL)
	if err != nil {
		return nil, fmt.Errorf("fetch upstream config: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status: %s", resp.Status)
	}
	var cfg Config
	if err := json.NewDecoder(resp.Body).Decode(&cfg); err != nil {
		return nil, fmt.Errorf("decode upstream config: %w", err)
	}
	if len(cfg.Upstreams) == 0 {
		return nil, fmt.Errorf("upstream config has no upstreams")
	}
	return &cfg, nil
}

// hopByHopHeaders 是需要按 RFC 2616 处理的逐跳头，转发时必须剔除。
var hopByHopHeaders = []string{
	"Connection",
	"Keep-Alive",
	"Proxy-Authenticate",
	"Proxy-Authorization",
	"Proxy-Connection",
	"Te",
	"Trailer",
	"Transfer-Encoding",
	"Upgrade",
}

// copyHeaders 复制 src 的请求/响应头到 dst，剔除逐跳头与上游 CORS 头
// （CORS 由本代理自定，透传会与 CORSMiddleware 产生重复冲突头）。
func copyHeaders(dst, src http.Header) {
	for k, vs := range src {
		l := strings.ToLower(k)
		if isHopByHop(k) || strings.HasPrefix(l, "access-control-") ||
			strings.HasPrefix(l, "content-security-policy") {
			continue
		}
		for _, v := range vs {
			dst.Add(k, v)
		}
	}
}

func isHopByHop(name string) bool {
	for _, h := range hopByHopHeaders {
		if http.CanonicalHeaderKey(name) == h {
			return true
		}
	}
	return false
}

// buildRewriter 预排序替换键（长域名优先，避免子串误伤），返回替换函数。
// port 非空时追加到替换目标末尾（如 pixiv.l.moonchan.xyz:8443）。
func buildRewriter(rt map[string]string) func([]byte, string) []byte {
	var exact, wildcard []string
	for k := range rt {
		if strings.HasPrefix(k, "*.") {
			wildcard = append(wildcard, k)
		} else {
			exact = append(exact, k)
		}
	}
	sort.Slice(exact, func(i, j int) bool { return len(exact[i]) > len(exact[j]) })
	sort.Slice(wildcard, func(i, j int) bool { return len(wildcard[i]) > len(wildcard[j]) })
	keys := append(exact, wildcard...)
	return func(body []byte, port string) []byte {
		for _, k := range keys {
			target := rt[k]
			if port != "" && !strings.Contains(target, ":") {
				target += ":" + port
			}
			if strings.HasPrefix(k, "*.") {
				body = replaceWildcardDomain(body, k[2:], target)
				continue
			}
			body = replaceDomainBounded(body, k, target)
		}
		return body
	}
}

// replaceWildcardDomain 把 <任意子域>+suffix 替换为 target 中的 "*" 部分,
// 例: "*.iwara.tv" -> "iwara-*.l.moonchan.xyz" 会把 filesq.iwara.tv 替换为
// iwara-filesq.l.moonchan.xyz (带端口由调用方拼入 target)。
// 裸 suffix (如 iwara.tv 无子域) 不在此处理, 由精确规则负责。
func replaceWildcardDomain(body []byte, suffix, target string) []byte {
	var out []byte
	rest := body
	for {
		i := bytes.Index(rest, []byte(suffix))
		if i < 0 {
			out = append(out, rest...)
			break
		}
		// 从后缀前回溯域名主体 (子域部分)。
		start := i
		for start > 0 && isDomainChar(rest[start-1]) {
			start--
		}
		sub := string(rest[start:i])
		if sub == "" {
			// 裸后缀(无子域): 跳过, 交给精确规则。
			out = append(out, rest[:i+len(suffix)]...)
			rest = rest[i+len(suffix):]
			continue
		}
		sub = strings.TrimSuffix(sub, ".")
		if sub == "" {
			out = append(out, rest[:i+len(suffix)]...)
			rest = rest[i+len(suffix):]
			continue
		}
		// 检查边界: 前缀和完整域名前后不能有域名残留。
		var left, right byte
		if start > 0 {
			left = rest[start-1]
		}
		j := i + len(suffix)
		if j < len(rest) {
			right = rest[j]
		}
		if !isDomainChar(left) && !isDomainChar(right) {
			out = append(out, rest[:start]...)
			out = append(out, strings.ReplaceAll(target, "*", sub)...)
			rest = rest[j:]
		} else {
			out = append(out, rest[:j]...)
			rest = rest[j:]
		}
	}
	return out
}

// replaceDomainBounded 把 from 域名替换为 to，要求匹配位置前后都不是
// 域名组成字符（[a-zA-Z0-9-.]），避免误伤 accounts.pixiv.net、
// pixiv.net.cn 这类仅子串相同的名字。
func replaceDomainBounded(body []byte, from, to string) []byte {
	var out []byte
	rest := body
	fromB := []byte(from)
	toB := []byte(to)
	for {
		i := bytes.Index(rest, fromB)
		if i < 0 {
			out = append(out, rest...)
			break
		}
		var left, right byte
		if i > 0 {
			left = rest[i-1]
		}
		if i+len(fromB) < len(rest) {
			right = rest[i+len(fromB)]
		}
		if isDomainChar(left) || isDomainChar(right) {
			out = append(out, rest[:i+1]...)
			rest = rest[i+1:]
			continue
		}
		out = append(out, rest[:i]...)
		out = append(out, toB...)
		rest = rest[i+len(fromB):]
	}
	return out
}

func isDomainChar(c byte) bool {
	return c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' ||
		c >= '0' && c <= '9' || c == '-' || c == '.'
}

// isServiceWorkerPath 判断请求是否为 service worker 脚本 (sw.js / workbox-*.js)。
func isServiceWorkerPath(p string) bool {
	base := p[strings.LastIndex(p, "/")+1:]
	return base == "sw.js" ||
		strings.HasPrefix(base, "service-worker") ||
		(strings.HasPrefix(base, "workbox-") && strings.HasSuffix(base, ".js"))
}

// buildSWProxyMap 收集「真实上游域名 → 代理入口域名[:端口]」映射，
// 供 service worker 注入使用: 前端动态请求上游域名时改道到代理。
func buildSWProxyMap(cfg UpstreamMap, port string) map[string]string {
	m := make(map[string]string)
	for entry, uc := range cfg {
		if uc.Host == "" || strings.Contains(uc.Host, "moonchan.xyz") {
			continue
		}
		target := entry
		if port != "" {
			target += ":" + port
		}
		m[uc.Host] = target
	}
	return m
}

// collectWildcardRules 收集配置中所有通配规则供 SW 注入使用。
func collectWildcardRules(cfg UpstreamMap) []WildcardRule {
	var rules []WildcardRule
	for _, uc := range cfg {
		if uc.Wildcard != nil {
			rules = append(rules, *uc.Wildcard)
		}
	}
	return rules
}

// swOverrideJS 生成注入到 service worker 的 fetch 拦截代码。
// 拦截真实域名请求并改道到代理入口（带原端口），兜住前端运行时动态
// 拼接的 URL（静态 rewriter 无法覆盖的场景）。
// 除显式映射外，还支持通配规则: 任意 <sub>+upstream_suffix 改道
// prefix+<sub>+entry_suffix。
func swOverrideJS(m map[string]string, rules []WildcardRule) string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var b strings.Builder
	b.WriteString(`self.addEventListener('install', () => self.skipWaiting());
self.addEventListener('activate', (e) => e.waitUntil(self.clients.claim()));
const __wtMap = {`)
	for i, k := range keys {
		if i > 0 {
			b.WriteByte(',')
		}
		fmt.Fprintf(&b, "\n  %q: %q", k, m[k])
	}
	b.WriteString(`
};
const __wtRules = [`)
	for i, r := range rules {
		if i > 0 {
			b.WriteByte(',')
		}
		fmt.Fprintf(&b, "\n  {p:%q, es:%q, us:%q}", r.Prefix, r.EntrySuffix, r.UpstreamSuffix)
	}
	b.WriteString(`
];
self.addEventListener('fetch', (e) => {
  try {
    const u = new URL(e.request.url);
    let p = __wtMap[u.hostname];
    if (!p) {
      const h = u.hostname;
      for (const r of __wtRules) {
        if (h === r.us.slice(1)) {
          p = r.p.slice(0, -1) + r.es;
          break;
        }
        if (h.endsWith(r.us)) {
          const sub = h.slice(0, -r.us.length);
          p = r.p + sub + r.es;
          break;
        }
      }
    }
    if (!p) return;
    const d = u.protocol + '//' + p + u.pathname + u.search;
    e.respondWith(fetch(d, {
      method: e.request.method,
      headers: e.request.headers,
      body: e.request.body,
      mode: e.request.mode,
      credentials: e.request.credentials,
      redirect: e.request.redirect,
    }));
  } catch (err) {}
});
`)
	return b.String()
}

// matchWildcard 按 WildcardRule 通配匹配入口:
// host = rule.Prefix + sub + rule.EntrySuffix → 上游 sub + rule.UpstreamSuffix。
// mode 未显式指定时自动探测: 上游在 Cloudflare 段走 ECH, 否则 SNI。
func matchWildcard(cfg UpstreamMap, host string) (UpstreamConfig, bool) {
	for entry, uc := range cfg {
		w := uc.Wildcard
		if w == nil {
			continue
		}
		sub := strings.TrimPrefix(host, w.Prefix)
		if sub == host || sub == "" {
			continue
		}
		if !strings.HasSuffix(sub, w.EntrySuffix) {
			continue
		}
		sub = strings.TrimSuffix(sub, w.EntrySuffix)
		if sub == "" || strings.ContainsAny(sub, ".:/") {
			continue
		}
		// 入口必须与通配规则同域(防止跨规则误匹配)。
		entryHost := strings.TrimPrefix(entry, w.Prefix)
		entryHost = strings.TrimSuffix(entryHost, w.EntrySuffix)
		_ = entryHost
		out := uc
		out.Host = sub + w.UpstreamSuffix
		if out.Referer == "" {
			out.Referer = w.Referer
		}
		if out.Mode == "" || out.Mode == "ech" {
			mode := ""
			if ip, err := resolveHostIP(context.Background(), out.Host); err == nil {
				if !isCloudflareIP(net.ParseIP(ip)) {
					mode = "sni"
				}
			}
			out.Mode = mode
		}
		log.Printf("[通配] %s -> %s (mode=%s referer=%s)", host, out.Host, out.Mode, out.Referer)
		return out, true
	}
	return UpstreamConfig{}, false
}

// cloudflareCIDRs Cloudflare 边缘 IP 段 (AS13335), 用于判断 ECH 是否可用。
var cloudflareCIDRs = []string{
	"104.16.0.0/13",
	"104.24.0.0/14",
	"172.64.0.0/13",
	"141.101.64.0/18",
	"173.245.48.0/20",
	"188.114.96.0/20",
	"190.93.240.0/20",
	"197.234.240.0/22",
	"198.41.128.0/17",
	"162.158.0.0/15",
	"103.21.244.0/22",
	"103.22.200.0/22",
	"103.31.4.0/22",
	"108.162.192.0/18",
	"131.0.72.0/22",
	"2400:cb00::/32",
	"2606:4700::/32",
	"2803:f800::/32",
	"2405:b500::/32",
	"2405:8100::/32",
	"2a06:98c0::/29",
	"2c0f:f248::/32",
}

var cloudflareNets = func() []*net.IPNet {
	var nets []*net.IPNet
	for _, c := range cloudflareCIDRs {
		if _, n, err := net.ParseCIDR(c); err == nil {
			nets = append(nets, n)
		}
	}
	return nets
}()

// isCloudflareIP 判断 IP 是否属于 Cloudflare 边缘段。
func isCloudflareIP(ip net.IP) bool {
	if ip == nil {
		return false
	}
	for _, n := range cloudflareNets {
		if n.Contains(ip) {
			return true
		}
	}
	return false
}

// ProxyHandler 返回一个 gin handler，根据请求 Host 匹配上游规则并转发。
// 命中规则的 Rewrites 非空时启用响应域名替换。
//
// Mode 决定出网方式:
//
//	"" / ech  — ECH 域前置（目标须在 Cloudflare 后）
//	sni       — SNI 伪装直连（DoH 解析真实 IP + 假 SNI + Host 路由）
//	direct    — 普通 HTTPS 直连（目标可直连时）
func ProxyHandler(cfg UpstreamMap) gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		clientIP := c.ClientIP()
		method := c.Request.Method
		rawPath := c.Request.URL.Path
		rawQuery := c.Request.URL.RawQuery

		host := c.Request.Host
		if h, _, err := net.SplitHostPort(host); err == nil {
			host = h
		}

		uc, ok := cfg[host]
		if !ok {
			uc, ok = matchWildcard(cfg, host)
		}
		if !ok {
			log.Printf("[%s] 未找到上游配置: %s", clientIP, host)
			c.String(http.StatusBadGateway, "no upstream for host: %s", host)
			return
		}

		var rewriter func([]byte, string) []byte
		if len(uc.Rewrites) > 0 {
			rewriter = buildRewriter(uc.Rewrites)
		}

		// service worker 注入: sw.js/workbox 响应前插 fetch 拦截,
		// 把动态出现的 *.iwara.tv 等真实域名请求改道到代理入口,
		// 兜住 JS 运行时拼接的 URL(rewriter 改不到)。
		if isServiceWorkerPath(rawPath) {
			port := ""
			if _, p, err := net.SplitHostPort(c.Request.Host); err == nil {
				port = p
			}
			swProxyMap := buildSWProxyMap(cfg, port)
			swRules := collectWildcardRules(cfg)
			if len(swProxyMap) > 0 {
				c.Header("Content-Type", "application/javascript")
				c.Status(http.StatusOK)
				c.Writer.Write([]byte(swOverrideJS(swProxyMap, swRules)))
				log.Printf("[%s] %s %s -> SW override 注入 %d 条规则 %d 条通配", clientIP, method, rawPath, len(swProxyMap), len(swRules))
				return
			}
		}

		targetURL := &url.URL{
			Scheme:   "https",
			Host:     uc.Host,
			Path:     rawPath,
			RawQuery: rawQuery,
		}
		urlStr := targetURL.String()

		log.Printf("[%s] %s %s -> %s", clientIP, method, rawPath, urlStr)

		outReq, err := http.NewRequest(method, urlStr, c.Request.Body)
		if err != nil {
			log.Printf("[%s] 创建请求失败: %v", clientIP, err)
			c.String(http.StatusInternalServerError, "create request: %v", err)
			return
		}

		copyHeaders(outReq.Header, c.Request.Header)
		if uc.Referer != "" {
			outReq.Header.Set("Referer", uc.Referer)
		}
		outReq.Host = uc.Host
		outReq.ContentLength = c.Request.ContentLength

		applyCookies(uc.Host, outReq)

		resp, err := proxyRoundTrip(outReq, uc.Mode)
		if err != nil {
			log.Printf("[%s] 上游请求失败: %v (耗时: %v)", clientIP, err, time.Since(start))
			c.String(http.StatusBadGateway, "upstream: %v", err)
			return
		}
		defer resp.Body.Close()

		saveCookies(uc.Host, resp)

		log.Printf("[%s] <- %s (耗时: %v)", clientIP, resp.Status, time.Since(start))

		copyHeaders(c.Writer.Header(), resp.Header)
		c.Status(resp.StatusCode)

		if rewriter != nil {
			port := ""
			if _, p, err := net.SplitHostPort(c.Request.Host); err == nil {
				port = p
			}
			if loc := resp.Header.Get("Location"); loc != "" {
				c.Writer.Header().Set("Location", string(rewriter([]byte(loc), port)))
			}
			if refresh := resp.Header.Get("Refresh"); refresh != "" {
				c.Writer.Header().Set("Refresh", string(rewriter([]byte(refresh), port)))
			}

			// 文本响应整体读入 → 解压 → 域名替换 → 原文输出（去掉 Content-Encoding）。
			// 已知大响应（>8MB）跳过替换，保持流式。
			if isTextContent(resp.Header.Get("Content-Type")) &&
				(resp.ContentLength <= 0 || resp.ContentLength <= 8<<20) {
				if body, err := io.ReadAll(resp.Body); err == nil {
					if body, err = decompressBody(body, resp.Header.Get("Content-Encoding")); err == nil {
						body = rewriter(body, port)
						c.Writer.Header().Del("Content-Encoding")
						c.Writer.Header().Set("Content-Length", strconv.Itoa(len(body)))
						if _, werr := c.Writer.Write(body); werr == nil {
							return
						}
					}
				}
			}
		}

		// 流式转发（SSE 等）：边读边写并 flush，避免缓冲导致的首字节延迟。
		buf := make([]byte, 32*1024)
		for {
			n, rerr := resp.Body.Read(buf)
			if n > 0 {
				if _, werr := c.Writer.Write(buf[:n]); werr != nil {
					break
				}
				if f, ok := c.Writer.(http.Flusher); ok {
					f.Flush()
				}
			}
			if rerr != nil {
				break
			}
		}
	}
}

// ---- 出网分发 ----

// ModeName 返回模式的中文描述，用于启动 banner。
func ModeName(mode string) string {
	switch mode {
	case "sni":
		return "SNI 伪装"
	case "direct":
		return "直接"
	default:
		return "ECH"
	}
}

// proxyRoundTrip 按 mode 分发请求到对应出网通道:
//
//	direct — 普通 HTTPS 直连（标准 DNS + TLS）
//	sni    — SNI 伪装直连（DoH 解析真实 IP + 假 SNI）
//	其他   — ECH 域前置
func proxyRoundTrip(req *http.Request, mode string) (*http.Response, error) {
	switch mode {
	case "direct":
		req.Host = ""
		return (netdial.Client(0)).Do(req)
	case "sni":
		log.Printf("-> SNI 伪装: %s %s (Host: %s)", req.Method, req.URL.String(), req.Host)
		return sniFrontDo(req)
	default:
		log.Printf("-> ECH Do: %s %s (Host: %s)", req.Method, req.URL.String(), req.Host)
		return cloudflare_ech.Do(req)
	}
}

// maxRewriteSize 超过该字节数的文本响应不做替换（直接流式透传）。
const maxRewriteSize = 8 << 20

// isTextContent 判断 Content-Type 是否为可替换的文本类型。
func isTextContent(ct string) bool {
	ct = strings.ToLower(ct)
	return strings.HasPrefix(ct, "text/") ||
		strings.Contains(ct, "json") ||
		strings.Contains(ct, "javascript") ||
		strings.Contains(ct, "xml") ||
		strings.Contains(ct, "x-www-form-urlencoded")
}

// decompressBody 按 Content-Encoding 解压响应体，支持 gzip/br/zstd/identity。
func decompressBody(body []byte, encoding string) ([]byte, error) {
	switch strings.ToLower(encoding) {
	case "gzip":
		r, err := gzip.NewReader(bytes.NewReader(body))
		if err != nil {
			return nil, err
		}
		defer r.Close()
		return io.ReadAll(r)
	case "br":
		return io.ReadAll(brotli.NewReader(bytes.NewReader(body)))
	case "zstd":
		r, err := zstd.NewReader(bytes.NewReader(body))
		if err != nil {
			return nil, err
		}
		defer r.Close()
		return io.ReadAll(r)
	default:
		return body, nil
	}
}

// ---- SNI 伪装直连 ----

// fakeSNI 是 TLS ClientHello 里对外可见的 SNI，必须是不可疑的域名。
// 目标站点 nginx 按 HTTP Host 头路由，不看 SNI。
const fakeSNI = "cloudflare-ech.com"

type ipCacheEntry struct {
	ip     string
	expiry time.Time
}

var (
	ipCacheMu sync.Mutex
	ipCache   = map[string]ipCacheEntry{}
)

// resolveHostIP 通过 DoH 解析域名真实 IP（绕过被污染的本地 DNS），带 TTL 缓存。
func resolveHostIP(ctx context.Context, host string) (string, error) {
	ipCacheMu.Lock()
	if e, ok := ipCache[host]; ok && time.Now().Before(e.expiry) {
		ipCacheMu.Unlock()
		return e.ip, nil
	}
	ipCacheMu.Unlock()

	u := fmt.Sprintf("https://moonchan.xyz/doh?name=%s&type=1", url.QueryEscape(host))
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("Accept", "application/dns-json")
	resp, err := (netdial.Client(8 * time.Second)).Do(req)
	if err != nil {
		return "", fmt.Errorf("DoH %s: %w", host, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("DoH %s status %d", host, resp.StatusCode)
	}
	var d struct {
		Answer []struct {
			Type int    `json:"type"`
			TTL  int    `json:"TTL"`
			Data string `json:"data"`
		} `json:"Answer"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&d); err != nil {
		return "", err
	}
	for _, ans := range d.Answer {
		if ans.Type != 1 || net.ParseIP(ans.Data) == nil {
			continue
		}
		ttl := ans.TTL
		if ttl <= 0 {
			ttl = 300
		}
		if ttl > 86400 {
			ttl = 86400
		}
		ipCacheMu.Lock()
		ipCache[host] = ipCacheEntry{ip: ans.Data, expiry: time.Now().Add(time.Duration(ttl) * time.Second)}
		ipCacheMu.Unlock()
		return ans.Data, nil
	}
	return "", fmt.Errorf("no A record for %s", host)
}

// newSNIFrontTransport 返回一个 TLS ClientHello 只声明 http/1.1 ALPN 的
// transport：TCP 直连目标真实 IP，SNI 使用不敏感域名。
// 源站证书为自签（CN=localhost 之类），故 InsecureSkipVerify；
// 如需更严格可改为固定证书公钥。
func newSNIFrontTransport(ip string) *http.Transport {
	return &http.Transport{
		DialTLSContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			d := &net.Dialer{Timeout: 8 * time.Second}
			conn, err := d.DialContext(ctx, "tcp", net.JoinHostPort(ip, "443"))
			if err != nil {
				return nil, err
			}
			tc := tls.Client(conn, &tls.Config{
				ServerName:         fakeSNI,
				InsecureSkipVerify: true,
				NextProtos:         []string{"http/1.1"},
			})
			if err := tc.HandshakeContext(ctx); err != nil {
				conn.Close()
				return nil, fmt.Errorf("SNI 伪装握手: %w", err)
			}
			return tc, nil
		},
		ForceAttemptHTTP2:   false,
		MaxIdleConns:        100,
		IdleConnTimeout:     90 * time.Second,
		TLSHandshakeTimeout: 10 * time.Second,
	}
}

// sniFrontDo 通过 SNI 伪装直连 req.Host 指向的站点：
// DoH 解析真实 IP → TCP 直连 → TLS 假 SNI → HTTP Host 填真实域名。
// GFW 对 TLS 的 RST 是概率性的，失败时清缓存重试一次。
func sniFrontDo(req *http.Request) (*http.Response, error) {
	ctx := req.Context()
	host := req.Host

	var lastErr error
	for attempt := 0; attempt < 2; attempt++ {
		ip, err := resolveHostIP(ctx, host)
		if err != nil {
			return nil, err
		}
		outReq := req.Clone(ctx)
		outReq.URL.Scheme = "https"
		outReq.URL.Host = ip
		outReq.Host = host

		tr := newSNIFrontTransport(ip)
		// 不用总 Timeout(会砍掉大文件下载), 只等响应头最多 30s。
		tr.ResponseHeaderTimeout = 30 * time.Second
		resp, err := (&http.Client{Transport: tr}).Do(outReq)
		if err == nil {
			return resp, nil
		}
		lastErr = err
		tr.CloseIdleConnections()
		ipCacheMu.Lock()
		delete(ipCache, host)
		ipCacheMu.Unlock()
	}
	return nil, lastErr
}

// ---- 代理内存 Cookie 存储 ----

var (
	cookieMu  sync.Mutex
	cookieJar = map[string][]*http.Cookie{} // 按上游域名分组
)

// saveCookies 把响应的 Set-Cookie 存入内存 jar（按上游域名分组，同名覆盖）。
func saveCookies(host string, resp *http.Response) {
	sc := resp.Header.Values("Set-Cookie")
	if len(sc) == 0 {
		return
	}
	cookieMu.Lock()
	defer cookieMu.Unlock()

	jar := cookieJar[host]
	keep := make(map[string]*http.Cookie, len(jar)+len(sc))
	now := time.Now()
	for _, c := range jar {
		if !isCookieAlive(c, now) {
			continue
		}
		keep[c.Name] = c
	}
	for _, s := range sc {
		c, err := http.ParseSetCookie(s)
		if err != nil {
			continue
		}
		// Max-Age 是相对秒数，转成绝对过期时间，统一用 isCookieAlive 判定。
		if c.MaxAge > 0 && c.Expires.IsZero() {
			c.Expires = now.Add(time.Duration(c.MaxAge) * time.Second)
		}
		if !isCookieAlive(c, now) {
			continue
		}
		keep[c.Name] = c
	}
	if len(keep) == 0 {
		delete(cookieJar, host)
		return
	}
	jar = jar[:0]
	for _, c := range keep {
		jar = append(jar, c)
	}
	cookieJar[host] = jar
}

func isCookieAlive(c *http.Cookie, now time.Time) bool {
	if c.MaxAge < 0 {
		return false
	}
	if !c.Expires.IsZero() && now.After(c.Expires) {
		return false
	}
	return true
}

// applyCookies 把内存 jar 中该上游域名的 cookie 合并进请求。
// jar 中同名 cookie 优先（服务端最近下发的为准），并顺带清理过期项。
func applyCookies(host string, req *http.Request) {	cookieMu.Lock()
	defer cookieMu.Unlock()

	now := time.Now()
	merged := map[string]string{}
	alive := cookieJar[host][:0]
	for _, c := range cookieJar[host] {
		if !isCookieAlive(c, now) {
			continue
		}
		alive = append(alive, c)
		merged[c.Name] = c.Value
	}
	if len(alive) == 0 {
		delete(cookieJar, host)
	} else {
		cookieJar[host] = alive
	}

	// 合并客户端（浏览器）已带的 cookie，jar 优先。
	for _, c := range req.Cookies() {
		if _, ok := merged[c.Name]; !ok {
			merged[c.Name] = c.Value
		}
	}
	if len(merged) == 0 {
		return
	}
	parts := make([]string, 0, len(merged))
	for name, val := range merged {
		parts = append(parts, name+"="+val)
	}
	req.Header.Set("Cookie", strings.Join(parts, "; "))
}
