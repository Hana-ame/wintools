// Package echproxy 提供 ECH 域前置反向代理的核心组件。
// 包括上游配置加载、TLS 证书下载和代理 HTTP handler。
package echproxy

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	cloudflare_ech "github.com/Hana-ame/wintools/pkg/ech"
	"github.com/gin-gonic/gin"
)

// UpstreamConfig 表示一条上游转发规则。
// Mode 为 "" / "ech" 时走 ECH 域前置（要求目标在 Cloudflare 后面），
// 为 "sni" 时走 SNI 伪装直连（DoH 解析真实 IP + 假 SNI + Host 路由，
// 用于不在 Cloudflare 后面、仅被 SNI 阻断的站点）。
type UpstreamConfig struct {
	Host    string `json:"host"`
	Referer string `json:"referer,omitempty"`
	Mode    string `json:"mode,omitempty"`
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
	client := &http.Client{Timeout: 30 * time.Second}
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
	client := &http.Client{Timeout: 30 * time.Second}
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

// copyHeaders 复制 src 的请求/响应头到 dst，剔除逐跳头。
func copyHeaders(dst, src http.Header) {
	for k, vs := range src {
		if isHopByHop(k) {
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

// ProxyHandler 返回一个 gin handler，根据请求 Host 匹配上游规则并通过 ECH 转发。
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
			log.Printf("[%s] 未找到上游配置: %s", clientIP, host)
			c.String(http.StatusBadGateway, "no upstream for host: %s", host)
			return
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

		var resp *http.Response
		if uc.Mode == "sni" {
			log.Printf("[%s] -> SNI 伪装: %s %s (Host: %s)", clientIP, method, urlStr, outReq.Host)
			resp, err = sniFrontDo(outReq)
		} else {
			log.Printf("[%s] -> ECH Do: %s %s (Host: %s)", clientIP, method, urlStr, outReq.Host)
			resp, err = cloudflare_ech.Do(outReq)
		}
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
	resp, err := (&http.Client{Timeout: 8 * time.Second}).Do(req)
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
		resp, err := (&http.Client{Transport: tr, Timeout: 30 * time.Second}).Do(outReq)
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
func applyCookies(host string, req *http.Request) {
	cookieMu.Lock()
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
