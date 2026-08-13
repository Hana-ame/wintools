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
	"regexp"
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
	utls "github.com/refraction-networking/utls"
	"golang.org/x/net/http2"
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
	Host    string `json:"host"`
	Referer string `json:"referer,omitempty"`
	// Cookie 固定注入上游请求 (初始化 cookie overrider):
	// 在 upstream.json 里直接写死需要携带的 Cookie 头原文,
	// 适用于 exhentai 等需要登录态/特殊 cookie 的站点, 不依赖浏览器。
	// 优先级: 此固定 cookie > 内存 jar > 客户端 cookie。
	Cookie string `json:"cookie,omitempty"`
	// SWInject 为无 service worker 的站点注入代理拦截 SW:
	// HTML 页面自动注册 /wt-sw.js, 代理对该路径返回生成的 fetch 拦截
	// 脚本, 兜住前端运行时动态拼接的 URL (响应重写覆盖不到)。
	// 仅用于没有自己的 SW 的站点 (如 dlsite); 有 workbox 的站点
	// (如 iwara) 绝不能开 — 注入会与 workbox 变量冲突/互相覆盖。
	SWInject bool              `json:"sw_inject,omitempty"`
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
	// BlockedHosts 直连/代理都无法到达的第三方域名 (完整 https:// 前缀):
	// 响应中出现的这些 URL 整段剔除, 浏览器不再发起请求避免挂起超时。
	// 用于 Google 字体/jsapi 等被墙资源、无法代理的 CDN (如 media.dlsite.com)。
	BlockedHosts []string `json:"blocked_hosts,omitempty"`
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

// rewriteSetCookieDomains 用的正则: 每请求都要执行, 提为包级变量避免热路径
// 反复编译 (regexp.MustCompile 有一次性分配成本)。
var (
	setCookieHasDomainRE = regexp.MustCompile(`(?i);\s*Domain=`)
	setCookieReplaceRE   = regexp.MustCompile(`(?i);\s*Domain=[^;]*`)
	setCookieSecureRE    = regexp.MustCompile(`(?i);\s*Secure`)
)

// rewriteSetCookieDomains 把响应 Set-Cookie 头规范化, 让浏览器能正常存储:
//  1. Domain=.dlsite.com 等上游域 → 改写为当前代理域 (dlsite.l.moonchan.xyz),
//     否则浏览器因域不匹配拒绝存储 → 前端 JS 读不到 cookie → 弹窗无限循环。
//  2. Secure 标志: 上游 https 下发 Secure cookie, 若代理跑在 http 模式
//     浏览器不会存 (Secure cookie 只能经 https 传输), 需移除。
//
// 内存 jar (按上游域名分组) 管代理→上游的认证 cookie, 与此无关;
// 这里只保证浏览器端能存下前端状态 cookie (语言/成人确认等)。
func rewriteSetCookieDomains(h http.Header, proxyHost string, httpMode bool) {
	scs := h.Values("Set-Cookie")
	if len(scs) == 0 {
		return
	}
	h.Del("Set-Cookie")
	domain := proxyHost
	if hh, _, err := net.SplitHostPort(proxyHost); err == nil {
		domain = hh
	}
	hasDomain := setCookieHasDomainRE
	replaceDomain := setCookieReplaceRE
	secureRE := setCookieSecureRE
	for _, s := range scs {
		if hasDomain.MatchString(s) {
			s = replaceDomain.ReplaceAllString(s, "; Domain="+domain)
		}
		if httpMode {
			s = secureRE.ReplaceAllString(s, "")
		}
		h.Add("Set-Cookie", s)
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
// buildEntryRewriter 从单条 UpstreamConfig 构造响应域名重写器:
// 精确 rewrites 原样使用; wildcard 非空时自动推导通配条目——
//
//	*.iwara.tv  -> iwara-*.l.moonchan.xyz (任意子域)
//	iwara.tv    -> iwara.l.moonchan.xyz   (裸域)
//
// 规则单一来源: 配置只写 wildcard 一次, 响应重写/SW 拦截都从这里推导。
// blocked 为全局剔除域名列表 (Config.BlockedHosts, upstream.json 可配)。
func buildEntryRewriter(uc UpstreamConfig, blocked []string) func([]byte, string) []byte {
	rules := make(map[string]string, len(uc.Rewrites)+2)
	for k, v := range uc.Rewrites {
		rules[k] = v
	}
	if uc.Wildcard != nil {
		w := uc.Wildcard
		// 通配: *.iwara.tv -> iwara-*.l.moonchan.xyz
		rules["*"+w.UpstreamSuffix] = w.Prefix + "*" + w.EntrySuffix
		// 裸域: iwara.tv -> iwara.l.moonchan.xyz
		base := strings.TrimSuffix(w.Prefix, "-")
		bare := strings.TrimPrefix(w.UpstreamSuffix, ".")
		rules[bare] = base + w.EntrySuffix
		// 带点前缀: .iwara.tv -> iwara.l.moonchan.xyz
		// 前端 JS 常写 document.cookie='...; Domain=.iwara.tv',
		// 通配回溯会把单点 trim 成空当裸域跳过, 这里显式补规则。
		rules["."+bare] = base + w.EntrySuffix
	}
	// 直连不可达的第三方域名(被墙): 响应里出现的这些 URL 整段删除,
	// 浏览器不再发起请求, 避免挂起超时。ECH/SNI 都无法到达这些域名。
	// 先删整段 URL(专用函数), 再做域名重写。
	// 列表来自 Config.BlockedHosts (upstream.json 可配)。
	base := buildRewriter(rules)
	return func(body []byte, port string) []byte {
		return base(stripBlockedURLs(body, blocked), port)
	}
}

// isJavascriptResponse 判断上游响应是否为 JS 脚本 (供 SW 处理判断:
// 是 JS 则注入前缀保留内容, 否则整体替换为生成的 SW)。
func isJavascriptResponse(resp *http.Response) bool {
	ct := resp.Header.Get("Content-Type")
	if ct == "" {
		return strings.HasSuffix(resp.Request.URL.Path, ".js")
	}
	return strings.Contains(strings.ToLower(ct), "javascript")
}

// stripBlockedURLs 从响应文本中移除被墙第三方域名的完整 URL 值。
// 处理形如 href="https://fonts.googleapis.com/..." / src="https://.../jsapi"
// 的整段引用: 把整个 URL(含引号内内容)替换为空串, 浏览器不再发起请求。
// 与域名重写互补: 重写只换域名, 这里删整段。
// 同时匹配 https://host 与 //host (协议相对) 两种形式。
func stripBlockedURLs(body []byte, blocked []string) []byte {
	out := body
	for _, b := range blocked {
		out = stripOneBlockedURL(out, []byte(b))
		// 协议相对形式: https://media.dlsite.com -> //media.dlsite.com
		if strings.HasPrefix(b, "https://") {
			out = stripOneBlockedURL(out, []byte("//"+b[len("https://"):]))
		}
	}
	return out
}

// stripOneBlockedURL 删除所有包含 blocked 前缀的引号内 URL。
// 支持 '...' 与 "..." 两种引号; blocked 匹配 URL 开头。
func stripOneBlockedURL(body, blocked []byte) []byte {
	var out []byte
	rest := body
	for {
		i := bytes.Index(rest, blocked)
		if i < 0 {
			out = append(out, rest...)
			break
		}
		// 向前找 URL 起点: 最近的 ' 或 "。
		start := i
		for start > 0 && rest[start-1] != '\'' && rest[start-1] != '"' {
			start--
		}
		if start == 0 {
			// 没有引号包裹(裸文本), 保守只删匹配前缀本身。
			out = append(out, rest[:i]...)
			rest = rest[i+len(blocked):]
			continue
		}
		// 从引号后到匹配起点之间应是 https:// 等协议前缀。
		// between 为空说明引号紧贴匹配点(URL 以 blocked 开头), 也合法。
		between := rest[start:i]
		if !bytes.HasPrefix(between, []byte("https://")) &&
			!bytes.HasPrefix(between, []byte("http://")) &&
			!bytes.HasPrefix(between, []byte("//")) &&
			len(between) > 0 {
			// 协议前缀不符, 视为普通文本(如 JS 字符串), 只删匹配前缀。
			out = append(out, rest[:i]...)
			rest = rest[i+len(blocked):]
			continue
		}
		// 找到右引号 (start 指向引号后第一个字符, 引号是 rest[start-1])。
		quote := rest[start-1]
		j := i
		for j < len(rest) && rest[j] != quote {
			j++
		}
		if j >= len(rest) {
			// 未闭合引号, 删到匹配结尾, 保留引号。
			out = append(out, rest[:start]...)
			rest = rest[i+len(blocked):]
			continue
		}
		out = append(out, rest[:start]...)
		rest = rest[j:]
	}
	return out
}

// buildRewriter 从重写规则表构造域名替换器。
// 精确 key 与通配 key("*.suffix") 并存, 精确优先。
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
			// 带点 key (如 .dlsite.com) 是 cookie Domain 专用:
			// Domain 属性不允许带端口, 不加 port; 其他 key 照常追加。
			if port != "" && !strings.HasPrefix(k, ".") && !strings.Contains(target, ":") {
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
			// 跳过 URL 编码 %XX: %2F 里的十六进制字符(F/2)不是子域,
			// 否则 https%3A%2F%2Fwww.dlsite.com 会被错拼成
			// dlsite-2Fwww.l.moonchan.xyz, 破坏 login 等带编码链接。
			if start >= 3 && rest[start-3] == '%' &&
				isHexDigit(rest[start-2]) && isHexDigit(rest[start-1]) {
				break
			}
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
		// left 是 %XX 的最后一个 hex 时(如 %2F 的 F)不算域名残留。
		leftOK := !isDomainChar(left)
		if start >= 3 && rest[start-3] == '%' && isHexDigit(rest[start-2]) && isHexDigit(left) {
			leftOK = true
		}
		if leftOK && !isDomainChar(right) {
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
		// left 是 %XX 的最后一个 hex 时(如 %2F 的 F)不算域名残留,
		// 否则 URL 编码链接里的域名不会被替换。
		leftOK := !isDomainChar(left)
		if i >= 3 && rest[i-3] == '%' && isHexDigit(rest[i-2]) && isHexDigit(left) {
			leftOK = true
		}
		if leftOK && !isDomainChar(right) {
			out = append(out, rest[:i]...)
			out = append(out, toB...)
			rest = rest[i+len(fromB):]
			continue
		}
		out = append(out, rest[:i+1]...)
		rest = rest[i+1:]
	}
	return out
}

func isDomainChar(c byte) bool {
	return c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' ||
		c >= '0' && c <= '9' || c == '-' || c == '.'
}

// isHexDigit 判断是否为 URL 编码 %XX 中的十六进制字符。
func isHexDigit(c byte) bool {
	return c >= '0' && c <= '9' || c >= 'a' && c <= 'f' || c >= 'A' && c <= 'F'
}

// buildSWProxyMap 收集「真实上游域名 → 代理入口域名[:端口]」映射，
// 供 service worker 注入使用: 前端动态请求上游域名时改道到代理。
// 本项目与 l.moonchan.xyz 强捆绑, 无需解耦:
// 跳过上游是代理自身域名的条目 (如 reminder.moonchan.xyz),
// 避免 SW 把代理入口当上游改道。
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
// blockedHosts 来自 Config.BlockedHosts: 直连不可达的第三方域名,
// SW 直接拦截返回 204 (与响应侧 stripBlockedURLs 互补, 覆盖动态请求)。
func swOverrideJS(m map[string]string, rules []WildcardRule, blockedHosts []string) string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	// SW 里匹配 hostname+path 前缀, 去掉配置里的 https:// 前缀。
	swBlocked := make([]string, 0, len(blockedHosts))
	for _, b := range blockedHosts {
		swBlocked = append(swBlocked, strings.TrimPrefix(b, "https://"))
	}
	sort.Strings(swBlocked)

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
// __wtBlock: 直连不可达的第三方域名(Config.BlockedHosts),
// 直接在 SW 里拦截返回 204, 避免页面挂起等待。
const __wtBlock = [`)
	for i, bl := range swBlocked {
		if i > 0 {
			b.WriteByte(',')
		}
		fmt.Fprintf(&b, "\n  %q", bl)
	}
	b.WriteString(`
];
self.addEventListener('fetch', (e) => {
  try {
    const u = new URL(e.request.url);
    if (__wtBlock.some((b) => (u.hostname + u.pathname).startsWith(b))) {
      e.respondWith(new Response('', { status: 204 }));
      return;
    }
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

// MatchWildcardForTest 导出通配匹配判断, 供 main 路由分发确认
// 请求是否命中通配入口(未匹配到精确配置时)。
func MatchWildcardForTest(cfg UpstreamMap, host string) (UpstreamConfig, bool) {
	return matchWildcard(cfg, host)
}

// matchWildcard 按 WildcardRule 通配匹配入口:
// host = rule.Prefix + sub + rule.EntrySuffix → 上游 sub + rule.UpstreamSuffix。
// mode 未显式指定时自动探测: 上游在 Cloudflare 段走 ECH, 否则 SNI。
func matchWildcard(cfg UpstreamMap, host string) (UpstreamConfig, bool) {
	for _, uc := range cfg {
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
		// 子域为空或含端口/路径分隔符时视为非法; 含点 (多级子域) 合法,
		// 由 C13 放开 (此前误拒 files.q.iwara.tv 这类多级子域)。
		if sub == "" || strings.ContainsAny(sub, ":/") {
			continue
		}
		out := uc
		out.Host = sub + w.UpstreamSuffix
		if out.Referer == "" {
			out.Referer = w.Referer
		}
		// 通配入口继承主入口固定 cookie (如整站需要同一登录态)。
		if out.Cookie == "" {
			out.Cookie = uc.Cookie
		}
		// 通配入口继承主入口的 sw_inject (无 SW 站点的 SW 兜底开关)。
		if !out.SWInject {
			out.SWInject = uc.SWInject
		}
		if out.Mode == "" || out.Mode == "ech" {
			out.Mode = wildcardMode(context.Background(), out.Host)
		}
		debugLogf("[通配] %s -> %s (mode=%s referer=%s cookie=%v)", host, out.Host, out.Mode, out.Referer, out.Cookie != "")
		return out, true
	}
	return UpstreamConfig{}, false
}

// wildcardModeCache 通配入口的 mode 探测结果缓存 (host -> entry)。
// 带 TTL (wildcardModeTTL), 上游域名从 Cloudflare 迁走后 mode 不会永远错误;
// 过期后下次请求重新探测。TTL 取 10 分钟, 兼顾探测成本与 IP 变动时效。
var wildcardModeCache sync.Map

// modeCacheEntry 缓存条目: mode 探测结果 + 过期时间。
type modeCacheEntry struct {
	mode   string
	expiry time.Time
}

const wildcardModeTTL = 10 * time.Minute

// wildcardMode 判断上游域名应走的通道: Cloudflare 段 ECH, 否则 SNI。
// 探测一次后缓存 wildcardModeTTL 时长, 过期自动重查。
func wildcardMode(ctx context.Context, host string) string {
	if v, ok := wildcardModeCache.Load(host); ok {
		e := v.(modeCacheEntry)
		if time.Now().Before(e.expiry) {
			return e.mode
		}
	}
	mode := ""
	if ip, err := resolveHostIP(ctx, host); err == nil {
		if !isCloudflareIP(net.ParseIP(ip)) {
			mode = "sni"
		}
	}
	wildcardModeCache.Store(host, modeCacheEntry{mode: mode, expiry: time.Now().Add(wildcardModeTTL)})
	return mode
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

// inheritWildcard 从配置中找到所属站族主入口的 wildcard 规则。
// 子入口(如 iwara-api.l.moonchan.xyz)未显式配置 wildcard 时,
// 继承主入口(iwara.l.moonchan.xyz)的通配规则, 保证响应重写一致。
// 判断依据: 入口名以 wildcard.Prefix 开头且共享 EntrySuffix。
func inheritWildcard(cfg UpstreamMap, entry string) *WildcardRule {
	for _, uc := range cfg {
		w := uc.Wildcard
		if w == nil {
			continue
		}
		if strings.HasPrefix(entry, w.Prefix) && strings.HasSuffix(entry, w.EntrySuffix) {
			return w
		}
	}
	return nil
}

// Debug 控制详细请求日志 (每请求的转发/响应行)。默认关闭;
// 错误日志与启动日志不受影响, 始终输出。
// 由 main 通过 -v flag 或环境变量开启, 远程部署日志量大时默认静默。
var Debug bool

// debugLogf 仅在 Debug 开启时输出, 用于每请求的转发日志。
func debugLogf(format string, args ...interface{}) {
	if Debug {
		log.Printf(format, args...)
	}
}

// ProxyHandler 返回一个 gin handler，根据请求 Host 匹配上游规则并转发。
// 命中规则的 Rewrites 非空时启用响应域名替换。
// blockedHosts 为 Config.BlockedHosts。
//
// Mode 决定出网方式:
//
//	"" / ech  — ECH 域前置（目标须在 Cloudflare 后）
//	sni       — SNI 伪装直连（DoH 解析真实 IP + 假 SNI + Host 路由）
//	direct    — 普通 HTTPS 直连（目标可直连时）
func ProxyHandler(cfg UpstreamMap, blockedHosts []string) gin.HandlerFunc {
	blocked := append([]string(nil), blockedHosts...)
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

		// 响应域名重写器: 精确 rewrites + wildcard 推导的通配条目,
		// 规则单一来源 (wildcard 字段), 不在此处重复配置。
		// 子入口未配 wildcard 时继承主入口的通配规则。
		ucForRewrite := uc
		if ucForRewrite.Wildcard == nil {
			ucForRewrite.Wildcard = inheritWildcard(cfg, host)
		}
		rewriter := buildEntryRewriter(ucForRewrite, blocked)

		// service worker 兜底标记: 仅对该入口配置了 SWInject (无 SW 站点
		// 如 dlsite) 且请求的是代理专用 /wt-sw.js 时生效。
		// 有 workbox 的站点 (iwara) 不配 SWInject, 代理绝不碰其 SW 文件
		// (注入会与 workbox 变量冲突, 导致整个 SW 崩溃)。
		swWant := uc.SWInject && rawPath == "/wt-sw.js"

		targetURL := &url.URL{
			Scheme:   "https",
			Host:     uc.Host,
			Path:     rawPath,
			RawQuery: rawQuery,
		}
		urlStr := targetURL.String()

		debugLogf("[%s] %s %s -> %s", clientIP, method, rawPath, urlStr)

		outReq, err := http.NewRequest(method, urlStr, c.Request.Body)
		if err != nil {
			log.Printf("[%s] 创建请求失败: %v", clientIP, err)
			c.String(http.StatusInternalServerError, "create request: %v", err)
			return
		}

		copyHeaders(outReq.Header, c.Request.Header)
		// X-Forwarded-For/Proto 透传: 不设置的话上游看到的全是代理 IP,
		// 防盗链/地域限制会误伤。追加而非覆盖已有链 (客户端可能经多层代理)。
		if xff := c.Request.Header.Get("X-Forwarded-For"); xff != "" {
			outReq.Header.Set("X-Forwarded-For", xff+", "+clientIP)
		} else {
			outReq.Header.Set("X-Forwarded-For", clientIP)
		}
		proto := "http"
		if c.Request.TLS != nil {
			proto = "https"
		}
		outReq.Header.Set("X-Forwarded-Proto", proto)
		if uc.Referer != "" {
			outReq.Header.Set("Referer", uc.Referer)
		}
		outReq.Host = uc.Host
		outReq.ContentLength = c.Request.ContentLength

		applyCookies(uc.Host, outReq)
		// 固定 cookie overrider: upstream.json 里配置的 Cookie 原样覆盖,
		// 覆盖内存 jar 与客户端 cookie (登录态/特殊 cookie 不依赖浏览器)。
		if uc.Cookie != "" {
			outReq.Header.Set("Cookie", uc.Cookie)
		}

		resp, err := proxyRoundTrip(outReq, uc.Mode)
		if err != nil {
			log.Printf("[%s] 上游请求失败: %v (耗时: %v)", clientIP, err, time.Since(start))
			c.String(http.StatusBadGateway, "upstream: %v", err)
			return
		}
		defer resp.Body.Close()

		saveCookies(uc.Host, resp)

		debugLogf("[%s] <- %s (耗时: %v)", clientIP, resp.Status, time.Since(start))

		copyHeaders(c.Writer.Header(), resp.Header)
		// Set-Cookie 规范化: Domain 改写为当前代理域 + http 模式去 Secure,
		// 保证浏览器能存下前端状态 cookie(语言/成人确认等), 不再弹窗循环。
		// 内存 jar 管代理→上游的认证 cookie, 与此无关。
		rewriteSetCookieDomains(c.Writer.Header(), host, c.Request.TLS == nil)

		// SW 兜底: 该入口配了 SWInject 且请求 /wt-sw.js (上游必然 404)
		// 时, 直接返回生成的拦截 SW。用于没有 service worker 的站点
		// (如 dlsite): 前端运行时动态拼接的 img.dlsite.jp 等 URL,
		// 响应重写覆盖不到, 靠 SW fetch 拦截改道代理入口。
		// 必须在 c.Status 之前: gin 的 c.Status 立即写响应头,
		// 之后 WriteHeader(200) 无效 (会返回上游 404)。
		if swWant && !isJavascriptResponse(resp) {
			port := ""
			if _, p, err := net.SplitHostPort(c.Request.Host); err == nil {
				port = p
			}
			swProxyMap := buildSWProxyMap(cfg, port)
			swRules := collectWildcardRules(cfg)
			c.Writer.Header().Del("Content-Length")
			c.Writer.Header().Set("Content-Type", "application/javascript")
			c.Writer.WriteHeader(200)
			c.Writer.Write([]byte(swOverrideJS(swProxyMap, swRules, blocked)))
			debugLogf("[%s] %s %s -> SW 兜底生成 %d 条规则 %d 条通配 %d 条屏蔽", clientIP, method, rawPath, len(swProxyMap), len(swRules), len(blocked))
			return
		}

		c.Status(resp.StatusCode)

		// 写超时防护: 所有响应写出都经过 deadline (见 setWriteDeadline)。
		rc := http.NewResponseController(c.Writer)

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
			// 未知长度（chunked / 无 Content-Length）同样限读 maxRewriteSize+1:
			// 超限放弃重写, 已读部分先写出, 剩余走流式 — 避免无限缓冲 OOM,
			// 也避免 SSE (text/event-stream 命中 isTextContent) 被整体缓存破坏实时性。
			if isTextContent(resp.Header.Get("Content-Type")) &&
				(resp.ContentLength <= 0 || resp.ContentLength <= maxRewriteSize) {
				body, err := io.ReadAll(io.LimitReader(resp.Body, maxRewriteSize+1))
				if err != nil {
					// 读失败: 上游连接已坏, 写出已读部分后结束, 不再继续流式。
					if len(body) > 0 {
						setWriteDeadline(rc)
						c.Writer.Write(body)
					}
					return
				}
				if len(body) > maxRewriteSize {
					// 超限: 已读部分原样写出 (Content-Encoding 头保留,
					// 客户端按原编码解压), 剩余数据由下方流式转发继续。
					if len(body) > 0 {
						setWriteDeadline(rc)
						c.Writer.Write(body)
					}
				} else if body, err = decompressBody(body, resp.Header.Get("Content-Encoding")); err == nil {
					body = rewriter(body, port)
					// HTML 页面注入 SW 自动注册 (该入口配了 SWInject 时):
					// 无 SW 的站点 (dlsite) 需要主动注册才能拦截动态请求,
					// 注册脚本插在 </head> 前, 页面加载即生效。
					// 只注入没有 SW 迹象的页面 (含 'serviceWorker' 的
					// 页面已有注册逻辑, 注入会冲突)。
					if swWant && strings.Contains(strings.ToLower(resp.Header.Get("Content-Type")), "text/html") &&
						!bytes.Contains(body, []byte("serviceWorker")) {
						reg := []byte(`<script>navigator.serviceWorker.register('/wt-sw.js').catch(function(){})</script>`)
						if idx := bytes.Index(body, []byte("</head>")); idx >= 0 {
							body = append(body[:idx], append(reg, body[idx:]...)...)
						} else {
							body = append(body, reg...)
						}
						debugLogf("[%s] %s -> HTML 注入 SW 注册", clientIP, rawPath)
					}
					c.Writer.Header().Del("Content-Encoding")
					c.Writer.Header().Set("Content-Length", strconv.Itoa(len(body)))
					setWriteDeadline(rc)
					if _, werr := c.Writer.Write(body); werr == nil {
						return
					}
				} else {
					// 解压失败: 原样输出压缩体 (保留 Content-Encoding),
					// 客户端自行解压; 不再走流式 (body 已读尽, 流式只会写出空响应)。
					if len(body) > 0 {
						setWriteDeadline(rc)
						c.Writer.Write(body)
					}
					return
				}
			}
		}

		// 流式转发（SSE 等）：边读边写并 flush，避免缓冲导致的首字节延迟。
		// 每次写前重设 deadline: 正常 SSE 每 chunk 刷新, 挂死客户端 60s 断开。
		buf := make([]byte, 32*1024)
		for {
			n, rerr := resp.Body.Read(buf)
			if n > 0 {
				setWriteDeadline(rc)
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
		debugLogf("-> SNI 伪装: %s %s (Host: %s)", req.Method, req.URL.String(), req.Host)
		return sniFrontDo(req)
	default:
		debugLogf("-> ECH Do: %s %s (Host: %s)", req.Method, req.URL.String(), req.Host)
		return cloudflare_ech.Do(req)
	}
}

// maxRewriteSize 超过该字节数的文本响应不做替换（直接流式透传）。
const maxRewriteSize = 8 << 20

// writeTimeout 单次写入的截止时间: 慢客户端 (不消费响应) 超时后断开,
// 防止连接/goroutine 无限堆积。SSE 等长连接每 chunk 都重设, 不受影响。
const writeTimeout = 60 * time.Second

// setWriteDeadline 为当前连接设置写截止时间 (每次写前重设)。
// 底层连接不支持 deadline 时 (如测试用内存 writer) 忽略错误。
func setWriteDeadline(rc *http.ResponseController) {
	rc.SetWriteDeadline(time.Now().Add(writeTimeout))
}

// isTextContent 判断 Content-Type 是否为可替换的文本类型。
func isTextContent(ct string) bool {
	ct = strings.ToLower(ct)
	return strings.HasPrefix(ct, "text/") ||
		strings.Contains(ct, "json") ||
		strings.Contains(ct, "javascript") ||
		strings.Contains(ct, "xml") ||
		strings.Contains(ct, "x-www-form-urlencoded")
}

// readDecompressed 限长读取解压流: 输入虽被 LimitReader 限制在 maxRewriteSize,
// 但压缩比可达数百倍, 8MB 压缩体可炸出 GB 级内存 (压缩炸弹)。
// 解压超过 maxRewriteSize+1 即报错, 调用方回退为原样透传压缩体。
func readDecompressed(r io.Reader) ([]byte, error) {
	const limit = maxRewriteSize + 1
	out, err := io.ReadAll(io.LimitReader(r, limit))
	if err != nil {
		return nil, err
	}
	if int64(len(out)) >= limit {
		return nil, fmt.Errorf("decompressed body exceeds %d bytes", limit)
	}
	return out, nil
}

// decompressBody 按 Content-Encoding 解压响应体，支持 gzip/br/zstd/identity。
// 解压结果限制在 maxRewriteSize 内 (readDecompressed), 超限报错让调用方
// 原样透传压缩体, 防止压缩炸弹 OOM。
// gzip/zstd reader 必须 Close (释放并发解压的 goroutine 与内存)。
func decompressBody(body []byte, encoding string) ([]byte, error) {
	switch strings.ToLower(encoding) {
	case "gzip":
		r, err := gzip.NewReader(bytes.NewReader(body))
		if err != nil {
			return nil, err
		}
		defer r.Close()
		return readDecompressed(r)
	case "br":
		// brotli.Reader 无 Close 方法 (纯内存解压, 无外部资源), 无需释放。
		return readDecompressed(brotli.NewReader(bytes.NewReader(body)))
	case "zstd":
		r, err := zstd.NewReader(bytes.NewReader(body))
		if err != nil {
			return nil, err
		}
		defer r.Close()
		return readDecompressed(r)
	default:
		return body, nil
	}
}

// ---- SNI 伪装直连 ----

// fakeSNI 是 TLS ClientHello 里对外可见的 SNI，必须是不可疑的域名。
// 目标站点 nginx 按 HTTP Host 头路由，不看 SNI。
const fakeSNI = "cloudflare-ech.com"

type ipCacheEntry struct {
	ips    []string
	expiry time.Time
}

var (
	ipCacheMu sync.Mutex
	ipCache   = map[string]ipCacheEntry{}
)

// resolveHostIP 通过 DoH 解析域名真实 IP（绕过被污染的本地 DNS），带 TTL 缓存。
// 返回第一个可用 A 记录，供 mode 探测等只需单 IP 的场景使用。
func resolveHostIP(ctx context.Context, host string) (string, error) {
	ips, err := resolveHostIPs(ctx, host)
	if err != nil {
		return "", err
	}
	return ips[0], nil
}

// resolveHostIPs 返回 DoH 解析出的全部 A 记录（去重），带 TTL 缓存。
// 只取第一个 A 记录的问题: CDN 多 IP 时可能固定连到坏 IP 或已被墙的 IP,
// 失败重试也只会拿到同一个; 全量候选让 sniFrontDo 能按序逐个尝试。
// TTL 取各记录最小值 (保守, IP 变动时尽早重查)。
func resolveHostIPs(ctx context.Context, host string) ([]string, error) {
	ipCacheMu.Lock()
	if e, ok := ipCache[host]; ok && time.Now().Before(e.expiry) {
		ipCacheMu.Unlock()
		return e.ips, nil
	}
	ipCacheMu.Unlock()

	u := fmt.Sprintf("https://moonchan.xyz/doh?name=%s&type=1", url.QueryEscape(host))
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/dns-json")
	resp, err := (netdial.Client(8 * time.Second)).Do(req)
	if err != nil {
		return nil, fmt.Errorf("DoH %s: %w", host, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("DoH %s status %d", host, resp.StatusCode)
	}
	var d struct {
		Answer []struct {
			Type int    `json:"type"`
			TTL  int    `json:"TTL"`
			Data string `json:"data"`
		} `json:"Answer"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&d); err != nil {
		return nil, err
	}
	ttl := 300
	seen := make(map[string]bool)
	var ips []string
	for _, ans := range d.Answer {
		if ans.Type != 1 || net.ParseIP(ans.Data) == nil {
			continue
		}
		if seen[ans.Data] {
			continue
		}
		seen[ans.Data] = true
		ips = append(ips, ans.Data)
		if ans.TTL > 0 && ans.TTL < ttl {
			ttl = ans.TTL
		}
	}
	if len(ips) == 0 {
		return nil, fmt.Errorf("no A record for %s", host)
	}
	if ttl > 86400 {
		ttl = 86400
	}
	ipCacheMu.Lock()
	ipCache[host] = ipCacheEntry{ips: ips, expiry: time.Now().Add(time.Duration(ttl) * time.Second)}
	ipCacheMu.Unlock()
	return ips, nil
}

// sniTransportPool 按目标 IP 复用的 http2.Transport 池:
// 每个 IP 一个 transport, http2.Transport 内部会复用该 IP 的空闲连接,
// 避免每次请求都重新 TCP+TLS 握手 (SNI 伪装握手开销大)。
// IP 集合有限 (代理配置的站点数量级), 不做逐 IP 过期回收;
// 超过 maxSNITransports 时整体重置, 防异常情况下无限增长。
const maxSNITransports = 64

var (
	sniTransportMu sync.Mutex
	sniTransports  = map[string]*http2.Transport{}
)

// getSNITransport 返回复用池中该 IP 的 transport, 不存在则新建。
func getSNITransport(ip string) *http2.Transport {
	sniTransportMu.Lock()
	defer sniTransportMu.Unlock()
	if tr, ok := sniTransports[ip]; ok {
		return tr
	}
	if len(sniTransports) >= maxSNITransports {
		// 超限整体重建: 旧连接由 IdleConnTimeout 自然回收。
		for _, tr := range sniTransports {
			tr.CloseIdleConnections()
		}
		sniTransports = map[string]*http2.Transport{}
	}
	tr := newSNIFrontTransport(ip)
	sniTransports[ip] = tr
	return tr
}

// newSNIFrontTransport 返回一个 TLS ClientHello 只声明 http/1.1 ALPN 的
// transport：TCP 直连目标真实 IP，SNI 使用不敏感域名。
// 源站证书为自签（CN=localhost 之类），故 InsecureSkipVerify；
// 如需更严格可改为固定证书公钥。
// newSNIFrontTransport 返回 SNI 伪装直连的 transport (HTTP/2):
// uTLS 模拟 Chrome TLS 指纹 → ALPN 协商 h2 → http2.Transport 发请求。
// Go http.Transport 对自定义 DialTLSContext 不自动启用 h2
// (TLSNextProto 只在标准 TLSClientConfig 路径注册), 必须直接用
// http2.Transport 承接 h2 帧, 否则收到 SETTINGS 帧报 malformed。
func newSNIFrontTransport(ip string) *http2.Transport {
	dial := func(ctx context.Context) (net.Conn, error) {
		d := &net.Dialer{Timeout: 8 * time.Second}
		conn, err := d.DialContext(ctx, "tcp", net.JoinHostPort(ip, "443"))
		if err != nil {
			return nil, err
		}
		// uTLS 模拟 Chrome TLS 指纹 (Ja3): Go crypto/tls 的 ClientHello
		// 特征明显, Cloudflare bot 检测一眼识别; 伪装浏览器指纹
		// 大幅降低被判定为 bot 的概率。SNI 仍用不敏感假域名
		// (GFW 对敏感域名 SNI 概率性 RST)。
		uconn := utls.UClient(conn, &utls.Config{
			ServerName:         fakeSNI,
			InsecureSkipVerify: true,
			NextProtos:         []string{"h2", "http/1.1"},
		}, utls.HelloChrome_Auto)
		if err := uconn.HandshakeContext(ctx); err != nil {
			conn.Close()
			return nil, fmt.Errorf("SNI 伪装握手: %w", err)
		}
		return uconn, nil
	}
	return &http2.Transport{
		DialTLSContext: func(ctx context.Context, network, addr string, _ *tls.Config) (net.Conn, error) {
			return dial(ctx)
		},
		IdleConnTimeout: 90 * time.Second,
	}
}

// sniFrontDo 通过 SNI 伪装直连 req.Host 指向的站点：
// DoH 解析全部真实 IP → 逐个 TCP 直连 → TLS 假 SNI → HTTP Host 填真实域名。
// GFW 对 TLS 的 RST 是概率性的：所有 IP 都失败时清缓存重查一轮再试。
// body 先读入内存并设置 GetBody：Clone 共享原 body，若失败发生在发送
// 中段，重试会发空请求体；GetBody 让每轮尝试都用重建的独立 body。
// 内存成本: SNI 站点以浏览/下载为主, 请求体都很小, 个人代理可接受。
func sniFrontDo(req *http.Request) (*http.Response, error) {
	ctx := req.Context()
	host := req.Host

	if req.Body != nil && req.Body != http.NoBody {
		bodyBytes, err := io.ReadAll(req.Body)
		if err != nil {
			return nil, err
		}
		req.Body.Close()
		req.Body = io.NopCloser(bytes.NewReader(bodyBytes))
		req.ContentLength = int64(len(bodyBytes))
		req.GetBody = func() (io.ReadCloser, error) {
			return io.NopCloser(bytes.NewReader(bodyBytes)), nil
		}
	}

	ips, err := resolveHostIPs(ctx, host)
	if err != nil {
		return nil, err
	}

	var lastErr error
	for attempt := 0; attempt < 2; attempt++ {
		for _, ip := range ips {
			outReq := req.Clone(ctx)
			outReq.URL.Scheme = "https"
			outReq.URL.Host = ip
			outReq.Host = host
			// 每轮用 GetBody 重建独立 body, 保证可重试。
			if outReq.GetBody != nil {
				if b, berr := outReq.GetBody(); berr == nil {
					outReq.Body = b
				}
			}

			// 复用按 IP 的 transport 池, 避免每次请求重做 TLS 握手。
			// 不用总 Timeout(会砍掉大文件下载), 握手超时由 http2.Transport
			// 内部 TLS 握手控制。
			resp, err := (&http.Client{Transport: getSNITransport(ip)}).Do(outReq)
			if err == nil {
				return resp, nil
			}
			lastErr = err
		}
		// 一轮全失败: 清 IP 缓存重查 (IP 可能已变/已被墙)。
		clearIPCache(host)
		ips, err = resolveHostIPs(ctx, host)
		if err != nil {
			return nil, err
		}
	}
	return nil, lastErr
}

// clearIPCache 删除指定主机的 IP 缓存, 强制下次 DoH 重查。
func clearIPCache(host string) {
	ipCacheMu.Lock()
	delete(ipCache, host)
	ipCacheMu.Unlock()
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
