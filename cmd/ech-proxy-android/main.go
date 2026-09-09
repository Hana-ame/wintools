// cmd/ech-proxy-android/main.go
// Android 版 ECH 代理，编译为 c-shared 库。
//
// 导出函数：
//   StartProxy(bootstrapIP *C.char) uint16  - 启动代理，返回监听端口
//   StopProxy()                             - 停止代理
//   GetProxyPort() uint16                   - 获取当前端口
//   IsEchReady() int                        - 检查 ECH 是否就绪
//   GetLogs() *C.char                       - 获取日志（调用者负责释放）
//
// 构建（Android）：
//   GOOS=android GOARCH=arm64 CC=aarch64-linux-android21-clang \
//     go build -buildmode=c-shared -o libechproxy.so .

package main

import (
	"context"
	"crypto/tls"
	_ "embed"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"C"

	cloudflare_ech "github.com/Hana-ame/wintools/pkg/ech"
	echproxy "github.com/Hana-ame/wintools/pkg/echproxy"
)

//go:embed web/index.html
var indexHTML string

// Twitter CDN 域名（fallback，当 upstream.json 未匹配时）
const defaultCdnHost = "video-cf.twimg.com"

// 上游配置（从 upstream.json 加载）
var (
	upstreamCfg  echproxy.UpstreamMap
	blockedHosts []string
)

// 版本信息（构建时注入）
var version = "2.5.1"
var buildTime = "unknown"
var startTime = time.Now()

var (
	proxyMu     sync.Mutex
	proxyServer *http.Server
	proxyPort   uint16
	echReady    bool

	logMu       sync.RWMutex
	logBuffer   []string
	maxLogLines = 500

	// 统计信息
	statsMu    sync.Mutex
	reqCount   int64
	bytesSent  int64
	errorCount int64

	// 超时配置
	// WriteTimeout: 30 分钟兜底——足够慢速大视频下载完，
	// 又能防止挂死连接永久占用资源。
	readTimeout  = 10 * time.Second
	writeTimeout = 30 * time.Minute
	idleTimeout  = 120 * time.Second
)

// 自定义日志 writer
type logWriter struct{}

func (w *logWriter) Write(p []byte) (int, error) {
	line := strings.TrimRight(string(p), "\n")
	logMu.Lock()
	logBuffer = append(logBuffer, line)
	if len(logBuffer) > maxLogLines {
		logBuffer = logBuffer[len(logBuffer)-maxLogLines:]
	}
	logMu.Unlock()
	return len(p), nil
}

func init() {
	log.SetOutput(&logWriter{})
	log.SetFlags(log.Ltime | log.Lmicroseconds)
}

//export StartProxy
func StartProxy(bootstrapIP *C.char) uint16 {
	proxyMu.Lock()
	defer proxyMu.Unlock()

	logBuffer = nil
	logBuffer = append(logBuffer, "=== Starting proxy ===")

	if proxyServer != nil {
		proxyServer.Close()
		proxyServer = nil
	}

	// 1. 配置 ECH
	bootstrap := C.GoString(bootstrapIP)
	if bootstrap != "" {
		log.Printf("ECH: DoH=moonchan.xyz, bootstrapIP=%s", bootstrap)
		cloudflare_ech.SetDoHConfig("moonchan.xyz", bootstrap)
	} else {
		log.Printf("ECH: DoH=https://moonchan.xyz/doh")
		cloudflare_ech.SetDohURL("https://moonchan.xyz/doh")
	}

	// 2. 初始化 ECH
	log.Printf("Initializing ECH client...")
	req, _ := http.NewRequest("HEAD", "https://pbs.twimg.com/favicon.ico", nil)
	resp, err := cloudflare_ech.Do(req)
	if err != nil {
		log.Printf("ECH init failed: %v", err)
		return 0
	}
	resp.Body.Close()
	echReady = true
	log.Printf("ECH ready")

	// 3. 获取 TLS 证书（每次启动都下载，参考 ech-proxy）
	proxyBase := "https://proxy.moonchan.xyz/Hana-ame/wintools/refs/heads/main/%s?proxy_host=raw.githubusercontent.com"
	upstreamConfigURL := fmt.Sprintf(proxyBase, "certs/l.moonchan.xyz/upstream.json")

	log.Printf("Loading upstream config: %s", upstreamConfigURL)
	cfg, err := echproxy.LoadConfig(upstreamConfigURL)
	if err != nil {
		log.Printf("Failed to load config: %v", err)
		return 0
	}

	var tlsCert *tls.Certificate
	if cfg.CertPath != "" && cfg.KeyPath != "" {
		log.Printf("Fetching certificate: %s", cfg.CertPath)
		certPEM, err := echproxy.FetchBytes(cfg.CertPath)
		if err != nil {
			log.Printf("Failed to fetch cert: %v", err)
			return 0
		}
		log.Printf("Fetching key: %s", cfg.KeyPath)
		keyPEM, err := echproxy.FetchBytes(cfg.KeyPath)
		if err != nil {
			log.Printf("Failed to fetch key: %v", err)
			return 0
		}
		cert, err := tls.X509KeyPair(certPEM, keyPEM)
		if err != nil {
			log.Printf("Failed to parse cert: %v", err)
			return 0
		}
		tlsCert = &cert
		log.Printf("Certificate loaded (*.l.moonchan.xyz)")
	}

	// 保存上游配置供 router 使用
	upstreamCfg = cfg.Upstreams
	blockedHosts = cfg.BlockedHosts
	log.Printf("Upstream config loaded: %d entries, %d blocked", len(upstreamCfg), len(blockedHosts))

	// 4. 监听端口（优先 8443，失败则随机）
	ln, err := net.Listen("tcp4", "127.0.0.1:8443")
	if err != nil {
		log.Printf("Port 8443 in use, trying random port...")
		ln, err = net.Listen("tcp4", "127.0.0.1:0")
		if err != nil {
			log.Printf("listen failed: %v", err)
			return 0
		}
	}
	proxyPort = uint16(ln.Addr().(*net.TCPAddr).Port)

	// 5. 配置 HTTP 服务器
	mux := http.NewServeMux()
	mux.HandleFunc("/", router)

	proxyServer = &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: readTimeout,
		WriteTimeout:      writeTimeout,
		IdleTimeout:       idleTimeout,
	}

	// 6. 启动 HTTPS 或 HTTP
	if tlsCert != nil {
		// HTTPS 模式：浏览器访问 https://twimg.l.moonchan.xyz:8443/
		// 需要 DNS 解析 twimg.l.moonchan.xyz 到 127.0.0.1
		log.Printf("Listening HTTPS on 127.0.0.1:%d", proxyPort)
		log.Printf("Access: https://twimg.l.moonchan.xyz:%d/", proxyPort)
		proxyServer.TLSConfig = &tls.Config{
			Certificates: []tls.Certificate{*tlsCert},
			MinVersion:   tls.VersionTLS12,
		}
		tlsLn := tls.NewListener(ln, proxyServer.TLSConfig)
		go func() {
			if err := proxyServer.Serve(tlsLn); err != nil && err != http.ErrServerClosed {
				log.Printf("server error: %v", err)
			}
		}()
	} else {
		// HTTP 模式：浏览器访问 http://127.0.0.1:8443/
		log.Printf("Listening HTTP on 127.0.0.1:%d", proxyPort)
		log.Printf("Access: http://127.0.0.1:%d/", proxyPort)
		go func() {
			if err := proxyServer.Serve(ln); err != nil && err != http.ErrServerClosed {
				log.Printf("server error: %v", err)
			}
		}()
	}

	log.Printf("Proxy started on port %d", proxyPort)
	return proxyPort
}

//export StopProxy
func StopProxy() {
	proxyMu.Lock()
	defer proxyMu.Unlock()

	if proxyServer != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		if err := proxyServer.Shutdown(ctx); err != nil {
			log.Printf("Shutdown error: %v", err)
		}
		proxyServer = nil
		proxyPort = 0
		echReady = false
		log.Printf("Proxy stopped")
	}
}

//export GetProxyPort
func GetProxyPort() uint16 {
	proxyMu.Lock()
	defer proxyMu.Unlock()
	return proxyPort
}

//export IsEchReady
func IsEchReady() C.int {
	if echReady {
		return 1
	}
	return 0
}

//export GetLogs
func GetLogs() *C.char {
	logMu.RLock()
	defer logMu.RUnlock()

	logs := strings.Join(logBuffer, "\n")
	return C.CString(logs)
}

func router(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Path
	host := r.Host

	// CORS 支持
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "GET, HEAD, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Range, If-Range, If-Modified-Since, If-None-Match, Cookie")
	w.Header().Set("Access-Control-Expose-Headers", "Content-Length, Content-Range, Content-Type, Cache-Control, ETag, Last-Modified, Set-Cookie")
	w.Header().Set("Access-Control-Max-Age", "86400")

	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusNoContent)
		return
	}

	// 健康检查：/healthz
	if path == "/healthz" {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]any{
			"status":    "ok",
			"ech":       echReady,
			"port":      proxyPort,
			"version":   version,
			"upstreams": len(upstreamCfg),
		})
		return
	}

	// 版本信息：/version
	if path == "/version" {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]string{
			"version":   version,
			"buildTime": buildTime,
		})
		return
	}

	// 根路径返回网页
	if path == "/" || path == "" {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, indexHTML)
		return
	}

	// 根据 Host 匹配上游配置
	var uc echproxy.UpstreamConfig
	var matched bool

	// 1. 精确匹配
	if cfg, ok := upstreamCfg[host]; ok {
		uc = cfg
		matched = true
	}

	// 2. 通配符匹配（如 iwara-xxx.l.moonchan.xyz → xxx.iwara.tv）
	if !matched {
		var wc echproxy.UpstreamConfig
		wc, matched = echproxy.MatchWildcardForTest(upstreamCfg, host)
		if matched {
			uc = wc
		}
	}

	// 3. 未匹配 → 404
	if !matched {
		http.NotFound(w, r)
		return
	}

	// 检查 blocked hosts
	for _, bh := range blockedHosts {
		if host == bh || strings.HasSuffix(host, "."+bh) {
			http.Error(w, "blocked", http.StatusForbidden)
			return
		}
	}

	// 转发到上游
	echProxyHandler(w, r, uc)
}

func echProxyHandler(w http.ResponseWriter, r *http.Request, uc echproxy.UpstreamConfig) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	targetHost := uc.Host
	path := r.URL.Path

	// 构建目标 URL
	targetURL := "https://" + targetHost + path
	if r.URL.RawQuery != "" {
		targetURL += "?" + r.URL.RawQuery
	}
	log.Printf("→ %s (host=%s from %s)", targetURL, r.Host, r.RemoteAddr)

	req, err := http.NewRequest(r.Method, targetURL, nil)
	if err != nil {
		statsMu.Lock()
		errorCount++
		statsMu.Unlock()
		log.Printf("Error: %v", err)
		http.Error(w, "invalid request: "+err.Error(), http.StatusBadRequest)
		return
	}

	// 设置请求头
	if uc.Referer != "" {
		req.Header.Set("Referer", uc.Referer)
	} else {
		req.Header.Set("Referer", "https://"+targetHost+"/")
	}
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")

	// 注入固定 Cookie（如 exhentai）
	if uc.Cookie != "" {
		req.Header.Set("Cookie", uc.Cookie)
	}

	// 转发客户端 Cookie（如果没有固定 Cookie）
	if uc.Cookie == "" {
		if cookie := r.Header.Get("Cookie"); cookie != "" {
			req.Header.Set("Cookie", cookie)
		}
	}

	// 转发 Range 请求（视频播放支持）
	if rangeHeader := r.Header.Get("Range"); rangeHeader != "" {
		req.Header.Set("Range", rangeHeader)
	}

	// 转发缓存验证头
	for _, h := range []string{"If-Modified-Since", "If-None-Match", "Cache-Control"} {
		if v := r.Header.Get(h); v != "" {
			req.Header.Set(h, v)
		}
	}

	// 执行 ECH 请求
	resp, err := cloudflare_ech.Do(req)
	if err != nil {
		statsMu.Lock()
		errorCount++
		statsMu.Unlock()
		log.Printf("ECH error: %v", err)
		http.Error(w, "ECH fetch failed: "+err.Error(), http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()

	log.Printf("  Status: %d", resp.StatusCode)

	// 304 Not Modified 直接返回
	if resp.StatusCode == http.StatusNotModified {
		w.WriteHeader(http.StatusNotModified)
		return
	}

	// 转发响应头（跳过 hop-by-hop 头）
	hopByHop := map[string]bool{
		"Connection": true, "Keep-Alive": true, "Proxy-Authenticate": true,
		"Proxy-Authorization": true, "Te": true, "Trailer": true,
		"Transfer-Encoding": true, "Upgrade": true,
	}
	for k, vs := range resp.Header {
		if hopByHop[k] {
			continue
		}
		for _, v := range vs {
			w.Header().Add(k, v)
		}
	}

	w.Header().Set("Access-Control-Expose-Headers", "Content-Length, Content-Range, Range, ETag, Last-Modified, Cache-Control, Set-Cookie")

	w.WriteHeader(resp.StatusCode)

	// HEAD 请求：只返回响应头
	if r.Method == http.MethodHead {
		return
	}

	// 流式转发（与原版 ech-proxy 一致）：边读边写并 flush
	buf := make([]byte, 32*1024)
	var n int64
	for {
		nr, rerr := resp.Body.Read(buf)
		if nr > 0 {
			if _, werr := w.Write(buf[:nr]); werr != nil {
				log.Printf("Write error: %v", werr)
				return
			}
			if f, ok := w.(http.Flusher); ok {
				f.Flush()
			}
			n += int64(nr)
		}
		if rerr != nil {
			break
		}
	}

	// 统计
	statsMu.Lock()
	bytesSent += n
	statsMu.Unlock()
	if n > 0 {
		log.Printf("  Bytes: %d", n)
	}
}

func main() {}
