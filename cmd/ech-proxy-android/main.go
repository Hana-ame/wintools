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
	_ "embed"
	"fmt"
	"log"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"C"

	cloudflare_ech "github.com/Hana-ame/wintools/pkg/ech"
)

//go:embed web/index.html
var indexHTML string

// Twitter CDN 域名映射
var cdnMap = map[string]string{
	"pbs.twimg.com":       "pbs.twimg.com",
	"video-cf.twimg.com": "video-cf.twimg.com",
	"abs.twimg.com":       "abs.twimg.com",
}

const apiHost = "x.moonchan.xyz"

var (
	proxyMu     sync.Mutex
	proxyServer *http.Server
	proxyPort   uint16
	echReady    bool

	logMu       sync.RWMutex
	logBuffer   []string
	maxLogLines = 500
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
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       120 * time.Second,
	}

	// 6. 启动 HTTP（本地通信，证书域名不匹配）
	log.Printf("Listening HTTP on 127.0.0.1:%d", proxyPort)
	go func() {
		if err := proxyServer.Serve(ln); err != nil && err != http.ErrServerClosed {
			log.Printf("server error: %v", err)
		}
	}()

	log.Printf("Proxy started on port %d", proxyPort)
	return proxyPort
}

//export StopProxy
func StopProxy() {
	proxyMu.Lock()
	defer proxyMu.Unlock()

	if proxyServer != nil {
		proxyServer.Close()
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

	if path == "/" || path == "" {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, indexHTML)
		return
	}

	for prefix, target := range cdnMap {
		if strings.HasPrefix(path, "/"+prefix+"/") || path == "/"+prefix {
			echProxyHandler(w, r, target, strings.TrimPrefix(path, "/"+prefix))
			return
		}
	}

	if strings.HasPrefix(path, "/api/") {
		apiProxyHandler(w, r, apiHost, strings.TrimPrefix(path, "/api"))
		return
	}

	http.NotFound(w, r)
}

func echProxyHandler(w http.ResponseWriter, r *http.Request, targetHost, path string) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	targetURL := "https://" + targetHost + "/" + path
	log.Printf("→ %s (from %s)", targetURL, r.RemoteAddr)

	req, err := http.NewRequest(r.Method, targetURL, nil)
	if err != nil {
		http.Error(w, "invalid request: "+err.Error(), http.StatusBadRequest)
		return
	}
	req.Header.Set("Referer", "https://x.com")
	req.Header.Set("User-Agent", "Mozilla/5.0 (TwitterPic)")

	resp, err := cloudflare_ech.Do(req)
	if err != nil {
		log.Printf("ECH error: %v", err)
		http.Error(w, "ECH fetch failed: "+err.Error(), http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()

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

	if cl := resp.Header.Get("Content-Length"); cl != "" {
		w.Header().Set("Content-Length", cl)
	}

	w.WriteHeader(resp.StatusCode)
	buf := make([]byte, 64*1024)
	for {
		n, err := resp.Body.Read(buf)
		if n > 0 {
			if _, werr := w.Write(buf[:n]); werr != nil {
				return
			}
		}
		if err != nil {
			return
		}
	}
}

func apiProxyHandler(w http.ResponseWriter, r *http.Request, targetHost, path string) {
	targetURL := "https://" + targetHost + path
	log.Printf("→ %s (from %s)", targetURL, r.RemoteAddr)

	req, err := http.NewRequest(r.Method, targetURL, nil)
	if err != nil {
		http.Error(w, "invalid request: "+err.Error(), http.StatusBadRequest)
		return
	}
	req.Header.Set("User-Agent", "Mozilla/5.0 (TwitterPic)")

	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		log.Printf("API error: %v", err)
		http.Error(w, "API fetch failed: "+err.Error(), http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()

	for k, vs := range resp.Header {
		for _, v := range vs {
			w.Header().Add(k, v)
		}
	}
	w.WriteHeader(resp.StatusCode)
	buf := make([]byte, 64*1024)
	for {
		n, err := resp.Body.Read(buf)
		if n > 0 {
			if _, werr := w.Write(buf[:n]); werr != nil {
				return
			}
		}
		if err != nil {
			return
		}
	}
}

func main() {}
