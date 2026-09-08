// cmd/ech-proxy-android/main.go
// Android 版 ECH 代理，编译为 c-shared 库。
//
// 导出函数：
//   StartProxy(bootstrapIP *C.char) uint16  - 启动代理，返回监听端口
//   StopProxy()                             - 停止代理
//   GetProxyPort() uint16                   - 获取当前端口
//   IsEchReady() int                        - 检查 ECH 是否就绪
//
// 构建（Android）：
//   GOOS=android GOARCH=arm64 CC=aarch64-linux-android21-clang \
//     go build -buildmode=c-shared -o libechproxy.so .

package main

import (
	"context"
	"crypto/tls"
	_ "embed"
	"fmt"
	"log"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"
	"unsafe"

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
)

//export StartProxy
func StartProxy(bootstrapIP *C.char) uint16 {
	proxyMu.Lock()
	defer proxyMu.Unlock()

	if proxyServer != nil {
		proxyServer.Close()
		proxyServer = nil
	}

	bootstrap := C.GoString(bootstrapIP)
	if bootstrap != "" {
		cloudflare_ech.SetDoHConfig("moonchan.xyz", bootstrap)
	} else {
		cloudflare_ech.SetDohURL("https://moonchan.xyz/doh")
	}

	req, _ := http.NewRequest("HEAD", "https://pbs.twimg.com/favicon.ico", nil)
	resp, err := cloudflare_ech.Do(req)
	if err != nil {
		log.Printf("[proxy] ECH init failed: %v", err)
		return 0
	}
	resp.Body.Close()
	echReady = true
	log.Printf("[proxy] ECH ready")

	ln, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		log.Printf("[proxy] listen failed: %v", err)
		return 0
	}
	proxyPort = uint16(ln.Addr().(*net.TCPAddr).Port)

	mux := http.NewServeMux()
	mux.HandleFunc("/", router)

	proxyServer = &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       120 * time.Second,
	}

	go func() {
		log.Printf("[proxy] listening on 127.0.0.1:%d", proxyPort)
		if err := proxyServer.Serve(ln); err != nil && err != http.ErrServerClosed {
			log.Printf("[proxy] server error: %v", err)
		}
	}()

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
		log.Printf("[proxy] stopped")
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
	log.Printf("[proxy] → %s (from %s)", targetURL, r.RemoteAddr)

	req, err := http.NewRequest(r.Method, targetURL, nil)
	if err != nil {
		http.Error(w, "invalid request: "+err.Error(), http.StatusBadRequest)
		return
	}
	req.Header.Set("Referer", "https://x.com")
	req.Header.Set("User-Agent", "Mozilla/5.0 (TwitterPic)")

	resp, err := cloudflare_ech.Do(req)
	if err != nil {
		log.Printf("[proxy] ECH error: %v", err)
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
	log.Printf("[api] → %s (from %s)", targetURL, r.RemoteAddr)

	req, err := http.NewRequest(r.Method, targetURL, nil)
	if err != nil {
		http.Error(w, "invalid request: "+err.Error(), http.StatusBadRequest)
		return
	}
	req.Header.Set("User-Agent", "Mozilla/5.0 (TwitterPic)")

	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		log.Printf("[api] error: %v", err)
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
