package peerfs

import (
	"bytes"
	"embed"
	"encoding/json"
	"net/http"
	"path"
	"strings"
)

//go:embed web/bridge.js web/index.html
var webFiles embed.FS

// MountConsole 把浏览器控制台页挂到 mux：
//
//	GET /__peerfs/             页面（bridge.js + index.html）
//	GET /__peerfs/config.json  节点配置（peer id + 信令坐标），页面自动加载
func (n *Node) MountConsole(mux *http.ServeMux) {
	mux.HandleFunc("/__peerfs", n.serveConsole)
	mux.HandleFunc("/__peerfs/", n.serveConsoleAssets)
}

// 配置占位符：index.html 里 window.__PEERFS__ = __PEERFS_CONFIG__; 的
// __PEERFS_CONFIG__ 在服务时替换为节点实际配置 JSON。
var configPlaceholder = []byte("__PEERFS_CONFIG__")

// serveConsole 返回控制台页；index.html 里的 __PEERFS_CONFIG__ 占位符在
// 服务时替换为节点实际配置 JSON。
func (n *Node) serveConsole(w http.ResponseWriter, _ *http.Request) {
	b, err := webFiles.ReadFile("web/index.html")
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	cfg, _ := json.Marshal(map[string]any{
		"peerId":    n.ID(),
		"signaling": n.signalingJSON(),
	})
	b = bytes.ReplaceAll(b, configPlaceholder, cfg)
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write(b)
}

// serveConsoleAssets 服务 bridge.js 等静态资源（/__peerfs/bridge.js）。
func (n *Node) serveConsoleAssets(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/__peerfs/" || r.URL.Path == "/__peerfs" {
		n.serveConsole(w, r)
		return
	}
	name := path.Clean("/" + strings.TrimPrefix(r.URL.Path, "/__peerfs"))
	switch name {
	case "/bridge.js":
		b, err := webFiles.ReadFile("web/bridge.js")
		if err != nil {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "text/javascript; charset=utf-8")
		_, _ = w.Write(b)
	case "/config.json":
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"peerId":    n.ID(),
			"signaling": n.signalingJSON(),
		})
	default:
		http.NotFound(w, r)
	}
}

func (n *Node) signalingJSON() map[string]any {
	sig := n.cfg.Signaling
	host := sig.Host
	port := sig.Port
	if host == "" && port == 0 {
		return nil // 未配置 → 页面走公共云默认或 URL 参数覆盖
	}
	if port == 0 {
		port = 443
	}
	return map[string]any{"host": host, "port": port, "secure": sig.Secure, "key": sig.Key}
}
