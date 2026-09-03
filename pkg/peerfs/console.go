package peerfs

import (
	"bytes"
	"embed"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"path"
	"strings"
	"time"

	"github.com/Hana-ame/wintools/pkg/peerjs"
	"github.com/pion/webrtc/v4"
)

//go:embed web/bridge.js web/index.html web/sw.js web/peerjs.min.js
var webFiles embed.FS

// MountConsole 把浏览器控制台页挂到 mux：
//
//	GET /__peerfs/             页面（bridge.js + index.html）
//	GET /__peerfs/peerjs.min.js 本地 PeerJS 库（离线零外网依赖）
//	GET /__peerfs/sw.js        Service Worker 虚拟 Range 代理
//	GET /__peerfs/config.json  节点配置（peer id + 信令坐标），页面自动加载
func (n *Node) MountConsole(mux *http.ServeMux) {
	mux.HandleFunc("/__peerfs/", n.serveConsoleAssets)
	mux.HandleFunc("/__peerfs", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/__peerfs/", http.StatusFound)
	})
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}
		http.Redirect(w, r, "/__peerfs/", http.StatusFound)
	})
}

// 配置占位符：index.html 里 window.__PEERFS__ = __PEERFS_CONFIG__; 的
// __PEERFS_CONFIG__ 在服务时替换为节点实际配置 JSON。
var configPlaceholder = []byte("__PEERFS_CONFIG__")
var peerIDPlaceholder = []byte("__TARGET_PEER_ID__")

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
		"root":      n.cfg.Root,
		"signaling": n.signalingJSON(),
	})
	b = bytes.ReplaceAll(b, peerIDPlaceholder, []byte(n.ID()))
	b = bytes.ReplaceAll(b, configPlaceholder, cfg)
	ts := time.Now().Unix()
	b = bytes.ReplaceAll(b, []byte(`src="bridge.js"`), []byte(fmt.Sprintf(`src="bridge.js?v=%d"`, ts)))
	b = bytes.ReplaceAll(b, []byte(`register('sw.js'`), []byte(fmt.Sprintf(`register('sw.js?v=%d'`, ts)))
	w.Header().Set("Cache-Control", "no-cache, no-store, must-revalidate")
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
	case "/peerjs.min.js":
		b, err := webFiles.ReadFile("web/peerjs.min.js")
		if err != nil {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Cache-Control", "no-cache, no-store, must-revalidate")
		w.Header().Set("Content-Type", "text/javascript; charset=utf-8")
		_, _ = w.Write(b)
	case "/bridge.js":
		b, err := webFiles.ReadFile("web/bridge.js")
		if err != nil {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Cache-Control", "no-cache, no-store, must-revalidate")
		w.Header().Set("Content-Type", "text/javascript; charset=utf-8")
		_, _ = w.Write(b)
	case "/sw.js":
		b, err := webFiles.ReadFile("web/sw.js")
		if err != nil {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Cache-Control", "no-cache, no-store, must-revalidate")
		w.Header().Set("Content-Type", "text/javascript; charset=utf-8")
		w.Header().Set("Service-Worker-Allowed", "/__peerfs/")
		_, _ = w.Write(b)
	case "/config.json":
		w.Header().Set("Cache-Control", "no-cache, no-store, must-revalidate")
		w.Header().Set("Content-Type", "application/json")
		resp := map[string]any{
			"peerId":    n.ID(),
			"root":      n.cfg.Root,
			"signaling": n.signalingJSON(),
		}
		if n.cfg.ConfigURL != "" {
			resp["configUrl"] = n.cfg.ConfigURL
		}
		if nodes := n.GetHubNodes(); len(nodes) > 0 {
			resp["hubNodes"] = nodes
		}
		_ = json.NewEncoder(w).Encode(resp)
	case "/nodes":
		w.Header().Set("Cache-Control", "no-cache, no-store, must-revalidate")
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"nodes": n.GetHubNodes()})
	case "/candidate":
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var req struct {
			Candidate    webrtc.ICECandidateInit `json:"candidate"`
			ConnectionID string                  `json:"connectionId"`
			PeerID       string                  `json:"peerId"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if req.Candidate.Candidate != "" {
			log.Printf("[peerfs-candidate-http] received candidate via HTTP 8080: %s", req.Candidate.Candidate)
			go peerjs.PunchCandidate(req.Candidate.Candidate)
			n.InjectCandidate(req.ConnectionID, req.Candidate)
		}
		w.Header().Set("Cache-Control", "no-cache, no-store, must-revalidate")
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"ok": true})
	default:
		http.NotFound(w, r)
	}
}

func (n *Node) signalingJSON() map[string]any {
	sig := n.cfg.Signaling
	host := sig.Host
	port := sig.Port
	// 内嵌信令：节点连 127.0.0.1/localhost，但浏览器应使用页面 host
	// 返回空 host 让页面侧用 location.hostname 而非公共云
	if host == "" || host == "127.0.0.1" || host == "localhost" {
		return map[string]any{"host": "", "port": 0, "secure": false, "key": sig.Key}
	}
	if port == 0 {
		port = 443
	}
	return map[string]any{"host": host, "port": port, "secure": sig.Secure, "key": sig.Key}
}
