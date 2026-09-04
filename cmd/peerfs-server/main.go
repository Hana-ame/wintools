// peerfs-server — standalone PeerJS signaling server + discovery + web.
//
// 持久信令服务器：所有 peer（Go 节点 + Web 浏览器）连到这里做发现与握手，
// 数据面走 P2P WebRTC DataChannel，不经过本服务器。
//
// Usage:
//
//	go run ./cmd/peerfs-server -addr :8000 -key mykey -web ./pkg/peerfs/web
//	go run ./cmd/peerfs-server -addr :8000 -key mykey -tokens SECRET1,SECRET2
package main

import (
	"flag"
	"log"
	"net/http"
	"strings"

	signalserver "github.com/Hana-ame/go-peerserver"
)

func main() {
	addr := flag.String("addr", "0.0.0.0:8000", "listen address")
	key := flag.String("key", "peerjs", "API key (client 必须一致)")
	web := flag.String("web", "", "静态文件目录（浏览器页面）；空 = 不 serve 静态文件")
	tokens := flag.String("tokens", "", "信令 token 白名单（逗号分隔）；空 = 不限制")
	flag.Parse()

	var opts []signalserver.Option
	if *tokens != "" {
		tlist := strings.Split(*tokens, ",")
		clean := make([]string, 0, len(tlist))
		for _, t := range tlist {
			if t = strings.TrimSpace(t); t != "" {
				clean = append(clean, t)
			}
		}
		if len(clean) > 0 {
			opts = append(opts, signalserver.WithTokenWhitelist(clean))
		}
	}

	srv := signalserver.NewServer(*key, opts...)
	srv.Start()

	mux := http.NewServeMux()

	// PeerJS signaling endpoints
	mux.HandleFunc("/peerjs", srv.HandleWS)
	mux.HandleFunc("/peerjs/id", srv.HandleID)

	// Discovery endpoints
	mux.HandleFunc("/discover/announce", srv.HandleAnnounce)
	mux.HandleFunc("/discover/nodes", srv.HandleNodes)

	// Server status API (dashboard polls this)
	mux.HandleFunc("/status", srv.HandleStatus)

	// Dashboard at / (only if -web is not set; -web takes precedence)
	if *web == "" {
		mux.HandleFunc("/", srv.HandleDashboard)
	} else {
		mux.Handle("/", http.FileServer(http.Dir(*web)))
	}

	log.Printf("[peerfs-server] listening on %s (key=%q, web=%q, dashboard=%q)",
		*addr, *key, *web, func() string {
			if *web == "" {
				return "on"
			}
			return "off"
		}())
	_ = http.ListenAndServe(*addr, mux)
}
