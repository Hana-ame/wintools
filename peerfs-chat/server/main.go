// PeerServer: 自托管 PeerJS 信令服务器 + 内置房间发现.
// 使用 go-peerserver 库, 挂载信令端点 + 发现端点 + 静态文件.
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
	web := flag.String("web", "./web", "静态文件目录")
	tokens := flag.String("tokens", "", "信令 token 白名单（逗号分隔；空 = 不限制）")
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
	// PeerJS 信令端点
	mux.HandleFunc("/peerjs", srv.HandleWS)
	mux.HandleFunc("/peerjs/id", srv.HandleID)
	// 发现端点
	mux.HandleFunc("/discover/announce", srv.HandleAnnounce)
	mux.HandleFunc("/discover/nodes", srv.HandleNodes)
	// 静态文件（浏览器页面）
	mux.Handle("/", http.FileServer(http.Dir(*web)))

	log.Printf("[server] peerserver listening on %s (key=%s, web=%s)", *addr, *key, *web)
	_ = http.ListenAndServe(*addr, mux)
}