package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/coder/websocket"
)

func runServer(args []string) {
	fs := flag.NewFlagSet("server", flag.ExitOnError)
	listen := fs.String("listen", "0.0.0.0:18444", "监听地址 (WS)")
	force := fs.String("force", "", "强制栈: v4 | v6 (空 = auto)")
	fs.Parse(args)

	if *force != "" && *force != "v4" && *force != "v6" {
		log.Fatalf("invalid --force %q (want v4|v6|空)", *force)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/connect", func(w http.ResponseWriter, r *http.Request) {
		handleServerConn(w, r, *force)
	})

	srv := &http.Server{
		Addr:              *listen,
		Handler:           mux,
		ReadHeaderTimeout: 30 * time.Second,
	}
	// 纯 ws 监听, wss 由外层 nginx 终止 TLS 后反代到本端口。
	log.Printf("server 监听 ws://%s (force=%s), WS 路径 /connect?target=host:port", *listen, *force)
	if err := srv.ListenAndServe(); err != nil {
		log.Fatal(err)
	}
}

// handleServerConn: 接受 WS 连接, query 里的 target 是要拨号的目标,
// 拨号 (可强制 v4/v6) 后把 WS 当 net.Conn 双向转发。
func handleServerConn(w http.ResponseWriter, r *http.Request, force string) {
	ws, err := websocket.Accept(w, r, nil)
	if err != nil {
		log.Printf("ws accept fail: %v", err)
		return
	}
	defer ws.Close(websocket.StatusInternalError, "")

	target := r.URL.Query().Get("target")
	if target == "" {
		log.Printf("ws %s: missing target", r.RemoteAddr)
		return
	}

	upstream, err := dialTarget(target, force)
	if err != nil {
		log.Printf("CONNECT %s dial fail (force=%s): %v", target, force, err)
		return
	}
	defer upstream.Close()

	conn := websocket.NetConn(context.Background(), ws, websocket.MessageBinary)
	defer conn.Close()
	log.Printf("CONNECT %s -> %s (force=%s) established via ws", target, upstream.RemoteAddr(), force)
	relay(conn, upstream)
}

func relay(a, b net.Conn) {
	done := make(chan struct{}, 1)
	go func() {
		ioCopy(a, b)
		done <- struct{}{}
	}()
	ioCopy(b, a)
	<-done
}

func ioCopy(dst, src net.Conn) {
	buf := make([]byte, 32*1024)
	for {
		n, err := src.Read(buf)
		if n > 0 {
			if _, werr := dst.Write(buf[:n]); werr != nil {
				return
			}
		}
		if err != nil {
			return
		}
	}
}

// dialTarget 按 force 指定的栈解析并连接目标:
//
//	v4: 只查 A 记录; v6: 只查 AAAA; 空: 系统解析。
func dialTarget(target, force string) (net.Conn, error) {
	host, port, err := net.SplitHostPort(target)
	if err != nil {
		host, port = target, "443"
	}
	d := net.Dialer{Timeout: 10 * time.Second}

	switch force {
	case "v4", "v6":
		network := "ip4"
		if force == "v6" {
			network = "ip6"
		}
		addrs, err := net.DefaultResolver.LookupNetIP(context.Background(), network, host)
		if err != nil || len(addrs) == 0 {
			return nil, fmt.Errorf("no IPv%s for %s", strings.TrimPrefix(force, "v"), host)
		}
		return d.Dial("tcp", net.JoinHostPort(addrs[0].String(), port))
	default:
		return d.Dial("tcp", net.JoinHostPort(host, port))
	}
}
