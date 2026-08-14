package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"
)

// opencode.ai 访问量统计: 总 CONNECT 次数 + 每分钟增量 + 上下行字节。
// 字节在双向转发时累计, 不依赖 HTTP 版本; 端到端流量 client 端也在计,
// 这里计的是本 server 出口这段 (WS 隧道 <-> 目标)。
var (
	statsTotal  atomic.Int64
	statsMin    atomic.Int64
	statsUp     atomic.Int64
	statsDown   atomic.Int64
	statsUpMin  atomic.Int64
	statsDownMin atomic.Int64
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
	// /status: 访问量统计 (本分钟 + 累计 + 上下行字节)
	mux.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.WriteHeader(200)
		fmt.Fprintf(w, `{"opencode_conns": %d, "last_minute": %d, "up_bytes": %d, "down_bytes": %d}`,
			statsTotal.Load(), statsMin.Load(), statsUp.Load(), statsDown.Load())
	})

	srv := &http.Server{
		Addr:              *listen,
		Handler:           mux,
		ReadHeaderTimeout: 30 * time.Second,
	}
	// 纯 ws 监听, wss 由外层 nginx 终止 TLS 后反代到本端口。
	log.Printf("server 监听 ws://%s (force=%s), WS 路径 /connect?target=host:port", *listen, *force)
	// 每分钟打印本分钟增量; 每天 UTC+0 00:00 重置当天计数。
	go func() {
		for {
			time.Sleep(time.Minute)
			log.Printf("opencode.ai 访问量: 本分钟 %d 次 (↑%.1fMB ↓%.1fMB), 当天 %d 次 (↑%.1fMB ↓%.1fMB)",
				statsMin.Swap(0), float64(statsUpMin.Swap(0))/1e6, float64(statsDownMin.Swap(0))/1e6,
				statsTotal.Load(), float64(statsUp.Load())/1e6, float64(statsDown.Load())/1e6)
		}
	}()
	go func() {
		for {
			now := time.Now().UTC()
			next := time.Date(now.Year(), now.Month(), now.Day()+1, 0, 0, 0, 0, time.UTC)
			time.Sleep(time.Until(next))
			log.Printf("UTC+0 00:00: 当天 opencode.ai 访问量 %d 次 (↑%.1fMB ↓%.1fMB), 重置", statsTotal.Load(),
				float64(statsUp.Load())/1e6, float64(statsDown.Load())/1e6)
			statsTotal.Store(0)
			statsMin.Store(0)
			statsUp.Store(0)
			statsDown.Store(0)
			statsUpMin.Store(0)
			statsDownMin.Store(0)
		}
	}()
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
	statsTotal.Add(1)
	statsMin.Add(1)
	log.Printf("CONNECT %s -> %s (force=%s) established via ws (累计 %d)", target, upstream.RemoteAddr(), force, statsTotal.Load())
	relay(conn, upstream)
}

func relay(a, b net.Conn) {
	done := make(chan struct{}, 1)
	go func() {
		// a->b: 从 client (WS) 读到目标, 计上行。
		ioCopy(&counterWriter{w: b, total: &statsUp, minute: &statsUpMin}, a)
		done <- struct{}{}
	}()
	// b->a: 从目标读到 client (WS), 计下行。
	ioCopy(&counterWriter{w: a, total: &statsDown, minute: &statsDownMin}, b)
	<-done
}

func ioCopy(dst io.Writer, src io.Reader) {
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
