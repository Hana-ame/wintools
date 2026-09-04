// peerfs-node — peer node: serves files over WebRTC DataChannel.
//
// 支持两种部署模式：
//
//	-signal=true   内嵌自托管信令（单进程调试），挂在本进程 HTTP 上
//	-signal=false  连远端信令服务器（-shost/-sport），适合生产部署
//
// 浏览器打开 http://<listen>/__peerfs/ 即可访问控制台。
//
// Usage:
//
//	# 生产模式：连远端信令
//	go run ./cmd/peerfs-node -listen :9000 -root /data -shost 127.0.0.1 -sport 8000 -signal=false
//
//	# 调试模式：内嵌信令（单进程）
//	go run ./cmd/peerfs-node -listen :9000 -root /data -signal -key mykey
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/Hana-ame/wintools/pkg/peerfs"
)

func main() {
	var (
		root      = flag.String("root", ".", "文件服务根目录")
		listen    = flag.String("listen", "0.0.0.0:9000", "HTTP 监听（控制台页 + 可选内嵌信令）")
		name      = flag.String("name", "", "节点 peer id 后缀，最终 id = peerfs-<name>；空 = 借随机 id")
		token     = flag.String("token", "", "数据面 hello 校验 token；空 = 不校验")

		signal  = flag.Bool("signal", false, "内嵌自托管信令服务器（调试用；生产用 -signal=false 连远端）")
		key     = flag.String("key", "peerjs", "信令 API key（浏览器端必须一致）")
		stoken  = flag.String("stoken", "", "信令 token 白名单（逗号分隔）；空 = 不限制")

		shost   = flag.String("shost", "", "信令 host（-signal=false 时；空 = 公共云 0.peerjs.com）")
		sport   = flag.Int("sport", 443, "信令 port（-signal=false 时）")
		ssecure = flag.Bool("ssecure", true, "信令 wss（-signal=false 时）")
		debug   = flag.Bool("debug", false, "信令详细日志")
		configURL = flag.String("config-url", os.Getenv("PEERFS_CONFIG_URL"), "中心配置/注册URL")
	)
	flag.Parse()

	mux := http.NewServeMux()

	if *signal {
		var tokens []string
		for _, t := range strings.Split(*stoken, ",") {
			if t = strings.TrimSpace(t); t != "" {
				tokens = append(tokens, t)
			}
		}
		peerfs.MountSignaling(mux, *key, tokens)
		log.Printf("[peerfs-node] embedded signaling on %s (ws /peerjs, key=%q)", *listen, *key)
	}

	sig := peerfs.Signaling{Host: *shost, Port: *sport, Secure: *ssecure, Key: *key}
	if *signal {
		// 内嵌信令：节点连本地信令（同源同端口），页面也连同源信令
		localPort := 9000
		if s := strings.Split(*listen, ":"); len(s) > 1 {
			if p, err := strconv.Atoi(s[len(s)-1]); err == nil {
				localPort = p
			}
		}
		sig = peerfs.Signaling{Host: "127.0.0.1", Port: localPort, Secure: false, Key: *key}
	}

	node := peerfs.New(peerfs.Config{
		PeerID:    joinID(*name),
		Root:      *root,
		Token:     *token,
		Signaling: sig,
		Debug:     *debug,
		ConfigURL: *configURL,
	})
	node.MountConsole(mux)

	// 先起 HTTP 再连信令：内嵌信令时节点要连自己的 HTTP 端口
	srv := &http.Server{Addr: *listen, Handler: mux}
	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("[peerfs-node] listen: %v", err)
		}
	}()

	// 等 HTTP 端口就绪（最长 5 秒）
	ready := make(chan struct{})
	go func() {
		for i := 0; i < 100; i++ {
			conn, err := net.DialTimeout("tcp", *listen, 50*time.Millisecond)
			if err == nil {
				conn.Close()
				close(ready)
				return
			}
			time.Sleep(50 * time.Millisecond)
		}
		log.Fatalf("[peerfs-node] port %s not ready after 5s", *listen)
	}()
	<-ready

	ctx := context.Background()
	if err := node.Start(ctx); err != nil {
		log.Fatalf("[peerfs-node] peer start: %v", err)
	}
	log.Printf("[peerfs-node] peer id: %s  root: %s", node.ID(), *root)

	consoleHost := *listen
	if strings.HasPrefix(consoleHost, ":") || strings.HasPrefix(consoleHost, "0.0.0.0:") {
		consoleHost = "127.0.0.1" + consoleHost[strings.Index(consoleHost, ":"):]
	}
	log.Printf("[peerfs-node] console: http://%s/__peerfs/", consoleHost)

	select {} // 阻塞主 goroutine
}

func joinID(name string) string {
	if name == "" {
		return ""
	}
	return fmt.Sprintf("peerfs-%s", name)
}
