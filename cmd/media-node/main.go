// media-node — pkg/peerfs 演示/独立入口：把一个目录通过 PeerJS/WebRTC
// 提供给浏览器加载图片/视频。
//
// 浏览器打开 http://<listen>/__peerfs/ 即可（同机时信令也由本进程提供）。
//
// 信令三选一：
//
//	-signal=true   内嵌自托管信令（go-peerserver，peerdrive 同款），挂在本进程 HTTP 上
//	-signal=false -shost x -sport y 连远端 peerserver / 公共云（-shost 留空 = 0.peerjs.com）
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/Hana-ame/wintools/pkg/peerfs"
)

func main() {
	var (
		dir    = flag.String("dir", ".", "文件服务根目录")
		listen = flag.String("listen", "0.0.0.0:8000", "HTTP 监听（控制台页 + 可选内嵌信令）")
		name   = flag.String("name", "", "节点 peer id 后缀，最终 id = wt-media-<name>；空 = 借随机 id")
		token  = flag.String("token", "", "数据面 hello 校验 token；空 = 不校验")

		signal = flag.Bool("signal", true, "内嵌自托管信令服务器")
		key    = flag.String("key", "peerjs", "信令 API key（浏览器端必须一致）")
		stok   = flag.String("stoken", "", "信令 token 白名单（逗号分隔）；空 = 不限制")

		shost   = flag.String("shost", "", "-signal=false 时：信令 host（空 = 公共云 0.peerjs.com）")
		sport   = flag.Int("sport", 443, "-signal=false 时：信令 port")
		ssecure = flag.Bool("ssecure", true, "-signal=false 时：信令 wss")
		debug   = flag.Bool("debug", false, "信令详细日志")
	)
	flag.Parse()

	mux := http.NewServeMux()

	if *signal {
		var tokens []string
		for _, t := range strings.Split(*stok, ",") {
			if t = strings.TrimSpace(t); t != "" {
				tokens = append(tokens, t)
			}
		}
		peerfs.MountSignaling(mux, *key, tokens)
		log.Printf("embedded signaling on %s (ws /peerjs, key=%q)", *listen, *key)
	}

	sig := peerfs.Signaling{Host: *shost, Port: *sport, Secure: *ssecure, Key: *key}
	if *signal {
		// 内嵌信令：节点连本地信令（同源同端口），页面也连同源信令
		localPort := 8000
		if s := strings.Split(*listen, ":"); len(s) > 1 {
			if p, err := strconv.Atoi(s[len(s)-1]); err == nil {
				localPort = p
			}
		}
		sig = peerfs.Signaling{Host: "127.0.0.1", Port: localPort, Secure: false, Key: *key}
	}

	node := peerfs.New(peerfs.Config{
		PeerID:    joinID(*name),
		Root:      *dir,
		Token:     *token,
		Signaling: sig,
		Debug:     *debug,
	})
	node.MountConsole(mux)

	// 先起 HTTP 再连信令：内嵌信令时节点要连自己的 HTTP 端口
	srv := &http.Server{Addr: *listen, Handler: mux}
	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("listen: %v", err)
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
		log.Fatalf("port %s not ready after 5s", *listen)
	}()
	<-ready

	ctx := context.Background()
	if err := node.Start(ctx); err != nil {
		log.Fatalf("peer start: %v", err)
	}
	log.Printf("peer id: %s  root: %s", node.ID(), *dir)
	consoleHost := *listen
	if strings.HasPrefix(consoleHost, ":") || strings.HasPrefix(consoleHost, "0.0.0.0:") {
		consoleHost = "127.0.0.1" + consoleHost[strings.Index(consoleHost, ":"):]
	}
	log.Printf("console: http://%s/__peerfs/", consoleHost)

	select {} // 阻塞主 goroutine
}

func joinID(name string) string {
	if name == "" {
		return ""
	}
	return fmt.Sprintf("wt-media-%s", name)
}
