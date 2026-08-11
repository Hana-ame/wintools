package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/coder/websocket"

	"github.com/Hana-ame/wintools/pkg/netdial"
)

// 每个本地监听地址对应的 server 出口 (空 = 直连目标)。
var exits = map[string]string{}

func runClient(args []string) {
	fs := flag.NewFlagSet("client", flag.ExitOnError)
	config := fs.String("config", "client.json", "JSON 配置文件 (监听地址 -> 出口)")
	fs.Parse(args)

	data, err := os.ReadFile(*config)
	if err != nil {
		log.Fatalf("读取配置 %s: %v", *config, err)
	}
	if err := json.Unmarshal(data, &exits); err != nil {
		log.Fatalf("解析配置 %s: %v", *config, err)
	}
	if len(exits) == 0 {
		log.Fatalf("配置 %s 为空 (至少一条 监听地址=出口)", *config)
	}

	var servers []*http.Server
	for addr, exit := range exits {
		addr, exit := addr, exit
		log.Printf("绑定 %s -> 出口 %s", addr, exit)
		srv := &http.Server{
			Addr: addr,
			Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method == http.MethodConnect {
					handleConnect(w, r, exit)
					return
				}
				log.Printf("%s HTTP %s %s", addr, r.Method, r.URL.String())
				http.Error(w, "only CONNECT supported", 405)
			}),
		}
		servers = append(servers, srv)
	}

	var wg sync.WaitGroup
	for _, srv := range servers {
		wg.Add(1)
		go func(s *http.Server) {
			defer wg.Done()
			log.Printf("client 监听 %s", s.Addr)
			if err := s.ListenAndServe(); err != nil {
				log.Printf("%s: %v", s.Addr, err)
			}
		}(srv)
	}
	wg.Wait()
}

// handleConnect 处理 CONNECT:
//   - 有出口 (server): 拨号出口, 把 CONNECT 请求原样转发, 等 200 后建隧道
//   - 无出口: 直接拨号目标
func handleConnect(w http.ResponseWriter, r *http.Request, exit string) {
	target := r.Host
	host := target
	if h, _, err := net.SplitHostPort(host); err == nil {
		host = h
	}

	var out net.Conn
	var err error
	if exit != "" {
		// 经 WebSocket 隧道到 server: server 拨号 target 后建立双向转发。
		// exit 支持 ws:// 或 wss:// 前缀, 无前缀默认 ws://
		scheme := "ws"
		wsAddr := exit
		if strings.HasPrefix(exit, "wss://") {
			scheme, wsAddr = "wss", strings.TrimPrefix(exit, "wss://")
		} else if strings.HasPrefix(exit, "ws://") {
			wsAddr = strings.TrimPrefix(exit, "ws://")
		}
		log.Printf("CONNECT %s -> exit %s://%s", target, scheme, wsAddr)
		wsURL := scheme + "://" + wsAddr + "/connect?target=" + url.QueryEscape(target)
		ws, _, werr := websocket.Dial(context.Background(), wsURL, netdial.WebsocketDialOptions())
		if werr != nil {
			log.Printf("CONNECT %s ws dial %s fail: %v", target, wsURL, werr)
			http.Error(w, werr.Error(), 502)
			return
		}
		out = websocket.NetConn(context.Background(), ws, websocket.MessageBinary)
		hijackAndRelay(w, out, nil)
		return
	}

	log.Printf("CONNECT %s -> direct", target)
	out, err = net.DialTimeout("tcp", target, 10*time.Second)
	if err != nil {
		log.Printf("CONNECT %s direct dial fail: %v", target, err)
		http.Error(w, err.Error(), 502)
		return
	}
	hijackAndRelay(w, out, nil)
}

func hijackAndRelay(w http.ResponseWriter, out net.Conn, _ *bufio.Reader) {
	hij, ok := w.(http.Hijacker)
	if !ok {
		out.Close()
		http.Error(w, "hijack unsupported", 500)
		return
	}
	client, _, err := hij.Hijack()
	if err != nil {
		out.Close()
		http.Error(w, err.Error(), 500)
		return
	}
	fmt.Fprintf(client, "HTTP/1.1 200 Connection Established\r\n\r\n")
	go func() {
		io.Copy(out, client)
		out.Close()
	}()
	io.Copy(client, out)
	client.Close()
}

type arrayFlags []string

func (a *arrayFlags) String() string { return strings.Join(*a, ",") }
func (a *arrayFlags) Set(v string) error {
	*a = append(*a, v)
	return nil
}
