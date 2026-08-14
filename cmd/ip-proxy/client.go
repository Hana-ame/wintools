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
	"sync/atomic"
	"time"

	"github.com/coder/websocket"

	"github.com/Hana-ame/wintools/pkg/netdial"
)

// 每个本地监听地址对应的 server 出口 (空 = 直连目标)。
var exits = map[string]string{}

// HTTPS 代理 CONNECT 请求统计: opencode 每发起一次 CONNECT 记一次。
// 注意: 这数的是"代理请求次数", 不是 VPS 端 /status 的 WS 隧道数 —
// opencode 的 keep-alive 会复用隧道, WS 数 ≈ TCP 连接数, 远小于请求数。
// 所以 CONNECT 计数必须在本机 client 上做, server 端只看到 WS 隧道。
// 按出口 (线路) 分开计, key = client.json 里的出口 URL, 空串 = 直连。
type exitStats struct {
	total atomic.Int64
	min   atomic.Int64
}

var connStats = map[string]*exitStats{}
var statsMu sync.Mutex

// statsFor 取某出口的计数器; 首次访问 (直连等配置外场景) 惰性创建。
func statsFor(exit string) *exitStats {
	statsMu.Lock()
	defer statsMu.Unlock()
	s, ok := connStats[exit]
	if !ok {
		s = &exitStats{}
		connStats[exit] = s
	}
	return s
}

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
				// /status: 按出口分开的 CONNECT 请求统计, 供本机查看各线流量。
				if r.URL.Path == "/status" {
					w.Header().Set("Content-Type", "application/json")
					w.Header().Set("Access-Control-Allow-Origin", "*")
					// 遍历 map 输出各出口计数, 输出前全部 Load 快照。
					var total, min int64
					byExit := map[string]map[string]int64{}
					statsMu.Lock()
					for name, s := range connStats {
						t, m := s.total.Load(), s.min.Load()
						if name == "" {
							name = "direct"
						}
						byExit[name] = map[string]int64{"total": t, "last_minute": m}
						total += t
						min += m
					}
					statsMu.Unlock()
					jsonOut, _ := json.Marshal(map[string]any{
						"total":        total,
						"last_minute":  min,
						"by_exit":      byExit,
					})
					w.Write(jsonOut)
					return
				}
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
	// 每分钟打印各出口 CONNECT 请求数, 并清零本分钟计数。
	go func() {
		for {
			time.Sleep(time.Minute)
			statsMu.Lock()
			for name, s := range connStats {
				if name == "" {
					name = "direct"
				}
				log.Printf("CONNECT %s: 本分钟 %d 次, 累计 %d 次", name, s.min.Swap(0), s.total.Load())
			}
			statsMu.Unlock()
		}
	}()

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
	// 收到 CONNECT 请求即计数 — 这就是"代理被访问的次数", 按出口分开。
	statsFor(exit).total.Add(1)
	statsFor(exit).min.Add(1)
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
