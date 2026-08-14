package main

import (
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

// HTTPS 代理流量统计: 按出口 (线路) 分开, key = client.json 里的出口 URL, 空串 = 直连。
//
// CONNECT 请求数 + 上下行字节数。字节数在 relay 双向拷贝时累计,
// 不依赖 HTTP 版本 (h1/h2 都能数), 是隧道流量最可靠的指标。
// 注意: 这里的 total 是 CONNECT 隧道数, 不是请求数 — opencode 的
// keep-alive 会复用隧道, WS 数 ≈ TCP 连接数, 远小于请求数。
// 本机 client 上做, server 端只看到 WS 隧道。
type exitStats struct {
	total atomic.Int64
	min   atomic.Int64
	up    atomic.Int64 // 上行字节: 本机 -> 目标
	down  atomic.Int64 // 下行字节: 目标 -> 本机
	upMin atomic.Int64
	downMin atomic.Int64
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
				// /status: 按出口分开的流量统计 (CONNECT 数 + 上下行字节)。
				if r.URL.Path == "/status" {
					w.Header().Set("Content-Type", "application/json")
					w.Header().Set("Access-Control-Allow-Origin", "*")
					// 遍历 map 输出各出口计数, 输出前全部 Load 快照。
					var total, min, up, down int64
					byExit := map[string]map[string]int64{}
					statsMu.Lock()
					for name, s := range connStats {
						t, m := s.total.Load(), s.min.Load()
						u, d := s.up.Load(), s.down.Load()
						if name == "" {
							name = "direct"
						}
						byExit[name] = map[string]int64{
							"total": t, "last_minute": m,
							"up_bytes": u, "down_bytes": d,
						}
						total += t
						min += m
						up += u
						down += d
					}
					statsMu.Unlock()
					jsonOut, _ := json.Marshal(map[string]any{
						"total":       total,
						"last_minute": min,
						"up_bytes":    up,
						"down_bytes":  down,
						"by_exit":     byExit,
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
	// 每分钟打印各出口 CONNECT 数 + 上下行字节, 并清零本分钟计数。
	go func() {
		for {
			time.Sleep(time.Minute)
			statsMu.Lock()
			for name, s := range connStats {
				if name == "" {
					name = "direct"
				}
				log.Printf("CONNECT %s: 本分钟 %d 次 (↑%.1fMB ↓%.1fMB), 累计 %d 次 (↑%.1fMB ↓%.1fMB)",
					name, s.min.Swap(0), float64(s.upMin.Swap(0))/1e6, float64(s.downMin.Swap(0))/1e6,
					s.total.Load(), float64(s.up.Load())/1e6, float64(s.down.Load())/1e6)
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
	s := statsFor(exit)
	s.total.Add(1)
	s.min.Add(1)
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
		hijackAndRelay(w, out, s)
		return
	}

	log.Printf("CONNECT %s -> direct", target)
	out, err = net.DialTimeout("tcp", target, 10*time.Second)
	if err != nil {
		log.Printf("CONNECT %s direct dial fail: %v", target, err)
		http.Error(w, err.Error(), 502)
		return
	}
	hijackAndRelay(w, out, s)
}

// counterWriter 包一层 io.Writer, 转发的同时把字节累计进 total/minute 两个计数器。
// client 按出口计 (up/down 各自 pair), server 按全局计, 共用这一个实现。
// 注意: 计数在 Write 层做, 不依赖 HTTP 版本, h1/h2 隧道都数得到。
type counterWriter struct {
	w      io.Writer
	total  *atomic.Int64
	minute *atomic.Int64
}

func (c *counterWriter) Write(p []byte) (int, error) {
	n, err := c.w.Write(p)
	if n > 0 {
		c.total.Add(int64(n))
		c.minute.Add(int64(n))
	}
	return n, err
}

func hijackAndRelay(w http.ResponseWriter, out net.Conn, s *exitStats) {
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
	// 双向转发时累计字节: client->out 为上行, out->client 为下行。
	go func() {
		io.Copy(&counterWriter{w: out, total: &s.up, minute: &s.upMin}, client)
		out.Close()
	}()
	io.Copy(&counterWriter{w: client, total: &s.down, minute: &s.downMin}, out)
	client.Close()
}

type arrayFlags []string

func (a *arrayFlags) String() string { return strings.Join(*a, ",") }
func (a *arrayFlags) Set(v string) error {
	*a = append(*a, v)
	return nil
}
