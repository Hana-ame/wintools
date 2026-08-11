// ip-proxy — 多 IP 多出口 HTTP CONNECT 分流代理 (client) + 隧道接收端 (server)。
//
// client 角色: 每个本地 IP 绑定一条线路 (server 出口), CONNECT opencode.ai:443
//
//	走该 IP 的线路, 其他 host 直连。opencode 设置 HTTPS_PROXY=http://127.0.0.1:7890,
//	换 IP (127.0.0.2/3/...) 即换线路。
//
// server 角色: 标准 HTTP CONNECT 代理, 部署在 VPS 上, 收到 client 转发的
//
//	CONNECT 请求后拨号真实目标 (可 --force v4|v6), 回 200 建立隧道。
//
// 用法:
//
//	server: ip-proxy -role server --listen 0.0.0.0:18444 [--force v4|v6]
//	client: ip-proxy -role client \
//	  --bind 127.0.0.1:7890=127.0.0.1:18444   # vps-v4
//	  --bind 127.0.0.2:7890=127.0.0.1:18445   # vps-v6
//	  ...
package main

import (
	"flag"
	"log"
	"os"
	"strings"
)

func main() {
	args := os.Args[1:]
	role := "client"
	rest := make([]string, 0, len(args))
	for i := 0; i < len(args); i++ {
		switch {
		case args[i] == "-role" && i+1 < len(args):
			role = args[i+1]
			i++
		case strings.HasPrefix(args[i], "-role="):
			role = strings.TrimPrefix(args[i], "-role=")
		default:
			rest = append(rest, args[i])
		}
	}

	switch role {
	case "client":
		runClient(rest)
	case "server":
		runServer(rest)
	default:
		log.Fatalf("unknown role %q (want client|server)", role)
	}
}

var _ = flag.ExitOnError
