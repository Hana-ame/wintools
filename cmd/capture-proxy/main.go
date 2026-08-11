// capture-proxy — 合并 zen 家族四个程序的单一入口。
//
// 角色 (--role):
//
//	provider = 旧 cmd/zen-proxy + cmd/local-proxy-detected + cmd/local-proxy
//	           + scripts/capture_proxy.go 合体
//	           (Ollama 兼容端点, v6/v4 failover, 流检测, 注入, 抓包,
//	            /mode 切换, usage 统计, vanilla 透传, 可选 TLS)
//	multi    = 旧 cmd/zen-multi        (聚合多个 provider 实例)
//
// 用法:
//
//	capture-proxy -role provider --listen 127.0.0.1:8000 --mode auto [--out dir] [--detect=false] [--cert/--key]
//	capture-proxy -role multi    --listen 127.0.0.1:8443 --source cp1=http://127.0.0.1:8000
package main

import (
	"log"
	"os"
	"strings"
)

func main() {
	args := os.Args[1:]
	role := "provider"
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
	case "provider":
		runProvider(rest)
	case "multi":
		runMulti(rest)
	default:
		log.Fatalf("unknown role %q (want provider|multi)", role)
	}
}
