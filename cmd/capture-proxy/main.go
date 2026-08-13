// capture-proxy — zen 家族三重整合的单一入口。
//
// 角色 (--role):
//
//	provider = 旧 cmd/zen-proxy + cmd/local-proxy-detected + cmd/local-proxy
//	           + scripts/capture_proxy.go 合体
//	           (Ollama 兼容端点, v6/v4 failover, 流检测, 注入, 抓包,
//	            /mode 切换, usage 统计, vanilla 透传, 可选 TLS)
//
// 用法:
//
//	capture-proxy --listen 127.0.0.1:8000 --mode auto [--out dir] [--detect=false] [--cert/--key]
//
// multi 角色 (旧 cmd/zen-multi 聚合) 在 v2.2.2 去掉: 不再需要聚合多个
// provider 实例, 保持单一 provider 形态。
package main

import (
	"os"
)

func main() {
	runProvider(os.Args[1:])
}
