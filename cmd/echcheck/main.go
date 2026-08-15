// command: echcheck — ECH 连通性探测 CLI (供 censor_probe.py 调用)
//
// 对目标域名执行一次 ECH (Encrypted Client Hello) TLS 握手 + HTTP 请求,
// 输出 JSON 结果。Python 侧通过子进程调用本工具获得权威的 ECH 可用性判断。
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"time"

	cloudflare_ech "github.com/Hana-ame/wintools/pkg/ech"
)

type Result struct {
	Domain      string `json:"domain"`
	ECHAccepted bool   `json:"ech_accepted"`
	HTTPStatus  int    `json:"http_status,omitempty"`
	Protocol    string `json:"protocol,omitempty"`
	Error       string `json:"error,omitempty"`
	ElapsedMS   int64  `json:"elapsed_ms"`
}

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: echcheck <domain>")
		os.Exit(2)
	}
	domain := os.Args[1]

	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()

	res := &Result{Domain: domain}

	// 1. 用共享 shell (cloudflare-ech.com) 初始化 ECH 配置
	if err := cloudflare_ech.InitDefault(); err != nil {
		res.Error = fmt.Sprintf("ECH init: %v", err)
		emit(res)
		os.Exit(1)
	}

	// 2. 发起真正带 ECH 的请求
	urlStr := fmt.Sprintf("https://%s/", domain)
	req, err := http.NewRequestWithContext(ctx, http.MethodHead, urlStr, nil)
	if err != nil {
		res.Error = fmt.Sprintf("new request: %v", err)
		emit(res)
		os.Exit(1)
	}
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64)")
	req.Host = domain

	start := time.Now()
	resp, err := cloudflare_ech.Do(req)
	res.ElapsedMS = time.Since(start).Milliseconds()
	if err != nil {
		res.Error = fmt.Sprintf("ECH request: %v", err)
		emit(res)
		os.Exit(1)
	}
	defer resp.Body.Close()

	res.HTTPStatus = resp.StatusCode
	res.Protocol = resp.Proto
	res.ECHAccepted = resp.TLS != nil && resp.TLS.ECHAccepted

	emit(res)
}

func emit(res *Result) {
	b, _ := json.Marshal(res)
	fmt.Println(string(b))
}