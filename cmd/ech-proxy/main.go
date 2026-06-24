package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"

	cloudflare_ech "github.com/Hana-ame/wintools/pkg/ech"
	"github.com/Hana-ame/wintools/pkg/echproxy"
	"github.com/gin-gonic/gin"
)

func main() {
	addr := flag.String("addr", "0.0.0.0:8443", "listen address")
	cert := flag.String("cert", "certs/l.moonchan.xyz/fullchain.cer", "TLS cert file")
	key := flag.String("key", "certs/l.moonchan.xyz/privkey.pem", "TLS key file")
	flag.Parse()

	log.Printf("正在初始化 ECH 客户端...")
	if err := cloudflare_ech.InitDefault(); err != nil {
		log.Fatalf("ECH 客户端初始化失败: %v", err)
	}
	log.Printf("ECH 客户端就绪")

	certDir := filepath.Dir(*cert)
	if err := os.MkdirAll(certDir, 0755); err != nil {
		log.Fatalf("创建证书目录失败: %s (%v)", certDir, err)
	}

	proxyBase := "https://proxy.moonchan.xyz/Hana-ame/wintools/refs/heads/main/%s?proxy_host=raw.githubusercontent.com"
	certURL := fmt.Sprintf(proxyBase, *cert)
	keyURL := fmt.Sprintf(proxyBase, *key)
	upstreamConfigURL := fmt.Sprintf(proxyBase, filepath.Join(certDir, "upstream.json"))

	log.Printf("正在下载证书: %s", certURL)
	if err := echproxy.DownloadFile(*cert, certURL); err != nil {
		log.Fatalf("下载证书失败: %v", err)
	}
	log.Printf("正在下载密钥: %s", keyURL)
	if err := echproxy.DownloadFile(*key, keyURL); err != nil {
		log.Fatalf("下载密钥失败: %v", err)
	}

	certFile, _ := filepath.Abs(*cert)
	keyFile, _ := filepath.Abs(*key)

	log.Printf("正在加载上游配置: %s", upstreamConfigURL)
	upstreamCfg, err := echproxy.LoadConfig(upstreamConfigURL)
	if err != nil {
		log.Fatalf("加载上游配置失败: %v", err)
	}
	log.Printf("上游配置加载成功: %d 条规则", len(upstreamCfg))

	gin.SetMode(gin.ReleaseMode)
	r := gin.New()
	r.Use(gin.Recovery())

	r.GET("/healthz", func(c *gin.Context) {
		c.String(200, "ok")
	})
	r.NoRoute(echproxy.ProxyHandler(upstreamCfg))

	fmt.Printf("=== ECH Proxy ===\n")
	fmt.Printf("  监听: %s\n", *addr)
	var domains []string
	for host := range upstreamCfg {
		domains = append(domains, host)
	}
	sort.Strings(domains)
	for _, d := range domains {
		uc := upstreamCfg[d]
		fmt.Printf("  域名: %s -> %s", d, uc.Host)
		if uc.Referer != "" {
			fmt.Printf(" (referer: %s)", uc.Referer)
		}
		fmt.Println()
	}
	fmt.Printf("  上游配置: %s\n", upstreamConfigURL)
	fmt.Printf("  证书: %s\n", certFile)
	fmt.Printf("  密钥: %s\n", keyFile)
	fmt.Printf("=================\n")

	if err := r.RunTLS(*addr, certFile, keyFile); err != nil {
		log.Fatalf("启动失败: %v", err)
	}
}
