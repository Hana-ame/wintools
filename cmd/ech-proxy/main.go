package main

import (
	"context"
	"crypto/tls"
	_ "embed"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"sort"

	"github.com/gin-gonic/gin"

	"github.com/Hana-ame/wintools/pkg/apifwd"
	cloudflare_ech "github.com/Hana-ame/wintools/pkg/ech"
	"github.com/Hana-ame/wintools/pkg/echproxy"
)

//go:embed static/index.html
var chatHTML string

func main() {
	addr := flag.String("addr", "0.0.0.0:8443", "listen address")
	httpMode := flag.Bool("http", false, "run in HTTP mode (no TLS, local proxy)")
	flag.Parse()

	localIP := os.Getenv("LOCALIP")
	if localIP != "" {
		log.Printf("使用自定义 DoH 接入 IP: %s", localIP)
		cloudflare_ech.SetDoHConfig("moonchan.xyz", localIP)
	}

	ipMode := os.Getenv("IP_MODE")
	if ipMode != "" {
		cloudflare_ech.SetIPMode(ipMode)
	}
	v4ok, v6ok := cloudflare_ech.CheckDualStack(context.Background())
	if v4ok || v6ok {
		suffix := ""
		if ipMode != "" {
			suffix = " (强制 " + ipMode + ")"
		}
		log.Printf("IP 栈检测: IPv4=%v IPv6=%v%s", v4ok, v6ok, suffix)
	} else {
		log.Printf("IP 栈检测失败（DNS 不可达）")
	}

	log.Printf("正在初始化 ECH 客户端...")
	if err := cloudflare_ech.InitDefault(); err != nil {
		log.Fatalf("ECH 客户端初始化失败: %v", err)
	}
	log.Printf("ECH 客户端就绪")

	gin.SetMode(gin.ReleaseMode)
	r := gin.New()
	r.Use(gin.Recovery())
	r.Use(apifwd.CORSMiddleware())

	r.GET("/healthz", func(c *gin.Context) {
		c.String(200, "ok")
	})

	zenAPIKey := os.Getenv("ZEN_API_KEY")
	if zenAPIKey == "" {
		zenAPIKey = "public"
	}
	zenHandler := NewZenProvider()

	var upstreamCfg echproxy.UpstreamMap
	var upstreamHandler gin.HandlerFunc
	var tlsCert *tls.Certificate

	// 配置单一来源: 无论 TLS 还是 --http 模式, 都从 GitHub 拉取同一份
	// upstream.json (避免 embeddedConfig 与仓库配置双份漂移)。
	proxyBase := "https://proxy.moonchan.xyz/Hana-ame/wintools/refs/heads/main/%s?proxy_host=raw.githubusercontent.com"
	upstreamConfigURL := fmt.Sprintf(proxyBase, "certs/l.moonchan.xyz/upstream.json")

	log.Printf("正在加载上游配置: %s", upstreamConfigURL)
	cfg, err := echproxy.LoadConfig(upstreamConfigURL)
	if err != nil {
		log.Fatalf("加载上游配置失败: %v", err)
	}
	upstreamCfg = cfg.Upstreams
	log.Printf("上游配置加载成功: %d 条规则", len(upstreamCfg))

	if !*httpMode {
		// TLS 模式额外拉取证书: 证书 URL 与上游路由都写死在 repo 的
		// upstream.json 配置里, 证书续期后只需更新该配置指向的 URL。
		log.Printf("正在拉取证书: %s", cfg.CertPath)
		certPEM, err := echproxy.FetchBytes(cfg.CertPath)
		if err != nil {
			log.Fatalf("拉取证书失败: %v", err)
		}
		log.Printf("正在拉取密钥: %s", cfg.KeyPath)
		keyPEM, err := echproxy.FetchBytes(cfg.KeyPath)
		if err != nil {
			log.Fatalf("拉取密钥失败: %v", err)
		}
		cert, err := tls.X509KeyPair(certPEM, keyPEM)
		if err != nil {
			log.Fatalf("解析证书密钥失败: %v", err)
		}
		tlsCert = &cert
	}

	upstreamHandler = echproxy.ProxyHandler(upstreamCfg)
	zenHost := "zen.l.moonchan.xyz"
	zenProxyHandler := func(c *gin.Context) {
		if c.GetHeader("Authorization") == "" {
			c.Request.Header.Set("Authorization", "Bearer "+zenAPIKey)
		}
		zenHandler.ServeHTTP(c.Writer, c.Request)
	}

	hostOf := func(c *gin.Context) string {
		h := c.Request.Host
		if hh, _, err := net.SplitHostPort(h); err == nil {
			h = hh
		}
		return h
	}
	isZen := func(c *gin.Context) bool { return hostOf(c) == zenHost }

	r.GET("/", func(c *gin.Context) {
		if isZen(c) {
			zenProxyHandler(c)
			return
		}
		if _, ok := upstreamCfg[hostOf(c)]; ok {
			upstreamHandler(c)
			return
		}
		c.Header("Content-Type", "text/html; charset=utf-8")
		c.String(200, chatHTML)
	})

	r.NoRoute(func(c *gin.Context) {
		if isZen(c) {
			zenProxyHandler(c)
		} else {
			upstreamHandler(c)
		}
	})

	fmt.Printf("=== ECH Proxy ===\n")
	fmt.Printf("  模式: %s\n", map[bool]string{true: "HTTP (本地代理)", false: "TLS (远程)"}[*httpMode])
	fmt.Printf("  监听: %s\n", *addr)
	if localIP != "" {
		fmt.Printf("  DoH IP: %s\n", localIP)
	}
	var domains []string
	for host := range upstreamCfg {
		domains = append(domains, host)
	}
	sort.Strings(domains)
	for _, d := range domains {
		if d == "zen.l.moonchan.xyz" {
			fmt.Printf("  域名: %s -> opencode.ai (Zen API 直连)\n", d)
		} else {
			uc := upstreamCfg[d]
			fmt.Printf("  域名: %s -> %s (%s)", d, uc.Host, echproxy.ModeName(uc.Mode))
			if uc.Referer != "" {
				fmt.Printf(" (referer: %s)", uc.Referer)
			}
			fmt.Println()
		}
	}
	fmt.Printf("=================\n")

	if *httpMode {
		if err := r.Run(*addr); err != nil {
			log.Fatalf("启动失败: %v", err)
		}
		return
	}

	ln, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatalf("监听失败: %v", err)
	}
	srv := &http.Server{
		Addr:    *addr,
		Handler: r,
		TLSConfig: &tls.Config{
			Certificates: []tls.Certificate{*tlsCert},
			MinVersion:   tls.VersionTLS12,
		},
	}
	tlsLn := tls.NewListener(ln, srv.TLSConfig)
	if err := srv.Serve(tlsLn); err != nil {
		log.Fatalf("启动失败: %v", err)
	}
}
