package main

import (
	"context"
	"crypto/tls"
	_ "embed"
	"encoding/json"
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

const embeddedConfig = `{
    "upstreams": {
        "l.moonchan.xyz": {
            "host": "reminder.moonchan.xyz",
            "mode": "direct"
        },
        "twimg.l.moonchan.xyz": {
            "host": "video-cf.twimg.com",
            "referer": "https://x.com"
        },
        "ex.l.moonchan.xyz": {
            "host": "exhentai.org"
        },
        "sukebei.l.moonchan.xyz": {
            "host": "sukebei.nyaa.si",
            "mode": "sni"
        },
        "ao3.l.moonchan.xyz": {
            "host": "archiveofourown.org"
        },
        "iwara.l.moonchan.xyz": {
            "host": "iwara.tv",
            "rewrites": {
                "api.iwara.tv": "iwara-api.l.moonchan.xyz",
                "files.iwara.tv": "iwara-files.l.moonchan.xyz"
            }
        },
        "iwara-api.l.moonchan.xyz": {
            "host": "api.iwara.tv"
        },
        "iwara-files.l.moonchan.xyz": {
            "host": "files.iwara.tv",
            "mode": "sni"
        },
        "pixiv.l.moonchan.xyz": {
            "host": "www.pixiv.net",
            "rewrites": {
                "www.pixiv.net": "pixiv.l.moonchan.xyz",
                "pixiv.net": "pixiv.l.moonchan.xyz",
                "app-api.pixiv.net": "pixiv-api.l.moonchan.xyz",
                "oauth.secure.pixiv.net": "pixiv-oauth.l.moonchan.xyz",
                "i.pximg.net": "pixiv-img.l.moonchan.xyz",
                "s.pximg.net": "pixiv-img.l.moonchan.xyz"
            }
        },
        "pixiv-api.l.moonchan.xyz": {
            "host": "app-api.pixiv.net"
        },
        "pixiv-oauth.l.moonchan.xyz": {
            "host": "oauth.secure.pixiv.net"
        },
        "pixiv-img.l.moonchan.xyz": {
            "host": "i.pximg.net",
            "mode": "sni",
            "referer": "https://www.pixiv.net/"
        },
        "zen.l.moonchan.xyz": {
            "host": "opencode.ai"
        }
    }
}`

func cleanZenPayload(body []byte) ([]byte, error) {
	var payload map[string]any
	if err := json.Unmarshal(body, &payload); err != nil {
		return nil, err
	}
	if _, ok := payload["model"]; !ok {
		payload["model"] = "deepseek-v4-flash-free"
	}
	if mt, ok := payload["max_tokens"].(float64); !ok || mt > 131072 {
		payload["max_tokens"] = 131072
	}
	return json.Marshal(payload)
}

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
	log.Printf("正在解析 opencode.ai  IP...")
	v4ep, err4 := apifwd.ResolveIP("opencode.ai", "/zen/v1", 4)
	v6ep, err6 := apifwd.ResolveIP("opencode.ai", "/zen/v1", 6)
	if v4ep != nil {
		log.Printf("  IPv4: %s", v4ep.URL)
	}
	if v6ep != nil {
		log.Printf("  IPv6: %s", v6ep.URL)
	}
	if err4 != nil && err6 != nil {
		log.Fatalf("解析 opencode.ai 失败: v4=%v v6=%v", err4, err6)
	}
	zenHandler := apifwd.Zen(v4ep, v6ep, zenAPIKey, cleanZenPayload)

	var upstreamCfg echproxy.UpstreamMap
	var upstreamHandler gin.HandlerFunc
	var tlsCert *tls.Certificate

	if *httpMode {
		var cfg echproxy.Config
		if err := json.Unmarshal([]byte(embeddedConfig), &cfg); err != nil {
			log.Fatalf("解析内置上游配置失败: %v", err)
		}
		upstreamCfg = cfg.Upstreams
		log.Printf("内置上游配置加载成功: %d 条规则", len(upstreamCfg))
		upstreamHandler = echproxy.ProxyHandler(upstreamCfg)
	} else {
		// 全部配置每次启动经 proxy.moonchan.xyz 拉取到内存，不落盘。
		// 证书 URL 与上游路由都写死在 repo 的 upstream.json 配置里，
		// 证书续期后只需更新该配置指向的 URL。
		proxyBase := "https://proxy.moonchan.xyz/Hana-ame/wintools/refs/heads/main/%s?proxy_host=raw.githubusercontent.com"
		upstreamConfigURL := fmt.Sprintf(proxyBase, "certs/l.moonchan.xyz/upstream.json")

		log.Printf("正在加载上游配置: %s", upstreamConfigURL)
		cfg, err := echproxy.LoadConfig(upstreamConfigURL)
		if err != nil {
			log.Fatalf("加载上游配置失败: %v", err)
		}
		upstreamCfg = cfg.Upstreams
		log.Printf("上游配置加载成功: %d 条规则", len(upstreamCfg))

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
		zenHandler(c)
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
