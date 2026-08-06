package main

import (
	"context"
	_ "embed"
	"encoding/json"
	"flag"
	"fmt"
	"io"
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
    "l.moonchan.xyz": {
        "host": "video-cf.twimg.com",
        "referer": "https://x.com"
    },
    "twimg.l.moonchan.xyz": {
        "host": "video-cf.twimg.com",
        "referer": "https://x.com"
    },
    "ex.l.moonchan.xyz": {
        "host": "exhentai.org"
    },
    "zen.l.moonchan.xyz": {
        "host": "opencode.ai"
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
	cert := flag.String("cert", "certs/l.moonchan.xyz/fullchain.cer", "TLS cert file")
	key := flag.String("key", "certs/l.moonchan.xyz/privkey.pem", "TLS key file")
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

	r.GET("/", func(c *gin.Context) {
		c.Header("Content-Type", "text/html; charset=utf-8")
		c.String(200, chatHTML)
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

	if *httpMode {
		if err := json.Unmarshal([]byte(embeddedConfig), &upstreamCfg); err != nil {
			log.Fatalf("解析内置上游配置失败: %v", err)
		}
		log.Printf("内置上游配置加载成功: %d 条规则", len(upstreamCfg))

		for domain, uc := range upstreamCfg {
			if uc.Host == "video-cf.twimg.com" {
				upstreamHandler = localProxyHandler(upstreamCfg, domain)
				break
			}
		}
	} else {
		proxyBase := "https://proxy.moonchan.xyz/Hana-ame/wintools/refs/heads/main/%s?proxy_host=raw.githubusercontent.com"
		certURL := fmt.Sprintf(proxyBase, *cert)
		keyURL := fmt.Sprintf(proxyBase, *key)
		upstreamConfigURL := fmt.Sprintf(proxyBase, "certs/l.moonchan.xyz/upstream.json")

		log.Printf("正在下载证书: %s", certURL)
		if err := echproxy.DownloadFile(*cert, certURL); err != nil {
			log.Fatalf("下载证书失败: %v", err)
		}
		log.Printf("正在下载密钥: %s", keyURL)
		if err := echproxy.DownloadFile(*key, keyURL); err != nil {
			log.Fatalf("下载密钥失败: %v", err)
		}

		log.Printf("正在加载上游配置: %s", upstreamConfigURL)
		var err error
		upstreamCfg, err = echproxy.LoadConfig(upstreamConfigURL)
		if err != nil {
			log.Fatalf("加载上游配置失败: %v", err)
		}
		log.Printf("上游配置加载成功: %d 条规则", len(upstreamCfg))

		upstreamHandler = echproxy.ProxyHandler(upstreamCfg)
	}

	r.NoRoute(func(c *gin.Context) {
		host := c.Request.Host
		if h, _, err := net.SplitHostPort(host); err == nil {
			host = h
		}
		if host == "zen.l.moonchan.xyz" {
			if c.GetHeader("Authorization") == "" {
				c.Request.Header.Set("Authorization", "Bearer "+zenAPIKey)
			}
			zenHandler(c)
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
			fmt.Printf("  域名: %s -> %s", d, uc.Host)
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
	} else {
		r.RunTLS(*addr, *cert, *key)
	}
}

func localProxyHandler(cfg echproxy.UpstreamMap, domain string) gin.HandlerFunc {
	return func(c *gin.Context) {
		uc := cfg[domain]

		clientIP := c.ClientIP()
		method := c.Request.Method
		rawPath := c.Request.URL.RequestURI()

		upstreamURL := fmt.Sprintf("https://%s%s", uc.Host, rawPath)

		log.Printf("[%s] %s %s -> %s", clientIP, method, rawPath, upstreamURL)

		outReq, err := http.NewRequest(method, upstreamURL, c.Request.Body)
		if err != nil {
			log.Printf("[%s] 创建请求失败: %v", clientIP, err)
			c.String(http.StatusInternalServerError, "创建请求失败: %v", err)
			return
		}

		for k, vs := range c.Request.Header {
			for _, v := range vs {
				outReq.Header.Add(k, v)
			}
		}
		if uc.Referer != "" {
			outReq.Header.Set("Referer", uc.Referer)
		}
		outReq.Host = uc.Host

		resp, err := cloudflare_ech.Do(outReq)
		if err != nil {
			log.Printf("[%s] ECH 请求失败: %v", clientIP, err)
			c.String(http.StatusBadGateway, "上游请求失败: %v", err)
			return
		}
		defer resp.Body.Close()

		for k, vs := range resp.Header {
			for _, v := range vs {
				c.Header(k, v)
			}
		}
		c.Status(resp.StatusCode)
		io.Copy(c.Writer, resp.Body)
	}
}
