package main

import (
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
	if mt, ok := payload["max_tokens"].(float64); !ok || mt > 65536 {
		payload["max_tokens"] = 65536
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

	zenOpt := apifwd.Option{
		Dest:    "https://opencode.ai/zen/v1",
		Local:   true,
		Headers: map[string]string{"Authorization": "Bearer public"},
		Modify:  cleanZenPayload,
	}
	r.Use(func(c *gin.Context) {
		host := c.Request.Host
		if h, _, err := net.SplitHostPort(host); err == nil {
			host = h
		}
		if host == "zen.l.moonchan.xyz" {
			apifwd.Handler(zenOpt)(c)
			c.Abort()
			return
		}
		c.Next()
	})

	var upstreamCfg echproxy.UpstreamMap

	if *httpMode {
		if err := json.Unmarshal([]byte(embeddedConfig), &upstreamCfg); err != nil {
			log.Fatalf("解析内置上游配置失败: %v", err)
		}
		log.Printf("内置上游配置加载成功: %d 条规则", len(upstreamCfg))

		for domain, uc := range upstreamCfg {
			if uc.Host == "video-cf.twimg.com" {
				r.NoRoute(localProxyHandler(upstreamCfg, domain))
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

		r.NoRoute(echproxy.ProxyHandler(upstreamCfg))
	}

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
