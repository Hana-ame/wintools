package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"sort"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"time"

	cloudflare_ech "github.com/Hana-ame/wintools/test/ech/lib"
	"github.com/gin-gonic/gin"
)

type UpstreamConfig struct {
	Host    string `json:"host"`
	Referer string `json:"referer,omitempty"`
}

type UpstreamMap map[string]UpstreamConfig

func downloadFile(path, url string) error {
	out, err := os.Create(path)
	if err != nil {
		return err
	}
	defer out.Close()

	resp, err := http.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status: %s", resp.Status)
	}

	_, err = io.Copy(out, resp.Body)
	return err
}

func loadUpstreamConfig(rawURL string) (UpstreamMap, error) {
	resp, err := http.Get(rawURL)
	if err != nil {
		return nil, fmt.Errorf("fetch upstream config: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status: %s", resp.Status)
	}
	var cfg UpstreamMap
	if err := json.NewDecoder(resp.Body).Decode(&cfg); err != nil {
		return nil, fmt.Errorf("decode upstream config: %w", err)
	}
	return cfg, nil
}

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
	if err := downloadFile(*cert, certURL); err != nil {
		log.Fatalf("下载证书失败: %v", err)
	}
	log.Printf("正在下载密钥: %s", keyURL)
	if err := downloadFile(*key, keyURL); err != nil {
		log.Fatalf("下载密钥失败: %v", err)
	}

	certFile, _ := filepath.Abs(*cert)
	keyFile, _ := filepath.Abs(*key)

	log.Printf("正在加载上游配置: %s", upstreamConfigURL)
	upstreamCfg, err := loadUpstreamConfig(upstreamConfigURL)
	if err != nil {
		log.Fatalf("加载上游配置失败: %v", err)
	}
	log.Printf("上游配置加载成功: %d 条规则", len(upstreamCfg))

	gin.SetMode(gin.ReleaseMode)
	r := gin.New()
	r.Use(gin.Recovery())

	r.GET("/healthz", func(c *gin.Context) {
		c.String(http.StatusOK, "ok")
	})

	r.NoRoute(func(c *gin.Context) {
		start := time.Now()
		clientIP := c.ClientIP()
		method := c.Request.Method
		rawPath := c.Request.URL.Path
		rawQuery := c.Request.URL.RawQuery

		host := c.Request.Host
		if h, _, err := net.SplitHostPort(host); err == nil {
			host = h
		}

		cfg, ok := upstreamCfg[host]
		if !ok {
			log.Printf("[%s] 未找到上游配置: %s", clientIP, host)
			c.String(http.StatusBadGateway, "no upstream for host: %s", host)
			return
		}

		targetURL := &url.URL{
			Scheme:   "https",
			Host:     cfg.Host,
			Path:     rawPath,
			RawQuery: rawQuery,
		}
		urlStr := targetURL.String()

		log.Printf("[%s] %s %s -> %s", clientIP, method, rawPath, urlStr)

		outReq, err := http.NewRequest(method, urlStr, c.Request.Body)
		if err != nil {
			log.Printf("[%s] 创建请求失败: %v", clientIP, err)
			c.String(http.StatusInternalServerError, "create request: %v", err)
			return
		}

		for k, vs := range c.Request.Header {
			for _, v := range vs {
				outReq.Header.Add(k, v)
			}
		}
		if cfg.Referer != "" {
			outReq.Header.Set("Referer", cfg.Referer)
		}
		outReq.Host = cfg.Host

		log.Printf("[%s] -> ECH Do: %s %s (Host: %s)", clientIP, method, urlStr, outReq.Host)

		resp, err := cloudflare_ech.Do(outReq)
		if err != nil {
			log.Printf("[%s] ECH Do 失败: %v (耗时: %v)", clientIP, err, time.Since(start))
			c.String(http.StatusBadGateway, "upstream: %v", err)
			return
		}
		defer resp.Body.Close()

		log.Printf("[%s] <- %s (耗时: %v)", clientIP, resp.Status, time.Since(start))

		for k, vs := range resp.Header {
			for _, v := range vs {
				c.Header(k, v)
			}
		}
		c.Status(resp.StatusCode)
		io.Copy(c.Writer, resp.Body)
	})

	fmt.Printf("=== ECH Proxy ===\n")
	fmt.Printf("  监听: %s\n", *addr)
	var domains []string
	for host := range upstreamCfg {
		domains = append(domains, host)
	}
	sort.Strings(domains)
	for _, d := range domains {
		cfg := upstreamCfg[d]
		fmt.Printf("  域名: %s -> %s", d, cfg.Host)
		if cfg.Referer != "" {
			fmt.Printf(" (referer: %s)", cfg.Referer)
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
