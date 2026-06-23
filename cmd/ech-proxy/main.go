package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"time"

	"github.com/Hana-ame/wintools/cloudflare_ech"
	"github.com/gin-gonic/gin"
)

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

func main() {
	addr := flag.String("addr", "0.0.0.0:8443", "listen address")
	domain := flag.String("domain", "l.moonchan.xyz", "TLS domain (for log only)")
	cert := flag.String("cert", "certs/l.moonchan.xyz/fullchain.cer", "TLS cert file")
	key := flag.String("key", "certs/l.moonchan.xyz/privkey.pem", "TLS key file")
	upstream := flag.String("upstream", "video-cf.twimg.com", "default upstream target")
	flag.Parse()

	if v := os.Getenv("UPSTREAM"); v != "" {
		*upstream = v
	}

	log.Printf("正在初始化 ECH 客户端...")
	if err := cloudflare_ech.InitDefault(); err != nil {
		log.Fatalf("ECH 客户端初始化失败: %v", err)
	}
	log.Printf("ECH 客户端就绪")

	certDir := filepath.Dir(*cert)
	if err := os.MkdirAll(certDir, 0755); err != nil {
		log.Fatalf("创建证书目录失败: %s (%v)", certDir, err)
	}

	certURL := fmt.Sprintf("https://proxy.moonchan.xyz/Hana-ame/wintools/refs/heads/main/%s?proxy_host=raw.githubusercontent.com", *cert)
	keyURL := fmt.Sprintf("https://proxy.moonchan.xyz/Hana-ame/wintools/refs/heads/main/%s?proxy_host=raw.githubusercontent.com", *key)

	if _, err := os.Stat(*cert); err != nil {
		log.Printf("证书文件不存在，正在下载: %s", certURL)
		if err := downloadFile(*cert, certURL); err != nil {
			log.Fatalf("下载证书失败: %v", err)
		}
	}
	if _, err := os.Stat(*key); err != nil {
		log.Printf("密钥文件不存在，正在下载: %s", keyURL)
		if err := downloadFile(*key, keyURL); err != nil {
			log.Fatalf("下载密钥失败: %v", err)
		}
	}

	certFile, _ := filepath.Abs(*cert)
	keyFile, _ := filepath.Abs(*key)

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

		targetURL := &url.URL{
			Scheme:   "https",
			Host:     *upstream,
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
		outReq.Header.Set("Referer", "https://x.com")
		outReq.Host = *upstream

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
	fmt.Printf("  域名: %s\n", *domain)
	fmt.Printf("  上游: %s\n", *upstream)
	fmt.Printf("  证书: %s\n", certFile)
	fmt.Printf("  密钥: %s\n", keyFile)
	fmt.Printf("=================\n")

	if err := r.RunTLS(*addr, certFile, keyFile); err != nil {
		log.Fatalf("启动失败: %v", err)
	}
}
