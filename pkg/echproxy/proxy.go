// Package echproxy 提供 ECH 域前置反向代理的核心组件。
// 包括上游配置加载、TLS 证书下载和代理 HTTP handler。
package echproxy

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"time"

	cloudflare_ech "github.com/Hana-ame/wintools/pkg/ech"
	"github.com/gin-gonic/gin"
)

// UpstreamConfig 表示一条上游转发规则。
type UpstreamConfig struct {
	Host    string `json:"host"`
	Referer string `json:"referer,omitempty"`
}

// UpstreamMap 按请求域名索引的上游配置集合。
type UpstreamMap map[string]UpstreamConfig

// DownloadFile 从 URL 下载文件保存到本地路径。
func DownloadFile(path, url string) error {
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

// LoadConfig 从远程 URL 加载上游配置 JSON。
func LoadConfig(rawURL string) (UpstreamMap, error) {
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

// ProxyHandler 返回一个 gin handler，根据请求 Host 匹配上游规则并通过 ECH 转发。
func ProxyHandler(cfg UpstreamMap) gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		clientIP := c.ClientIP()
		method := c.Request.Method
		rawPath := c.Request.URL.Path
		rawQuery := c.Request.URL.RawQuery

		host := c.Request.Host
		if h, _, err := net.SplitHostPort(host); err == nil {
			host = h
		}

		uc, ok := cfg[host]
		if !ok {
			log.Printf("[%s] 未找到上游配置: %s", clientIP, host)
			c.String(http.StatusBadGateway, "no upstream for host: %s", host)
			return
		}

		targetURL := &url.URL{
			Scheme:   "https",
			Host:     uc.Host,
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
		if uc.Referer != "" {
			outReq.Header.Set("Referer", uc.Referer)
		}
		outReq.Host = uc.Host

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
	}
}
