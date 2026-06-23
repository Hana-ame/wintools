package main

import (
	"fmt"
	"log"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/gin-gonic/gin"
)

const (
	listenAddr    = "0.0.0.0:8443"
	domain        = "l.moonchan.xyz"
	sshScript     = "/home/lumin/script/ssh/vps.sh"
	remoteCertDir = "~/.acme.sh/*.moonchan.xyz_ecc"
)

func fetchFile(remotePath string) ([]byte, error) {
	cmd := exec.Command("bash", "-c", sshScript+" 'cat "+remotePath+"'")
	out, err := cmd.Output()
	if err != nil {
		if ee, ok := err.(*exec.ExitError); ok {
			return nil, fmt.Errorf("ssh cat failed: %w\nstderr: %s", err, string(ee.Stderr))
		}
		return nil, fmt.Errorf("ssh cat failed: %w", err)
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("empty file: %s", remotePath)
	}
	return out, nil
}

func main() {
	log.Printf("正在从 VPS 获取 TLS 证书...")

	certPEM, err := fetchFile(remoteCertDir + "/fullchain.cer")
	if err != nil {
		log.Fatalf("获取证书失败: %v", err)
	}
	log.Printf("证书: %d bytes", len(certPEM))

	keyPEM, err := fetchFile(remoteCertDir + "/*.moonchan.xyz.key")
	if err != nil {
		log.Fatalf("获取密钥失败: %v", err)
	}
	log.Printf("密钥: %d bytes", len(keyPEM))

	tmpDir, err := os.MkdirTemp("", "ech-proxy-")
	if err != nil {
		log.Fatalf("创建临时目录失败: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	certFile := filepath.Join(tmpDir, "cert.pem")
	keyFile := filepath.Join(tmpDir, "key.pem")
	if err := os.WriteFile(certFile, certPEM, 0644); err != nil {
		log.Fatalf("写入证书失败: %v", err)
	}
	if err := os.WriteFile(keyFile, keyPEM, 0644); err != nil {
		log.Fatalf("写入密钥失败: %v", err)
	}

	gin.SetMode(gin.ReleaseMode)
	r := gin.New()
	r.Use(gin.Recovery())

	// 健康检查
	r.GET("/healthz", func(c *gin.Context) {
		c.String(http.StatusOK, "ok")
	})

	// 通用反向代理：将请求代理到目标 upstream
	r.NoRoute(func(c *gin.Context) {
		target := c.Query("upstream")
		if target == "" {
			target = os.Getenv("UPSTREAM")
		}
		if target == "" {
			// 默认回显，方便测试
			c.JSON(http.StatusOK, gin.H{
				"host":   c.Request.Host,
				"path":   c.Request.URL.Path,
				"domain": domain,
				"tls":    true,
			})
			return
		}
		if !strings.HasPrefix(target, "http://") && !strings.HasPrefix(target, "https://") {
			target = "https://" + target
		}
		targetURL, err := url.Parse(target)
		if err != nil {
			c.String(http.StatusBadRequest, "bad upstream: %v", err)
			return
		}
		proxy := httputil.NewSingleHostReverseProxy(targetURL)
		proxy.ServeHTTP(c.Writer, c.Request)
	})

	log.Printf("ECH Proxy 启动: https://%s:%s", domain, "8443")
	log.Printf("测试: curl -x '' 'https://%s:%s/healthz'", domain, "8443")

	if err := r.RunTLS(listenAddr, certFile, keyFile); err != nil {
		log.Fatalf("启动失败: %v", err)
	}
}
