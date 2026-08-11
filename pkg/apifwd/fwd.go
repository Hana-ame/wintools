package apifwd

import (
	"bytes"
	"io"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"

	cloudflare_ech "github.com/Hana-ame/wintools/pkg/ech"
)

type Option struct {
	Dest    string
	Local   bool
	Headers map[string]string
	Modify  func([]byte) ([]byte, error)
}

func CORSMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Header("Access-Control-Allow-Origin", "*")
		c.Header("Access-Control-Allow-Methods", "GET, POST, PUT, PATCH, DELETE, OPTIONS")
		c.Header("Access-Control-Allow-Headers", "Content-Type, Authorization")
		c.Header("Access-Control-Max-Age", "86400")
		if c.Request.Method == http.MethodOptions {
			c.AbortWithStatus(204)
			return
		}
		c.Next()
	}
}

func Handler(opt Option) gin.HandlerFunc {
	dest := strings.TrimRight(opt.Dest, "/")

	return func(c *gin.Context) {
		clientIP := c.ClientIP()
		method := c.Request.Method
		path := c.Request.URL.Path
		rawQuery := c.Request.URL.RawQuery

		if method == http.MethodGet && path == "/" {
			c.String(200, "proxy running\n")
			return
		}

		urlStr := dest + path
		if rawQuery != "" {
			urlStr += "?" + rawQuery
		}

		log.Printf("[%s] %s %s -> %s", clientIP, method, path, urlStr)

		var body []byte
		if method == http.MethodPost || method == http.MethodPut || method == http.MethodPatch {
			var err error
			body, err = io.ReadAll(c.Request.Body)
			if err != nil {
				c.JSON(500, gin.H{"error": "read body: " + err.Error()})
				return
			}
			if opt.Modify != nil {
				body, err = opt.Modify(body)
				if err != nil {
					c.JSON(500, gin.H{"error": "modify body: " + err.Error()})
					return
				}
			}
		}

		var outReq *http.Request
		var err error
		if body != nil {
			outReq, err = http.NewRequest(method, urlStr, bytes.NewReader(body))
		} else {
			outReq, err = http.NewRequest(method, urlStr, nil)
		}
		if err != nil {
			c.JSON(500, gin.H{"error": err.Error()})
			return
		}

		for k, vs := range c.Request.Header {
			k = http.CanonicalHeaderKey(k)
			if k == "Host" || k == "Connection" {
				continue
			}
			for _, v := range vs {
				outReq.Header.Add(k, v)
			}
		}
		for k, v := range opt.Headers {
			outReq.Header.Set(k, v)
		}
		outReq.Host = outReq.URL.Host

		var resp *http.Response
		if opt.Local {
			client := &http.Client{Timeout: 120 * time.Second}
			resp, err = client.Do(outReq)
		} else {
			resp, err = cloudflare_ech.Do(outReq)
		}
		if err != nil {
			log.Printf("[%s] request failed: %v", clientIP, err)
			c.JSON(502, gin.H{"error": err.Error()})
			return
		}
		defer resp.Body.Close()

		for k, vs := range resp.Header {
			for _, v := range vs {
				c.Header(k, v)
			}
		}
		c.Status(resp.StatusCode)

		if strings.Contains(resp.Header.Get("Content-Type"), "text/event-stream") {
			flusher, _ := c.Writer.(http.Flusher)
			buf := make([]byte, 4096)
			for {
				n, readErr := resp.Body.Read(buf)
				if n > 0 {
					if _, writeErr := c.Writer.Write(buf[:n]); writeErr != nil {
						break
					}
					if flusher != nil {
						flusher.Flush()
					}
				}
				if readErr != nil {
					break
				}
			}
		} else {
			io.Copy(c.Writer, resp.Body)
		}
	}
}
