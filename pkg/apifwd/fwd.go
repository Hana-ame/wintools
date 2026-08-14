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
	"github.com/Hana-ame/wintools/pkg/netdial"
)

type Option struct {
	Dest    string
	Local   bool
	Headers map[string]string
	Modify  func([]byte) ([]byte, error)
}

// maxBodySize POST/PUT/PATCH body 上限 (64MB): 反代把 body 整体缓存到
// 内存做 Modify, 不限长会被超大上传打爆 (OOM)。
const maxBodySize = 64 << 20

// writeTimeout 单次写入截止时间: 慢客户端 (不消费响应) 超时断开,
// 防止连接/goroutine 无限堆积。SSE 每 chunk 重设不受影响。
const writeTimeout = 60 * time.Second

func CORSMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Header("Access-Control-Allow-Origin", "*")
		c.Header("Access-Control-Allow-Methods", "GET, POST, PUT, PATCH, DELETE, OPTIONS")
		// 回显前端预检声明的请求头, 无论带什么自定义头都放行。
		if h := c.GetHeader("Access-Control-Request-Headers"); h != "" {
			c.Header("Access-Control-Allow-Headers", h)
		} else {
			c.Header("Access-Control-Allow-Headers", "Content-Type, Authorization, Accept, Origin, X-Requested-With, Cache-Control, User-Agent")
		}
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
			// body 限长读入: 反代无脑缓存整个 body 到内存, 超大上传会 OOM。
			body, err = io.ReadAll(io.LimitReader(c.Request.Body, maxBodySize+1))
			if err != nil {
				c.JSON(500, gin.H{"error": "read body: " + err.Error()})
				return
			}
			if len(body) > maxBodySize {
				c.JSON(http.StatusRequestEntityTooLarge, gin.H{"error": "body too large"})
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
			client := netdial.Client(120 * time.Second)
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

		// 统一循环转发 (SSE 与非 SSE 同路): 每次写前设 deadline 防慢客户端,
		// 边读边写边 flush, 避免整体缓冲破坏 SSE 实时性。
		rc := http.NewResponseController(c.Writer)
		buf := make([]byte, 4096)
		for {
			n, readErr := resp.Body.Read(buf)
			if n > 0 {
				rc.SetWriteDeadline(time.Now().Add(writeTimeout))
				if _, writeErr := c.Writer.Write(buf[:n]); writeErr != nil {
					break
				}
				if f, ok := c.Writer.(http.Flusher); ok {
					f.Flush()
				}
			}
			if readErr != nil {
				break
			}
		}
	}
}
