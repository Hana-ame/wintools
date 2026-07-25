package apifwd

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
)

type ResolvedEndpoint struct {
	URL     string
	SNIHost string
}

func ResolveIP(host, path string, af int) (*ResolvedEndpoint, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	addrs, err := net.DefaultResolver.LookupIPAddr(ctx, host)
	if err != nil {
		return nil, err
	}
	for _, a := range addrs {
		if af == 4 && a.IP.To4() != nil {
			return &ResolvedEndpoint{URL: "https://" + a.IP.String() + path, SNIHost: host}, nil
		}
		if af == 6 && a.IP.To4() == nil && a.IP.To16() != nil {
			return &ResolvedEndpoint{URL: "https://[" + a.IP.String() + "]" + path, SNIHost: host}, nil
		}
	}
	return nil, fmt.Errorf("no IPv%d for %s", af, host)
}

func transportForEndpoint(ep *ResolvedEndpoint) *http.Transport {
	return &http.Transport{
		TLSClientConfig:   &tls.Config{ServerName: ep.SNIHost},
		ForceAttemptHTTP2: true,
		MaxIdleConns:      100,
		IdleConnTimeout:   90 * time.Second,
	}
}

type zenState struct {
	v4       *ResolvedEndpoint
	v6       *ResolvedEndpoint
	v4t      *http.Transport
	v6t      *http.Transport
	apiKey   string
	modify   func([]byte) ([]byte, error)
	cooldown map[string]time.Time
	mu       sync.Mutex
}

func Zen(v4, v6 *ResolvedEndpoint, apiKey string, modify func([]byte) ([]byte, error)) gin.HandlerFunc {
	z := &zenState{
		v4:       v4,
		v6:       v6,
		v4t:      transportForEndpoint(v4),
		v6t:      transportForEndpoint(v6),
		apiKey:   apiKey,
		modify:   modify,
		cooldown: make(map[string]time.Time),
	}
	return z.handler
}

func (z *zenState) isCool(fam string) bool {
	z.mu.Lock()
	defer z.mu.Unlock()
	until, ok := z.cooldown[fam]
	return ok && time.Now().Before(until)
}

func (z *zenState) cool(fam string, until time.Time) {
	z.mu.Lock()
	z.cooldown[fam] = until
	z.mu.Unlock()
}

func (z *zenState) try(fam string, c *gin.Context) (*http.Response, bool) {
	var ep *ResolvedEndpoint
	var tr *http.Transport
	if fam == "v4" {
		ep, tr = z.v4, z.v4t
	} else {
		ep, tr = z.v6, z.v6t
	}
	if ep == nil || tr == nil {
		return nil, false
	}

	method := c.Request.Method
	path := c.Request.URL.Path
	urlStr := ep.URL + path
	if c.Request.URL.RawQuery != "" {
		urlStr += "?" + c.Request.URL.RawQuery
	}

	body, err := io.ReadAll(c.Request.Body)
	if err != nil {
		return nil, false
	}
	if len(body) > 0 && z.modify != nil {
		body, err = z.modify(body)
		if err != nil {
			return nil, false
		}
	}

	var outReq *http.Request
	if len(body) > 0 {
		outReq, err = http.NewRequest(method, urlStr, bytes.NewReader(body))
	} else {
		outReq, err = http.NewRequest(method, urlStr, nil)
	}
	if err != nil {
		return nil, false
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
	if outReq.Header.Get("Authorization") == "" {
		outReq.Header.Set("Authorization", "Bearer "+z.apiKey)
	}
	outReq.Host = ep.SNIHost

	client := &http.Client{Transport: tr, Timeout: 120 * time.Second}
	resp, err := client.Do(outReq)
	if err != nil {
		return nil, false
	}
	return resp, true
}

func (z *zenState) handler(c *gin.Context) {
	clientIP := c.ClientIP()
	path := c.Request.URL.Path
	log.Printf("[%s] Zen dual %s", clientIP, path)

	order := []string{"v4", "v6"}
	resp, ok := z.try(order[0], c)
	if ok && resp != nil {
		if !strings.Contains(resp.Header.Get("Content-Type"), "text/event-stream") {
			body, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			var result map[string]any
			if json.Unmarshal(body, &result) == nil {
				if errObj, _ := result["error"].(map[string]any); errObj != nil {
					if t, _ := errObj["type"].(string); t == "FreeUsageLimitError" {
						z.cool(order[0], nextUTCMidnight())
						log.Printf("[%s] %s rate limited, cooling until midnight", clientIP, order[0])
						resp2, ok2 := z.try(order[1], c)
						if ok2 && resp2 != nil {
							z.write(c, resp2)
							return
						}
						c.JSON(429, result)
						return
					}
				}
			}
			resp.Body = io.NopCloser(bytes.NewReader(body))
		}
		z.write(c, resp)
		return
	}
	if !ok || resp == nil && order[1] != "" {
		resp2, ok2 := z.try(order[1], c)
		if ok2 && resp2 != nil {
			z.write(c, resp2)
			return
		}
	}
	c.JSON(503, gin.H{"error": "All upstream IPs exhausted"})
}

func (z *zenState) write(c *gin.Context, resp *http.Response) {
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
			n, err := resp.Body.Read(buf)
			if n > 0 {
				if _, werr := c.Writer.Write(buf[:n]); werr != nil {
					break
				}
				if flusher != nil {
					flusher.Flush()
				}
			}
			if err != nil {
				break
			}
		}
	} else {
		io.Copy(c.Writer, resp.Body)
	}
}

func nextUTCMidnight() time.Time {
	now := time.Now().UTC()
	return time.Date(now.Year(), now.Month(), now.Day()+1, 0, 0, 0, 0, time.UTC)
}
