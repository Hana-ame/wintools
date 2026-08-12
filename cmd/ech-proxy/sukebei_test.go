package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/Hana-ame/wintools/pkg/echproxy"
)

type dohAnswer struct {
	Answer []struct {
		Type int    `json:"type"`
		Data string `json:"data"`
	} `json:"Answer"`
}

// dohResolveA 通过 moonchan.xyz DoH 解析域名的 A 记录（绕过被污染的本地 DNS）。
func dohResolveA(ctx context.Context, host string) (string, error) {
	u := fmt.Sprintf("https://moonchan.xyz/doh?name=%s&type=1", host)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("Accept", "application/dns-json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("DoH status %d", resp.StatusCode)
	}
	var d dohAnswer
	if err := json.NewDecoder(resp.Body).Decode(&d); err != nil {
		return "", err
	}
	for _, ans := range d.Answer {
		if ans.Type == 1 && net.ParseIP(ans.Data) != nil {
			return ans.Data, nil
		}
	}
	return "", fmt.Errorf("no A record for %s", host)
}

// TestSukebeiSNIFrontingNoProxy 验证不经过本地 proxy、不用 ECH 的 SNI 伪装直连：
//
//	1. DoH 解析 sukebei.nyaa.si 的真实 IP（本地 DNS 被污染）
//	2. TCP 直连真实 IP，TLS 携带不敏感 SNI（绕过 GFW 的 SNI 阻断）
//	3. HTTP Host 头指向 sukebei.nyaa.si，源站 nginx 按 Host 路由
//
// sukebei.nyaa.si 不在 Cloudflare 后面（FranTech VPS，自签证书），
// ECH 域前置无效，但 SNI 伪装实测可用。
func TestSukebeiSNIFrontingNoProxy(t *testing.T) {
	for _, k := range []string{"HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "NO_PROXY",
		"http_proxy", "https_proxy", "all_proxy", "no_proxy"} {
		t.Setenv(k, "")
	}

	const host = "sukebei.nyaa.si"

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	ip, err := dohResolveA(ctx, host)
	if err != nil {
		t.Fatalf("DoH 解析 %s 失败: %v", host, err)
	}
	t.Logf("真实 IP: %s", ip)

	newClient := func() *http.Client {
		tr := &http.Transport{
			DialTLSContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
				d := &net.Dialer{Timeout: 5 * time.Second}
				conn, err := d.DialContext(ctx, "tcp", net.JoinHostPort(ip, "443"))
				if err != nil {
					return nil, err
				}
				tc := tls.Client(conn, &tls.Config{
					ServerName:         "cloudflare-ech.com",
					InsecureSkipVerify: true,
					NextProtos:         []string{"http/1.1"},
				})
				if err := tc.HandshakeContext(ctx); err != nil {
					conn.Close()
					return nil, err
				}
				return tc, nil
			},
		}
		return &http.Client{Transport: tr, Timeout: 8 * time.Second}
	}

	// GFW 对 TLS 的 RST 是概率性的，失败重试。
	var lastErr error
	for attempt := 1; attempt <= 3; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://"+ip+"/", nil)
		if err != nil {
			t.Fatalf("创建请求失败: %v", err)
		}
		req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64)")
		req.Host = host

		resp, err := newClient().Do(req)
		if err != nil {
			lastErr = err
			t.Logf("attempt %d 失败: %v", attempt, err)
			continue
		}
		io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
		resp.Body.Close()
		if resp.StatusCode >= 400 {
			t.Fatalf("第 %d 次直连返回 %d，预期 2xx", attempt, resp.StatusCode)
		}
		t.Logf("attempt %d 成功: %s", attempt, resp.Status)
		return
	}
	t.Fatalf("SNI 伪装直连 %s 失败（3 次重试）: %v", host, lastErr)
}

// TestProxyHandlerSNIMode 验证本地代理按 Host 路由 sukebei.l.moonchan.xyz，
// 走 SNI 伪装模式并能取回 nyaa 页面。
func TestProxyHandlerSNIMode(t *testing.T) {
	for _, k := range []string{"HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "NO_PROXY",
		"http_proxy", "https_proxy", "all_proxy", "no_proxy"} {
		t.Setenv(k, "")
	}

	var cfg echproxy.Config
	cfg.Upstreams = echproxy.UpstreamMap{
		"sukebei.l.moonchan.xyz": {Host: "sukebei.nyaa.si", Mode: "sni"},
	}
	uc, ok := cfg.Upstreams["sukebei.l.moonchan.xyz"]
	if !ok {
		t.Fatal("配置缺少 sukebei.l.moonchan.xyz")
	}
	if uc.Mode != "sni" {
		t.Errorf("sukebei mode = %q, want \"sni\"", uc.Mode)
	}

	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.NoRoute(echproxy.ProxyHandler(cfg.Upstreams))
	ts := httptest.NewServer(r)
	defer ts.Close()

	req, err := http.NewRequest(http.MethodGet, ts.URL+"/", nil)
	if err != nil {
		t.Fatalf("创建请求失败: %v", err)
	}
	req.Host = "sukebei.l.moonchan.xyz"

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("代理请求失败: %v", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 64<<10))

	if resp.StatusCode >= 400 {
		t.Fatalf("代理返回 %d，预期 2xx", resp.StatusCode)
	}
	if !strings.Contains(string(body), "Sukebei") {
		t.Fatalf("返回内容不是 nyaa 页面: %.300s", body)
	}
}
