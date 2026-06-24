package cloudflare_ech

import (
	"context"
	"crypto/tls"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"regexp"
	"sync"
	"sync/atomic"
	"time"
)

// Client 是一个基于 cloudflare-ech.com ECH 域前置的 HTTP 客户端。
// 所有请求通过 cloudflare-ech.com 的 IP 发出，真正的目标域名
// 通过 ECH (Encrypted Client Hello) 加密传输给 Cloudflare 路由。
type Client struct {
	inner *http.Client
}

// ---- ECH 配置缓存 ----

type echEntry struct {
	config []byte
	expiry time.Time
}

var (
	cacheMu sync.Mutex
	cache   = map[string]*echEntry{}
)

const (
	minTTL = 60
	maxTTL = 24 * 3600
)

func getCachedECH(domain string) []byte {
	cacheMu.Lock()
	defer cacheMu.Unlock()
	e, ok := cache[domain]
	if !ok || time.Now().After(e.expiry) {
		return nil
	}
	return e.config
}

func setCachedECH(domain string, config []byte, ttl int) {
	if ttl < minTTL {
		ttl = minTTL
	}
	if ttl > maxTTL {
		ttl = maxTTL
	}
	cacheMu.Lock()
	cache[domain] = &echEntry{config: config, expiry: time.Now().Add(time.Duration(ttl) * time.Second)}
	cacheMu.Unlock()
}

// ---- DNS wire format parser ----

type dohResponse struct {
	Answer []struct {
		Type int    `json:"type"`
		TTL  int    `json:"TTL"`
		Data string `json:"data"`
	} `json:"Answer"`
}

func parseSVCBWire(wire []byte) ([]byte, error) {
	if len(wire) < 2 {
		return nil, fmt.Errorf("wire too short")
	}
	off := 2
	for off < len(wire) {
		labelLen := int(wire[off])
		off++
		if labelLen == 0 {
			break
		}
		if off+labelLen > len(wire) {
			return nil, fmt.Errorf("truncated target name")
		}
		off += labelLen
	}
	for off+4 <= len(wire) {
		key := int(wire[off])<<8 | int(wire[off+1])
		valLen := int(wire[off+2])<<8 | int(wire[off+3])
		off += 4
		if off+valLen > len(wire) {
			return nil, fmt.Errorf("truncated SvcParam")
		}
		val := wire[off : off+valLen]
		off += valLen
		if key == 5 {
			return val, nil
		}
	}
	return nil, fmt.Errorf("ECH SvcParam not found")
}

func fetchECHConfig(ctx context.Context, domain string) ([]byte, error) {
	if cached := getCachedECH(domain); cached != nil {
		return cached, nil
	}

	u := fmt.Sprintf("https://moonchan.xyz/doh?name=%s&type=65", url.QueryEscape(domain))
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/dns-json")

	dohClient := &http.Client{Timeout: 5 * time.Second}
	resp, err := dohClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("DoH failed: %d", resp.StatusCode)
	}

	var d dohResponse
	if err := json.NewDecoder(resp.Body).Decode(&d); err != nil {
		return nil, err
	}

	echRe := regexp.MustCompile(`ech="?([A-Za-z0-9+/=]+)"?`)
	wireRe := regexp.MustCompile(`\\#\s+(\d+)\s+([0-9a-fA-F\s]+)`)

	for _, ans := range d.Answer {
		if ans.Type != 65 {
			continue
		}
		ttl := ans.TTL
		if ttl <= 0 {
			ttl = 300
		}

		var cfg []byte
		if m := echRe.FindStringSubmatch(ans.Data); len(m) > 1 {
			cfg, err = base64.StdEncoding.DecodeString(m[1])
		} else if m := wireRe.FindStringSubmatch(ans.Data); len(m) > 1 {
			nonHex := regexp.MustCompile(`[^0-9a-fA-F]`)
			hexData := nonHex.ReplaceAllString(m[2], "")
			var wire []byte
			wire, err = hex.DecodeString(hexData)
			if err == nil {
				cfg, err = parseSVCBWire(wire)
			}
		}
		if err != nil {
			return nil, fmt.Errorf("ECH parse error: %w", err)
		}
		if len(cfg) > 0 {
			setCachedECH(domain, cfg, ttl)
			return cfg, nil
		}
	}
	return nil, fmt.Errorf("no ECH config found for %s", domain)
}

// ---- public API ----

const (
	shellDomain = "cloudflare-ech.com"
	dialTimeout = 10 * time.Second
)

// New 初始化一个 ECH 域前置 HTTP 客户端。
// 首次调用时会通过 DoH 获取 cloudflare-ech.com 的 ECH 密钥并缓存。
func New() (*Client, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	echConfig, err := fetchECHConfig(ctx, shellDomain)
	if err != nil {
		return nil, fmt.Errorf("fetch ECH config: %w", err)
	}

	dialer := &net.Dialer{
		Timeout:   dialTimeout,
		KeepAlive: 30 * time.Second,
	}

	transport := &http.Transport{
		DialTLSContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			host, _, err := net.SplitHostPort(addr)
			if err != nil {
				return nil, err
			}

			rawConn, err := dialer.DialContext(ctx, network, net.JoinHostPort(shellDomain, "443"))
			if err != nil {
				return nil, fmt.Errorf("dial shell: %w", err)
			}

			tlsCfg := &tls.Config{
				ServerName:                     host,
				EncryptedClientHelloConfigList: echConfig,
				MinVersion:                     tls.VersionTLS13,
				NextProtos:                     []string{"h2", "http/1.1"},
			}

			tlsConn := tls.Client(rawConn, tlsCfg)
			if err := tlsConn.HandshakeContext(ctx); err != nil {
				rawConn.Close()
				return nil, fmt.Errorf("TLS handshake: %w", err)
			}
			return tlsConn, nil
		},
		ForceAttemptHTTP2:   true,
		MaxIdleConns:        100,
		IdleConnTimeout:     90 * time.Second,
		TLSHandshakeTimeout: 10 * time.Second,
	}

	return &Client{
		inner: &http.Client{
			Transport: transport,
			Timeout:   30 * time.Second,
		},
	}, nil
}

// Do 执行 HTTP 请求，请求通过 cloudflare-ech.com ECH 域前置发出。
// req.Host 会被设置为真正的目标域名用于 Inner SNI。
func (c *Client) Do(req *http.Request) (*http.Response, error) {
	if req.Host == "" {
		req.Host = req.URL.Host
	}
	return c.inner.Do(req)
}

// DoWithAddr 同 Do，但允许指定目标域名（用于 IP 直连场景）。
// host 会作为 Inner SNI 和 HTTP Host 头。
func (c *Client) DoWithAddr(req *http.Request, host string) (*http.Response, error) {
	req.Host = host
	return c.inner.Do(req)
}

// ---- 便捷函数 ----

var defaultClient atomic.Pointer[Client]

// Do 使用全局默认客户端执行 ECH 请求。
// 首次调用会触发 New() 初始化。
func Do(req *http.Request) (*http.Response, error) {
	c := defaultClient.Load()
	if c == nil {
		var err error
		c, err = New()
		if err != nil {
			return nil, err
		}
		if !defaultClient.CompareAndSwap(nil, c) {
			c = defaultClient.Load()
		}
	}
	return c.Do(req)
}

// InitDefault 显式初始化全局默认客户端（可在程序启动时调用）。
func InitDefault() error {
	c, err := New()
	if err != nil {
		return err
	}
	defaultClient.Store(c)
	go refreshLoop()
	return nil
}

var refreshCtx, refreshCancel = context.WithCancel(context.Background())

func refreshLoop() {
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-refreshCtx.Done():
			return
		case <-ticker.C:
		}

		ctx, cancel := context.WithTimeout(refreshCtx, 10*time.Second)
		echConfig, err := fetchECHConfig(ctx, shellDomain)
		cancel()
		if err != nil {
			continue
		}

		c, err := rebuildClient(echConfig)
		if err != nil {
			continue
		}
		defaultClient.Store(c)
	}
}

func rebuildClient(echConfig []byte) (*Client, error) {
	dialer := &net.Dialer{
		Timeout:   dialTimeout,
		KeepAlive: 30 * time.Second,
	}

	transport := &http.Transport{
		DialTLSContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			host, _, err := net.SplitHostPort(addr)
			if err != nil {
				return nil, err
			}

			rawConn, err := dialer.DialContext(ctx, network, net.JoinHostPort(shellDomain, "443"))
			if err != nil {
				return nil, fmt.Errorf("dial shell: %w", err)
			}

			tlsCfg := &tls.Config{
				ServerName:                     host,
				EncryptedClientHelloConfigList: echConfig,
				MinVersion:                     tls.VersionTLS13,
				NextProtos:                     []string{"h2", "http/1.1"},
			}

			tlsConn := tls.Client(rawConn, tlsCfg)
			if err := tlsConn.HandshakeContext(ctx); err != nil {
				rawConn.Close()
				return nil, fmt.Errorf("TLS handshake: %w", err)
			}
			return tlsConn, nil
		},
		ForceAttemptHTTP2:   true,
		MaxIdleConns:        100,
		IdleConnTimeout:     90 * time.Second,
		TLSHandshakeTimeout: 10 * time.Second,
	}

	return &Client{
		inner: &http.Client{
			Transport: transport,
			Timeout:   30 * time.Second,
		},
	}, nil
}
