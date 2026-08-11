package main

import (
	"context"
	"crypto/tls"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"regexp"
	"sync"
	"time"
)

type DoHResponse struct {
	Answer []struct {
		Type int    `json:"type"`
		TTL  int    `json:"TTL"`
		Data string `json:"data"`
	} `json:"Answer"`
}

// --- ECH 配置缓存 ---

type echEntry struct {
	config []byte
	expiry time.Time
}

var (
	cacheMu sync.Mutex
	cache   = map[string]*echEntry{}
)

const (
	minTTL = 60        // 最短缓存 60s
	maxTTL = 24 * 3600 // 最长缓存 24h
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

// parseSVCBWire 解析 SVCB/HTTPS 记录的线格式（RFC 9460）并提取 ech SvcParam（key=5）
// wire 是 DNS 应答中的 RDATA 字节（不含 RDLENGTH）
func parseSVCBWire(wire []byte) ([]byte, error) {
	if len(wire) < 2 {
		return nil, fmt.Errorf("wire too short")
	}
	// 跳过 priority（2 bytes）
	off := 2
	// 跳过 target name（DNS 名字，以 length=0 结尾）
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
	// 遍历 SvcParams
	for off+4 <= len(wire) {
		key := int(wire[off])<<8 | int(wire[off+1])
		valLen := int(wire[off+2])<<8 | int(wire[off+3])
		off += 4
		if off+valLen > len(wire) {
			return nil, fmt.Errorf("truncated SvcParam")
		}
		val := wire[off : off+valLen]
		off += valLen
		if key == 5 { // ech
			return val, nil
		}
	}
	return nil, fmt.Errorf("未找到 ECH SvcParam")
}

// fetchECHConfig 从 DoH JSON 应答中提取 ECH 配置列表（带缓存）
func fetchECHConfig(ctx context.Context, domain string) ([]byte, error) {
	// 1. 查内存/磁盘缓存
	if cached := getCachedECH(domain); cached != nil {
		fmt.Printf("[Cache] 命中 %s 的 ECH 缓存\n", domain)
		return cached, nil
	}

	// 2. 发起 DoH 查询
	reqURL := fmt.Sprintf("https://moonchan.xyz/doh?name=%s&type=65", url.QueryEscape(domain))
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
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

	var dohResp DoHResponse
	if err := json.NewDecoder(resp.Body).Decode(&dohResp); err != nil {
		return nil, err
	}

	// 3. 解析应答
	echRegex := regexp.MustCompile(`ech="?([A-Za-z0-9+/=]+)"?`)
	wireRegex := regexp.MustCompile(`\\#\s+(\d+)\s+([0-9a-fA-F\s]+)`)

	for _, ans := range dohResp.Answer {
		if ans.Type != 65 {
			continue
		}
		// 获取 TTL，优先使用最小的 Answer TTL
		ttl := ans.TTL
		if ttl <= 0 {
			ttl = 300
		}

		var cfg []byte
		var err error

		// 尝试 Base64 格式
		if matches := echRegex.FindStringSubmatch(ans.Data); len(matches) > 1 {
			cfg, err = base64.StdEncoding.DecodeString(matches[1])
		} else if matches := wireRegex.FindStringSubmatch(ans.Data); len(matches) > 1 {
			// 尝试线格式
			nonHex := regexp.MustCompile(`[^0-9a-fA-F]`)
			hexData := nonHex.ReplaceAllString(matches[2], "")
			var wire []byte
			wire, err = hex.DecodeString(hexData)
			if err == nil {
				cfg, err = parseSVCBWire(wire)
			}
		}

		if err != nil {
			return nil, fmt.Errorf("ECH 解析失败: %w", err)
		}
		if len(cfg) > 0 {
			setCachedECH(domain, cfg, ttl)
			return cfg, nil
		}
	}
	return nil, fmt.Errorf("未找到 ECH 配置")
}

func main() {
	ctx := context.Background()

	domains := []string{
		"e-hentai.org",
		"video-cf.twimg.com",
	}

	shellDomain := "cloudflare-ech.com"
	fmt.Printf("[Init] 正在获取全局伪装外壳 %s 的 ECH 密钥...\n", shellDomain)
	echConfig, err := fetchECHConfig(ctx, shellDomain)
	if err != nil {
		fmt.Printf("[Init] 致命错误: 无法获取共享 ECH 密钥: %v\n", err)
		return
	}
	fmt.Printf("[Init] 成功获取外壳 ECH 密钥！(长度: %d bytes)\n", len(echConfig))

	dialer := &net.Dialer{
		Timeout:   10 * time.Second,
		KeepAlive: 30 * time.Second,
	}

	transport := &http.Transport{
		DialTLSContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			host, _, err := net.SplitHostPort(addr)
			if err != nil {
				return nil, err
			}

			tcpTarget := shellDomain + ":443"
			fmt.Printf("\n[TCP] 拦截拨号: 目标网站 [%s] -> 实际建立 TCP 连接至 [%s]\n", host, tcpTarget)

			rawConn, err := dialer.DialContext(ctx, network, tcpTarget)
			if err != nil {
				return nil, fmt.Errorf("TCP 连接外壳失败: %w", err)
			}

			tlsCfg := &tls.Config{
				ServerName:                     host,
				EncryptedClientHelloConfigList: echConfig,
				MinVersion:                     tls.VersionTLS13,
				NextProtos:                     []string{"h2", "http/1.1"},
			}

			tlsConn := tls.Client(rawConn, tlsCfg)
			err = tlsConn.HandshakeContext(ctx)
			if err != nil {
				rawConn.Close()
				return nil, fmt.Errorf("TLS 握手失败: %w", err)
			}

			if tlsConn.ConnectionState().ECHAccepted {
				fmt.Printf("[TLS] >>> 成功！服务器通过了 ECH 解密，Inner SNI: %s 路由成功。\n", host)
			} else {
				fmt.Printf("[TLS] >>> 失败或降级！服务器未能使用 ECH。\n")
			}

			return tlsConn, nil
		},
		ForceAttemptHTTP2:   true,
		MaxIdleConns:        100,
		IdleConnTimeout:     90 * time.Second,
		TLSHandshakeTimeout: 10 * time.Second,
	}

	client := &http.Client{
		Transport: transport,
		Timeout:   20 * time.Second,
	}

	for _, domain := range domains {
		urlStr := fmt.Sprintf("https://%s/", domain)
		fmt.Printf("\n--- 开始发起 HTTP 请求: %s ---\n", urlStr)

		req, err := http.NewRequest(http.MethodGet, urlStr, nil)
		if err != nil {
			fmt.Printf("创建请求失败: %v\n", err)
			continue
		}

		req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64)")
		req.Host = domain

		resp, err := client.Do(req)
		if err != nil {
			fmt.Printf("[HTTP] 请求失败: %v\n", err)
			continue
		}

		body, _ := io.ReadAll(io.LimitReader(resp.Body, 200))
		resp.Body.Close()

		fmt.Printf("[HTTP] 状态码: %s\n", resp.Status)
		fmt.Printf("[HTTP] 协议: %s\n", resp.Proto)
		fmt.Printf("[HTTP] 内容截取: %s\n", string(body))
	}
}
