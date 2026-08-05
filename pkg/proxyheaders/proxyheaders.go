// Package proxyheaders 提供反向代理 header 透传的共用规则，
// 供 cmd/zen-proxy、cmd/zen-multi、cmd/local-proxy* 保持一致。
//
// 转发原则：
//   - 剔除 hop-by-hop 逐跳头（RFC 2616 §13.5.1）；
//   - 剔除 TLS/SSL 及连接安全相关头（HSTS、HPKP、Expect-CT、Alt-Svc 等），
//     这些头语义绑定于原连接，透传会误导客户端或泄露代理层信息；
//   - 剔除会被代理自身重写或与转发语义冲突的头
//     （Content-Length/Type、Accept-Encoding、Host、Range、X-Forwarded-* 等）；
//   - CORS（Access-Control-*）一律不转发，由各端自定。
package proxyheaders

import (
	"net/http"
	"strings"
)

// forbidden 反向代理不应转发的 header（小写名）。
var forbidden = map[string]bool{
	// hop-by-hop（RFC 2616 §13.5.1）
	"connection":          true,
	"keep-alive":          true,
	"proxy-authenticate":  true,
	"proxy-authorization": true,
	"proxy-connection":    true,
	"te":                  true,
	"trailer":             true,
	"transfer-encoding":   true,
	"upgrade":             true,
	// TLS/SSL 与连接安全
	"strict-transport-security": true,
	"public-key-pins":           true,
	"expect-ct":                 true,
	"alt-svc":                   true,
	"ssl-cert":                  true,
	"ssl-client-cert":           true,
	"ssl-client-verify":         true,
	"x-ssl":                     true,
	// 代理自身管理 / 与转发语义冲突
	"content-length":    true,
	"content-type":      true,
	"content-encoding":  true,
	"accept-encoding":   true,
	"host":              true,
	"expect":            true,
	"accept-ranges":     true,
	"content-range":     true,
	"if-range":          true,
	"range":             true,
	"authorization":     true,
	"forwarded":         true,
	"x-forwarded-for":   true,
	"x-forwarded-host":  true,
	"x-forwarded-proto": true,
	"x-forwarded-port":  true,
	"x-real-ip":         true,
}

// forbiddenHeader 判断 header 是否应剔除。
func forbiddenHeader(name string) bool {
	if forbidden[name] {
		return true
	}
	return strings.HasPrefix(name, "proxy-")
}

// copySafe 把 src 中可安全透传的 header 拷贝到 dst。
func copySafe(dst, src http.Header) {
	for k, vv := range src {
		l := strings.ToLower(k)
		if forbiddenHeader(l) || strings.HasPrefix(l, "access-control-") {
			continue
		}
		for _, v := range vv {
			dst.Add(k, v)
		}
	}
}

// ForwardRequestHeaders 把客户端请求的安全 header 拷贝到上游请求。
// dst 为 req.Header，src 为客户端的 r.Header。
// Authorization 不在此转发（各代理按需显式处理）。
func ForwardRequestHeaders(dst, src http.Header) {
	copySafe(dst, src)
}

// ForwardResponseHeaders 把上游响应的安全 header 拷贝到客户端响应。
// dst 为 w.Header()，src 为上游 resp.Header。
func ForwardResponseHeaders(dst, src http.Header) {
	copySafe(dst, src)
}

// MergeHeaders 把 src 整体合并进 dst（同 key 覆盖），
// 用于错误回写前应用先前捕获的上游 header。
func MergeHeaders(dst, src http.Header) {
	for k, vv := range src {
		dst[k] = vv
	}
}
