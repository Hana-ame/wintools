package proxyheaders

import (
	"net/http"
	"testing"
)

func TestForwardRequestHeaders(t *testing.T) {
	src := http.Header{}
	src.Set("Authorization", "Bearer client-key")
	src.Set("User-Agent", "opencode")
	src.Set("Connection", "keep-alive")
	src.Set("Upgrade", "websocket")
	src.Set("Host", "evil.example.com")
	src.Set("X-Forwarded-For", "1.2.3.4")
	src.Set("Strict-Transport-Security", "max-age=63072000")
	src.Set("Content-Type", "text/plain")
	src.Set("Origin", "https://local")
	src.Add("Cookie", "a=1")

	dst := http.Header{}
	ForwardRequestHeaders(dst, src)

	check := map[string]bool{
		"Authorization":              true, // 显式处理，不透传
		"Connection":                 true, // hop-by-hop
		"Upgrade":                    true,
		"Host":                       true,
		"X-Forwarded-For":            true,
		"Strict-Transport-Security":  true, // TLS 类
		"Content-Type":               true,
		"Origin":                     false,
		"Cookie":                     false,
		"User-Agent":                 false,
	}
	for h, wantGone := range check {
		if got := dst.Get(h); (got != "") == wantGone {
			t.Errorf("header %s: present=%v, want %v (value=%q)", h, got != "", !wantGone, got)
		}
	}
}

func TestForwardResponseHeaders(t *testing.T) {
	src := http.Header{}
	src.Set("X-Request-Id", "req_123")
	src.Set("Retry-After", "60")
	src.Set("RateLimit-Remaining", "42")
	src.Set("Content-Encoding", "gzip")
	src.Set("Alt-Svc", "h2=\":443\"")
	src.Set("Access-Control-Allow-Origin", "*")
	src.Set("ETag", "\"abc\"")

	dst := http.Header{}
	ForwardResponseHeaders(dst, src)

	for _, h := range []string{"X-Request-Id", "Retry-After", "RateLimit-Remaining", "ETag"} {
		if dst.Get(h) == "" {
			t.Errorf("header %s should be forwarded", h)
		}
	}
	for _, h := range []string{"Content-Encoding", "Alt-Svc", "Access-Control-Allow-Origin"} {
		if dst.Get(h) != "" {
			t.Errorf("header %s should be stripped", h)
		}
	}
}

func TestMergeHeaders(t *testing.T) {
	dst := http.Header{}
	dst.Set("A", "1")
	src := http.Header{}
	src.Set("A", "2")
	src.Set("B", "3")
	MergeHeaders(dst, src)
	if dst.Get("A") != "2" || dst.Get("B") != "3" {
		t.Errorf("MergeHeaders: got A=%q B=%q, want A=2 B=3", dst.Get("A"), dst.Get("B"))
	}
}
