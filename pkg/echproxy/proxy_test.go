package echproxy

import (
	"net/http"
	"strings"
	"testing"
	"time"
)

// TestMemJar 验证内存 cookie jar：Set-Cookie 存取、过期清理、与客户端 cookie 合并。
func TestMemJar(t *testing.T) {
	cookieMu.Lock()
	cookieJar = map[string][]*http.Cookie{}
	cookieMu.Unlock()
	defer func() {
		cookieMu.Lock()
		cookieJar = map[string][]*http.Cookie{}
		cookieMu.Unlock()
	}()

	resp := &http.Response{Header: http.Header{}}
	resp.Header.Add("Set-Cookie", "sid=abc123; Path=/; HttpOnly")
	resp.Header.Add("Set-Cookie", "gone=1; Path=/; Max-Age=-1")
	resp.Header.Add("Set-Cookie", "old=1; Expires=Mon, 01 Jan 2020 00:00:00 GMT")
	saveCookies("sukebei.nyaa.si", resp)

	req, _ := http.NewRequest(http.MethodGet, "https://sukebei.nyaa.si/", nil)
	req.Header.Set("Cookie", "client=1")
	applyCookies("sukebei.nyaa.si", req)

	got := req.Header.Get("Cookie")
	if !strings.Contains(got, "sid=abc123") {
		t.Fatalf("jar cookie 未带出: %q", got)
	}
	if !strings.Contains(got, "client=1") {
		t.Fatalf("客户端 cookie 丢失: %q", got)
	}
	if strings.Contains(got, "gone=1") || strings.Contains(got, "old=1") {
		t.Fatalf("过期 cookie 未清理: %q", got)
	}

	// 同名覆盖：新 Set-Cookie 覆盖旧值。
	resp2 := &http.Response{Header: http.Header{}}
	resp2.Header.Add("Set-Cookie", "sid=newvalue; Path=/")
	saveCookies("sukebei.nyaa.si", resp2)
	req2, _ := http.NewRequest(http.MethodGet, "https://sukebei.nyaa.si/", nil)
	applyCookies("sukebei.nyaa.si", req2)
	if got := req2.Header.Get("Cookie"); !strings.Contains(got, "sid=newvalue") || strings.Contains(got, "sid=abc123") {
		t.Fatalf("同名 cookie 未覆盖: %q", got)
	}
}

func TestMemJarExpiry(t *testing.T) {
	cookieMu.Lock()
	cookieJar = map[string][]*http.Cookie{}
	cookieMu.Unlock()
	defer func() {
		cookieMu.Lock()
		cookieJar = map[string][]*http.Cookie{}
		cookieMu.Unlock()
	}()

	// 存一个 1 秒后过期的 cookie，等它过期后再 apply 应被清理。
	resp := &http.Response{Header: http.Header{}}
	resp.Header.Add("Set-Cookie", "tmp=1; Path=/; Max-Age=1")
	saveCookies("example.com", resp)

	time.Sleep(1500 * time.Millisecond)

	req, _ := http.NewRequest(http.MethodGet, "https://example.com/", nil)
	applyCookies("example.com", req)
	if got := req.Header.Get("Cookie"); strings.Contains(got, "tmp=") {
		t.Fatalf("过期 cookie 仍被带出: %q", got)
	}
}
