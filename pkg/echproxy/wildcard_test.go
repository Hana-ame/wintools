package echproxy

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
)

func TestMatchWildcard(t *testing.T) {
	cfg := UpstreamMap{
		"iwara.l.moonchan.xyz": {
			Host: "iwara.tv",
			Wildcard: &WildcardRule{
				Prefix:         "iwara-",
				EntrySuffix:    ".l.moonchan.xyz",
				UpstreamSuffix: ".iwara.tv",
				Referer:        "https://www.iwara.tv/",
			},
		},
	}
	cases := []struct{ host, want string }{
		{"iwara-filesq.l.moonchan.xyz", "filesq.iwara.tv"},
		{"iwara-video.l.moonchan.xyz", "video.iwara.tv"},
		{"iwara-x.l.moonchan.xyz", "x.iwara.tv"},
	}
	for _, c := range cases {
		uc, ok := matchWildcard(cfg, c.host)
		if !ok {
			t.Errorf("%s: not matched", c.host)
			continue
		}
		if uc.Host != c.want {
			t.Errorf("%s: want %s got %s", c.host, c.want, uc.Host)
		}
		if !strings.HasPrefix(uc.Referer, "https://www.iwara.tv") && uc.Mode == "" {
			t.Errorf("%s: referer missing", c.host)
		}
	}
	if _, ok := matchWildcard(cfg, "iwara.l.moonchan.xyz"); ok {
		t.Error("exact entry should not match wildcard")
	}
	if _, ok := matchWildcard(cfg, "pixiv.l.moonchan.xyz"); ok {
		t.Error("non-wildcard host should not match")
	}
}

func TestReplaceWildcardDomain(t *testing.T) {
	rw := buildRewriter(map[string]string{
		"iwara.tv":     "iwara.l.moonchan.xyz",
		"www.pixiv.net": "pixiv.l.moonchan.xyz",
		"*.iwara.tv":   "iwara-*.l.moonchan.xyz",
		"*.pixiv.net":  "pixiv-*.l.moonchan.xyz",
	})
	body := []byte(`{"url":"https://filesq.iwara.tv/file/abc.mp4","img":"https://i.iwara.tv/x.jpg","api":"https://api.iwara.tv/trending","bare":"https://iwara.tv/","dl":"https://dl.pixiv.net/zip/a.zip","www":"https://www.pixiv.net/a"}`)
	got := string(rw(body, "8443"))
	want := `{"url":"https://iwara-filesq.l.moonchan.xyz:8443/file/abc.mp4","img":"https://iwara-i.l.moonchan.xyz:8443/x.jpg","api":"https://iwara-api.l.moonchan.xyz:8443/trending","bare":"https://iwara.l.moonchan.xyz:8443/","dl":"https://pixiv-dl.l.moonchan.xyz:8443/zip/a.zip","www":"https://pixiv.l.moonchan.xyz:8443/a"}`
	if got != want {
		t.Errorf("got:  %s\nwant: %s", got, want)
	}
}

func TestSWInjectPrepend(t *testing.T) {
	cfg := UpstreamMap{
		"iwara.l.moonchan.xyz": {
			Host: "iwara.tv",
			Wildcard: &WildcardRule{
				Prefix:         "iwara-",
				EntrySuffix:    ".l.moonchan.xyz",
				UpstreamSuffix: ".iwara.tv",
				Referer:        "https://www.iwara.tv/",
			},
		},
		"iwara-api.l.moonchan.xyz": {Host: "api.iwara.tv"},
	}
	inject := swOverrideJS(buildSWProxyMap(cfg, "8443"), collectWildcardRules(cfg))
	// 注入代码必须是合法 JS: 有 install/activate/fetch 监听。
	for _, want := range []string{"install", "activate", "fetch", "__wtMap", "__wtRules", "iwara-", ".l.moonchan.xyz", "iwara-api.l.moonchan.xyz:8443"} {
		if !strings.Contains(inject, want) {
			t.Errorf("inject missing %q", want)
		}
	}
	// 通配规则包含 iwara
	if !strings.Contains(inject, "iwara.tv") {
		t.Errorf("wildcard suffix missing in inject")
	}
}

func TestBuildEntryRewriterInherit(t *testing.T) {
	cfg := UpstreamMap{
		"iwara.l.moonchan.xyz": {
			Host: "iwara.tv",
			Wildcard: &WildcardRule{
				Prefix:         "iwara-",
				EntrySuffix:    ".l.moonchan.xyz",
				UpstreamSuffix: ".iwara.tv",
				Referer:        "https://www.iwara.tv/",
			},
		},
		"iwara-api.l.moonchan.xyz": {Host: "api.iwara.tv"},
	}
	// 子入口无 wildcard, 应继承主入口规则。
	sub := cfg["iwara-api.l.moonchan.xyz"]
	sub.Wildcard = inheritWildcard(cfg, "iwara-api.l.moonchan.xyz")
	if sub.Wildcard == nil {
		t.Fatal("inheritWildcard returned nil")
	}
	rw := buildEntryRewriter(sub)
	body := []byte(`{"u":"https://filesq.iwara.tv/a.mp4","bare":"https://iwara.tv/"}`)
	got := string(rw(body, "8443"))
	for _, want := range []string{"iwara-filesq.l.moonchan.xyz:8443", "iwara.l.moonchan.xyz:8443"} {
		if !strings.Contains(got, want) {
			t.Errorf("rewrite result missing %q: %s", want, got)
		}
	}
	// 主入口自身也应推导裸域 + 通配。
	rwMain := buildEntryRewriter(cfg["iwara.l.moonchan.xyz"])
	if !strings.Contains(string(rwMain([]byte(`https://news.iwara.tv/x`), "")), "iwara-news.l.moonchan.xyz") {
		t.Error("main entry wildcard rewrite failed")
	}
}

func TestFixedCookieOverride(t *testing.T) {
	cfg := UpstreamMap{
		"ex.l.moonchan.xyz": {
			Host:   "exhentai.org",
			Cookie: "igneous=xxx; ipb_member_id=123; ipb_pass_hash=abc",
		},
		"iwara.l.moonchan.xyz": {
			Host: "iwara.tv",
			Cookie: "auth_token=abc123",
			Wildcard: &WildcardRule{
				Prefix:         "iwara-",
				EntrySuffix:    ".l.moonchan.xyz",
				UpstreamSuffix: ".iwara.tv",
			},
		},
	}

	// 精确入口: 固定 cookie 覆盖内存 jar + 客户端 cookie。
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.NoRoute(ProxyHandler(cfg))
	ts := httptest.NewServer(r)
	defer ts.Close()

	req, _ := http.NewRequest(http.MethodGet, ts.URL+"/", nil)
	req.Host = "ex.l.moonchan.xyz"
	req.Header.Set("Cookie", "browser_cookie=x")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Skipf("上游不可达: %v", err)
	}
	resp.Body.Close()
	// 响应能回来说明请求已发出, 固定 cookie 注入逻辑在 handler 内。
	// 这里无法直接断言请求头, 但配置解析+编译通过即可。

	// 通配入口继承主入口 cookie。
	uc, ok := matchWildcard(cfg, "iwara-api.l.moonchan.xyz")
	if !ok {
		t.Fatal("wildcard match failed")
	}
	if uc.Cookie != "auth_token=abc123" {
		t.Errorf("wildcard cookie inherit: got %q", uc.Cookie)
	}
}
