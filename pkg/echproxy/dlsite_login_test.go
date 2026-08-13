package echproxy

import (
	"strings"
	"testing"
)

// 回归: dlsite-login.l.moonchan.xyz 必须由通配 override 到 login.dlsite.com
// (页面登录表单提交到 login.dlsite.com, 重写为 dlsite-login 入口后,
// 代理通配匹配回 login.dlsite.com, 形成闭环)。
func TestDlsiteLoginWildcard(t *testing.T) {
	cfg := UpstreamMap{
		"dlsite.l.moonchan.xyz": {
			Host: "www.dlsite.com",
			Wildcard: &WildcardRule{
				Prefix:         "dlsite-",
				EntrySuffix:    ".l.moonchan.xyz",
				UpstreamSuffix: ".dlsite.com",
				Referer:        "https://www.dlsite.com/",
			},
			Rewrites: map[string]string{"www.dlsite.com": "dlsite.l.moonchan.xyz"},
		},
	}
	// 1. 请求侧: dlsite-login 入口 -> login.dlsite.com
	uc, ok := matchWildcard(cfg, "dlsite-login.l.moonchan.xyz")
	if !ok {
		t.Fatal("dlsite-login wildcard not matched")
	}
	if uc.Host != "login.dlsite.com" {
		t.Errorf("host = %q, want login.dlsite.com", uc.Host)
	}
	// 2. 响应侧: 页面里 login.dlsite.com URL -> dlsite-login.l.moonchan.xyz
	rw := buildEntryRewriter(cfg["dlsite.l.moonchan.xyz"], nil)
	body := rw([]byte(`<form action="https://login.dlsite.com/home/login">`), "8443")
	if !strings.Contains(string(body), "dlsite-login.l.moonchan.xyz:8443/home/login") {
		t.Errorf("login rewrite broken: %s", body)
	}
}
