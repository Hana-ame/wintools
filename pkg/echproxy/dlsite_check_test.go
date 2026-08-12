package echproxy

import (
	"strings"
	"testing"
)

// 验证 dlsite 通配入口: 非 CF 子域自动 SNI, 重写规则来自主入口 wildcard。
func TestDlsiteWildcard(t *testing.T) {
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
	uc, ok := matchWildcard(cfg, "dlsite-home.l.moonchan.xyz")
	if !ok {
		t.Fatal("dlsite-home wildcard not matched")
	}
	if uc.Host != "home.dlsite.com" {
		t.Errorf("host = %q, want home.dlsite.com", uc.Host)
	}
	if uc.Mode != "sni" {
		t.Errorf("mode = %q, want sni (AWS 非 CF)", uc.Mode)
	}
	rw := buildEntryRewriter(uc)
	body := rw([]byte(`{"img":"https://home.dlsite.com/a.jpg","www":"https://www.dlsite.com/x","bare":"https://dlsite.com/"}`), "8443")
	for _, want := range []string{"dlsite-home.l.moonchan.xyz:8443", "dlsite.l.moonchan.xyz:8443"} {
		if !strings.Contains(string(body), want) {
			t.Errorf("rewrite missing %q: %s", want, body)
		}
	}
}
