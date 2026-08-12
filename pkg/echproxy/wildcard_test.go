package echproxy

import (
	"strings"
	"testing"
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
