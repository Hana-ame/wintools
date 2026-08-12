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
