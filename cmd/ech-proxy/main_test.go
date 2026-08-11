package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Hana-ame/wintools/pkg/echproxy"
)

func TestLoadUpstreamConfig(t *testing.T) {
	sample := echproxy.Config{
		CertPath: "https://example.com/fullchain.cer",
		KeyPath:  "https://example.com/privkey.pem",
		Upstreams: echproxy.UpstreamMap{
			"twimg.l.moonchan.xyz": {
				Host:    "video-cf.twimg.com",
				Referer: "https://x.com",
			},
			"ex.l.moonchan.xyz": {
				Host: "exhentai.org",
			},
		},
	}

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(sample)
	}))
	defer ts.Close()

	cfg, err := echproxy.LoadConfig(ts.URL)
	if err != nil {
		t.Fatalf("LoadConfig failed: %v", err)
	}

	if len(cfg.Upstreams) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(cfg.Upstreams))
	}

	twimg, ok := cfg.Upstreams["twimg.l.moonchan.xyz"]
	if !ok {
		t.Fatal("missing twimg.l.moonchan.xyz")
	}
	if twimg.Host != "video-cf.twimg.com" {
		t.Errorf("twimg host = %q, want %q", twimg.Host, "video-cf.twimg.com")
	}
	if twimg.Referer != "https://x.com" {
		t.Errorf("twimg referer = %q, want %q", twimg.Referer, "https://x.com")
	}

	ex, ok := cfg.Upstreams["ex.l.moonchan.xyz"]
	if !ok {
		t.Fatal("missing ex.l.moonchan.xyz")
	}
	if ex.Host != "exhentai.org" {
		t.Errorf("ex host = %q, want %q", ex.Host, "exhentai.org")
	}
	if ex.Referer != "" {
		t.Errorf("ex referer = %q, want empty", ex.Referer)
	}

	if cfg.CertPath != "https://example.com/fullchain.cer" {
		t.Errorf("cert_path = %q, want URL", cfg.CertPath)
	}
	if cfg.KeyPath != "https://example.com/privkey.pem" {
		t.Errorf("key_path = %q, want URL", cfg.KeyPath)
	}
}

func TestLoadUpstreamConfigHTTPError(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer ts.Close()

	_, err := echproxy.LoadConfig(ts.URL)
	if err == nil {
		t.Fatal("expected error for 404, got nil")
	}
}

func TestUpstreamMapRoundtrip(t *testing.T) {
	raw := `{
		"cert_path": "https://example.com/fullchain.cer",
		"key_path": "https://example.com/privkey.pem",
		"upstreams": {
			"a.l.moonchan.xyz": {"host": "example.com", "referer": "https://x.com"},
			"b.l.moonchan.xyz": {"host": "other.com"}
		}
	}`
	var cfg echproxy.Config
	if err := json.Unmarshal([]byte(raw), &cfg); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}
	if len(cfg.Upstreams) != 2 {
		t.Fatalf("expected 2, got %d", len(cfg.Upstreams))
	}
	if cfg.Upstreams["a.l.moonchan.xyz"].Referer != "https://x.com" {
		t.Errorf("referer not loaded")
	}
	if cfg.Upstreams["b.l.moonchan.xyz"].Referer != "" {
		t.Errorf("expected empty referer")
	}
	if cfg.CertPath == "" || cfg.KeyPath == "" {
		t.Errorf("cert_path/key_path not loaded")
	}
}
