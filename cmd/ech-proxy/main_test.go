package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestLoadUpstreamConfig(t *testing.T) {
	sample := UpstreamMap{
		"twimg.l.moonchan.xyz": {
			Host:    "video-cf.twimg.com",
			Referer: "https://x.com",
		},
		"ex.l.moonchan.xyz": {
			Host: "exhentai.org",
		},
	}

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(sample)
	}))
	defer ts.Close()

	cfg, err := loadUpstreamConfig(ts.URL)
	if err != nil {
		t.Fatalf("loadUpstreamConfig failed: %v", err)
	}

	if len(cfg) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(cfg))
	}

	twimg, ok := cfg["twimg.l.moonchan.xyz"]
	if !ok {
		t.Fatal("missing twimg.l.moonchan.xyz")
	}
	if twimg.Host != "video-cf.twimg.com" {
		t.Errorf("twimg host = %q, want %q", twimg.Host, "video-cf.twimg.com")
	}
	if twimg.Referer != "https://x.com" {
		t.Errorf("twimg referer = %q, want %q", twimg.Referer, "https://x.com")
	}

	ex, ok := cfg["ex.l.moonchan.xyz"]
	if !ok {
		t.Fatal("missing ex.l.moonchan.xyz")
	}
	if ex.Host != "exhentai.org" {
		t.Errorf("ex host = %q, want %q", ex.Host, "exhentai.org")
	}
	if ex.Referer != "" {
		t.Errorf("ex referer = %q, want empty", ex.Referer)
	}
}

func TestLoadUpstreamConfigHTTPError(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer ts.Close()

	_, err := loadUpstreamConfig(ts.URL)
	if err == nil {
		t.Fatal("expected error for 404, got nil")
	}
}

func TestUpstreamMapRoundtrip(t *testing.T) {
	raw := `{
		"a.l.moonchan.xyz": {"host": "example.com", "referer": "https://x.com"},
		"b.l.moonchan.xyz": {"host": "other.com"}
	}`
	var cfg UpstreamMap
	if err := json.Unmarshal([]byte(raw), &cfg); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}
	if len(cfg) != 2 {
		t.Fatalf("expected 2, got %d", len(cfg))
	}
	if cfg["a.l.moonchan.xyz"].Referer != "https://x.com" {
		t.Errorf("referer not loaded")
	}
	if cfg["b.l.moonchan.xyz"].Referer != "" {
		t.Errorf("expected empty referer")
	}
}
