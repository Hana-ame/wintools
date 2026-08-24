package main

import (
	"encoding/json"
	"os"
	"testing"

	"github.com/Hana-ame/wintools/pkg/echproxy"
)

// TestRepoUpstreamJSONValid 解析仓库内真实 upstream.json:
// 线上 ech-proxy 启动时从 GitHub main 拉取该文件, JSON 非法或条目字段
// 打错会直接 Fatalf, 这里在 CI 里提前拦住。
func TestRepoUpstreamJSONValid(t *testing.T) {
	raw, err := os.ReadFile("../../certs/l.moonchan.xyz/upstream.json")
	if err != nil {
		t.Fatalf("read upstream.json: %v", err)
	}
	var cfg echproxy.Config
	if err := json.Unmarshal(raw, &cfg); err != nil {
		t.Fatalf("upstream.json 非法: %v", err)
	}
	if cfg.CertPath == "" || cfg.KeyPath == "" {
		t.Fatal("cert_path/key_path 不能为空")
	}
	if len(cfg.Upstreams) < 5 {
		t.Fatalf("上游条目过少: %d", len(cfg.Upstreams))
	}

	// sensenova 入口: SenseNova 生图 API 的 CORS 转发端点。
	sn, ok := cfg.Upstreams["sensenova.l.moonchan.xyz"]
	if !ok {
		t.Fatal("missing sensenova.l.moonchan.xyz")
	}
	if sn.Host != "token.sensenova.cn" {
		t.Errorf("sensenova host = %q, want %q", sn.Host, "token.sensenova.cn")
	}
	if sn.Mode != "direct" {
		t.Errorf("sensenova mode = %q, want %q (非 Cloudflare 段, ECH 不适用)", sn.Mode, "direct")
	}
}
