package peerfs

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// TestMountSignalingEndpoints 验证 MountSignaling 挂载了发现/状态/优雅下线端点。
//
// 注意（2026-10-05）：/status 与全量名册现要求 ops token（peerdrive 安全 Round 2，
// commit 68a4bbf/c669cf5）。策略是「空 token 一律不放行」——正是这条堵掉了
// 任意网页读取名册的洞；配了白名单时 token 还必须在名单内。因此本用例必须
// 带 Authorization 头，且顺带断言不带 token 时确实被拒（回归护栏）。
func TestMountSignalingEndpoints(t *testing.T) {
	mux := http.NewServeMux()
	MountSignaling(mux, "testkey", nil)

	const opsTok = "ops-token-for-test"
	do := func(method, path, body, token string) int {
		rd := strings.NewReader(body)
		req := httptest.NewRequest(method, path, rd)
		if token != "" {
			req.Header.Set("Authorization", "Bearer "+token)
		}
		rr := httptest.NewRecorder()
		mux.ServeHTTP(rr, req)
		return rr.Code
	}

	cases := []struct {
		method string
		path   string
		body   string
		want   int
	}{
		{"GET", "/status", "", http.StatusOK},
		{"GET", "/discover/nodes", "", http.StatusOK},
		{"POST", "/discover/announce", `{"peerId":"node-1","collections":["media"]}`, http.StatusOK},
		{"POST", "/discover/leave", `{"peerId":"node-1"}`, http.StatusOK},
	}
	for _, c := range cases {
		if got := do(c.method, c.path, c.body, opsTok); got != c.want {
			t.Errorf("%s %s: got %d want %d", c.method, c.path, got, c.want)
		}
	}

	// 回归护栏：不带 token 读 /status 必须被拒（401），否则「任意网页读名册」的洞回归。
	if got := do("GET", "/status", "", ""); got != http.StatusUnauthorized {
		t.Errorf("GET /status without token: got %d want %d", got, http.StatusUnauthorized)
	}
}
