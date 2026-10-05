package peerfs

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// TestMountSignalingEndpoints 验证 MountSignaling 挂载了发现/状态/优雅下线端点。
func TestMountSignalingEndpoints(t *testing.T) {
	mux := http.NewServeMux()
	MountSignaling(mux, "testkey", nil)

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
		var body *strings.Reader
		if c.body == "" {
			body = strings.NewReader("")
		} else {
			body = strings.NewReader(c.body)
		}
		rr := httptest.NewRecorder()
		mux.ServeHTTP(rr, httptest.NewRequest(c.method, c.path, body))
		if rr.Code != c.want {
			t.Errorf("%s %s: got %d want %d", c.method, c.path, rr.Code, c.want)
		}
	}
}
