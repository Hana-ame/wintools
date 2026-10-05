package signalserver

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// testServer starts an in-memory signaling server and returns a connection factory.
func testServer(t *testing.T) (*Server, *httptest.Server) {
	t.Helper()
	srv := NewServer("testkey")
	hs := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/peerjs"):
			srv.HandleWS(w, r)
		case strings.HasSuffix(r.URL.Path, "/id"):
			srv.HandleID(w, r)
		case strings.HasSuffix(r.URL.Path, "/announce"):
			srv.HandleAnnounce(w, r)
		case strings.HasSuffix(r.URL.Path, "/nodes"):
			srv.HandleNodes(w, r)
		case strings.HasSuffix(r.URL.Path, "/status"):
			srv.HandleStatus(w, r)
		case r.URL.Path == "/":
			srv.HandleDashboard(w, r)
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(hs.Close)
	return srv, hs
}

// opsGet issues a GET against an ops-facing endpoint (/status, /nodes?type=…) carrying
// an ops token.
//
// Why the token (2026-10-04): those endpoints stopped being world-readable. The public
// panel's own queries (with ?coll=, and the plain no-coll "who is online" form) stay public —
// gating those breaks every panel user — but /status and the ?type= inventory query now
// require a token. Presenting one in tests is harmless either way, which keeps these
// helpers usable for both shapes; the rejection behavior is pinned by
// TestOpsEndpointsRequireToken below.
func opsGet(hs *httptest.Server, path string) (*http.Response, error) {
	req, err := http.NewRequest(http.MethodGet, hs.URL+path, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer ops-token")
	return http.DefaultClient.Do(req)
}

// publicGet issues a GET **without** a token, i.e. what a random web page can do.
func publicGet(hs *httptest.Server, path string) (*http.Response, error) {
	return http.Get(hs.URL + path)
}

// dialWS connects to the signaling server with a given id.
func dialWS(t *testing.T, hs *httptest.Server, id, token string) *websocket.Conn {
	t.Helper()
	url := "ws" + strings.TrimPrefix(hs.URL, "http") + "/peerjs?key=testkey&id=" + id + "&token=" + token
	conn, _, err := websocket.DefaultDialer.Dial(url, nil)
	require.NoError(t, err)
	return conn
}

// readMsg reads a single signaling message.
func readMsg(t *testing.T, conn *websocket.Conn) Message {
	t.Helper()
	conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	var m Message
	require.NoError(t, conn.ReadJSON(&m))
	return m
}

// TestSignal_OpenAndForward Register -> OPEN; messages forwarded by dst (src overridden by server).
//
// Discovery background: functional test -- register OPEN + message forwarding (server-side src
// override is a protocol requirement)
func TestSignal_OpenAndForward(t *testing.T) {
	_, hs := testServer(t)
	a := dialWS(t, hs, "node-a", "tok-a")
	defer a.Close()
	require.Equal(t, Message{Type: "OPEN"}, readMsg(t, a))

	b := dialWS(t, hs, "node-b", "tok-b")
	defer b.Close()
	require.Equal(t, Message{Type: "OPEN"}, readMsg(t, b))

	// A -> B forwarding
	payload := json.RawMessage(`{"type":"OFFER","connectionId":"c1"}`)
	require.NoError(t, a.WriteJSON(Message{Type: "OFFER", Dst: "node-b", Payload: payload}))
	m := readMsg(t, b)
	assert.Equal(t, "OFFER", m.Type)
	assert.Equal(t, "node-a", m.Src, "server must override src")
	assert.Equal(t, "node-b", m.Dst)
	assert.Equal(t, payload, m.Payload)
}

// TestSignal_OfflineQueue Target offline -> queue, resend after coming online (OFFER not lost).
// Discovery background: peerjs-server behavior alignment -- OFFER arriving before target comes online must not be lost.
func TestSignal_OfflineQueue(t *testing.T) {
	_, hs := testServer(t)
	a := dialWS(t, hs, "node-a", "tok-a")
	defer a.Close()
	readMsg(t, a)

	// B not online, A sends OFFER -> queued
	require.NoError(t, a.WriteJSON(Message{Type: "OFFER", Dst: "node-b", Payload: json.RawMessage(`{"x":1}`)}))

	b := dialWS(t, hs, "node-b", "tok-b")
	defer b.Close()
	readMsg(t, b) // OPEN
	m := readMsg(t, b)
	assert.Equal(t, "OFFER", m.Type, "after coming online, the offline queue should be resent")
	assert.Equal(t, "node-a", m.Src)
}

// TestSignal_LeaveBroadcast Disconnect -> other nodes receive LEAVE.
//
// Discovery background: functional test -- LEAVE broadcast lets peers detect disconnection
// (peerjs-server behavior alignment)
func TestSignal_LeaveBroadcast(t *testing.T) {
	_, hs := testServer(t)
	a := dialWS(t, hs, "node-a", "tok-a")
	b := dialWS(t, hs, "node-b", "tok-b")
	defer b.Close()
	readMsg(t, a)
	readMsg(t, b)

	a.Close()
	m := readMsg(t, b)
	assert.Equal(t, "LEAVE", m.Type)
	assert.Equal(t, "node-a", m.Src)
}

// TestSignal_IDTaken Same ID with mismatched token is rejected; with matched token it takes over.
//
// Discovery background: functional test -- ID occupation protection: mismatched token is rejected
// (prevents hijacking someone else's ID)
func TestSignal_IDTaken(t *testing.T) {
	_, hs := testServer(t)
	a := dialWS(t, hs, "same-id", "tok-1")
	defer a.Close()
	readMsg(t, a)

	// Different token -> ID-TAKEN
	conn2, _, err := websocket.DefaultDialer.Dial(
		"ws"+strings.TrimPrefix(hs.URL, "http")+"/peerjs?key=testkey&id=same-id&token=wrong", nil)
	require.NoError(t, err)
	var m Message
	require.NoError(t, conn2.ReadJSON(&m))
	assert.Equal(t, "ID-TAKEN", m.Type)
	conn2.Close()
}

// TestSignal_InvalidKey Wrong key is rejected.
//
// Discovery background: defensive test -- failed key validation must reject the connection
func TestSignal_InvalidKey(t *testing.T) {
	_, hs := testServer(t)
	conn, resp, err := websocket.DefaultDialer.Dial(
		"ws"+strings.TrimPrefix(hs.URL, "http")+"/peerjs?key=wrong&id=x&token=t", nil)
	if err == nil {
		conn.Close()
		t.Fatal("wrong key should be rejected")
	}
	_ = resp
}

// TestSignal_TokenWhitelist Token whitelist: reject upgrade for tokens not on the list;
// normal OPEN for tokens on the list.
//
// Discovery background: code review 2026-08-18 -- tokens previously only served ID occupation
// protection; any client could self-define a token and connect to register any ID, impersonating
// nodes to receive signaling/induce OFFERs; the whitelist makes self-hosted deployments trust
// only known nodes (fix: WithTokenWhitelist + HandleWS validation).
func TestSignal_TokenWhitelist(t *testing.T) {
	srv := NewServer("testkey", WithTokenWhitelist([]string{"tok-a", "tok-b"}))
	hs := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		srv.HandleWS(w, r)
	}))
	defer hs.Close()

	// Token not on list -> reject (HTTP 400, no OPEN)
	conn, resp, err := websocket.DefaultDialer.Dial(
		"ws"+strings.TrimPrefix(hs.URL, "http")+"/peerjs?key=testkey&id=evil&token=not-in-list", nil)
	if err == nil {
		conn.Close()
		t.Fatal("token not on whitelist should be rejected")
	}
	require.NotNil(t, resp)
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)

	// Token on list -> normal OPEN
	a := dialWS(t, hs, "node-a", "tok-a")
	defer a.Close()
	m := readMsg(t, a)
	assert.Equal(t, "OPEN", m.Type)
}

// TestDiscover_AnnounceAndQuery Node announces to a room -> query returns online nodes (expired ones evicted).
// Discovery background: after self-hosting, room discovery was merged into the signaling server (replacing MQTT broadcast).
func TestDiscover_AnnounceAndQuery(t *testing.T) {
	_, hs := testServer(t)
	announce := func(peerID, coll string) {
		body := strings.NewReader(`{"peerId":"` + peerID + `","collections":["` + coll + `"]}`)
		resp, err := http.Post(hs.URL+"/announce", "application/json", body)
		require.NoError(t, err)
		resp.Body.Close()
	}
	announce("node-1", "coll-a")
	announce("node-2", "coll-a")
	announce("node-3", "coll-b")

	// coll-a should return node-1/node-2
	resp, err := http.Get(hs.URL + "/nodes?coll=coll-a")
	require.NoError(t, err)
	defer resp.Body.Close()
	var out struct {
		Nodes []NodeInfo `json:"nodes"`
	}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
	ids := map[string]bool{}
	for _, n := range out.Nodes {
		ids[n.PeerID] = true
	}
	assert.True(t, ids["node-1"], "coll-a should contain node-1")
	assert.True(t, ids["node-2"])
	assert.False(t, ids["node-3"], "coll-b nodes should not appear")
}

// TestGraph_AnnouncePeersCreatesLinks After two nodes announce peers, /nodes returns the corresponding edge.
func TestGraph_AnnouncePeersCreatesLinks(t *testing.T) {
	_, hs := testServer(t)
	announce := func(peerID string, peers []string) {
		body, _ := json.Marshal(map[string]any{"peerId": peerID, "collections": []string{"media"}, "peers": peers})
		resp, err := http.Post(hs.URL+"/announce", "application/json", strings.NewReader(string(body)))
		require.NoError(t, err)
		resp.Body.Close()
	}
	announce("node-1", []string{"node-2"})
	announce("node-2", []string{"node-1"})

	resp, err := opsGet(hs, "/nodes")
	require.NoError(t, err)
	defer resp.Body.Close()
	var out struct {
		Nodes []NodeInfo  `json:"nodes"`
		Links []GraphLink `json:"links"`
	}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
	assert.Len(t, out.Nodes, 2)
	require.Len(t, out.Links, 1, "A->B and B->A should be deduplicated to one edge")
	assert.Equal(t, "node-1", out.Links[0].Source)
	assert.Equal(t, "node-2", out.Links[0].Target)
}

// TestGraph_EmptyPeersClearsLinks Announcing empty peers again clears old edges.
func TestGraph_EmptyPeersClearsLinks(t *testing.T) {
	_, hs := testServer(t)
	announce := func(peerID string, peers []string) {
		body, _ := json.Marshal(map[string]any{"peerId": peerID, "collections": []string{"media"}, "peers": peers})
		resp, err := http.Post(hs.URL+"/announce", "application/json", strings.NewReader(string(body)))
		require.NoError(t, err)
		resp.Body.Close()
	}
	announce("node-1", []string{"node-2"})
	announce("node-2", []string{"node-1"})
	// Both sides must clear peers for old edges to disappear (if only one side clears, the other may still report that edge)
	announce("node-1", []string{})
	announce("node-2", []string{})

	resp, err := opsGet(hs, "/nodes")
	require.NoError(t, err)
	defer resp.Body.Close()
	var out struct {
		Links []GraphLink `json:"links"`
	}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
	assert.Empty(t, out.Links, "both sides with empty peers should clear old edges")
}

// TestGraph_LeaveRemovesLinks After a node leaves, related edges disappear.
func TestGraph_LeaveRemovesLinks(t *testing.T) {
	srv, hs := testServer(t)
	announce := func(peerID string, peers []string) {
		body, _ := json.Marshal(map[string]any{"peerId": peerID, "collections": []string{"media"}, "peers": peers})
		resp, err := http.Post(hs.URL+"/announce", "application/json", strings.NewReader(string(body)))
		require.NoError(t, err)
		resp.Body.Close()
	}
	announce("node-1", []string{"node-2"})
	announce("node-2", []string{"node-1"})

	body, _ := json.Marshal(map[string]string{"peerId": "node-2"})
	req := httptest.NewRequest(http.MethodPost, "/discover/leave", strings.NewReader(string(body)))
	w := httptest.NewRecorder()
	srv.HandleLeave(w, req)
	assert.Equal(t, http.StatusOK, w.Code)

	getResp, err := opsGet(hs, "/nodes")
	require.NoError(t, err)
	defer getResp.Body.Close()
	var out struct {
		Nodes []NodeInfo  `json:"nodes"`
		Links []GraphLink `json:"links"`
	}
	require.NoError(t, json.NewDecoder(getResp.Body).Decode(&out))
	assert.Len(t, out.Nodes, 1, "after node-2 goes offline, only node-1 remains")
	assert.Empty(t, out.Links, "after node-2 goes offline, edges should disappear")
}

// TestGraph_SelfPeerIgnored When announce peers contains the node's own ID, it is ignored.
func TestGraph_SelfPeerIgnored(t *testing.T) {
	_, hs := testServer(t)
	announce := func(peerID string, peers []string) {
		body, _ := json.Marshal(map[string]any{"peerId": peerID, "collections": []string{"media"}, "peers": peers})
		resp, err := http.Post(hs.URL+"/announce", "application/json", strings.NewReader(string(body)))
		require.NoError(t, err)
		resp.Body.Close()
	}
	announce("node-1", []string{"node-1", "node-2"})
	announce("node-2", []string{"node-1"})

	getResp, err := opsGet(hs, "/nodes")
	require.NoError(t, err)
	defer getResp.Body.Close()
	var out struct {
		Links []GraphLink `json:"links"`
	}
	require.NoError(t, json.NewDecoder(getResp.Body).Decode(&out))
	require.Len(t, out.Links, 1)
	assert.NotEqual(t, out.Links[0].Source, out.Links[0].Target, "self-edges should be ignored")
}

// TestNodes_EmptyCollReturnsAll Empty coll returns all collection nodes (behavior aligned with wintools).
func TestNodes_EmptyCollReturnsAll(t *testing.T) {
	_, hs := testServer(t)
	announce := func(peerID, coll string) {
		body, _ := json.Marshal(map[string]any{"peerId": peerID, "collections": []string{coll}})
		resp, err := http.Post(hs.URL+"/announce", "application/json", strings.NewReader(string(body)))
		require.NoError(t, err)
		resp.Body.Close()
	}
	announce("node-1", "coll-a")
	announce("node-2", "coll-b")

	resp, err := opsGet(hs, "/nodes")
	require.NoError(t, err)
	defer resp.Body.Close()
	var out struct {
		Nodes []NodeInfo `json:"nodes"`
	}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
	assert.Len(t, out.Nodes, 2)
}

// TestNodes_TypeFilter Supports ?type= to filter node types (behavior aligned with wintools).
func TestNodes_TypeFilter(t *testing.T) {
	_, hs := testServer(t)
	announce := func(peerID, nodeType string) {
		body, _ := json.Marshal(map[string]any{"peerId": peerID, "collections": []string{"media"}, "nodeType": nodeType})
		resp, err := http.Post(hs.URL+"/announce", "application/json", strings.NewReader(string(body)))
		require.NoError(t, err)
		resp.Body.Close()
	}
	announce("go-1", "go-persistent")
	announce("web-1", "web-temp")

	resp, err := opsGet(hs, "/nodes?type=go-persistent")
	require.NoError(t, err)
	defer resp.Body.Close()
	var out struct {
		Nodes []NodeInfo `json:"nodes"`
	}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
	require.Len(t, out.Nodes, 1)
	assert.Equal(t, "go-1", out.Nodes[0].PeerID)
}

// TestNodes_IncludesNodeMetadata After announce reports nodeType/collections/loadInfo, nodes returns full metadata.
func TestNodes_IncludesNodeMetadata(t *testing.T) {
	_, hs := testServer(t)
	body, _ := json.Marshal(map[string]any{
		"peerId": "node-1", "collections": []string{"media"}, "nodeType": "go-persistent",
		"loadInfo": map[string]any{"connections": 3},
	})
	resp, err := http.Post(hs.URL+"/announce", "application/json", strings.NewReader(string(body)))
	require.NoError(t, err)
	resp.Body.Close()

	getResp, err := opsGet(hs, "/nodes")
	require.NoError(t, err)
	defer getResp.Body.Close()
	var out struct {
		Nodes []NodeInfo `json:"nodes"`
	}
	require.NoError(t, json.NewDecoder(getResp.Body).Decode(&out))
	require.Len(t, out.Nodes, 1)
	n := out.Nodes[0]
	assert.Equal(t, "go-persistent", n.NodeType)
	assert.Equal(t, []string{"media"}, n.Collections)
	assert.Equal(t, float64(3), n.LoadInfo["connections"])
}

// TestGraph_TypeFilterLinksExcludeFilteredNodes Verifies that when type filtering is applied, graph edges do not include filtered-out nodes.
func TestGraph_TypeFilterLinksExcludeFilteredNodes(t *testing.T) {
	_, hs := testServer(t)
	announce := func(peerID, nodeType string, peers []string) {
		body, _ := json.Marshal(map[string]any{"peerId": peerID, "collections": []string{"media"}, "nodeType": nodeType, "peers": peers})
		resp, err := http.Post(hs.URL+"/announce", "application/json", strings.NewReader(string(body)))
		require.NoError(t, err)
		resp.Body.Close()
	}
	announce("go-1", "go-persistent", []string{"web-1"})
	announce("web-1", "web-temp", []string{"go-1"})

	// Only querying go-persistent type: nodes only has go-1, links should be empty (web-1 filtered out)
	resp, err := opsGet(hs, "/nodes?type=go-persistent")
	require.NoError(t, err)
	defer resp.Body.Close()
	var out struct {
		Nodes []NodeInfo  `json:"nodes"`
		Links []GraphLink `json:"links"`
	}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
	require.Len(t, out.Nodes, 1)
	assert.Empty(t, out.Links, "filtered-out nodes should not appear in graph edges")
}

// TestNodes_EmptyReturnsEmptyArray When nodes/edges are empty, JSON should return [] not null.
func TestNodes_EmptyReturnsEmptyArray(t *testing.T) {
	_, hs := testServer(t)
	resp, err := opsGet(hs, "/nodes")
	require.NoError(t, err)
	defer resp.Body.Close()
	var raw map[string]json.RawMessage
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&raw))
	require.Contains(t, raw, "nodes")
	require.Contains(t, raw, "links")
	assert.True(t, len(raw["nodes"]) > 0 && raw["nodes"][0] == '[', "nodes should be a JSON array")
	assert.True(t, len(raw["links"]) > 0 && raw["links"][0] == '[', "links should be a JSON array")
}

// TestSweepDiscovery_CleansExpiredNodes Expired nodes should be cleaned from disc/peerLinks/peerStats/peerColls.
func TestSweepDiscovery_CleansExpiredNodes(t *testing.T) {
	srv, hs := testServer(t)
	// Announce a node so it enters disc/peerStats/peerColls
	body, _ := json.Marshal(map[string]any{
		"peerId": "node-1", "collections": []string{"media"}, "nodeType": "go-persistent",
		"peers": []string{"node-2"}, "loadInfo": map[string]any{"connections": 1},
	})
	resp, err := http.Post(hs.URL+"/announce", "application/json", strings.NewReader(string(body)))
	require.NoError(t, err)
	resp.Body.Close()

	// Set its lastSeen to before the heartbeat TTL
	srv.mu.Lock()
	srv.disc["media"]["node-1"] = time.Now().Add(-2 * srv.heartbeatTTL)
	srv.mu.Unlock()

	srv.sweepDiscovery()

	srv.mu.Lock()
	_, hasDisc := srv.disc["media"]["node-1"]
	_, hasLinks := srv.peerLinks["node-1"]
	_, hasStats := srv.peerStats["node-1"]
	_, hasColls := srv.peerColls["node-1"]
	srv.mu.Unlock()
	assert.False(t, hasDisc, "expired node should be cleaned from disc")
	assert.False(t, hasLinks, "expired node should be cleaned from peerLinks")
	assert.False(t, hasStats, "expired node should be cleaned from peerStats")
	assert.False(t, hasColls, "expired node should be cleaned from peerColls")
}

// TestHandleStatus GET /status returns a server status snapshot (including nodes/graph/counts).
//
// Discovery background: 2026-09-05 dashboard/status/leave API was added without tests;
// this test verifies response structure, node filtering (only active), deduplicated edges, msgCount.
func TestHandleStatus(t *testing.T) {
	_, hs := testServer(t)

	// Register two nodes + one link
	announce := func(id string, peers []string) {
		body, _ := json.Marshal(map[string]any{
			"peerId": id, "collections": []string{"media"}, "nodeType": "go-persistent",
			"peers": peers, "loadInfo": map[string]any{"connections": 1},
		})
		resp, err := http.Post(hs.URL+"/announce", "application/json", strings.NewReader(string(body)))
		require.NoError(t, err)
		resp.Body.Close()
	}
	announce("node-a", []string{"node-b"})
	announce("node-b", []string{"node-a"})

	resp, err := opsGet(hs, "/status")
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "application/json", resp.Header.Get("Content-Type"))

	var st map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&st))

	// Structure completeness
	assert.Contains(t, st, "key")
	assert.Contains(t, st, "uptimeSec")
	assert.Contains(t, st, "uptimeStr")
	assert.Contains(t, st, "clients")
	assert.Contains(t, st, "queues")
	assert.Contains(t, st, "discovered")
	assert.Contains(t, st, "nodes")
	assert.Contains(t, st, "links")
	assert.Contains(t, st, "msgCount")

	// Node count: 2 active nodes
	assert.Equal(t, float64(2), st["discovered"])

	// Edges: 1 after deduplication (a-b or b-a)
	links, _ := st["links"].([]any)
	assert.Len(t, links, 1, "bidirectional links should be deduplicated to 1")

	// Node metadata includes nodeType
	nodes, _ := st["nodes"].([]any)
	assert.Len(t, nodes, 2)
	n0, _ := nodes[0].(map[string]any)
	assert.Equal(t, "go-persistent", n0["nodeType"])
	assert.Contains(t, n0, "collections")
	assert.Contains(t, n0, "loadInfo")
}

// TestHandleDashboard GET / returns embedded dashboard.html; non-/ paths return 404.
func TestHandleDashboard(t *testing.T) {
	_, hs := testServer(t)

	// Root path returns HTML
	resp, err := http.Get(hs.URL + "/")
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Contains(t, resp.Header.Get("Content-Type"), "text/html")
	assert.Contains(t, resp.Header.Get("Cache-Control"), "no-cache")

	// Non-root path returns 404 (prevents / prefix mis-matching)
	resp2, err := http.Get(hs.URL + "/nonexistent")
	require.NoError(t, err)
	resp2.Body.Close()
	assert.Equal(t, http.StatusNotFound, resp2.StatusCode)
}

// TestFormatDuration Seconds -> human-readable duration (used for dashboard uptime display).
func TestFormatDuration(t *testing.T) {
	tests := []struct {
		seconds float64
		want    string
	}{
		{0, "0s"},
		{30, "30s"},
		{59.9, "60s"},
		{60, "1.0m"},
		{120, "2.0m"},
		{3599, "60.0m"},
		{3600, "1.0h"},
		{7200, "2.0h"},
		{86399, "24.0h"},
		{86400, "1.0d"},
		{172800, "2.0d"},
	}
	for _, tt := range tests {
		assert.Equal(t, tt.want, formatDuration(tt.seconds), "formatDuration(%v)", tt.seconds)
	}
}

// TestHandleDeadDst Failed forwarding target: removed from clients/disc/peerLinks/peerStats/peerColls,
// LEAVE broadcast to other surviving nodes, message sender notified.
//
// Discovery background: handleDeadDst is the key cleanup logic for handleForward's error path;
// previously had no direct tests (only indirectly covered LEAVE broadcast through TestSignal_LeaveBroadcast).
func TestHandleDeadDst(t *testing.T) {
	srv, hs := testServer(t)

	// Register A and B (token must be non-empty; HandleWS enforces validation)
	connA := dialWS(t, hs, "dead-node", "tok-a")
	defer connA.Close()
	connB := dialWS(t, hs, "survivor", "tok-b")
	defer connB.Close()
	readMsg(t, connA) // OPEN
	readMsg(t, connB) // OPEN

	// A announces itself + link to B
	body, _ := json.Marshal(map[string]any{
		"peerId": "dead-node", "collections": []string{"media"}, "nodeType": "go-persistent",
		"peers": []string{"survivor"},
	})
	resp, err := http.Post(hs.URL+"/announce", "application/json", strings.NewReader(string(body)))
	require.NoError(t, err)
	resp.Body.Close()

	// Let A's connection drop (simulate dead target)
	connA.Close()

	// B sends a message to A -> forwarding fails -> handleDeadDst cleans up A's remnants
	connB.WriteJSON(Message{Type: "ICE", Dst: "dead-node"})
	time.Sleep(200 * time.Millisecond)

	// B should receive LEAVE (from dead-node direction, notifying other surviving nodes)
	connB.SetReadDeadline(time.Now().Add(3 * time.Second))
	var m Message
	require.NoError(t, connB.ReadJSON(&m))
	assert.Equal(t, "LEAVE", m.Type)
	assert.Equal(t, "dead-node", m.Src)

	// A's remnants should be cleaned from disc/peerLinks/peerStats/peerColls
	srv.mu.Lock()
	_, hasDisc := srv.disc["media"]["dead-node"]
	_, hasLinks := srv.peerLinks["dead-node"]
	_, hasStats := srv.peerStats["dead-node"]
	_, hasColls := srv.peerColls["dead-node"]
	_, hasClient := srv.clients["dead-node"]
	srv.mu.Unlock()
	assert.False(t, hasDisc, "dead-node should be cleaned from disc")
	assert.False(t, hasLinks, "dead-node should be cleaned from peerLinks")
	assert.False(t, hasStats, "dead-node should be cleaned from peerStats")
	assert.False(t, hasColls, "dead-node should be cleaned from peerColls")
	assert.False(t, hasClient, "dead-node should be cleaned from clients")
}

// TestHandleID_CORS -- Browser direct-connect consumer side (public static panel) must be able to fetch id cross-origin.
//
// Discovery background: 2026-09-20 while building the single-file public panel for packages/peerdrive-client,
// discovered that a file:// opened panel requesting a temporary id from self-hosted signaling "GET /peerjs/id"
// gets blocked by same-origin policy (page origin is null), and the PeerJS side only reports a vague
// server-error with no CORS indication. For the panel to be "public", the signaling must open these public
// endpoints to all origins.
func TestHandleID_CORS(t *testing.T) {
	srv := NewServer("testkey")

	// GET: response must carry cross-origin headers, and still return a random id normally
	rec := httptest.NewRecorder()
	srv.HandleID(rec, httptest.NewRequest(http.MethodGet, "/peerjs/id", nil))
	require.Equal(t, http.StatusOK, rec.Code)
	assert.NotEmpty(t, strings.TrimSpace(rec.Body.String()), "should return a random id")
	assert.Equal(t, "*", rec.Header().Get("Access-Control-Allow-Origin"))
	assert.NotEmpty(t, rec.Header().Get("Access-Control-Allow-Methods"))

	// OPTIONS preflight: should short-circuit to 204 with the same cross-origin headers
	// (otherwise the browser won't send the real request)
	rec = httptest.NewRecorder()
	srv.HandleID(rec, httptest.NewRequest(http.MethodOptions, "/peerjs/id", nil))
	require.Equal(t, http.StatusNoContent, rec.Code)
	assert.Equal(t, "*", rec.Header().Get("Access-Control-Allow-Origin"))
	assert.Empty(t, rec.Body.String(), "preflight should not return a body")

	// The discovery REST endpoints must stay open: the public panel and browser-side
	// debugging need to read discovery results. **/status is deliberately excluded** since
	// 2026-10-04 — it enumerates every node on the network and now requires an ops token
	// (pinned by TestOpsEndpointsRequireToken below).
	for name, h := range map[string]http.HandlerFunc{
		"/discover/announce": srv.HandleAnnounce,
		"/discover/leave":    srv.HandleLeave,
		"/discover/nodes":    srv.HandleNodes,
	} {
		r := httptest.NewRecorder()
		h(r, httptest.NewRequest(http.MethodGet, name, nil))
		assert.Equal(t, "*", r.Header().Get("Access-Control-Allow-Origin"), name+" missing cross-origin headers")
	}

	// /status must NOT be readable cross-origin, even before the token check runs.
	r := httptest.NewRecorder()
	srv.HandleStatus(r, httptest.NewRequest(http.MethodGet, "/status", nil))
	assert.Empty(t, r.Header().Get("Access-Control-Allow-Origin"),
		"/status must not advertise a wildcard origin")
}

// TestOpsEndpointsRequireToken (2026-10-04) pins the actual security property, not just the
// CORS header: a request with **no token** cannot read the full roster or /status — which is
// exactly what a random web page's fetch() looks like.
func TestOpsEndpointsRequireToken(t *testing.T) {
	_, hs := testServer(t)

	body, _ := json.Marshal(map[string]any{"peerId": "node-1", "collections": []string{"coll-a"}})
	resp, err := http.Post(hs.URL+"/announce", "application/json", strings.NewReader(string(body)))
	require.NoError(t, err)
	resp.Body.Close()

	for _, path := range []string{"/status", "/nodes?type=go-persistent"} {
		resp, err := publicGet(hs, path)
		require.NoError(t, err)
		resp.Body.Close()
		assert.Equal(t, http.StatusUnauthorized, resp.StatusCode,
			path+" must reject a token-less caller (ops status / inventory query)")
	}

	// The panel's path must stay open, or the public panel's "auto-search" dies for
	// every user. This covers BOTH panel shapes:
	//   - /nodes?coll=<hash>  — a room-scoped lookup;
	//   - /nodes              — the plain "who is online" query the panel actually sends
	//     (discoverNodes is called with neither coll nor type).
	// The second one is a regression guard: the first version of this change gated on
	// "no coll", which broke exactly that call and turned CI's E2E panel step red.
	for _, path := range []string{"/nodes?coll=coll-a", "/nodes"} {
		panelResp, err := publicGet(hs, path)
		require.NoError(t, err)
		panelResp.Body.Close()
		assert.Equal(t, http.StatusOK, panelResp.StatusCode,
			path+" must remain public — the public panel depends on it")
		assert.Equal(t, "*", panelResp.Header.Get("Access-Control-Allow-Origin"),
			path+" must keep the wildcard CORS header for the panel")
	}

	// With a token, the inventory query works and returns the node.
	invResp, err := opsGet(hs, "/nodes?type=go-persistent")
	require.NoError(t, err)
	defer invResp.Body.Close()
	assert.Equal(t, http.StatusOK, invResp.StatusCode, "token-bearing ?type= query must succeed")
}

// TestOpsTokenAcceptsQueryAndHeader: both token shapes must work, since the WebSocket upgrade
// can only carry a query param while curl/dashboard scripts send a header.
func TestOpsTokenAcceptsQueryAndHeader(t *testing.T) {
	_, hs := testServer(t)

	viaHeader, err := opsGet(hs, "/status")
	require.NoError(t, err)
	viaHeader.Body.Close()
	assert.Equal(t, http.StatusOK, viaHeader.StatusCode, "Authorization: Bearer must be accepted")

	viaQuery, err := publicGet(hs, "/status?token=ops-token")
	require.NoError(t, err)
	viaQuery.Body.Close()
	assert.Equal(t, http.StatusOK, viaQuery.StatusCode, "?token= must be accepted")
}

// TestOpsTokenHonoursWhitelist: when -tokens is configured, a token outside the list is
// rejected. Without this, enabling the whitelist would look like it did nothing for /status.
func TestOpsTokenHonoursWhitelist(t *testing.T) {
	srv := NewServer("testkey", WithTokenWhitelist([]string{"good-token"}))
	hs := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		srv.HandleStatus(w, r)
	}))
	defer hs.Close()

	get := func(token string) int {
		req, _ := http.NewRequest(http.MethodGet, hs.URL+"/status", nil)
		if token != "" {
			req.Header.Set("Authorization", "Bearer "+token)
		}
		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		resp.Body.Close()
		return resp.StatusCode
	}

	assert.Equal(t, http.StatusOK, get("good-token"))
	assert.Equal(t, http.StatusUnauthorized, get("bad-token"))
	assert.Equal(t, http.StatusUnauthorized, get(""))
}

// --- 2026-10-04: explicit CORS allow-list (WithCORSOrigins) ---

// TestCORSOrigins_DefaultKeepsWildcard is the backward-compatibility guard: an operator
// who configures nothing must get byte-identical behavior to before this change,
func TestCORSOrigins_DefaultKeepsWildcard(t *testing.T) {
	srv := NewServer("testkey") // no WithCORSOrigins

	rec := httptest.NewRecorder()
	srv.HandleID(rec, httptest.NewRequest(http.MethodGet, "/peerjs/id", nil))
	assert.Equal(t, "*", rec.Header().Get("Access-Control-Allow-Origin"),
		"unconfigured server must keep the wildcard")
}

// TestCORSOrigins_AllowListEchoesMatchAndBlocksOthers: with a list configured, an
// allow-listed Origin is echoed back (so the browser lets the read through) and a
// non-listed Origin gets **no** Allow-Origin header at all (so the browser blocks it).
func TestCORSOrigins_AllowListEchoesMatchAndBlocksOthers(t *testing.T) {
	srv := NewServer("testkey", WithCORSOrigins([]string{"https://peerdrive.pages.dev", "null"}))

	// Allowed: exact origin echoed.
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/peerjs/id", nil)
	req.Header.Set("Origin", "https://peerdrive.pages.dev")
	srv.HandleID(rec, req)
	assert.Equal(t, "https://peerdrive.pages.dev", rec.Header().Get("Access-Control-Allow-Origin"),
		"allow-listed origin must be echoed")

	// Allowed: file:// panels send Origin "null".
	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "/peerjs/id", nil)
	req.Header.Set("Origin", "null")
	srv.HandleID(rec, req)
	assert.Equal(t, "null", rec.Header().Get("Access-Control-Allow-Origin"),
		"file:// panels need an explicit null entry")

	// Not allowed: no header at all -> browser blocks the read.
	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "/peerjs/id", nil)
	req.Header.Set("Origin", "https://evil.example")
	srv.HandleID(rec, req)
	assert.Empty(t, rec.Header().Get("Access-Control-Allow-Origin"),
		"non-listed origin must receive no Allow-Origin header")
	assert.Equal(t, http.StatusOK, rec.Code,
		"status is unaffected — the browser enforces via the header")
}

// TestCORSOrigins_ExplicitStarInListRestoresWildcard: an operator can put "*" in the
// list to opt back into permissive behavior without clearing the flag.
func TestCORSOrigins_ExplicitStarInListRestoresWildcard(t *testing.T) {
	srv := NewServer("testkey", WithCORSOrigins([]string{"*"}))

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/peerjs/id", nil)
	req.Header.Set("Origin", "https://anything.example")
	srv.HandleID(rec, req)
	assert.Equal(t, "*", rec.Header().Get("Access-Control-Allow-Origin"))
}
