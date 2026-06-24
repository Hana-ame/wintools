package signaling

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func testServer(t *testing.T) (*Server, *httptest.Server) {
	t.Helper()
	s := New()
	ts := httptest.NewServer(s.Handler())
	return s, ts
}

func TestCreateRoom(t *testing.T) {
	_, ts := testServer(t)
	defer ts.Close()

	resp, err := http.Post(ts.URL+"/kv/room", "application/json", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	var body map[string]string
	json.NewDecoder(resp.Body).Decode(&body)
	if _, ok := body["room_id"]; !ok {
		t.Fatalf("expected room_id, got %v", body)
	}
}

func TestCreateAndJoinRoom(t *testing.T) {
	_, ts := testServer(t)
	defer ts.Close()

	resp, _ := http.Post(ts.URL+"/kv/room", "application/json", nil)
	var created map[string]string
	json.NewDecoder(resp.Body).Decode(&created)
	roomID := created["room_id"]

	resp, _ = http.Post(ts.URL+"/kv/room/"+roomID+"/join", "application/json", nil)
	var p1 map[string]string
	json.NewDecoder(resp.Body).Decode(&p1)
	if p1["peer"] != "p1" {
		t.Fatalf("expected p1, got %v", p1)
	}

	resp, _ = http.Post(ts.URL+"/kv/room/"+roomID+"/join", "application/json", nil)
	var p2 map[string]string
	json.NewDecoder(resp.Body).Decode(&p2)
	if p2["peer"] != "p2" {
		t.Fatalf("expected p2, got %v", p2)
	}
}

func TestRoomFull(t *testing.T) {
	_, ts := testServer(t)
	defer ts.Close()

	resp, _ := http.Post(ts.URL+"/kv/room", "application/json", nil)
	var created map[string]string
	json.NewDecoder(resp.Body).Decode(&created)

	for i := 0; i < 3; i++ {
		http.Post(ts.URL+"/kv/room/"+created["room_id"]+"/join", "application/json", nil)
	}

	resp, _ = http.Post(ts.URL+"/kv/room/"+created["room_id"]+"/join", "application/json", nil)
	var result map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&result)
	if err, _ := result["err"].(string); err != "room full" {
		t.Fatalf("expected 'room full', got %v", result)
	}
}

func TestSDPExchange(t *testing.T) {
	_, ts := testServer(t)
	defer ts.Close()

	resp, _ := http.Post(ts.URL+"/kv/room", "application/json", nil)
	var created map[string]string
	json.NewDecoder(resp.Body).Decode(&created)
	rid := created["room_id"]
	http.Post(ts.URL+"/kv/room/"+rid+"/join", "application/json", nil)
	http.Post(ts.URL+"/kv/room/"+rid+"/join", "application/json", nil)

	offer := `{"type":"offer","sdp":"v=0\no=..."}`
	resp, _ = http.Post(ts.URL+"/kv/room/"+rid+"/sdp?peer=p1", "application/json", strings.NewReader(offer))
	var postResp map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&postResp)
	if postResp["ok"] != true {
		t.Fatalf("post sdp failed: %v", postResp)
	}

	resp, _ = http.Get(ts.URL + "/kv/room/" + rid + "/sdp?peer=p2")
	var sdp SDPBody
	json.NewDecoder(resp.Body).Decode(&sdp)
	if sdp.Type != "offer" || sdp.SDP == "" {
		t.Fatalf("expected offer, got %+v", sdp)
	}

	answer := `{"type":"answer","sdp":"v=0\n..."}`
	http.Post(ts.URL+"/kv/room/"+rid+"/sdp?peer=p2", "application/json", strings.NewReader(answer))

	resp, _ = http.Get(ts.URL + "/kv/room/" + rid + "/sdp?peer=p1")
	var ans SDPBody
	json.NewDecoder(resp.Body).Decode(&ans)
	if ans.Type != "answer" || ans.SDP == "" {
		t.Fatalf("expected answer, got %+v", ans)
	}
}

func TestICEExchange(t *testing.T) {
	_, ts := testServer(t)
	defer ts.Close()

	resp, _ := http.Post(ts.URL+"/kv/room", "application/json", nil)
	var created map[string]string
	json.NewDecoder(resp.Body).Decode(&created)
	rid := created["room_id"]
	http.Post(ts.URL+"/kv/room/"+rid+"/join", "application/json", nil)
	http.Post(ts.URL+"/kv/room/"+rid+"/join", "application/json", nil)

	ice := `{"candidate":"candidate:1 1 UDP 123","sdpMid":"0"}`
	resp, _ = http.Post(ts.URL+"/kv/room/"+rid+"/ice?peer=p1", "application/json", strings.NewReader(ice))
	var postResp map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&postResp)
	if postResp["ok"] != true {
		t.Fatalf("post ice failed: %v", postResp)
	}

	resp, _ = http.Get(ts.URL + "/kv/room/" + rid + "/ice?peer=p2")
	var got map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&got)
	if got["candidate"] != "candidate:1 1 UDP 123" {
		t.Fatalf("expected ice candidate, got %+v", got)
	}
}

func TestICEAll(t *testing.T) {
	_, ts := testServer(t)
	defer ts.Close()

	resp, _ := http.Post(ts.URL+"/kv/room", "application/json", nil)
	var created map[string]string
	json.NewDecoder(resp.Body).Decode(&created)
	rid := created["room_id"]
	http.Post(ts.URL+"/kv/room/"+rid+"/join", "application/json", nil)
	http.Post(ts.URL+"/kv/room/"+rid+"/join", "application/json", nil)

	http.Post(ts.URL+"/kv/room/"+rid+"/ice?peer=p1", "application/json", strings.NewReader(`{"candidate":"c1"}`))
	http.Post(ts.URL+"/kv/room/"+rid+"/ice?peer=p1", "application/json", strings.NewReader(`{"candidate":"c2"}`))

	resp, _ = http.Get(ts.URL + "/kv/room/" + rid + "/ice?peer=p2&all")
	var all []interface{}
	json.NewDecoder(resp.Body).Decode(&all)
	if len(all) != 2 {
		t.Fatalf("expected 2 ice candidates, got %d: %+v", len(all), all)
	}
}

func TestRoomNotFound(t *testing.T) {
	_, ts := testServer(t)
	defer ts.Close()

	resp, _ := http.Post(ts.URL+"/kv/room/nonexistent/join", "application/json", nil)
	var result map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&result)
	if err, _ := result["err"].(string); err != "room not found" {
		t.Fatalf("expected 'room not found', got %v", result)
	}

	resp, _ = http.Get(ts.URL + "/kv/room/nonexistent/sdp?peer=p1")
	json.NewDecoder(resp.Body).Decode(&result)
	if err, _ := result["err"].(string); err != "room not found" {
		t.Fatalf("expected 'room not found', got %v", result)
	}
}

func TestSDPNoData(t *testing.T) {
	_, ts := testServer(t)
	defer ts.Close()

	resp, _ := http.Post(ts.URL+"/kv/room", "application/json", nil)
	var created map[string]string
	json.NewDecoder(resp.Body).Decode(&created)
	rid := created["room_id"]
	http.Post(ts.URL+"/kv/room/"+rid+"/join", "application/json", nil)

	resp, _ = http.Get(ts.URL + "/kv/room/" + rid + "/sdp?peer=p1")
	var result map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&result)
	if err, _ := result["err"].(string); err != "no data" {
		t.Fatalf("expected 'no data', got %v", result)
	}
}

func TestKVCreation(t *testing.T) {
	_, ts := testServer(t)
	defer ts.Close()

	resp, _ := http.Post(ts.URL+"/kv/create", "application/json", nil)
	var created map[string]string
	json.NewDecoder(resp.Body).Decode(&created)
	if created["id"] == "" {
		t.Fatal("expected non-empty id")
	}

	resp, _ = http.Get(ts.URL + "/kv/" + created["id"])
	var nullVal interface{}
	json.NewDecoder(resp.Body).Decode(&nullVal)
	if nullVal != nil {
		t.Fatalf("expected null, got %v", nullVal)
	}

	req, _ := http.NewRequest("PUT", ts.URL+"/kv/"+created["id"], strings.NewReader(`{"key":"val"}`))
	req.Header.Set("Content-Type", "application/json")
	resp, _ = http.DefaultClient.Do(req)
	var putResult map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&putResult)
	if putResult["ok"] != true {
		t.Fatalf("put failed: %v", putResult)
	}

	resp, _ = http.Get(ts.URL + "/kv/" + created["id"])
	var got map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&got)
	if got["key"] != "val" {
		t.Fatalf("expected key=val, got %v", got)
	}

	req, _ = http.NewRequest("DELETE", ts.URL+"/kv/"+created["id"], nil)
	resp, _ = http.DefaultClient.Do(req)
	json.NewDecoder(resp.Body).Decode(&putResult)
	if putResult["ok"] != true {
		t.Fatalf("delete failed: %v", putResult)
	}

	resp, _ = http.Get(ts.URL + "/kv/" + created["id"])
	var errResult map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&errResult)
	if errResult["err"] != "not found" {
		t.Fatalf("expected 'not found', got %v", errResult)
	}
}
