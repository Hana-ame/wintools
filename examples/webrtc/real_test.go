// AI: generated with assistance from AI (2026-06-23)
//
// 真实 WebRTC 端到端测试
// 通过远程信令服务器交换 SDP（含 embedded ICE candidates），建立 P2P DataChannel
package webrtc

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/pion/webrtc/v4"
)

const sigServer = "http://bwh.moonchan.xyz:8080"

var sigClient = &http.Client{
	Transport: &http.Transport{
		Proxy: nil,
	},
}

type SigClient struct {
	Server string
	RoomID string
	Peer   string
}

func (s *SigClient) post(path, body string) ([]byte, error) {
	r, err := sigClient.Post(s.Server+path, "application/json", strings.NewReader(body))
	if err != nil {
		return nil, err
	}
	defer r.Body.Close()
	return io.ReadAll(r.Body)
}

func (s *SigClient) get(path string) ([]byte, error) {
	r, err := sigClient.Get(s.Server + path)
	if err != nil {
		return nil, err
	}
	defer r.Body.Close()
	return io.ReadAll(r.Body)
}

func (s *SigClient) CreateRoom() error {
	b, err := s.post("/room", "")
	if err != nil {
		return err
	}
	var res struct{ RoomID string `json:"room_id"` }
	json.Unmarshal(b, &res)
	if res.RoomID == "" {
		return fmt.Errorf("create room failed")
	}
	s.RoomID = res.RoomID
	return nil
}

func (s *SigClient) Join() error {
	b, err := s.post("/room/"+s.RoomID+"/join", "")
	if err != nil {
		return err
	}
	var res struct{ Peer string `json:"peer"` }
	json.Unmarshal(b, &res)
	if res.Peer == "" {
		return fmt.Errorf("join failed")
	}
	s.Peer = res.Peer
	return nil
}

func (s *SigClient) SendSDP(sdpType, sdp string) error {
	d, _ := json.Marshal(map[string]string{"type": sdpType, "sdp": sdp})
	_, err := s.post("/room/"+s.RoomID+"/sdp?peer="+s.Peer, string(d))
	return err
}

func (s *SigClient) RecvSDP(wait int) (string, error) {
	p := fmt.Sprintf("/room/%s/sdp?peer=%s", s.RoomID, s.Peer)
	if wait > 0 {
		p += fmt.Sprintf("&wait=%d", wait)
	}
	b, err := s.get(p)
	if err != nil {
		return "", err
	}
	var res struct {
		Type string `json:"type"`
		SDP  string `json:"sdp"`
	}
	if err := json.Unmarshal(b, &res); err != nil || res.SDP == "" {
		return "", fmt.Errorf("no sdp: %s", string(b))
	}
	return res.SDP, nil
}

func TestRealSignaledP2P(t *testing.T) {
	if _, err := sigClient.Get(sigServer + "/"); err != nil {
		t.Skipf("signaling server unreachable: %v", err)
	}
	sigP1 := &SigClient{Server: sigServer}
	sigP2 := &SigClient{Server: sigServer}

	// 1. 创建房间
	if err := sigP1.CreateRoom(); err != nil {
		t.Fatal(err)
	}
	sigP2.RoomID = sigP1.RoomID
	t.Logf("room=%s", sigP1.RoomID)

	// 2. 加入
	for _, s := range []*SigClient{sigP1, sigP2} {
		if err := s.Join(); err != nil {
			t.Fatal(err)
		}
	}
	t.Logf("p1=%s p2=%s", sigP1.Peer, sigP2.Peer)

	// 3. 创建 Peer（p1 预建 DataChannel，p2 等接收）
	p1, err := NewPeer(Config{
		ICEServers:       []string{"stun:stun.l.google.com:19302"},
		DataChannelLabel: "chat",
		Ordered:          true,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer p1.Close()

	p2, err := NewPeer(Config{
		ICEServers: []string{"stun:stun.l.google.com:19302"},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer p2.Close()

	// 4. 非 Trickle ICE：SDP 交换使用 embedded candidates
	//    p1 创建 Offer，等 ICE 候选收集完，发含候选的 SDP
	//    p2 收 Offer → SetRemoteDescription → CreateAnswer → 等候选收集完 → 发含候选的 Answer
	//    p1 收 Answer → SetRemoteDescription
	//    全程不需要单独交换 ICE 候选

	offer, err := p1.CreateOffer()
	if err != nil {
		t.Fatal(err)
	}
	_ = offer // 我们不等初始 offer，等 gathering 完成后的完整版

	// 等 p1 ICE 候选收集完成
	<-p1.GatheringComplete()
	finalOffer, err := p1.LocalDescriptionSDP()
	if err != nil {
		t.Fatalf("get final offer sdp: %v", err)
	}
	if err := sigP1.SendSDP("offer", finalOffer); err != nil {
		t.Fatal(err)
	}
	t.Log("p1 sent offer (with ICE candidates)")

	// p2 收 Offer
	offerRecv, err := sigP2.RecvSDP(10)
	if err != nil {
		t.Fatal(err)
	}
	if err := p2.SetRemoteDescription(offerRecv); err != nil {
		t.Fatal(err)
	}

	// p2 创建 Answer
	answer, err := p2.CreateAnswer()
	if err != nil {
		t.Fatal(err)
	}
	_ = answer

	// 等 p2 ICE 候选收集完成
	<-p2.GatheringComplete()
	finalAnswer, err := p2.LocalDescriptionSDP()
		if err != nil {
			t.Fatalf("get final answer sdp: %v", err)
		}
	if err := sigP2.SendSDP("answer", finalAnswer); err != nil {
		t.Fatal(err)
	}
	t.Log("p2 sent answer (with ICE candidates)")

	// p1 收 Answer
	answerRecv, err := sigP1.RecvSDP(10)
	if err != nil {
		t.Fatal(err)
	}
	if err := p1.SetRemoteDescription(answerRecv); err != nil {
		t.Fatal(err)
	}

	// 5. 等 DataChannel 就绪
	p1dc := make(chan *webrtc.DataChannel, 1)
	dc1raw := p1.DataChannel("chat")
	if dc1raw != nil {
		dc1raw.OnOpen(func() { p1dc <- dc1raw })
	}

	p2dc := make(chan *webrtc.DataChannel, 1)
	p2.OnDataChannel(func(dc *webrtc.DataChannel) { dc.OnOpen(func() { p2dc <- dc }) })

	var dc1, dc2 *webrtc.DataChannel
	select {
	case dc1 = <-p1dc:
	case <-time.After(15 * time.Second):
		t.Fatal("p1 dc timeout")
	}
	select {
	case dc2 = <-p2dc:
	case <-time.After(15 * time.Second):
		t.Fatal("p2 dc timeout")
	}
	t.Log("both DataChannels open")

	// 6. 双向数据传输
	p1Got := make(chan string, 1)
	p2Got := make(chan string, 1)
	dc1.OnMessage(func(msg webrtc.DataChannelMessage) { p1Got <- string(msg.Data) })
	dc2.OnMessage(func(msg webrtc.DataChannelMessage) { p2Got <- string(msg.Data) })

	if err := dc1.Send([]byte("hello from p1")); err != nil {
		t.Fatal(err)
	}
	select {
	case v := <-p2Got:
		if v != "hello from p1" {
			t.Fatalf("p2 got wrong: %s", v)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("p2 recv timeout")
	}
	t.Log("p1 → p2 OK")

	if err := dc2.Send([]byte("hello from p2")); err != nil {
		t.Fatal(err)
	}
	select {
	case v := <-p1Got:
		if v != "hello from p2" {
			t.Fatalf("p1 got wrong: %s", v)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("p1 recv timeout")
	}
	t.Log("p2 → p1 OK")

	t.Log("=== REAL P2P PASS ===")
}

func TestMain(m *testing.M) {
	resp, err := sigClient.Get(sigServer + "/")
	if err != nil {
		fmt.Printf("signaling server unreachable: %v (test will skip)\n", err)
	}
	if resp != nil {
		resp.Body.Close()
	}
	m.Run()
}
