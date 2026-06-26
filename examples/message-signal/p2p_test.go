package main

import (
	"fmt"
	"testing"
	"time"

	pionwebrtc "github.com/pion/webrtc/v4"
	wrtc "github.com/Hana-ame/wintools/examples/webrtc"
)

func TestMessageSignalP2P(t *testing.T) {
	sessionID := fmt.Sprintf("test-%d", time.Now().UnixMilli())

	// p1: create offer, send via message API
	p1, err := wrtc.NewPeer(wrtc.Config{
		ICEServers:       []string{"stun:stun.l.google.com:19302"},
		DataChannelLabel: "chat",
		Ordered:          true,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer p1.Close()

	// p2: will receive offer via message API
	p2, err := wrtc.NewPeer(wrtc.Config{
		ICEServers: []string{"stun:stun.l.google.com:19302"},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer p2.Close()

	sig1 := &signal{SessionID: sessionID, PeerID: "p1"}
	sig2 := &signal{SessionID: sessionID, PeerID: "p2"}

	// p1: create offer, wait for ICE, send full SDP
	go func() {
		p1.CreateOffer()
		<-p1.GatheringComplete()
		offer, err := p1.LocalDescriptionSDP()
		if err != nil {
			t.Errorf("p1 get offer: %v", err)
			return
		}
		sig1.sendSDP("offer", offer)
		t.Log("p1 sent offer")

		answer, err := sig1.recvSDP(30)
		if err != nil {
			t.Errorf("p1 recv answer: %v", err)
			return
		}
		p1.SetRemoteDescription(answer)
		t.Log("p1 got answer")
	}()

	time.Sleep(500 * time.Millisecond)

	// p2: receive offer, create answer, wait for ICE, send full SDP
	go func() {
		offer, err := sig2.recvSDP(30)
		if err != nil {
			t.Errorf("p2 recv offer: %v", err)
			return
		}
		p2.SetRemoteDescription(offer)
		t.Log("p2 got offer")

		p2.CreateAnswer()
		<-p2.GatheringComplete()
		answer, err := p2.LocalDescriptionSDP()
		if err != nil {
			t.Errorf("p2 get answer: %v", err)
			return
		}
		sig2.sendSDP("answer", answer)
		t.Log("p2 sent answer")
	}()

	// wait for data channel
	dc1Ch := make(chan *pionwebrtc.DataChannel, 1)
	dc2Ch := make(chan *pionwebrtc.DataChannel, 1)

	dc1 := p1.DataChannel("chat")
	if dc1 != nil {
		dc1.OnOpen(func() { dc1Ch <- dc1 })
	}
	p2.OnDataChannel(func(dc *pionwebrtc.DataChannel) {
		dc.OnOpen(func() { dc2Ch <- dc })
	})

	var p1dc, p2dc *pionwebrtc.DataChannel
	select {
	case p1dc = <-dc1Ch:
	case <-time.After(15 * time.Second):
		t.Fatal("dc1 timeout")
	}
	select {
	case p2dc = <-dc2Ch:
	case <-time.After(15 * time.Second):
		t.Fatal("dc2 timeout")
	}
	t.Log("both DataChannels open")

	// bidirectional data
	got := make(chan string, 2)
	p1dc.OnMessage(func(msg pionwebrtc.DataChannelMessage) {
		got <- fmt.Sprintf("p1dc:%s", string(msg.Data))
	})
	p2dc.OnMessage(func(msg pionwebrtc.DataChannelMessage) {
		got <- fmt.Sprintf("p2dc:%s", string(msg.Data))
	})

	p1dc.Send([]byte("hello from p1"))
	select {
	case v := <-got:
		if v != "p2dc:hello from p1" {
			t.Fatalf("wrong: %s", v)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("p1→p2 timeout")
	}

	p2dc.Send([]byte("hello from p2"))
	select {
	case v := <-got:
		if v != "p1dc:hello from p2" {
			t.Fatalf("wrong: %s", v)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("p2→p1 timeout")
	}

	t.Log("=== P2P PASS ===")
}
