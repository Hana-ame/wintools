// AI: generated with assistance from AI (2026-06-23)
//
// WebRTC 包单元测试
//
// =====================================================
// 测试内容
// =====================================================
// TestListenerAccept 测试完整的 WebRTC P2P 建立流程:
//   1. 创建 Peer（主动方，预创建 DataChannel "chat"）
//   2. 创建 Listener（被动方）
//   3. 设置双向 ICE 候选转发（OnCandidate → AddICECandidate）
//   4. Peer CreateOffer → Listener.Offer → 返回 Answer
//   5. Peer SetRemoteDescription 设置 Answer
//   6. Accept 获取远程 DataChannel
//   7. 双向数据测试: Peer.Send("ping") → dc.OnData 接收
//                       dc.Send("pong")  → peer.OnData 接收
//
// =====================================================
// 运行
// =====================================================
// go test -v -run TestListenerAccept ./webrtc/
package webrtc

import (
	"testing"
	"time"
)

func TestListenerAccept(t *testing.T) {
	// ===== 对端 (发起方) =====
	peer, err := NewPeer(Config{
		ICEServers:       []string{"stun:stun.l.google.com:19302"},
		DataChannelLabel: "chat",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer peer.Close()

	// ===== Listener (接收方) =====
	listener := NewListener()
	defer listener.Close()

	// 启动双向候选转发
	listener.OnCandidate(func(connID, candidate string) {
		peer.AddICECandidate(candidate)
	})
	peer.OnICECandidate(func(candidate string) {
		// listener.Candidate 需要在 connID 已知后才能调用
		// 通过闭包延迟赋值
	})

	// 对端创建 Offer
	offer, err := peer.CreateOffer()
	if err != nil {
		t.Fatal(err)
	}

	// Listener 处理 Offer
	connID, answer, err := listener.Offer(offer)
	if err != nil {
		t.Fatal(err)
	}

	// 现在有了 connID，设置对端候选转发
	peer.OnICECandidate(func(candidate string) {
		listener.Candidate(connID, candidate)
	})

	// 对端设置 Answer
	if err := peer.SetRemoteDescription(answer); err != nil {
		t.Fatal(err)
	}

	// Accept DataChannel (阻塞等待)
	dc := listener.Accept()
	time.Sleep(300 * time.Millisecond)

	// 双向测试
	got := make(chan []byte, 1)
	dc.OnData(func(data []byte) { got <- data })

	if err := peer.SendDefault([]byte("ping")); err != nil {
		t.Fatal(err)
	}
	select {
	case v := <-got:
		if string(v) != "ping" {
			t.Fatalf("got %q", v)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timeout rcv")
	}

	got2 := make(chan []byte, 1)
	peer.OnData(func(label string, data []byte) { got2 <- data })

	if err := dc.Send([]byte("pong")); err != nil {
		t.Fatal(err)
	}
	select {
	case v := <-got2:
		if string(v) != "pong" {
			t.Fatalf("got %q", v)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timeout rcv2")
	}

	peer.OnState(nil)
	peer.Close()
}
