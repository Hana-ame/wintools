package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	pionwebrtc "github.com/pion/webrtc/v4"

	wrtc "github.com/Hana-ame/wintools/examples/webrtc"
)

type sigClient struct {
	Server string
	RoomID string
	Peer   string
}

func (s *sigClient) post(path, body string) ([]byte, error) {
	r, err := http.Post(s.Server+path, "application/json", strings.NewReader(body))
	if err != nil {
		return nil, err
	}
	defer r.Body.Close()
	return io.ReadAll(r.Body)
}

func (s *sigClient) get(path string) ([]byte, error) {
	r, err := http.Get(s.Server + path)
	if err != nil {
		return nil, err
	}
	defer r.Body.Close()
	return io.ReadAll(r.Body)
}

func (s *sigClient) createRoom() error {
	b, err := s.post("/kv/room", "")
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

func (s *sigClient) join() error {
	b, err := s.post("/kv/room/"+s.RoomID+"/join", "")
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

func (s *sigClient) sendSDP(sdpType, sdp string) error {
	d, _ := json.Marshal(map[string]string{"type": sdpType, "sdp": sdp})
	_, err := s.post("/kv/room/"+s.RoomID+"/sdp?peer="+s.Peer, string(d))
	return err
}

func (s *sigClient) recvSDP(wait int) (string, error) {
	p := fmt.Sprintf("/kv/room/%s/sdp?peer=%s", s.RoomID, s.Peer)
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

func (s *sigClient) sendICE(candidateJSON string) error {
	_, err := s.post("/kv/room/"+s.RoomID+"/ice?peer="+s.Peer, candidateJSON)
	return err
}

func (s *sigClient) recvICE(wait int) (string, error) {
	p := fmt.Sprintf("/kv/room/%s/ice?peer=%s&wait=%d", s.RoomID, s.Peer, wait)
	b, err := s.get(p)
	if err != nil {
		return "", err
	}
	var ice struct {
		Candidate string `json:"candidate"`
	}
	if err := json.Unmarshal(b, &ice); err != nil || ice.Candidate == "" {
		return "", fmt.Errorf("no ice")
	}
	return string(b), nil
}

func main() {
	server := flag.String("server", "http://bwh.moonchan.xyz:8080", "signaling server URL")
	mode := flag.String("mode", "", "p1 (create room) or p2 (join room)")
	room := flag.String("room", "", "room ID (required for p2)")
	trickle := flag.Bool("trickle", true, "use trickle ICE (falls back to non-trickle on timeout)")
	trickleTimeout := flag.Int("trickle-timeout", 15, "seconds to wait for trickle ICE before fallback")
	flag.Parse()

	if *mode != "p1" && *mode != "p2" {
		fmt.Println("Usage: peer-client --mode p1|p2 [--room ID] [--trickle] [--trickle-timeout N] --server URL")
		return
	}

	sig := &sigClient{Server: *server}

	if *mode == "p1" {
		if err := sig.createRoom(); err != nil {
			fmt.Printf("create room: %v\n", err)
			return
		}
		if err := sig.join(); err != nil {
			fmt.Printf("join: %v\n", err)
			return
		}
		fmt.Printf("ROOM_ID=%s (peer=%s)\n", sig.RoomID, sig.Peer)
		fmt.Println("Give this room ID to p2, then press Enter to wait...")
		fmt.Scanln()
	} else {
		if *room == "" {
			fmt.Println("--room required for p2 mode")
			return
		}
		sig.RoomID = *room
		if err := sig.join(); err != nil {
			fmt.Printf("join: %v\n", err)
			return
		}
		fmt.Printf("peer=%s joining room=%s\n", sig.Peer, sig.RoomID)
	}

	// ——— 尝试建立连接（trickle ICE → 可选回退 non-trickle） ———
	connect := func(useTrickle bool) (*wrtc.Peer, *pionwebrtc.DataChannel, error) {
		cfg := wrtc.Config{
			ICEServers:       []string{"stun:stun.l.google.com:19302"},
			DataChannelLabel: "chat",
			Ordered:          true,
		}
		if *mode == "p2" {
			cfg.DataChannelLabel = ""
		}

		peer, err := wrtc.NewPeer(cfg)
		if err != nil {
			return nil, nil, fmt.Errorf("new peer: %v", err)
		}

		peer.OnData(func(label string, data []byte) {
			fmt.Printf("[recv] %s\n", string(data))
		})

		dcReady := make(chan *pionwebrtc.DataChannel, 1)
		if *mode == "p1" {
			dc := peer.DataChannel("chat")
			if dc != nil {
				dc.OnOpen(func() { dcReady <- dc })
			}
		} else {
			peer.OnDataChannel(func(dc *pionwebrtc.DataChannel) {
				dc.OnOpen(func() { dcReady <- dc })
			})
		}

		// ICE 候选：本端收集 → 发信令
		peer.OnICECandidate(func(candidate string) {
			if err := sig.sendICE(candidate); err != nil {
				fmt.Printf("send ice: %v\n", err)
			}
		})

		// ——— SDP 交换 ———
		if *mode == "p1" {
			offerSDP, err := peer.CreateOffer()
			if err != nil {
				peer.Close()
				return nil, nil, fmt.Errorf("create offer: %v", err)
			}

			if useTrickle {
				if err := sig.sendSDP("offer", offerSDP); err != nil {
					peer.Close()
					return nil, nil, fmt.Errorf("send offer: %v", err)
				}
				fmt.Println("sent offer (trickle)")

				answerRecv, err := sig.recvSDP(30)
				if err != nil {
					peer.Close()
					return nil, nil, fmt.Errorf("recv answer: %v", err)
				}
				if err := peer.SetRemoteDescription(answerRecv); err != nil {
					peer.Close()
					return nil, nil, fmt.Errorf("set remote desc (answer): %v", err)
				}
			} else {
				<-peer.GatheringComplete()
				finalOffer, err := peer.LocalDescriptionSDP()
			if err != nil {
				peer.Close()
				return nil, nil, fmt.Errorf("get offer sdp: %v", err)
			}
				if err := sig.sendSDP("offer", finalOffer); err != nil {
					peer.Close()
					return nil, nil, fmt.Errorf("send offer: %v", err)
				}
				fmt.Println("sent offer (non-trickle)")

				answerRecv, err := sig.recvSDP(30)
				if err != nil {
					peer.Close()
					return nil, nil, fmt.Errorf("recv answer: %v", err)
				}
				if err := peer.SetRemoteDescription(answerRecv); err != nil {
					peer.Close()
					return nil, nil, fmt.Errorf("set remote desc (answer): %v", err)
				}
			}
		} else {
			offerRecv, err := sig.recvSDP(30)
			if err != nil {
				peer.Close()
				return nil, nil, fmt.Errorf("recv offer: %v", err)
			}
			if err := peer.SetRemoteDescription(offerRecv); err != nil {
				peer.Close()
				return nil, nil, fmt.Errorf("set remote desc (offer): %v", err)
			}

			answerSDP, err := peer.CreateAnswer()
			if err != nil {
				peer.Close()
				return nil, nil, fmt.Errorf("create answer: %v", err)
			}

			if useTrickle {
				if err := sig.sendSDP("answer", answerSDP); err != nil {
					peer.Close()
					return nil, nil, fmt.Errorf("send answer: %v", err)
				}
				fmt.Println("sent answer (trickle)")
			} else {
				<-peer.GatheringComplete()
				finalAnswer, err := peer.LocalDescriptionSDP()
				if err != nil {
					peer.Close()
					return nil, nil, fmt.Errorf("get answer sdp: %v", err)
				}
				if err := sig.sendSDP("answer", finalAnswer); err != nil {
					peer.Close()
					return nil, nil, fmt.Errorf("send answer: %v", err)
				}
				fmt.Println("sent answer (non-trickle)")
			}
		}

		// ——— ICE 候选接收（trickle 模式：必须在 SetRemoteDescription 之后） ———
		stopRecv := make(chan struct{})
		if useTrickle {
			go func() {
				for {
					select {
					case <-stopRecv:
						return
					default:
					}
					c, err := sig.recvICE(30)
					if err != nil {
						time.Sleep(1 * time.Second)
						continue
					}
					if err := peer.AddICECandidate(c); err != nil {
						fmt.Printf("add ice: %v\n", err)
					}
				}
			}()
		}

		timeout := time.Duration(*trickleTimeout) * time.Second
		if !useTrickle {
			timeout = 30 * time.Second
		}
		select {
		case dc := <-dcReady:
			close(stopRecv)
			return peer, dc, nil
		case <-time.After(timeout):
			close(stopRecv)
			peer.Close()
			return nil, nil, fmt.Errorf("dc timeout")
		}
	}

	tryTrickle := *trickle
	attempts := 0
	for {
		attempts++
		if attempts > 2 {
			fmt.Println("max attempts reached")
			return
		}

		peer, dc, err := connect(tryTrickle)
		if err != nil {
			fmt.Printf("connect failed (%s): %v\n", map[bool]string{true: "trickle", false: "non-trickle"}[tryTrickle], err)
			if tryTrickle {
				fmt.Println("falling back to non-trickle ICE...")
				tryTrickle = false
				continue
			}
			return
		}
		defer peer.Close()

		fmt.Println("DataChannel open!")
		go func() {
			n := 0
			for {
				msg := fmt.Sprintf("%s says: msg %d", *mode, n)
				if err := dc.Send([]byte(msg)); err != nil {
					fmt.Printf("send error: %v\n", err)
					return
				}
				fmt.Printf("[send] %s\n", msg)
				n++
				time.Sleep(3 * time.Second)
			}
		}()

		select {}
	}
}
