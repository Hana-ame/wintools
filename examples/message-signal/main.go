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

const sigServer = "http://bwh.moonchan.xyz:8080"

var hc = &http.Client{}

type signal struct {
	SessionID string
	PeerID    string
}

func (s *signal) post(path string, body any) error {
	b, _ := json.Marshal(body)
	r, err := hc.Post(sigServer+path, "application/json", strings.NewReader(string(b)))
	if err != nil {
		return err
	}
	return r.Body.Close()
}

func (s *signal) get(path string, wait int, out any) error {
	url := sigServer + path
	if wait > 0 {
		url += fmt.Sprintf("?wait=%d", wait)
	}
	r, err := hc.Get(url)
	if err != nil {
		return err
	}
	defer r.Body.Close()
	b, _ := io.ReadAll(r.Body)
	return json.Unmarshal(b, out)
}

func (s *signal) sendSDP(sdpType, sdp string) error {
	return s.post("/message/sdp-"+s.SessionID, map[string]string{
		"type": sdpType, "sdp": sdp,
	})
}

func (s *signal) recvSDP(wait int) (string, error) {
	var res struct {
		Type string `json:"type"`
		SDP  string `json:"sdp"`
	}
	if err := s.get("/message/sdp-"+s.SessionID, wait, &res); err != nil {
		return "", err
	}
	if res.SDP == "" {
		return "", fmt.Errorf("no sdp")
	}
	return res.SDP, nil
}

func (s *signal) sendICE(seq int, candidate string) error {
	return s.post(fmt.Sprintf("/message/ice-%s-%d", s.SessionID, seq), map[string]string{
		"candidate": candidate,
	})
}

func (s *signal) recvICE(seq, wait int) (string, error) {
	var res struct {
		Candidate string `json:"candidate"`
	}
	if err := s.get(fmt.Sprintf("/message/ice-%s-%d", s.SessionID, seq), wait, &res); err != nil {
		return "", err
	}
	if res.Candidate == "" {
		return "", fmt.Errorf("no ice")
	}
	b, _ := json.Marshal(res)
	return string(b), nil
}

func main() {
	mode := flag.String("mode", "", "p1 (create session) or p2 (join)")
	session := flag.String("session", "", "session ID (required for p2)")
	trickle := flag.Bool("trickle", false, "use trickle ICE")
	flag.Parse()

	if *mode != "p1" && *mode != "p2" {
		fmt.Println("Usage: message-signal --mode p1|p2 [--session ID] [--trickle]")
		return
	}

	sig := &signal{PeerID: *mode}

	if *mode == "p1" {
		sig.SessionID = fmt.Sprintf("dc-%d", time.Now().UnixMilli())
		fmt.Printf("SESSION_ID=%s\n", sig.SessionID)
		fmt.Println("Give this ID to p2, then press Enter...")
		fmt.Scanln()
	} else {
		if *session == "" {
			fmt.Println("--session required for p2 mode")
			return
		}
		sig.SessionID = *session
	}

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
		fmt.Printf("new peer: %v\n", err)
		return
	}
	defer peer.Close()

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

	if *trickle {
		// ——— Trickle ICE ———
		iceSeq := 0
		peer.OnICECandidate(func(candidate string) {
			iceSeq++
			if err := sig.sendICE(iceSeq, candidate); err != nil {
				fmt.Printf("send ice[%d]: %v\n", iceSeq, err)
			}
		})

		if *mode == "p1" {
			offer, err := peer.CreateOffer()
			if err != nil {
				fmt.Printf("create offer: %v\n", err)
				return
			}
			sig.sendSDP("offer", offer)
			fmt.Println("sent offer (trickle)")

			answer, err := sig.recvSDP(60)
			if err != nil {
				fmt.Printf("recv answer: %v\n", err)
				return
			}
			peer.SetRemoteDescription(answer)
			fmt.Println("got answer")
		} else {
			offer, err := sig.recvSDP(60)
			if err != nil {
				fmt.Printf("recv offer: %v\n", err)
				return
			}
			peer.SetRemoteDescription(offer)

			answer, err := peer.CreateAnswer()
			if err != nil {
				fmt.Printf("create answer: %v\n", err)
				return
			}
			sig.sendSDP("answer", answer)
			fmt.Println("sent answer (trickle)")
		}

		stopRecv := make(chan struct{})
		go func() {
			n := 0
			for {
				select {
				case <-stopRecv:
					return
				default:
				}
				n++
				c, err := sig.recvICE(n, 30)
				if err != nil {
					time.Sleep(500 * time.Millisecond)
					continue
				}
				peer.AddICECandidate(c)
			}
		}()

		select {
		case dc := <-dcReady:
			close(stopRecv)
			fmt.Println("DataChannel open!")
			startChat(*mode, dc)
		case <-time.After(30 * time.Second):
			close(stopRecv)
			fmt.Println("timeout")
		}
	} else {
		// ——— Non-Trickle ICE (embedded candidates in SDP) ———
		if *mode == "p1" {
			peer.CreateOffer()
			<-peer.GatheringComplete()
			finalOffer, err := peer.LocalDescriptionSDP()
			if err != nil {
				fmt.Printf("get offer: %v\n", err)
				return
			}
			sig.sendSDP("offer", finalOffer)
			fmt.Println("sent offer (with ICE candidates)")

			answer, err := sig.recvSDP(60)
			if err != nil {
				fmt.Printf("recv answer: %v\n", err)
				return
			}
			peer.SetRemoteDescription(answer)
			fmt.Println("got answer (with ICE candidates)")
		} else {
			offer, err := sig.recvSDP(60)
			if err != nil {
				fmt.Printf("recv offer: %v\n", err)
				return
			}
			peer.SetRemoteDescription(offer)

			peer.CreateAnswer()
			<-peer.GatheringComplete()
			finalAnswer, err := peer.LocalDescriptionSDP()
			if err != nil {
				fmt.Printf("get answer: %v\n", err)
				return
			}
			sig.sendSDP("answer", finalAnswer)
			fmt.Println("sent answer (with ICE candidates)")
		}

		select {
		case dc := <-dcReady:
			fmt.Println("DataChannel open!")
			startChat(*mode, dc)
		case <-time.After(30 * time.Second):
			fmt.Println("timeout")
		}
	}
}

func startChat(mode string, dc *pionwebrtc.DataChannel) {
	go func() {
		n := 0
		for {
			msg := fmt.Sprintf("%s says: msg %d", mode, n)
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
