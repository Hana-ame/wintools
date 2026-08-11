package main

import (
    "bytes"
    "encoding/json"
    "flag"
    "fmt"
    "log"
    "net/http"
    "time"

    "github.com/google/uuid"
    "github.com/pion/webrtc/v4"
)

type sigMessage struct {
    Type string `json:"type"`
    SDP  string `json:"sdp"`
}

func postJSON(url string, v interface{}) error {
    b, err := json.Marshal(v)
    if err != nil {
        return err
    }
    resp, err := http.Post(url, "application/json", bytes.NewReader(b))
    if err != nil {
        return err
    }
    defer resp.Body.Close()
    if resp.StatusCode >= 400 {
        return fmt.Errorf("post %s returned %d", url, resp.StatusCode)
    }
    return nil
}

func getJSON(url string, out interface{}) error {
    resp, err := http.Get(url)
    if err != nil {
        return err
    }
    defer resp.Body.Close()
    if resp.StatusCode >= 400 {
        return fmt.Errorf("get %s returned %d", url, resp.StatusCode)
    }
    return json.NewDecoder(resp.Body).Decode(out)
}

func newPeer() (*webrtc.PeerConnection, *webrtc.DataChannel, <-chan struct{}, error) {
    // STUN server list – default Google public STUN
    iceServers := []webrtc.ICEServer{{URLs: []string{"stun:stun.l.google.com:19302"}}}
    cfg := webrtc.Configuration{ICEServers: iceServers}
    pc, err := webrtc.NewPeerConnection(cfg)
    if err != nil {
        return nil, nil, nil, err
    }

    // Gather complete signal
    gatheringDone := make(chan struct{})
    pc.OnICECandidate(func(c *webrtc.ICECandidate) {
        if c == nil {
            close(gatheringDone)
        }
    })

    // Create a reliable ordered DataChannel "chat"
    ordered := true
    dc, err := pc.CreateDataChannel("chat", &webrtc.DataChannelInit{Ordered: &ordered})
    if err != nil {
        pc.Close()
        return nil, nil, nil, err
    }
    return pc, dc, gatheringDone, nil
}

func waitGathering(ch <-chan struct{}) {
    select {
    case <-ch:
    case <-time.After(30 * time.Second):
        log.Println("ICE gathering timeout")
    }
}

func runP1(server string) {
    roomID := uuid.New().String()
    fmt.Printf("Created room ID: %s\n", roomID)

    pc, dc, gatheringDone, err := newPeer()
    if err != nil {
        log.Fatalf("peer creation failed: %v", err)
    }
    defer pc.Close()

    // Handle inbound messages
    dc.OnMessage(func(msg webrtc.DataChannelMessage) {
        fmt.Printf("[p1] received: %s\n", string(msg.Data))
    })
    // Wait for DataChannel open
    opened := make(chan struct{})
    dc.OnOpen(func() { close(opened) })

    // Create Offer (non‑trickle – wait for ICE gathering)
    offer, err := pc.CreateOffer(nil)
    if err != nil {
        log.Fatalf("create offer: %v", err)
    }
    if err = pc.SetLocalDescription(offer); err != nil {
        log.Fatalf("set local desc: %v", err)
    }
    waitGathering(gatheringDone)
    local := pc.LocalDescription().SDP
    // Send offer via KV store
    offerPath := fmt.Sprintf("%s/kv/room/%s/offer", server, roomID)
    if err = postJSON(offerPath, sigMessage{Type: "offer", SDP: local}); err != nil {
        log.Fatalf("post offer: %v", err)
    }
    fmt.Println("Offer posted, waiting for answer...")
    // Wait for answer
    answerPath := fmt.Sprintf("%s/kv/room/%s/answer?wait=30", server, roomID)
    var ansMsg sigMessage
    if err = getJSON(answerPath, &ansMsg); err != nil {
        log.Fatalf("get answer: %v", err)
    }
    // Apply remote answer
    if err = pc.SetRemoteDescription(webrtc.SessionDescription{Type: webrtc.SDPTypeAnswer, SDP: ansMsg.SDP}); err != nil {
        log.Fatalf("set remote desc: %v", err)
    }
    fmt.Println("Answer applied, connection should be ready")

    // Wait for DataChannel to open
    select {
    case <-opened:
        fmt.Println("DataChannel open, sending ping...")
    case <-time.After(15 * time.Second):
        log.Fatal("data channel never opened")
    }

    // Send a test message
    if err = dc.Send([]byte("ping from p1")); err != nil {
        log.Fatalf("send ping: %v", err)
    }
    // Keep running to receive response
    select {}
}

func runP2(server, roomID string) {
    fmt.Printf("Joining room ID: %s\n", roomID)
    pc, dc, gatheringDone, err := newPeer()
    if err != nil {
        log.Fatalf("peer creation failed: %v", err)
    }
    defer pc.Close()

    // Handle inbound messages
    dc.OnMessage(func(msg webrtc.DataChannelMessage) {
        fmt.Printf("[p2] received: %s\n", string(msg.Data))
    })
    opened := make(chan struct{})
    dc.OnOpen(func() { close(opened) })

    // Retrieve offer
    offerPath := fmt.Sprintf("%s/kv/room/%s/offer?wait=30", server, roomID)
    var offerMsg sigMessage
    if err = getJSON(offerPath, &offerMsg); err != nil {
        log.Fatalf("get offer: %v", err)
    }
    // Apply remote offer
    if err = pc.SetRemoteDescription(webrtc.SessionDescription{Type: webrtc.SDPTypeOffer, SDP: offerMsg.SDP}); err != nil {
        log.Fatalf("set remote offer: %v", err)
    }

    // Create answer
    answer, err := pc.CreateAnswer(nil)
    if err != nil {
        log.Fatalf("create answer: %v", err)
    }
    if err = pc.SetLocalDescription(answer); err != nil {
        log.Fatalf("set local answer: %v", err)
    }
    waitGathering(gatheringDone)
    local := pc.LocalDescription().SDP
    // Store answer
    answerPath := fmt.Sprintf("%s/kv/room/%s/answer", server, roomID)
    if err = postJSON(answerPath, sigMessage{Type: "answer", SDP: local}); err != nil {
        log.Fatalf("post answer: %v", err)
    }
    fmt.Println("Answer posted, waiting for data channel...")
    // Wait for DataChannel open
    select {
    case <-opened:
        fmt.Println("DataChannel open, sending reply...")
    case <-time.After(15 * time.Second):
        log.Fatal("data channel never opened on p2")
    }
    // Send reply
    if err = dc.Send([]byte("pong from p2")); err != nil {
        log.Fatalf("send pong: %v", err)
    }
    // Keep alive to receive ping
    select {}
}

func main() {
    server := flag.String("server", "http://bwh.moonchan.xyz:8080", "signaling server base URL")
    mode := flag.String("mode", "", "p1 to create room, p2 to join")
    room := flag.String("room", "", "room ID (required for p2)")
    flag.Parse()
    if *mode != "p1" && *mode != "p2" {
        fmt.Println("usage: -mode p1|p2 [-room ID] [-server URL]")
        return
    }
    if *mode == "p1" {
        runP1(*server)
    } else {
        if *room == "" {
            fmt.Println("-room is required for p2 mode")
            return
        }
        runP2(*server, *room)
    }
}
