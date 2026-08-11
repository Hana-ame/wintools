package peerjs

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/pion/webrtc/v4"
)

// HTTP helpers (kept local to avoid extra deps).
func newGET(ctx context.Context, url string) (*http.Request, error) {
	return http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
}

func doHTTP(req *http.Request, timeout time.Duration) (*http.Response, error) {
	c := &http.Client{Timeout: timeout}
	return c.Do(req)
}

func newPeerConnection(connectionId string) (*webrtc.PeerConnection, error) {
	config := webrtc.Configuration{
		ICEServers: []webrtc.ICEServer{
			{URLs: []string{"stun:stun.l.google.com:19302"}},
			{
				URLs: []string{
					"turn:eu-0.turn.peerjs.com:3478",
					"turn:us-0.turn.peerjs.com:3478",
				},
				Username:   "peerjs",
				Credential: "peerjsp",
			},
		},
	}
	pc, err := webrtc.NewPeerConnection(config)
	if err != nil {
		return nil, fmt.Errorf("new peer connection: %w", err)
	}
	return pc, nil
}

// DataConnection is one WebRTC DataChannel between two peers.
type DataConnection struct {
	peer          *Peer
	remote        string
	connectionId  string
	label         string
	serialization string
	reliable      bool

	pc  *webrtc.PeerConnection
	rdc *webrtc.DataChannel

	mu      sync.Mutex
	open    bool
	msgs    chan []byte
	closeCh chan struct{}
	onMsg   func([]byte)
	onOpen  func()
	onClose func()

	iceMu sync.Mutex
}

func (dc *DataConnection) Remote() string { return dc.remote }

func (dc *DataConnection) Open() bool {
	dc.mu.Lock()
	defer dc.mu.Unlock()
	return dc.open
}

func (dc *DataConnection) OnMessage(cb func([]byte)) {
	dc.mu.Lock()
	dc.onMsg = cb
	dc.mu.Unlock()
}

func (dc *DataConnection) OnOpen(cb func()) {
	dc.mu.Lock()
	dc.onOpen = cb
	dc.mu.Unlock()
}

func (dc *DataConnection) OnClose(cb func()) {
	dc.mu.Lock()
	dc.onClose = cb
	dc.mu.Unlock()
}

// onOffer is called on the answering side when the OFFER arrives, before
// SDP processing, to create the PC and wire the DataChannel.
func (dc *DataConnection) onOffer() {
	pc, err := newPeerConnection(dc.connectionId)
	if err != nil {
		dc.peer.Debugf("onOffer: %v", err)
		return
	}
	dc.pc = pc

	pc.OnDataChannel(func(ch *webrtc.DataChannel) {
		dc.rdc = ch
		dc.wireDataChannel()
	})

	pc.OnICECandidate(func(c *webrtc.ICECandidate) {
		if c == nil {
			return
		}
		dc.peer.send(ServerMessage{
			Type: "CANDIDATE",
			Dst:  dc.remote,
			Payload: mustJSON(map[string]any{
				"candidate":    c.ToJSON(),
				"type":         "data",
				"connectionId": dc.connectionId,
			}),
		})
	})
}

func (dc *DataConnection) wireDataChannel() {
	dc.rdc.OnOpen(func() {
		dc.mu.Lock()
		dc.open = true
		cb := dc.onOpen
		dc.mu.Unlock()
		dc.peer.Debugf("datachannel open conn=%s", dc.connectionId)
		if cb != nil {
			cb()
		}
	})
	dc.rdc.OnMessage(func(msg webrtc.DataChannelMessage) {
		dc.mu.Lock()
		cb := dc.onMsg
		dc.mu.Unlock()
		if cb != nil {
			cb(msg.Data)
		}
	})
	dc.rdc.OnClose(func() {
		dc.Close()
	})
}

func (dc *DataConnection) handleOffer(src string, sdp json.RawMessage) {
	var sd struct {
		SDP string `json:"sdp"`
	}
	json.Unmarshal(sdp, &sd)
	offer := webrtc.SessionDescription{Type: webrtc.SDPTypeOffer, SDP: sd.SDP}
	if err := dc.pc.SetRemoteDescription(offer); err != nil {
		dc.peer.Debugf("set remote offer: %v", err)
		return
	}
	answer, err := dc.pc.CreateAnswer(nil)
	if err != nil {
		dc.peer.Debugf("create answer: %v", err)
		return
	}
	if err := dc.pc.SetLocalDescription(answer); err != nil {
		dc.peer.Debugf("set local answer: %v", err)
		return
	}
	dc.peer.send(ServerMessage{
		Type: "ANSWER",
		Dst:  src,
		Payload: mustJSON(map[string]any{
			"sdp": map[string]string{
				"type": "answer",
				"sdp":  answer.SDP,
			},
			"type":         "data",
			"connectionId": dc.connectionId,
		}),
	})
	dc.peer.Debugf("answer sent to %s conn=%s", src, dc.connectionId)
}

func (dc *DataConnection) handleAnswer(sdp json.RawMessage) {
	var sd struct {
		SDP string `json:"sdp"`
	}
	json.Unmarshal(sdp, &sd)
	answer := webrtc.SessionDescription{Type: webrtc.SDPTypeAnswer, SDP: sd.SDP}
	if err := dc.pc.SetRemoteDescription(answer); err != nil {
		dc.peer.Debugf("set remote answer: %v", err)
	}
}

func (dc *DataConnection) handleCandidate(cand json.RawMessage) {
	var c webrtc.ICECandidateInit
	if err := json.Unmarshal(cand, &c); err != nil {
		dc.peer.Debugf("bad candidate: %v", err)
		return
	}
	if err := dc.pc.AddICECandidate(c); err != nil {
		dc.peer.Debugf("add candidate: %v", err)
	}
}

// Send writes raw bytes on the DataChannel (ordered).
func (dc *DataConnection) Send(data []byte) error {
	if !dc.Open() {
		return fmt.Errorf("dataconnection not open")
	}
	return dc.rdc.Send(data)
}

func (dc *DataConnection) Close() {
	dc.mu.Lock()
	if !dc.open {
		if dc.pc != nil {
			dc.pc.Close()
		}
		dc.mu.Unlock()
		return
	}
	dc.open = false
	cb := dc.onClose
	dc.mu.Unlock()

	if dc.rdc != nil {
		dc.rdc.Close()
	}
	if dc.pc != nil {
		dc.pc.Close()
	}
	dc.peer.mu.Lock()
	delete(dc.peer.conns, dc.connectionId)
	dc.peer.mu.Unlock()
	if cb != nil {
		cb()
	}
}
