// goclient: PeerJS Go client - file server with discovery.
// Connects to a signaling server, announces itself with collections,
// and serves files over WebRTC DataChannel.
package main

import (
"bytes"
"crypto/rand"
"encoding/binary"
"encoding/hex"
"encoding/json"
"flag"
"fmt"
"io"
"log"
"net/http"
"net/url"
"os"
"path"
"path/filepath"
"strings"
"sync"
"sync/atomic"
"time"

"github.com/gorilla/websocket"
"github.com/pion/webrtc/v4"
)

const peerVersion = "1.5.4"
const chunkSize = 64 << 10

type Header struct {
Type     string  `json:"type"`
Path     string  `json:"path,omitempty"`
Offset   int64   `json:"offset,omitempty"`
Size     int64   `json:"size,omitempty"`
Total    int64   `json:"total,omitempty"`
ReqID    string  `json:"reqId,omitempty"`
StreamID uint32  `json:"streamId,omitempty"`
Msg      string  `json:"msg,omitempty"`
Entries  []Entry `json:"entries,omitempty"`
}

type Entry struct {
Name string `json:"name"`
Dir  bool   `json:"dir,omitempty"`
Size int64  `json:"size,omitempty"`
}

type peerClient struct {
serverURL   string
discoverURL string
id          string
dir         string
collections []string

ws *websocket.Conn
wm sync.Mutex

pc *webrtc.PeerConnection
dc *webrtc.DataChannel

remoteID   string
connected  atomic.Bool
iceServers []webrtc.ICEServer
quit       chan struct{}

dcMu      sync.Mutex
streamSeq atomic.Uint32
	startTime    time.Time
	uploadBytes  atomic.Int64
	downloadBytes atomic.Int64
}

func main() {
server := flag.String("server", "ws://127.0.0.1:8000/peerjs", "PeerServer websocket url")
key := flag.String("key", "peerjs", "server key")
id := flag.String("id", "go-peer", "our peer id")
dir := flag.String("dir", ".", "directory to serve")
collections := flag.String("collections", "media", "comma separated collections for discovery")
stun := flag.String("stun", "stun:stun.l.google.com:19302", "comma separated stun/turn urls")
flag.Parse()

absDir, err := filepath.Abs(*dir)
if err != nil {
log.Fatalf("bad dir: %v", err)
}

cfg := webrtc.Configuration{}
for _, u := range strings.Split(*stun, ",") {
u = strings.TrimSpace(u)
if u == "" {
continue
}
cfg.ICEServers = append(cfg.ICEServers, webrtc.ICEServer{URLs: []string{u}})
}

colls := make([]string, 0)
for _, c := range strings.Split(*collections, ",") {
if c = strings.TrimSpace(c); c != "" {
colls = append(colls, c)
}
}

p := &peerClient{
id:          *id,
dir:         absDir,
iceServers:  cfg.ICEServers,
quit:        make(chan struct{}),
collections: colls,
		startTime:    time.Now(),
}

u, err := url.Parse(*server)
if err != nil {
log.Fatalf("bad server url: %v", err)
}
q := u.Query()
q.Set("key", *key)
q.Set("id", p.id)
q.Set("token", randomToken())
q.Set("version", peerVersion)
u.RawQuery = q.Encode()
p.serverURL = u.String()

// Build discover URL (http instead of ws)
discU := *u
discU.Scheme = "http"
discU.RawQuery = ""
	discU.Path = ""
p.discoverURL = discU.String()

if err := p.connectSignaling(); err != nil {
log.Fatalf("signaling connect failed: %v", err)
}

// Announce self with collections
p.announce()

log.Printf("[go-peer] serving %s as %q (collections: %v), waiting for browser...", absDir, p.id, colls)
go p.heartbeatLoop()
// Periodic re-announce (every 60s)
go func() {
t := time.NewTicker(60 * time.Second)
defer t.Stop()
for {
select {
case <-p.quit:
return
case <-t.C:
p.announce()
}
}
}()
select {}
}

func (p *peerClient) announce() {
body, _ := json.Marshal(map[string]any{
"peerId":      p.id,
		"uptime":        int64(time.Since(p.startTime).Seconds()),
		"uploadBytes":   p.uploadBytes.Load(),
		"downloadBytes": p.downloadBytes.Load(),
"nodeType": "file",
		"collections": p.collections,
})
resp, err := http.Post(p.discoverURL+"/discover/announce", "application/json", bytes.NewReader(body))
if err != nil {
log.Printf("[go-peer] announce failed: %v", err)
return
}
resp.Body.Close()
log.Printf("[go-peer] announced: %s uptime=%d upload=%d download=%d collections=%v", p.id, int64(time.Since(p.startTime).Seconds()), p.uploadBytes.Load(), p.downloadBytes.Load(), p.collections)
}

func (p *peerClient) connectSignaling() error {
var err error
for i := 0; i < 10; i++ {
conn, _, derr := websocket.DefaultDialer.Dial(p.serverURL, nil)
if derr == nil {
p.ws = conn
log.Printf("[go-peer] connected to signaling %s", p.serverURL)
go p.readLoop()
return nil
}
err = derr
log.Printf("[go-peer] signaling dial retry %d: %v", i+1, derr)
time.Sleep(time.Second)
}
return err
}

func (p *peerClient) sendSignaling(v any) error {
data, err := json.Marshal(v)
if err != nil {
return err
}
p.wm.Lock()
defer p.wm.Unlock()
if p.ws == nil {
return fmt.Errorf("signaling socket closed")
}
return p.ws.WriteMessage(websocket.TextMessage, data)
}

func (p *peerClient) heartbeatLoop() {
t := time.NewTicker(5 * time.Second)
defer t.Stop()
for {
select {
case <-p.quit:
return
case <-t.C:
p.sendSignaling(map[string]string{"type": "HEARTBEAT"})
}
}
}

func (p *peerClient) readLoop() {
for {
_, data, err := p.ws.ReadMessage()
if err != nil {
log.Printf("[go-peer] signaling closed: %v", err)
return
}
var m struct {
Type    string          `json:"type"`
Src     string          `json:"src"`
Payload json.RawMessage `json:"payload"`
}
if err := json.Unmarshal(data, &m); err != nil {
continue
}
switch m.Type {
case "OPEN":
log.Printf("[go-peer] registered on signaling as %q", p.id)
case "OFFER":
p.handleOffer(m.Src, m.Payload)
case "ANSWER":
p.handleAnswer(m.Src, m.Payload)
case "CANDIDATE":
p.handleCandidate(m.Payload)
case "LEAVE":
log.Printf("[go-peer] remote %s left", m.Src)
case "ERROR", "ID-TAKEN":
log.Printf("[go-peer] server error: %s", string(data))
}
}
}

type offerPayload struct {
SDP struct {
Type string `json:"type"`
SDP  string `json:"sdp"`
} `json:"sdp"`
Type          string `json:"type"`
ConnectionID  string `json:"connectionId"`
Label         string `json:"label"`
Reliable      bool   `json:"reliable"`
Serialization string `json:"serialization"`
}

type answerPayload struct {
SDP struct {
Type string `json:"type"`
SDP  string `json:"sdp"`
} `json:"sdp"`
Type         string `json:"type"`
ConnectionID string `json:"connectionId"`
}

type candidatePayload struct {
Candidate    webrtc.ICECandidateInit `json:"candidate"`
Type         string                  `json:"type"`
ConnectionID string                  `json:"connectionId"`
}

func (p *peerClient) handleOffer(src string, raw json.RawMessage) error {
if p.pc != nil {
p.pc.Close()
p.pc = nil
p.dc = nil
}
var off offerPayload
if err := json.Unmarshal(raw, &off); err != nil {
return err
}
if off.Type != "data" {
return fmt.Errorf("unsupported connection type %q", off.Type)
}
log.Printf("[go-peer] OFFER from %s (serialization=%s)", src, off.Serialization)
p.remoteID = src
pc, err := p.newPeerConnection(off.ConnectionID)
if err != nil {
return err
}
p.pc = pc
if err := pc.SetRemoteDescription(webrtc.SessionDescription{
Type: webrtc.SDPTypeOffer, SDP: off.SDP.SDP,
}); err != nil {
return fmt.Errorf("setRemoteDescription: %w", err)
}
answer, err := pc.CreateAnswer(nil)
if err != nil {
return fmt.Errorf("createAnswer: %w", err)
}
if err := pc.SetLocalDescription(answer); err != nil {
return fmt.Errorf("setLocalDescription: %w", err)
}
payload := answerPayload{Type: "data", ConnectionID: off.ConnectionID}
payload.SDP.Type = "answer"
payload.SDP.SDP = answer.SDP
return p.sendSignaling(map[string]any{
"type":    "ANSWER",
"dst":     src,
"payload": payload,
})
}

func (p *peerClient) handleAnswer(src string, raw json.RawMessage) error {
var ans answerPayload
if err := json.Unmarshal(raw, &ans); err != nil {
return err
}
if p.pc == nil {
return fmt.Errorf("ANSWER without a peer connection")
}
return p.pc.SetRemoteDescription(webrtc.SessionDescription{
Type: webrtc.SDPTypeAnswer, SDP: ans.SDP.SDP,
})
}

func (p *peerClient) handleCandidate(raw json.RawMessage) error {
var cp candidatePayload
if err := json.Unmarshal(raw, &cp); err != nil {
return err
}
if p.pc == nil {
return fmt.Errorf("CANDIDATE before peer connection")
}
return p.pc.AddICECandidate(cp.Candidate)
}

func (p *peerClient) newPeerConnection(connID string) (*webrtc.PeerConnection, error) {
cfg := webrtc.Configuration{ICEServers: p.iceServers}
pc, err := webrtc.NewPeerConnection(cfg)
if err != nil {
return nil, err
}
pc.OnICECandidate(func(c *webrtc.ICECandidate) {
if c == nil {
return
}
payload := candidatePayload{
Candidate:    c.ToJSON(),
Type:         "data",
ConnectionID: connID,
}
p.sendSignaling(map[string]any{
"type":    "CANDIDATE",
"dst":     p.remoteID,
"payload": payload,
})
})
pc.OnConnectionStateChange(func(s webrtc.PeerConnectionState) {
log.Printf("[go-peer] peerconnection state: %s", s)
})
pc.OnDataChannel(func(dc *webrtc.DataChannel) {
p.setupDataChannel(dc)
})
return pc, nil
}

func (p *peerClient) setupDataChannel(dc *webrtc.DataChannel) {
p.dc = dc
dc.OnOpen(func() {
log.Printf("[go-peer] data channel OPEN, serving %s", p.dir)
})
dc.OnMessage(func(msg webrtc.DataChannelMessage) {
if !msg.IsString {
return
}
var h Header
if err := json.Unmarshal(msg.Data, &h); err != nil {
return
}
switch h.Type {
case "list":
p.handleList(dc, h)
case "read":
go p.handleRead(dc, h)
}
})
dc.OnClose(func() {
log.Printf("[go-peer] data channel closed")
p.dc = nil
})
}

func (p *peerClient) sendHeader(dc *webrtc.DataChannel, h Header) {
b, _ := json.Marshal(h)
p.dcMu.Lock()
dc.SendText(string(b))
p.dcMu.Unlock()
}

func (p *peerClient) sendBinaryChunk(dc *webrtc.DataChannel, streamID uint32, data []byte) error {
frame := make([]byte, 4+len(data))
binary.BigEndian.PutUint32(frame[:4], streamID)
copy(frame[4:], data)
p.dcMu.Lock()
err := dc.Send(frame)
p.dcMu.Unlock()
return err
}

func (p *peerClient) handleList(dc *webrtc.DataChannel, h Header) {
name := path.Clean("/" + h.Path)
full := filepath.Join(p.dir, name)
entries, err := os.ReadDir(full)
if err != nil {
p.sendHeader(dc, Header{Type: "err", ReqID: h.ReqID, Msg: err.Error()})
return
}
out := make([]Entry, 0, len(entries))
for _, e := range entries {
ent := Entry{Name: e.Name(), Dir: e.IsDir()}
if !e.IsDir() {
if info, err := e.Info(); err == nil {
ent.Size = info.Size()
}
}
out = append(out, ent)
}
p.sendHeader(dc, Header{Type: "entries", ReqID: h.ReqID, Entries: out})
}

func (p *peerClient) handleRead(dc *webrtc.DataChannel, h Header) {
name := path.Clean("/" + h.Path)
full := filepath.Join(p.dir, name)
f, err := os.Open(full)
if err != nil {
p.sendHeader(dc, Header{Type: "err", ReqID: h.ReqID, Msg: err.Error()})
return
}
defer f.Close()
fi, err := f.Stat()
if err != nil {
p.sendHeader(dc, Header{Type: "err", ReqID: h.ReqID, Msg: err.Error()})
return
}
if fi.IsDir() {
p.sendHeader(dc, Header{Type: "err", ReqID: h.ReqID, Msg: "is a directory"})
return
}
size := fi.Size()
if h.Offset < 0 || h.Offset > size {
p.sendHeader(dc, Header{Type: "err", ReqID: h.ReqID, Msg: "offset out of range"})
return
}
if _, err := f.Seek(h.Offset, io.SeekStart); err != nil {
p.sendHeader(dc, Header{Type: "err", ReqID: h.ReqID, Msg: err.Error()})
return
}
total := size - h.Offset
if h.Size >= 0 && h.Size < total {
total = h.Size
}
streamID := p.streamSeq.Add(1)
p.sendHeader(dc, Header{Type: "meta", ReqID: h.ReqID, Total: total, StreamID: streamID})
buf := make([]byte, chunkSize)
remaining := total
for remaining > 0 {
want := int64(chunkSize)
if remaining < want {
want = remaining
}
nr, err := io.ReadFull(f, buf[:want])
if nr > 0 {
if err := p.sendBinaryChunk(dc, streamID, buf[:nr]); err != nil {
return
}
remaining -= int64(nr)
}
if err != nil {
p.sendHeader(dc, Header{Type: "err", ReqID: h.ReqID, Msg: "read: " + err.Error()})
return
}
}
p.sendHeader(dc, Header{Type: "done", ReqID: h.ReqID})
}

func randomToken() string {
b := make([]byte, 8)
rand.Read(b)
return hex.EncodeToString(b)
}
