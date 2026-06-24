package signaling

import (
	"encoding/json"
	"net/http"
	"sync"

	"github.com/gorilla/websocket"
)

// SDPBody SDP 消息体
type SDPBody struct {
	Type string `json:"type"`
	SDP  string `json:"sdp"`
}

// ICEBody ICE 候选消息体
type ICEBody struct {
	Candidate        string  `json:"candidate"`
	SDPMid           string  `json:"sdpMid,omitempty"`
	SDPMLineIndex    *uint16 `json:"sdpMLineIndex,omitempty"`
	UsernameFragment string  `json:"usernameFragment,omitempty"`
}

// WSPush 服务器 → 客户端推送
type WSPush struct {
	Type string      `json:"type"`
	Data interface{} `json:"data"`
}

// WSMsg 客户端 → 服务器消息
type WSMsg struct {
	Type string          `json:"type"`
	Data json.RawMessage `json:"data"`
}

type wsClient struct {
	conn *websocket.Conn
	mu   sync.Mutex
}

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}
