package signaling

import (
	"encoding/json"
	"log"

	"github.com/gin-gonic/gin"
)

// registerRoutes 挂载所有路由
func (s *Server) registerRoutes() {
	// CORS 中间件
	s.engine.Use(func(c *gin.Context) {
		c.Header("Access-Control-Allow-Origin", "*")
		c.Header("Access-Control-Allow-Methods", "GET,POST,OPTIONS,PUT,DELETE")
		c.Header("Access-Control-Allow-Headers", "Content-Type")
		if c.Request.Method == "OPTIONS" {
			c.AbortWithStatus(204)
			return
		}
		c.Next()
	})

	// KV + 房间 API 全部由 kv.Store 处理
	s.kvStore.RegisterRoutes(s.engine.Group("/kv"))

	// WebSocket
	s.engine.GET("/ws", s.handleWS)

	// 根路径
	s.engine.GET("/", s.handleRoot)
}

// handleWS  WebSocket 信令（通过 KV 后端）
func (s *Server) handleWS(c *gin.Context) {
	roomID := c.Query("room")
	peer := c.Query("peer")
	if roomID == "" || (peer != "p1" && peer != "p2") {
		c.JSON(400, gin.H{"ok": false, "err": "?room=&peer=p1|p2 required"})
		return
	}

	// 检查 room 是否存在
	if _, err := s.kvStore.GetRaw(roomID); err != nil {
		c.JSON(404, gin.H{"ok": false, "err": "room not found"})
		return
	}

	conn, err := upgrader.Upgrade(c.Writer, c.Request, nil)
	if err != nil {
		log.Printf("ws upgrade: %v", err)
		return
	}

	client := &wsClient{conn: conn}
	log.Printf("ws connected: room=%s peer=%s", roomID, peer)

	defer func() {
		conn.Close()
		log.Printf("ws disconnected: room=%s peer=%s", roomID, peer)
	}()

	// 读 goroutine：接收 WS 消息，写入 KV
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			var msg WSMsg
			if err := conn.ReadJSON(&msg); err != nil {
				return
			}
			switch msg.Type {
			case "sdp":
				var body SDPBody
				if err := json.Unmarshal(msg.Data, &body); err != nil {
					continue
				}
				raw, _ := json.Marshal(body)
				s.kvStore.PutRaw(roomID+"/sdp/"+peer, raw)
			case "ice":
				var body ICEBody
				if err := json.Unmarshal(msg.Data, &body); err != nil {
					continue
				}
				other := "p2"
				if peer == "p2" {
					other = "p1"
				}
				raw, _ := json.Marshal(body)
				s.kvStore.AppendRaw(roomID+"/ice/"+other, raw)
			}
		}
	}()

	// 监听 SDP / ICE 变更，推送给 WS 客户端
	// 使用 kv 的 NotifyChan 监听对方写入
	sdpKey := roomID + "/sdp/" + peer
	iceKey := roomID + "/ice/" + peer

	sdpCh, _ := s.kvStore.NotifyChan(sdpKey)
	iceCh, _ := s.kvStore.NotifyChan(iceKey)

	for {
		select {
		case <-done:
			return
		case <-sdpCh:
			if raw, err := s.kvStore.GetRaw(sdpKey); err == nil && raw != nil {
				var sdp SDPBody
				json.Unmarshal(raw, &sdp)
				client.mu.Lock()
				conn.WriteJSON(WSPush{Type: "sdp", Data: sdp})
				client.mu.Unlock()
			}
		case <-iceCh:
			if rawArr, err := s.kvStore.ClearRaw(iceKey); err == nil && len(rawArr) > 0 {
				for _, raw := range rawArr {
					var ice ICEBody
					json.Unmarshal(raw, &ice)
					client.mu.Lock()
					conn.WriteJSON(WSPush{Type: "ice", Data: ice})
					client.mu.Unlock()
				}
			}
		}
	}
}

func (s *Server) handleRoot(c *gin.Context) {
	c.String(200, `Signaling Server — KV Backend

KV API:
  POST   /kv/create              — create KV entry
  GET    /kv/:id[?wait=N]         — get KV entry (with long-poll)
  PUT    /kv/:id                  — update KV entry (merge)
  DELETE /kv/:id                  — delete KV entry
  POST   /kv/:id/append           — append to array
  POST   /kv/:id/pop              — pop from array
  POST   /kv/:id/clear            — clear and return array

Room API:
  POST   /kv/room                 — create room
  POST   /kv/room/:id/join        — join room
  POST   /kv/room/:id/sdp?peer=p1 — send SDP
  GET    /kv/room/:id/sdp?peer=p1[&wait=N] — receive SDP (long-poll)
  POST   /kv/room/:id/ice?peer=p1 — send ICE
  GET    /kv/room/:id/ice?peer=p1[&wait=N][&all] — receive ICE

WebSocket:
  WS     /ws?room=xxx&peer=p1`)
}
