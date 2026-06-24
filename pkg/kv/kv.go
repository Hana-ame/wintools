package kv

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

type entry struct {
	data      json.RawMessage
	lastTouch time.Time
	notifyCh  chan struct{}
	once      sync.Once
}

func (e *entry) getNotifyCh() chan struct{} {
	e.once.Do(func() {
		e.notifyCh = make(chan struct{}, 1)
	})
	return e.notifyCh
}

func (e *entry) notify() {
	ch := e.getNotifyCh()
	select {
	case ch <- struct{}{}:
	default:
	}
}

type Store struct {
	mu    sync.RWMutex
	m     map[string]*entry
	done  chan struct{}
	ttl   time.Duration
	tick  time.Duration
}

func NewStore() *Store {
	s := &Store{
		m:    make(map[string]*entry),
		done: make(chan struct{}),
		ttl:  5 * time.Minute,
		tick: 30 * time.Second,
	}
	go s.evictLoop()
	return s
}

func (s *Store) Stop() {
	close(s.done)
}

func (s *Store) evictLoop() {
	t := time.NewTicker(s.tick)
	defer t.Stop()
	for {
		select {
		case <-s.done:
			return
		case now := <-t.C:
			s.mu.Lock()
			for id, e := range s.m {
				if now.After(e.lastTouch.Add(s.ttl)) {
					delete(s.m, id)
				}
			}
			s.mu.Unlock()
		}
	}
}

func (s *Store) touch(id string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.m[id]
	if !ok {
		return false
	}
	e.lastTouch = time.Now()
	return true
}

// getEntry returns entry with lock held (caller must unlock)
func (s *Store) getEntry(id string) (*entry, bool) {
	e, ok := s.m[id]
	if !ok {
		return nil, false
	}
	e.lastTouch = time.Now()
	return e, true
}

// ========== 通用 KV 操作 ==========

// Create  POST /kv/create
func (s *Store) Create(c *gin.Context) {
	id := uuid.NewString()
	s.mu.Lock()
	s.m[id] = &entry{lastTouch: time.Now()}
	s.mu.Unlock()
	c.JSON(http.StatusOK, gin.H{"id": id})
}

// Get  GET /kv/:id[?wait=N]
func (s *Store) Get(c *gin.Context) {
	id := c.Param("id")
	wait := 0
	fmt.Sscanf(c.Query("wait"), "%d", &wait)

	s.mu.RLock()
	e, ok := s.m[id]
	if !ok {
		s.mu.RUnlock()
		c.JSON(http.StatusNotFound, gin.H{"ok": false, "err": "not found"})
		return
	}
	e.lastTouch = time.Now()

	if wait > 0 && e.data == nil {
		ch := e.getNotifyCh()
		s.mu.RUnlock()
		select {
		case <-ch:
			s.mu.RLock()
			e2, ok2 := s.m[id]
			if !ok2 {
				s.mu.RUnlock()
				c.JSON(http.StatusNotFound, gin.H{"ok": false, "err": "not found"})
				return
			}
			e2.lastTouch = time.Now()
			data := e2.data
			s.mu.RUnlock()
			if data == nil {
				c.JSON(http.StatusOK, nil)
				return
			}
			var v interface{}
			if err := json.Unmarshal(data, &v); err != nil {
				c.JSON(http.StatusOK, data)
				return
			}
			c.JSON(http.StatusOK, v)
			return
		case <-time.After(time.Duration(wait) * time.Second):
			c.JSON(http.StatusOK, gin.H{"ok": false, "err": "timeout"})
			return
		}
	}

	data := e.data
	s.mu.RUnlock()

	if data == nil {
		c.JSON(http.StatusOK, nil)
		return
	}
	var v interface{}
	if err := json.Unmarshal(data, &v); err != nil {
		c.JSON(http.StatusOK, data)
		return
	}
	c.JSON(http.StatusOK, v)
}

// Update  PUT /kv/:id
func (s *Store) Update(c *gin.Context) {
	id := c.Param("id")
	var body interface{}
	if err := c.ShouldBindJSON(&body); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"ok": false, "err": "bad json"})
		return
	}

	s.mu.Lock()
	e, ok := s.m[id]
	if !ok {
		s.mu.Unlock()
		c.JSON(http.StatusNotFound, gin.H{"ok": false, "err": "not found"})
		return
	}
	e.lastTouch = time.Now()

	var cur map[string]interface{}
	if e.data != nil {
		if err := json.Unmarshal(e.data, &cur); err != nil {
			cur = nil
		}
	}
	update, ok := body.(map[string]interface{})
	if ok && cur != nil {
		for k, v := range update {
			cur[k] = v
		}
		merged, _ := json.Marshal(cur)
		e.data = merged
	} else {
		raw, _ := json.Marshal(body)
		e.data = raw
	}
	s.mu.Unlock()

	c.JSON(http.StatusOK, gin.H{"ok": true})
}

// Delete  DELETE /kv/:id
func (s *Store) Delete(c *gin.Context) {
	id := c.Param("id")
	s.mu.Lock()
	_, ok := s.m[id]
	if !ok {
		s.mu.Unlock()
		c.JSON(http.StatusNotFound, gin.H{"ok": false, "err": "not found"})
		return
	}
	delete(s.m, id)
	s.mu.Unlock()
	c.JSON(http.StatusOK, gin.H{"ok": true})
}

// ========== 数组操作 (ICE 队列用) ==========

// AppendToArray POST /kv/:id/append
func (s *Store) AppendToArray(c *gin.Context) {
	id := c.Param("id")
	var val interface{}
	if err := c.ShouldBindJSON(&val); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"ok": false, "err": "bad json"})
		return
	}

	s.mu.Lock()
	e, ok := s.m[id]
	if !ok {
		s.mu.Unlock()
		c.JSON(http.StatusNotFound, gin.H{"ok": false, "err": "not found"})
		return
	}
	e.lastTouch = time.Now()

	var arr []interface{}
	if e.data != nil {
		json.Unmarshal(e.data, &arr)
	}
	arr = append(arr, val)
	e.data, _ = json.Marshal(arr)
	e.notify()
	s.mu.Unlock()

	c.JSON(http.StatusOK, gin.H{"ok": true})
}

// PopFromArray POST /kv/:id/pop
func (s *Store) PopFromArray(c *gin.Context) {
	id := c.Param("id")

	s.mu.Lock()
	e, ok := s.m[id]
	if !ok {
		s.mu.Unlock()
		c.JSON(http.StatusNotFound, gin.H{"ok": false, "err": "not found"})
		return
	}
	e.lastTouch = time.Now()

	var arr []interface{}
	if e.data != nil {
		json.Unmarshal(e.data, &arr)
	}
	if len(arr) == 0 {
		s.mu.Unlock()
		c.JSON(http.StatusOK, gin.H{"ok": false, "err": "empty"})
		return
	}
	val := arr[0]
	arr = arr[1:]
	e.data, _ = json.Marshal(arr)
	s.mu.Unlock()

	c.JSON(http.StatusOK, val)
}

// ClearArray POST /kv/:id/clear
func (s *Store) ClearArray(c *gin.Context) {
	id := c.Param("id")

	s.mu.Lock()
	e, ok := s.m[id]
	if !ok {
		s.mu.Unlock()
		c.JSON(http.StatusNotFound, gin.H{"ok": false, "err": "not found"})
		return
	}
	e.lastTouch = time.Now()

	var arr []interface{}
	if e.data != nil {
		json.Unmarshal(e.data, &arr)
	}
	e.data = nil
	s.mu.Unlock()

	c.JSON(http.StatusOK, arr)
}

// ========== 房间操作 ==========

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

// CreateRoom  POST /kv/room
func (s *Store) CreateRoom(c *gin.Context) {
	id := uuid.NewString()
	s.mu.Lock()
	s.m[id] = &entry{lastTouch: time.Now()}
	s.m[id+"/sdp/p1"] = &entry{lastTouch: time.Now()}
	s.m[id+"/sdp/p2"] = &entry{lastTouch: time.Now()}
	s.m[id+"/ice/p1"] = &entry{lastTouch: time.Now()}
	s.m[id+"/ice/p2"] = &entry{lastTouch: time.Now()}
	s.mu.Unlock()
	c.JSON(http.StatusOK, gin.H{"room_id": id})
}

// JoinRoom  POST /kv/room/:id/join
func (s *Store) JoinRoom(c *gin.Context) {
	roomID := c.Param("id")

	s.mu.Lock()
	_, ok := s.m[roomID]
	if !ok {
		s.mu.Unlock()
		c.JSON(http.StatusNotFound, gin.H{"ok": false, "err": "room not found"})
		return
	}

	_, p1Taken := s.m[roomID+"/p1"]
	_, p2Taken := s.m[roomID+"/p2"]

	if !p1Taken {
		s.m[roomID+"/p1"] = &entry{lastTouch: time.Now()}
		s.mu.Unlock()
		c.JSON(http.StatusOK, gin.H{"peer": "p1"})
		return
	}
	if !p2Taken {
		s.m[roomID+"/p2"] = &entry{lastTouch: time.Now()}
		s.mu.Unlock()
		c.JSON(http.StatusOK, gin.H{"peer": "p2"})
		return
	}
	s.mu.Unlock()
	c.JSON(http.StatusOK, gin.H{"ok": false, "err": "room full"})
}

// PutSDP  POST /kv/room/:id/sdp
func (s *Store) PutSDP(c *gin.Context) {
	roomID := c.Param("id")
	peer := c.Query("peer")
	if peer != "p1" && peer != "p2" {
		c.JSON(http.StatusBadRequest, gin.H{"ok": false, "err": "?peer=p1|p2 required"})
		return
	}

	var body SDPBody
	if err := c.ShouldBindJSON(&body); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"ok": false, "err": "bad json"})
		return
	}
	raw, _ := json.Marshal(body)

	key := roomID + "/sdp/" + peer
	s.mu.Lock()
	e, ok := s.m[key]
	if !ok {
		s.mu.Unlock()
		c.JSON(http.StatusNotFound, gin.H{"ok": false, "err": "room not found"})
		return
	}
	e.lastTouch = time.Now()
	e.data = raw
	e.notify()
	s.mu.Unlock()

	c.JSON(http.StatusOK, gin.H{"ok": true})
}

// GetSDP  GET /kv/room/:id/sdp?peer=p1[&wait=N]
func (s *Store) GetSDP(c *gin.Context) {
	roomID := c.Param("id")
	peer := c.Query("peer")
	if peer != "p1" && peer != "p2" {
		c.JSON(http.StatusBadRequest, gin.H{"ok": false, "err": "?peer=p1|p2 required"})
		return
	}

	wait := 0
	fmt.Sscanf(c.Query("wait"), "%d", &wait)

	// 读对方的 SDP (p1 读 sdp/p2, p2 读 sdp/p1)
	other := "p2"
	if peer == "p2" {
		other = "p1"
	}
	srcKey := roomID + "/sdp/" + other

	// 检查 room 是否存在
	s.mu.RLock()
	_, roomExists := s.m[roomID]
	s.mu.RUnlock()
	if !roomExists {
		c.JSON(http.StatusNotFound, gin.H{"ok": false, "err": "room not found"})
		return
	}

	// 先看对方有没有写 SDP
	s.mu.RLock()
	e, ok := s.m[srcKey]
	if ok && e.data != nil {
		data := e.data
		e.lastTouch = time.Now()
		e.data = nil // 清除源码（已消费）
		s.mu.RUnlock()
		var sdp SDPBody
		json.Unmarshal(data, &sdp)
		c.JSON(http.StatusOK, sdp)
		return
	}
	s.mu.RUnlock()

	if wait <= 0 {
		c.JSON(http.StatusOK, gin.H{"ok": false, "err": "no data"})
		return
	}

	// 长轮询
	s.mu.RLock()
	we, wok := s.m[srcKey]
	if !wok {
		s.mu.RUnlock()
		c.JSON(http.StatusNotFound, gin.H{"ok": false, "err": "room not found"})
		return
	}
	ch := we.getNotifyCh()
	s.mu.RUnlock()

	timeout := time.After(time.Duration(wait) * time.Second)
	for {
		select {
		case <-ch:
			s.mu.Lock()
			e2, ok2 := s.m[srcKey]
			if ok2 && e2.data != nil {
				data := e2.data
				e2.lastTouch = time.Now()
				e2.data = nil // 清除
				s.mu.Unlock()
				var sdp SDPBody
				json.Unmarshal(data, &sdp)
				c.JSON(http.StatusOK, sdp)
				return
			}
			s.mu.Unlock()
		case <-timeout:
			c.JSON(http.StatusOK, gin.H{"ok": false, "err": "timeout"})
			return
		}
	}
}

// PutICE  POST /kv/room/:id/ice
func (s *Store) PutICE(c *gin.Context) {
	roomID := c.Param("id")
	peer := c.Query("peer")
	if peer != "p1" && peer != "p2" {
		c.JSON(http.StatusBadRequest, gin.H{"ok": false, "err": "?peer=p1|p2 required"})
		return
	}

	var body ICEBody
	if err := c.ShouldBindJSON(&body); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"ok": false, "err": "bad json"})
		return
	}

	// 存入对方队列 (p1 的 ICE → p2 读)
	other := "p2"
	if peer == "p2" {
		other = "p1"
	}
	key := roomID + "/ice/" + other

	s.mu.Lock()
	e, ok := s.m[key]
	if !ok {
		s.mu.Unlock()
		c.JSON(http.StatusNotFound, gin.H{"ok": false, "err": "room not found"})
		return
	}
	e.lastTouch = time.Now()

	var arr []interface{}
	if e.data != nil {
		json.Unmarshal(e.data, &arr)
	}
	arr = append(arr, body)
	e.data, _ = json.Marshal(arr)
	e.notify()
	s.mu.Unlock()

	c.JSON(http.StatusOK, gin.H{"ok": true})
}

// GetICE  GET /kv/room/:id/ice?peer=p1[&wait=N][&all]
func (s *Store) GetICE(c *gin.Context) {
	roomID := c.Param("id")
	peer := c.Query("peer")
	if peer != "p1" && peer != "p2" {
		c.JSON(http.StatusBadRequest, gin.H{"ok": false, "err": "?peer=p1|p2 required"})
		return
	}

	// 检查 room 是否存在
	s.mu.RLock()
	_, roomExists := s.m[roomID]
	s.mu.RUnlock()
	if !roomExists {
		c.JSON(http.StatusNotFound, gin.H{"ok": false, "err": "room not found"})
		return
	}

	key := roomID + "/ice/" + peer

	// ?all — 返回全部并清除
	if _, exists := c.GetQuery("all"); exists {
		s.mu.Lock()
		e, ok := s.m[key]
		if !ok {
			s.mu.Unlock()
			c.JSON(http.StatusNotFound, gin.H{"ok": false, "err": "room not found"})
			return
		}
		e.lastTouch = time.Now()
		var arr []interface{}
		if e.data != nil {
			json.Unmarshal(e.data, &arr)
		}
		e.data = nil
		s.mu.Unlock()
		c.JSON(http.StatusOK, arr)
		return
	}

	wait := 0
	fmt.Sscanf(c.Query("wait"), "%d", &wait)

	// 先看看有没有
	s.mu.Lock()
	e, ok := s.m[key]
	if !ok {
		s.mu.Unlock()
		c.JSON(http.StatusNotFound, gin.H{"ok": false, "err": "room not found"})
		return
	}
	e.lastTouch = time.Now()

	var arr []interface{}
	if e.data != nil {
		json.Unmarshal(e.data, &arr)
	}
	if len(arr) > 0 {
		val := arr[0]
		arr = arr[1:]
		e.data, _ = json.Marshal(arr)
		s.mu.Unlock()
		c.JSON(http.StatusOK, val)
		return
	}
	s.mu.Unlock()

	if wait <= 0 {
		c.JSON(http.StatusOK, gin.H{"ok": false, "err": "no data"})
		return
	}

	// 长轮询
	s.mu.RLock()
	we, wok := s.m[key]
	if !wok {
		s.mu.RUnlock()
		c.JSON(http.StatusNotFound, gin.H{"ok": false, "err": "room not found"})
		return
	}
	ch := we.getNotifyCh()
	s.mu.RUnlock()

	timeout := time.After(time.Duration(wait) * time.Second)
	for {
		select {
		case <-ch:
			s.mu.Lock()
			e2, ok2 := s.m[key]
			if ok2 {
				e2.lastTouch = time.Now()
				var arr2 []interface{}
				if e2.data != nil {
					json.Unmarshal(e2.data, &arr2)
				}
				if len(arr2) > 0 {
					val := arr2[0]
					arr2 = arr2[1:]
					e2.data, _ = json.Marshal(arr2)
					s.mu.Unlock()
					c.JSON(http.StatusOK, val)
					return
				}
			}
			s.mu.Unlock()
		case <-timeout:
			c.JSON(http.StatusOK, gin.H{"ok": false, "err": "timeout"})
			return
		}
	}
}

// ========== 路由注册 ==========

// RegisterRoutes 挂载所有路由到 router 上，路径前缀 /kv
func (s *Store) RegisterRoutes(r *gin.RouterGroup) {
	// 通用 KV
	r.POST("/create", s.Create)
	r.GET("/:id", s.Get)
	r.PUT("/:id", s.Update)
	r.DELETE("/:id", s.Delete)

	// 数组操作
	r.POST("/:id/append", s.AppendToArray)
	r.POST("/:id/pop", s.PopFromArray)
	r.POST("/:id/clear", s.ClearArray)

	// 房间操作
	r.POST("/room", s.CreateRoom)
	r.POST("/room/:id/join", s.JoinRoom)

		r.POST("/room/:id/sdp", s.PutSDP)
	r.GET("/room/:id/sdp", s.GetSDP)
	r.POST("/room/:id/ice", s.PutICE)
	r.GET("/room/:id/ice", s.GetICE)
}

// ========== 程序化接口（非 HTTP handler） ==========

func ErrIsNotFound(err error) bool {
	return err != nil && err.Error() == "not found"
}

func (s *Store) GetRaw(id string) (json.RawMessage, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.m[id]
	if !ok {
		return nil, fmt.Errorf("not found")
	}
	e.lastTouch = time.Now()
	return e.data, nil
}

// PutRaw sets raw data for a key (notify waiters)
func (s *Store) PutRaw(id string, data json.RawMessage) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.m[id]
	if !ok {
		return fmt.Errorf("not found")
	}
	e.lastTouch = time.Now()
	e.data = data
	e.notify()
	return nil
}

// AppendRaw appends to a JSON array stored at key (notify waiters)
func (s *Store) AppendRaw(id string, val json.RawMessage) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.m[id]
	if !ok {
		return fmt.Errorf("not found")
	}
	e.lastTouch = time.Now()
	var arr []json.RawMessage
	if e.data != nil {
		json.Unmarshal(e.data, &arr)
	}
	arr = append(arr, val)
	e.data, _ = json.Marshal(arr)
	e.notify()
	return nil
}

// ClearRaw clears and returns a JSON array stored at key
func (s *Store) ClearRaw(id string) ([]json.RawMessage, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.m[id]
	if !ok {
		return nil, fmt.Errorf("not found")
	}
	e.lastTouch = time.Now()
	var arr []json.RawMessage
	if e.data != nil {
		json.Unmarshal(e.data, &arr)
	}
	e.data = nil
	return arr, nil
}

// NotifyChan returns the notification channel for a key
func (s *Store) NotifyChan(id string) (<-chan struct{}, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	e, ok := s.m[id]
	if !ok {
		return nil, fmt.Errorf("not found")
	}
	return e.getNotifyCh(), nil
}
