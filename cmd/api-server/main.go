package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"sync"

	"github.com/gin-gonic/gin"
)

type Store struct {
	mu sync.RWMutex
	m  map[string]interface{}
}

func NewStore() *Store {
	return &Store{m: make(map[string]interface{})}
}

func (s *Store) Put(id string, v interface{}) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.m[id] = v
}

func (s *Store) Get(id string) (interface{}, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.m[id]
	return v, ok
}

func main() {
	port := flag.Int("port", 8080, "listen port")
	flag.Parse()

	store := NewStore()

	gin.SetMode(gin.ReleaseMode)
	r := gin.New()
	r.Use(gin.Recovery())

	r.GET("/healthz", func(c *gin.Context) {
		c.String(http.StatusOK, "ok")
	})

	r.POST("/kv/:id", func(c *gin.Context) {
		id := c.Param("id")
		var body interface{}
		if err := c.ShouldBindJSON(&body); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"ok": false, "err": "bad json"})
			return
		}
		store.Put(id, body)
		c.JSON(http.StatusOK, gin.H{"ok": true})
	})

	r.GET("/kv/:id", func(c *gin.Context) {
		id := c.Param("id")
		v, ok := store.Get(id)
		if !ok {
			c.JSON(http.StatusNotFound, gin.H{"ok": false, "err": "not found"})
			return
		}
		c.JSON(http.StatusOK, v)
	})

	addr := fmt.Sprintf(":%d", *port)
	log.Printf("API server listening on %s", addr)
	if err := r.Run(addr); err != nil {
		log.Fatalf("start server: %v", err)
	}
}
