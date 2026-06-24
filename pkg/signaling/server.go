package signaling

import (
	"log"
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/Hana-ame/wintools/pkg/kv"
)

// Server 信令服务器，管理 KV Store + Gin 引擎
type Server struct {
	kvStore *kv.Store
	engine  *gin.Engine
}

// New 创建信令服务器
func New() *Server {
	s := &Server{
		kvStore: kv.NewStore(),
	}
	s.engine = gin.Default()
	s.registerRoutes()
	return s
}

// Run 启动 HTTP 服务器（阻塞）
func (s *Server) Run(addr string) error {
	defer s.kvStore.Stop()
	log.Printf("signaling server on %s", addr)
	return s.engine.Run(addr)
}

// Handler 返回 http.Handler（用于测试）
func (s *Server) Handler() http.Handler {
	return s.engine
}
