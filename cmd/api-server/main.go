package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/Hana-ame/wintools/pkg/api"
	"github.com/Hana-ame/wintools/pkg/kv"
	"github.com/Hana-ame/wintools/pkg/relay"
)

func main() {
	port := flag.Int("port", 8080, "listen port")
	ttl := flag.Duration("ttl", 0, "key TTL (0=no eviction)")
	tick := flag.Duration("tick", 30*time.Second, "eviction check interval")
	flag.Parse()

	store := kv.NewStore(*ttl, *tick)
	defer store.Stop()

	h := api.NewHandler(store)

	gin.SetMode(gin.ReleaseMode)
	r := gin.New()
	r.Use(gin.Recovery())

	r.GET("/healthz", func(c *gin.Context) {
		c.String(http.StatusOK, "ok")
	})

	grp := r.Group("/kv")
	h.RegisterRoutes(grp)

	relayStore := relay.NewRelay(0)
	defer relayStore.Stop()

	mh := api.NewMessageHandler(relayStore)
	grpMsg := r.Group("/message")
	mh.RegisterRoutes(grpMsg)

	addr := fmt.Sprintf(":%d", *port)
	log.Printf("API server listening on %s", addr)
	if err := r.Run(addr); err != nil {
		log.Fatalf("start server: %v", err)
	}
}
