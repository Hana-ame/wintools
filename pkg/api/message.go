package api

import (
	"context"
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/Hana-ame/wintools/pkg/relay"
)

type MessageHandler struct {
	relay *relay.Relay
}

func NewMessageHandler(r *relay.Relay) *MessageHandler {
	return &MessageHandler{relay: r}
}

func (h *MessageHandler) RegisterRoutes(r *gin.RouterGroup) {
	r.POST("/:id", h.Post)
	r.GET("/:id", h.Get)
}

func (h *MessageHandler) Post(c *gin.Context) {
	id := c.Param("id")

	var body any
	if err := c.ShouldBindJSON(&body); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "body must be valid JSON"})
		return
	}

	h.relay.Put(id, body)
	c.JSON(http.StatusOK, gin.H{"ok": true})
}

func (h *MessageHandler) Get(c *gin.Context) {
	id := c.Param("id")
	waitStr := c.Query("wait")

	if waitStr == "" {
		val, ok := h.relay.Get(c.Request.Context(), id)
		if !ok {
			c.JSON(http.StatusNotFound, gin.H{"error": "not found"})
			return
		}
		c.JSON(http.StatusOK, val)
		return
	}

	sec, err := strconv.Atoi(waitStr)
	if err != nil || sec <= 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid wait"})
		return
	}

	ctx, cancel := context.WithTimeout(c.Request.Context(), time.Duration(sec)*time.Second)
	defer cancel()

	val, ok := h.relay.GetWait(ctx, id)
	if !ok {
		switch ctx.Err() {
		case context.DeadlineExceeded:
			c.JSON(http.StatusRequestTimeout, gin.H{"error": "timeout"})
		case context.Canceled:
			return
		default:
			c.JSON(http.StatusNotFound, gin.H{"error": "not found"})
		}
		return
	}
	c.JSON(http.StatusOK, val)
}
