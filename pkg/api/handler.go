package api

import (
	"context"
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/Hana-ame/wintools/pkg/kv"
)

type Handler struct {
	store *kv.Store
}

func NewHandler(store *kv.Store) *Handler {
	return &Handler{store: store}
}

func (h *Handler) RegisterRoutes(r *gin.RouterGroup) {
	r.GET("/:key", h.Get)
	r.POST("/:key", h.Set)
	r.PUT("/:key", h.ShallowMerge)
	r.PATCH("/:key", h.DeepMerge)
	r.DELETE("/:key", h.Delete)
}

func (h *Handler) Get(c *gin.Context) {
	key := c.Param("key")
	waitStr := c.Query("wait")

	if waitStr == "" {
		data, ok := h.store.Peek(key)
		if !ok {
			c.JSON(http.StatusNotFound, gin.H{"error": "not found"})
			return
		}
		c.JSON(http.StatusOK, data)
		return
	}

	sec, err := strconv.Atoi(waitStr)
	if err != nil || sec <= 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid wait"})
		return
	}

	ctx, cancel := context.WithTimeout(c.Request.Context(), time.Duration(sec)*time.Second)
	defer cancel()

	data, ok := h.store.Get(ctx, key)
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
	c.JSON(http.StatusOK, data)
}

func (h *Handler) Set(c *gin.Context) {
	key := c.Param("key")
	var body map[string]any
	if err := c.ShouldBindJSON(&body); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "body must be a JSON object"})
		return
	}
	h.store.Set(key, body)
	c.JSON(http.StatusOK, gin.H{"ok": true})
}

func (h *Handler) ShallowMerge(c *gin.Context) {
	key := c.Param("key")
	var body map[string]any
	if err := c.ShouldBindJSON(&body); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "body must be a JSON object"})
		return
	}
	h.store.ShallowMerge(key, body)
	c.JSON(http.StatusOK, gin.H{"ok": true})
}

func (h *Handler) DeepMerge(c *gin.Context) {
	key := c.Param("key")
	var body map[string]any
	if err := c.ShouldBindJSON(&body); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "body must be a JSON object"})
		return
	}
	h.store.DeepMerge(key, body)
	c.JSON(http.StatusOK, gin.H{"ok": true})
}

func (h *Handler) Delete(c *gin.Context) {
	key := c.Param("key")
	h.store.Delete(key)
	c.JSON(http.StatusOK, gin.H{"ok": true})
}
