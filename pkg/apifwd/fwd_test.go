package apifwd

// TestHandlerBodyLimit 验证 POST body 超限返回 413, 不触达上游。
// 发现背景: 代码审阅时发现 handler 用 io.ReadAll 无上限缓存 body,
// 超大上传会 OOM (反代把 body 整体读进内存做 Modify)。
// 修复: LimitReader 限长读入, 超 maxBodySize 直接 413。
import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
)

func TestHandlerBodyLimit(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	// Dest 指向不可达地址: 若超限检查失效, 测试会因连接失败走 502,
	// 而不是 413, 从而暴露问题。
	r.Any("/*any", Handler(Option{Dest: "http://127.0.0.1:1", Local: true}))

	body := bytes.NewReader(make([]byte, maxBodySize+1))
	req := httptest.NewRequest(http.MethodPost, "/kv", body)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("超限 body 应返回 413, got %d", w.Code)
	}

	// 小 body 正常进入转发流程 (上游不可达 → 502), 证明限制没有误伤。
	small := httptest.NewRequest(http.MethodPost, "/kv", bytes.NewReader([]byte(`{"a":1}`)))
	w2 := httptest.NewRecorder()
	r.ServeHTTP(w2, small)
	if w2.Code != http.StatusBadGateway {
		t.Fatalf("小 body 应进入转发 (502), got %d", w2.Code)
	}
}
