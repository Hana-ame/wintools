package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/Hana-ame/wintools/pkg/kv"
)

func setupTest(t *testing.T) (*kv.Store, *Handler, *gin.Engine) {
	t.Helper()
	store := kv.NewStore(0, time.Hour)
	t.Cleanup(store.Stop)

	h := NewHandler(store)

	gin.SetMode(gin.TestMode)
	r := gin.New()
	grp := r.Group("/kv")
	h.RegisterRoutes(grp)
	r.GET("/healthz", func(c *gin.Context) {
		c.String(http.StatusOK, "ok")
	})

	return store, h, r
}

func readBody(t *testing.T, resp *http.Response) map[string]any {
	t.Helper()
	var body map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	return body
}

func TestSetAndGet(t *testing.T) {
	_, _, r := setupTest(t)

	// POST set
	w := httptest.NewRecorder()
	req := httptest.NewRequest("POST", "/kv/test", strings.NewReader(`{"a":1}`))
	req.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(w, req)
	if w.Code != 200 {
		t.Fatalf("POST status = %d", w.Code)
	}

	// GET without wait
	w = httptest.NewRecorder()
	req = httptest.NewRequest("GET", "/kv/test", nil)
	r.ServeHTTP(w, req)
	if w.Code != 200 {
		t.Fatalf("GET status = %d", w.Code)
	}
	body := readBody(t, w.Result())
	if body["a"] != 1.0 {
		t.Errorf("body[a] = %v, want 1", body["a"])
	}
}

func TestGetMissing(t *testing.T) {
	_, _, r := setupTest(t)

	w := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/kv/none", nil)
	r.ServeHTTP(w, req)
	if w.Code != 404 {
		t.Fatalf("GET status = %d, want 404", w.Code)
	}
}

func TestGetWithWaitTimeout(t *testing.T) {
	_, _, r := setupTest(t)

	w := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/kv/none?wait=1", nil)
	r.ServeHTTP(w, req)
	if w.Code != 408 {
		t.Fatalf("GET with wait status = %d, want 408", w.Code)
	}
}

func TestShallowMerge(t *testing.T) {
	_, _, r := setupTest(t)

	// POST set
	w := httptest.NewRecorder()
	req := httptest.NewRequest("POST", "/kv/m", strings.NewReader(`{"a":1,"b":2}`))
	req.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(w, req)

	// PUT merge
	w = httptest.NewRecorder()
	req = httptest.NewRequest("PUT", "/kv/m", strings.NewReader(`{"b":3,"c":4}`))
	req.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(w, req)

	// GET verify
	w = httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest("GET", "/kv/m", nil))
	body := readBody(t, w.Result())
	if body["a"] != 1.0 {
		t.Errorf("body[a] = %v, want 1", body["a"])
	}
	if body["b"] != 3.0 {
		t.Errorf("body[b] = %v, want 3", body["b"])
	}
	if body["c"] != 4.0 {
		t.Errorf("body[c] = %v, want 4", body["c"])
	}
}

func TestDeepMerge(t *testing.T) {
	_, _, r := setupTest(t)

	// POST set
	w := httptest.NewRecorder()
	req := httptest.NewRequest("POST", "/kv/d", strings.NewReader(`{"top":{"inner":1,"keep":2}}`))
	req.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(w, req)

	// PATCH deep merge
	w = httptest.NewRecorder()
	req = httptest.NewRequest("PATCH", "/kv/d", strings.NewReader(`{"top":{"inner":99,"new":3}}`))
	req.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(w, req)

	// GET verify
	w = httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest("GET", "/kv/d", nil))
	body := readBody(t, w.Result())
	top := body["top"].(map[string]any)
	if top["inner"] != 99.0 {
		t.Errorf("top[inner] = %v, want 99", top["inner"])
	}
	if top["keep"] != 2.0 {
		t.Errorf("top[keep] = %v, want 2", top["keep"])
	}
	if top["new"] != 3.0 {
		t.Errorf("top[new] = %v, want 3", top["new"])
	}
}

func TestDelete(t *testing.T) {
	_, _, r := setupTest(t)

	// POST set
	w := httptest.NewRecorder()
	req := httptest.NewRequest("POST", "/kv/del", strings.NewReader(`{"x":1}`))
	req.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(w, req)

	// DELETE
	w = httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest("DELETE", "/kv/del", nil))

	// GET verify gone
	w = httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest("GET", "/kv/del", nil))
	if w.Code != 404 {
		t.Errorf("GET after DELETE status = %d, want 404", w.Code)
	}
}

func TestRejectNonObject(t *testing.T) {
	_, _, r := setupTest(t)

	cases := []string{`"string"`, `[1,2,3]`, `123`, `true`}
	for _, body := range cases {
		w := httptest.NewRecorder()
		req := httptest.NewRequest("POST", "/kv/bad", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		r.ServeHTTP(w, req)
		if w.Code != 400 {
			t.Errorf("POST with %s status = %d, want 400", body, w.Code)
		}
	}
}

func TestInvalidWaitParam(t *testing.T) {
	_, _, r := setupTest(t)

	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest("GET", "/kv/x?wait=abc", nil))
	if w.Code != 400 {
		t.Errorf("GET with invalid wait status = %d, want 400", w.Code)
	}
}

func TestHealthz(t *testing.T) {
	_, _, r := setupTest(t)

	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest("GET", "/healthz", nil))
	if w.Code != 200 {
		t.Fatalf("healthz status = %d", w.Code)
	}
	if w.Body.String() != "ok" {
		t.Errorf("healthz body = %q, want ok", w.Body.String())
	}
}

func TestGetWithWaitWakesOnSet(t *testing.T) {
	_, _, r := setupTest(t)

	// long poll for "wake"
	done := make(chan map[string]any)
	go func() {
		w := httptest.NewRecorder()
		r.ServeHTTP(w, httptest.NewRequest("GET", "/kv/wake?wait=5", nil))
		body := readBody(t, w.Result())
		done <- body
	}()

	time.Sleep(50 * time.Millisecond)

	// Set to wake the long poll
	w := httptest.NewRecorder()
	req := httptest.NewRequest("POST", "/kv/wake", strings.NewReader(`{"got":"ya"}`))
	req.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(w, req)

	select {
	case body := <-done:
		if body["got"] != "ya" {
			t.Errorf("body[got] = %v, want ya", body["got"])
		}
	case <-time.After(3 * time.Second):
		t.Fatal("long poll not woken by Set")
	}
}
