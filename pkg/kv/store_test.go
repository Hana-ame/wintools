package kv

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestSetAndPeek(t *testing.T) {
	s := NewStore(0, time.Hour)
	defer s.Stop()

	s.Set("k1", map[string]any{"a": 1})
	data, ok := s.Peek("k1")
	if !ok {
		t.Fatal("Peek returned false after Set")
	}
	if data["a"] != 1 {
		t.Errorf("data[a] = %v, want 1", data["a"])
	}
}

func TestPeekMissing(t *testing.T) {
	s := NewStore(0, time.Hour)
	defer s.Stop()

	_, ok := s.Peek("nonexist")
	if ok {
		t.Fatal("Peek should return false for missing key")
	}
}

func TestShallowMerge(t *testing.T) {
	s := NewStore(0, time.Hour)
	defer s.Stop()

	s.Set("k", map[string]any{"a": 1, "b": 2})
	s.ShallowMerge("k", map[string]any{"b": 3, "c": 4})

	data, _ := s.Peek("k")
	if data["a"] != 1 {
		t.Errorf("data[a] = %v, want 1", data["a"])
	}
	if data["b"] != 3 {
		t.Errorf("data[b] = %v, want 3 (overwritten)", data["b"])
	}
	if data["c"] != 4 {
		t.Errorf("data[c] = %v, want 4 (added)", data["c"])
	}
}

func TestDeepMerge(t *testing.T) {
	s := NewStore(0, time.Hour)
	defer s.Stop()

	s.Set("k", map[string]any{
		"top": map[string]any{"inner": 1, "keep": 2},
	})
	s.DeepMerge("k", map[string]any{
		"top": map[string]any{"inner": 99, "new": 3},
	})

	data, _ := s.Peek("k")
	top := data["top"].(map[string]any)
	if top["inner"] != 99 {
		t.Errorf("top[inner] = %v, want 99", top["inner"])
	}
	if top["keep"] != 2 {
		t.Errorf("top[keep] = %v, want 2 (preserved)", top["keep"])
	}
	if top["new"] != 3 {
		t.Errorf("top[new] = %v, want 3 (added)", top["new"])
	}
}

func TestDelete(t *testing.T) {
	s := NewStore(0, time.Hour)
	defer s.Stop()

	s.Set("k", map[string]any{"v": 1})
	s.Delete("k")
	_, ok := s.Peek("k")
	if ok {
		t.Fatal("Peek should return false after Delete")
	}
}

func TestGetBlockingThenSet(t *testing.T) {
	s := NewStore(0, time.Hour)
	defer s.Stop()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		data, ok := s.Get(ctx, "block")
		if !ok {
			t.Errorf("Get returned false after Set")
		}
		if data["v"] != 1 {
			t.Errorf("data[v] = %v, want 1", data["v"])
		}
	}()

	time.Sleep(50 * time.Millisecond)
	s.Set("block", map[string]any{"v": 1})
	wg.Wait()
}

func TestGetTimeout(t *testing.T) {
	s := NewStore(0, time.Hour)
	defer s.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	_, ok := s.Get(ctx, "timeout")
	if ok {
		t.Fatal("Get should return false on timeout")
	}
}

func TestGetCancel(t *testing.T) {
	s := NewStore(0, time.Hour)
	defer s.Stop()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, ok := s.Get(ctx, "cancel")
	if ok {
		t.Fatal("Get should return false on cancelled context")
	}
}

func TestDeleteWakesWaiters(t *testing.T) {
	s := NewStore(0, time.Hour)
	defer s.Stop()

	done := make(chan bool)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, ok := s.Get(ctx, "wakeme")
		done <- ok
	}()

	time.Sleep(50 * time.Millisecond)
	s.Delete("wakeme")

	if <-done {
		t.Fatal("Get should return false after Delete")
	}
}

func TestTTLEviction(t *testing.T) {
	s := NewStore(100*time.Millisecond, 20*time.Millisecond)
	defer s.Stop()

	s.Set("k", map[string]any{"x": 1})
	time.Sleep(200 * time.Millisecond)

	_, ok := s.Peek("k")
	if ok {
		t.Fatal("key should be evicted after TTL")
	}
}

func TestTTLTouchOnPeek(t *testing.T) {
	s := NewStore(200*time.Millisecond, 50*time.Millisecond)
	defer s.Stop()

	s.Set("k", map[string]any{"x": 1})
	time.Sleep(150 * time.Millisecond)
	s.Peek("k")
	time.Sleep(100 * time.Millisecond)

	_, ok := s.Peek("k")
	if !ok {
		t.Fatal("Peek should refresh lastTouch and prevent eviction")
	}
}

func TestSetOnNilEntry(t *testing.T) {
	s := NewStore(0, time.Hour)
	defer s.Stop()

	s.Set("new", map[string]any{"hello": "world"})
	data, ok := s.Peek("new")
	if !ok {
		t.Fatal("Set on new key should succeed")
	}
	if data["hello"] != "world" {
		t.Errorf("data[hello] = %v, want world", data["hello"])
	}
}

func TestDeepMergeOnNilEntry(t *testing.T) {
	s := NewStore(0, time.Hour)
	defer s.Stop()

	s.DeepMerge("new", map[string]any{"a": 1})
	data, ok := s.Peek("new")
	if !ok {
		t.Fatal("DeepMerge on new key should succeed")
	}
	if data["a"] != 1 {
		t.Errorf("data[a] = %v, want 1", data["a"])
	}
}

func TestShallowMergeOnNilEntry(t *testing.T) {
	s := NewStore(0, time.Hour)
	defer s.Stop()

	s.ShallowMerge("new", map[string]any{"a": 1})
	data, ok := s.Peek("new")
	if !ok {
		t.Fatal("ShallowMerge on new key should succeed")
	}
	if data["a"] != 1 {
		t.Errorf("data[a] = %v, want 1", data["a"])
	}
}

func TestConcurrentSetAndGet(t *testing.T) {
	s := NewStore(0, time.Hour)
	defer s.Stop()

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			key := "k"
			s.Set(key, map[string]any{"n": n})
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			_, ok := s.Get(ctx, key)
			if !ok {
				t.Errorf("Get returned false after Set (n=%d)", n)
			}
		}(i)
	}
	wg.Wait()
}
