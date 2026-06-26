// Package kv 提供一个并发安全的内存键值存储。
// 支持设置、读取（含阻塞等待）、合并（浅层/深层）、删除和 TTL 自动过期。
package kv

import (
	"context"
	"sync"
	"time"
)

// Entry 是单个键对应的存储条目。
// 每个条目独立加锁，减少锁竞争。
type Entry struct {
	mu        sync.RWMutex
	data      map[string]any   // 实际存储的 JSON 对象数据
	lastTouch time.Time        // 最后一次访问时间，用于 TTL 过期判断
	broadCh   chan struct{}    // 广播通道，关闭时唤醒所有阻塞的 Get 调用
}

// newEntry 创建一个新的空条目。
// lastTouch 初始化为当前时间，broadCh 为未关闭的 channel。
func newEntry() *Entry {
	return &Entry{
		lastTouch: time.Now(),
		broadCh:   make(chan struct{}),
	}
}

// deepMerge 递归合并 src 到 dst。
// 仅当 dst 和 src 的同一字段都是 map[string]any 时才会递归，
// 否则 src 直接覆盖 dst。
func deepMerge(dst, src map[string]any) map[string]any {
	for k, v := range src {
		if dstV, ok := dst[k]; ok {
			if dstMap, ok1 := dstV.(map[string]any); ok1 {
				if srcMap, ok2 := v.(map[string]any); ok2 {
					dst[k] = deepMerge(dstMap, srcMap)
					continue
				}
			}
		}
		dst[k] = v
	}
	return dst
}

// Store 是内存键值存储的核心结构。
// 零值不可用，须通过 NewStore 创建。
type Store struct {
	mu   sync.RWMutex        // 保护 m 映射表
	m    map[string]*Entry   // 键到条目的映射
	ttl  time.Duration       // 过期时间（0 表示永不过期）
	done chan struct{}       // 关闭时通知 evictLoop 退出
}

// NewStore 创建并启动一个 Store。
//
//   - ttl:  条目过期时间。设为 0 则永不过期。
//   - tick: 过期清理循环的执行间隔。
//
// 返回的 Store 会在后台启动一个 goroutine 定期清理过期条目，
// 不再使用时应调用 Stop() 停止该 goroutine。
func NewStore(ttl, tick time.Duration) *Store {
	s := &Store{
		m:    make(map[string]*Entry),
		ttl:  ttl,
		done: make(chan struct{}),
	}
	go s.evictLoop(tick)
	return s
}

// Stop 停止后台过期清理 goroutine。
// 调用后 Store 仍可正常读写，但不再自动清理过期条目。
func (s *Store) Stop() {
	close(s.done)
}

// getOrCreateEntry 返回指定 key 的条目，如果不存在则创建新条目。
// 使用双重检查锁定（double-checked locking）减少锁争用。
func (s *Store) getOrCreateEntry(key string) *Entry {
	// 第一次：乐观读锁检查
	s.mu.RLock()
	e, ok := s.m[key]
	s.mu.RUnlock()
	if ok {
		return e
	}

	// 第二次：写锁，确认确实不存在后创建
	s.mu.Lock()
	defer s.mu.Unlock()
	if e, ok = s.m[key]; ok {
		return e
	}
	e = newEntry()
	s.m[key] = e
	return e
}

// Set 设置 key 的值为 val。
// 如果 key 已存在，旧值被完全替换。
// 每次 Set 都会关闭旧的 broadcast channel 并创建新的，
// 从而唤醒所有阻塞在该 key 上的 Get 调用。
func (s *Store) Set(key string, val map[string]any) {
	e := s.getOrCreateEntry(key)
	e.mu.Lock()
	defer e.mu.Unlock()

	e.data = val
	e.lastTouch = time.Now()

	// 唤醒等待者：关闭旧 channel，新建一个替换
	close(e.broadCh)
	e.broadCh = make(chan struct{})
}

// ShallowMerge 对 key 的值执行浅层合并。
// 仅覆盖 val 中存在的顶层字段，不递归嵌套。
// 如果 key 不存在，则直接设置 val 为新值。
func (s *Store) ShallowMerge(key string, val map[string]any) {
	e := s.getOrCreateEntry(key)
	e.mu.Lock()
	defer e.mu.Unlock()

	e.lastTouch = time.Now()

	if e.data == nil {
		e.data = val
	} else {
		for k, v := range val {
			e.data[k] = v
		}
	}

	close(e.broadCh)
	e.broadCh = make(chan struct{})
}

// DeepMerge 对 key 的值执行深层递归合并。
// 嵌套的 map 会递归合并，非 map 字段直接覆盖。
// 如果 key 不存在，则直接设置 val 为新值。
func (s *Store) DeepMerge(key string, val map[string]any) {
	e := s.getOrCreateEntry(key)
	e.mu.Lock()
	defer e.mu.Unlock()

	e.lastTouch = time.Now()

	if e.data == nil {
		e.data = val
	} else {
		deepMerge(e.data, val)
	}

	close(e.broadCh)
	e.broadCh = make(chan struct{})
}

// Get 返回 key 的值。
//
//   - 如果 key 已有数据则立即返回 true。
//   - 如果 key 无数据，阻塞等待直到数据到达（Set/Merge）或 ctx 被取消/超时。
//   - 如果 key 被删除，broadCh 也会关闭，此时返回 (nil, false)。
//
// 注意：Get 在无数据时会一直阻塞，建议传入带超时的 context。Peek
// 提供了非阻塞版本。
func (s *Store) Get(ctx context.Context, key string) (map[string]any, bool) {
	e := s.getOrCreateEntry(key)

	// 先读一下数据和 channel 的快照
	e.mu.RLock()
	data := e.data
	broadCh := e.broadCh
	e.mu.RUnlock()

	// 数据已存在，立即返回
	if data != nil {
		e.mu.Lock()
		e.lastTouch = time.Now()
		e.mu.Unlock()
		return data, true
	}

	// 阻塞等待数据到达或 context 取消
	select {
	case <-broadCh:
		// 被 Set/Merge/Delete 唤醒
		e.mu.Lock()
		e.lastTouch = time.Now()
		data = e.data
		e.mu.Unlock()
		return data, data != nil
	case <-ctx.Done():
		// 超时或 context 取消
		return nil, false
	}
}

// Peek 非阻塞地获取 key 的值。
// 如果 key 不存在或无数据，返回 (nil, false)。
// 如果 key 有数据，返回数据并刷新 lastTouch 防止 TTL 过期。
func (s *Store) Peek(key string) (map[string]any, bool) {
	e := s.getOrCreateEntry(key)
	e.mu.RLock()
	data := e.data
	e.mu.RUnlock()
	if data != nil {
		// 刷新访问时间
		e.mu.Lock()
		e.lastTouch = time.Now()
		e.mu.Unlock()
	}
	return data, data != nil
}

// Delete 删除指定 key 的条目。
// 会唤醒所有阻塞在该 key 上的 Get 调用，它们将收到 (nil, false)。
// 删除不存在的 key 是安全的无操作。
func (s *Store) Delete(key string) {
	// 从映射表中移除
	s.mu.Lock()
	e, ok := s.m[key]
	if ok {
		delete(s.m, key)
	}
	s.mu.Unlock()

	if ok {
		// 唤醒等待者
		e.mu.Lock()
		close(e.broadCh)
		e.mu.Unlock()
	}
}

// evictLoop 是后台 goroutine，按 tick 间隔定期清理过期条目。
// 使用双重检查锁定：先读锁扫描，再写锁二次确认后删除。
// ttl=0 表示永不过期，跳过清理。
func (s *Store) evictLoop(tick time.Duration) {
	if s.ttl == 0 {
		return
	}
	ticker := time.NewTicker(tick)
	defer ticker.Stop()
	for {
		select {
		case <-s.done:
			return
		case now := <-ticker.C:
			var expiredKeys []string

			// 第一阶段：读锁扫描，收集可能过期的 key
			s.mu.RLock()
			for k, e := range s.m {
				e.mu.RLock()
				if now.After(e.lastTouch.Add(s.ttl)) {
					expiredKeys = append(expiredKeys, k)
				}
				e.mu.RUnlock()
			}
			s.mu.RUnlock()

			if len(expiredKeys) == 0 {
				continue
			}

			// 第二阶段：写锁二次确认，防止在扫描间隙被刷新
			s.mu.Lock()
			for _, k := range expiredKeys {
				if e, ok := s.m[k]; ok {
					e.mu.Lock()
					if time.Now().After(e.lastTouch.Add(s.ttl)) {
						delete(s.m, k)
						close(e.broadCh)
					}
					e.mu.Unlock()
				}
			}
			s.mu.Unlock()
		}
	}
}
