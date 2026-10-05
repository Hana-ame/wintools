package peerjs

import (
	"sync"
	"testing"
)

// TestCloseSignalChConcurrent 回归: readLoop 错误路径与 Close 并发关闭
// closeCh 的 double-close panic。
// 发现背景: 代码审阅时发现 readLoop 用 select-default-close 直接 close,
// Close 用 once.Do(close), 两者并发时 readLoop 可能对已关闭的 channel 二次
// close → panic (close of closed channel)。修复为统一走 closeSignalCh
// (共享同一 once), 并发调用幂等。
// 该测试在 -race 下并发调用 16 次, 复现旧实现的竞态并验证新实现安全。
func TestCloseSignalChConcurrent(t *testing.T) {
	p := NewPeer(Config{})
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			p.closeSignalCh()
		}()
	}
	wg.Wait()
	select {
	case <-p.closeCh:
	default:
		t.Fatal("closeCh 应已关闭")
	}
}

// TestConnectedPeers 验证 ConnectedPeers 只返回 open 连接且去重。
func TestConnectedPeers(t *testing.T) {
	p := NewPeer(Config{})

	add := func(remote, id string, open bool) {
		dc := &DataConnection{
			peer:         p,
			remote:       remote,
			connectionId: id,
			msgs:         make(chan []byte),
			openCh:       make(chan struct{}),
			closeCh:      make(chan struct{}),
		}
		dc.mu.Lock()
		dc.open = open
		dc.mu.Unlock()
		p.mu.Lock()
		p.conns[id] = dc
		p.mu.Unlock()
	}

	// 初始无连接
	if got := p.ConnectedPeers(); len(got) != 0 {
		t.Fatalf("初始应无连接, got %v", got)
	}

	// 未 open 不计入
	add("remote-1", "c1", false)
	if got := p.ConnectedPeers(); len(got) != 0 {
		t.Fatalf("未 open 连接不应计入, got %v", got)
	}

	// open 后计入，且同一 remote 多条连接去重
	add("remote-1", "c1-open", true)
	add("remote-1", "c1-dup", true)
	add("remote-2", "c2", true)
	got := p.ConnectedPeers()
	if len(got) != 2 {
		t.Fatalf("应返回 2 个远端, got %v", got)
	}
	seen := map[string]bool{}
	for _, id := range got {
		seen[id] = true
	}
	if !seen["remote-1"] || !seen["remote-2"] {
		t.Fatalf("返回远端集合不正确: %v", got)
	}

	// 关闭 open 后移除
	p.mu.Lock()
	p.conns["c1-open"].open = false
	p.conns["c1-open"].Open()
	p.mu.Unlock()
	got = p.ConnectedPeers()
	if len(got) != 2 {
		t.Fatalf("c1-open 关闭后 remote-1 仍有 dup 连接, 应仍返回 2 个远端, got %v", got)
	}

	p.mu.Lock()
	p.conns["c1-dup"].open = false
	p.mu.Unlock()
	got = p.ConnectedPeers()
	if len(got) != 1 || got[0] != "remote-2" {
		t.Fatalf("remote-1 全部关闭后只剩 remote-2, got %v", got)
	}
}
