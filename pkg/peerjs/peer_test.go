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
