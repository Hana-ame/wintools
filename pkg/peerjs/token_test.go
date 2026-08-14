package peerjs

import (
	"strings"
	"testing"
)

// TestRandomToken 验证 token/connectionId 生成:
// 16 位十六进制 + 两次调用不同。
// 发现背景: 代码审阅时发现 randomToken 用 math/rand (可预测),
// connectionId 被猜到可被中间人注入 OFFER/ANSWER 劫持数据连接。
// 修复: 换 crypto/rand (密码学安全)。
func TestRandomToken(t *testing.T) {
	const hexdig = "0123456789abcdef"
	a := randomToken()
	if len(a) != 16 {
		t.Fatalf("token 长度应为 16, got %d (%q)", len(a), a)
	}
	for _, c := range a {
		if !strings.ContainsRune(hexdig, c) {
			t.Fatalf("token 含非十六进制字符: %q", a)
			return
		}
	}
	b := randomToken()
	if a == b {
		t.Fatalf("两次 token 相同: %q", a)
	}
}
