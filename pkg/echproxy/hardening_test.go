package echproxy

// 本文件集中本次加固 (hardening) 的回归测试:
// 解压炸弹防护 / cookie 持久化 / 通配多级子域。
// 每个测试的 doc comment 注明发现背景与修复方式 (AGENTS.md 要求)。

import (
	"bytes"
	"compress/gzip"
	"testing"
)

// TestDecompressBodyBombLimit 验证解压炸弹防护: 压缩体本身很小,
// 但解压后超过 maxRewriteSize 必须报错而不是返回巨量内存。
// 发现背景: 代码审阅时注意到 decompressBody 对 gzip/br/zstd 用无上限
// io.ReadAll, 输入虽被 LimitReader 限 8MB, 但压缩比可达数百倍,
// 8MB 压缩体可炸出 GB 级内存 (OOM)。修复: readDecompressed 限长解压,
// 超限报错, 调用方回退原样透传压缩体。
func TestDecompressBodyBombLimit(t *testing.T) {
	// 重复文本压缩比极高: ~8MB 明文压缩后仅几 KB。
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	chunk := bytes.Repeat([]byte("echo proxy hardening test data "), 64)
	for buf.Len() < maxRewriteSize/4 {
		zw.Write(chunk)
	}
	zw.Close()

	body, err := decompressBody(buf.Bytes(), "gzip")
	if err == nil {
		t.Fatalf("解压炸弹应报错, 却返回 %d 字节", len(body))
	}
}

// TestWildcardMultiLevelSubdomain 验证通配支持多级子域:
// files.q.iwara.tv 这类 <sub> 含点的子域应能匹配并转发。
// 发现背景: 代码审阅时发现 matchWildcard 用 ContainsAny(sub, ".:/")
// 拒绝一切含点子域, 多级子域站点永远无法走通配。
// 修复: 只拒绝端口/路径分隔符 (":/"), 点作为合法子域分隔符放开。
func TestWildcardMultiLevelSubdomain(t *testing.T) {
	cfg := UpstreamMap{
		"iwara.l.moonchan.xyz": {
			Host: "iwara.tv",
			Mode: "sni", // 显式指定, 避免触发 DoH 网络探测
			Wildcard: &WildcardRule{
				Prefix:         "iwara-",
				EntrySuffix:    ".l.moonchan.xyz",
				UpstreamSuffix: ".iwara.tv",
			},
		},
	}
	uc, ok := matchWildcard(cfg, "iwara-files.q.l.moonchan.xyz")
	if !ok {
		t.Fatal("多级子域应匹配")
	}
	if uc.Host != "files.q.iwara.tv" {
		t.Fatalf("want files.q.iwara.tv got %s", uc.Host)
	}
	// 含端口/路径分隔符的仍应拒绝。
	if _, ok := matchWildcard(cfg, "iwara-a:b.l.moonchan.xyz"); ok {
		t.Fatal("含冒号应拒绝")
	}
}
