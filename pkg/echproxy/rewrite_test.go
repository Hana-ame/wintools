package echproxy

import (
	"bytes"
	"compress/gzip"
	"strings"
	"testing"

	"github.com/andybalholm/brotli"
	"github.com/klauspost/compress/zstd"
)

func TestBuildRewriter(t *testing.T) {
	rt := map[string]string{
		"www.pixiv.net":          "pixiv.l.moonchan.xyz",
		"pixiv.net":              "pixiv.l.moonchan.xyz",
		"i.pximg.net":            "pixivimg.l.moonchan.xyz",
		"app-api.pixiv.net":      "pixivapi.l.moonchan.xyz",
		"oauth.secure.pixiv.net": "pixivoauth.l.moonchan.xyz",
	}
	rewrite := buildRewriter(rt)

	body := []byte(`<a href="https://www.pixiv.net/works/123">x</a>
<img src="https://i.pximg.net/img-master/img/1.png">
fetch("https://app-api.pixiv.net/v1/artworks/123")
"https://oauth.secure.pixiv.net/token"
"//www.pixiv.net/relative"
"https://accounts.pixiv.net/login"
"https://pixiv.net.cn/other"
"https://www.pixiv.net.evil.com/hack"
"notpixiv.net"`)
	got := string(rewrite(body, "8443"))

	for _, want := range []string{
		"https://pixiv.l.moonchan.xyz:8443/works/123",
		"https://pixivimg.l.moonchan.xyz:8443/img-master/img/1.png",
		"https://pixivapi.l.moonchan.xyz:8443/v1/artworks/123",
		"https://pixivoauth.l.moonchan.xyz:8443/token",
		"//pixiv.l.moonchan.xyz:8443/relative",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("rewrite output missing %q\nfull: %s", want, got)
		}
	}
	// 边界保护:子域/后缀/单词内嵌不得被误伤
	for _, keep := range []string{
		"https://accounts.pixiv.net/login",
		"https://pixiv.net.cn/other",
		"https://www.pixiv.net.evil.com/hack",
		"notpixiv.net",
	} {
		if !strings.Contains(got, keep) {
			t.Errorf("boundary broken: %q not preserved\nfull: %s", keep, got)
		}
	}
	if strings.Contains(got, "www.pixiv.net/") || strings.Contains(got, "pixiv.l.moonchan.xyz.l.moonchan.xyz") {
		t.Errorf("leftover real domain:\n%s", got)
	}

	// 无端口时不追加
	got2 := string(rewrite([]byte("https://www.pixiv.net/"), ""))
	if got2 != "https://pixiv.l.moonchan.xyz/" {
		t.Errorf("no-port rewrite = %q", got2)
	}
}

func TestDecompressBody(t *testing.T) {
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	zw.Write([]byte("hello www.pixiv.net"))
	zw.Close()

	raw, err := decompressBody(buf.Bytes(), "gzip")
	if err != nil {
		t.Fatalf("decompress: %v", err)
	}
	if string(raw) != "hello www.pixiv.net" {
		t.Errorf("gzip roundtrip = %q", raw)
	}

	plain, err := decompressBody([]byte("plain"), "")
	if err != nil || string(plain) != "plain" {
		t.Errorf("identity: %q %v", plain, err)
	}
}

func TestDecompressBodyBrotli(t *testing.T) {
	var buf bytes.Buffer
	bw := brotli.NewWriter(&buf)
	bw.Write([]byte("hello www.pixiv.net br"))
	bw.Close()

	raw, err := decompressBody(buf.Bytes(), "br")
	if err != nil {
		t.Fatalf("decompress br: %v", err)
	}
	if string(raw) != "hello www.pixiv.net br" {
		t.Errorf("br roundtrip = %q", raw)
	}
}

func TestDecompressBodyZstd(t *testing.T) {
	var buf bytes.Buffer
	zw, err := zstd.NewWriter(&buf)
	if err != nil {
		t.Fatalf("zstd writer: %v", err)
	}
	zw.Write([]byte("hello www.pixiv.net zstd"))
	zw.Close()

	raw, err := decompressBody(buf.Bytes(), "zstd")
	if err != nil {
		t.Fatalf("decompress zstd: %v", err)
	}
	if string(raw) != "hello www.pixiv.net zstd" {
		t.Errorf("zstd roundtrip = %q", raw)
	}
}
