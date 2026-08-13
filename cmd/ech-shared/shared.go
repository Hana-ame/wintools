package main

/*
#include <stdlib.h>
*/
import "C"
import (
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"runtime/cgo"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	cloudflare_ech "github.com/Hana-ame/wintools/pkg/ech"
)

var (
	initMu   sync.Mutex
	initing  bool
	initDone atomic.Bool
	initErr  atomic.Value

	logMu  sync.Mutex
	logBuf []string
)

func logMsg(fmt string, args ...interface{}) {
	logMu.Lock()
	logBuf = append(logBuf, fmt)
	if len(logBuf) > 200 {
		logBuf = logBuf[len(logBuf)-200:]
	}
	logMu.Unlock()
}

//export ECHSetDohURL
func ECHSetDohURL(url *C.char) {
	cloudflare_ech.SetDohURL(C.GoString(url))
}

//export ECHInit
func ECHInit() {
	if initDone.Load() {
		return
	}
	initMu.Lock()
	if initDone.Load() || initing {
		initMu.Unlock()
		return
	}
	initing = true
	initMu.Unlock()

	logMsg("ECHInit: starting goroutine")
	go func() {
		if err := cloudflare_ech.InitDefault(); err != nil {
			logMsg("ECHInit error: " + err.Error())
			initErr.Store(err.Error())
			initMu.Lock()
			initing = false
			initMu.Unlock()
			return
		}
		initErr.Store("")
		initDone.Store(true)
		logMsg("ECHInit: success")
	}()
}

//export ECHInitWithBootstrap
func ECHInitWithBootstrap(cHost, cIP *C.char) {
	host := C.GoString(cHost)
	ip := C.GoString(cIP)
	if host != "" {
		cloudflare_ech.SetDoHConfig(host, ip)
	}
	ECHInit()
}

//export ECHInitReady
func ECHInitReady() C.int {
	if initDone.Load() {
		return 1
	}
	if v := initErr.Load(); v != nil && v.(string) != "" {
		return -1
	}
	return 0
}

//export ECHInitLastError
func ECHInitLastError() *C.char {
	if v := initErr.Load(); v != nil {
		s := v.(string)
		if s == "" {
			return nil
		}
		return C.CString(s)
	}
	return nil
}

// buildFetchRequest 构造 ECH 请求：改写 host、设置 UA/Referer/Accept。
// 注意：返回的 request 不带 context 超时 —— 大文件/视频流式读取时
// body 绑定 ctx，ctx 到期会中断 body 读取（这就是旧 ECHFetch 大视频
// 下载失败/超时的根因）。连接与握手阶段由 pkg/ech 的 transport
// 内部 timeout（dialTimeout/TLSHandshakeTimeout）兜底。
func buildFetchRequest(goURL, goHost, goRef string) (*http.Request, error) {
	parsed, err := url.Parse(goURL)
	if err != nil {
		return nil, err
	}
	parsed.Host = goHost
	parsed.Scheme = "https"
	rewritten := parsed.String()

	outReq, err := http.NewRequest("GET", rewritten, nil)
	if err != nil {
		return nil, err
	}
	outReq.Header.Set("User-Agent", "Mozilla/5.0 (Linux; Android 14; Pixel 8) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Mobile Safari/537.36")
	outReq.Header.Set("Accept", "image/avif,image/webp,image/apng,image/svg+xml,image/*,*/*;q=0.8")
	if goRef != "" {
		outReq.Header.Set("Referer", goRef)
	}
	outReq.Host = goHost
	return outReq, nil
}

// ---- 流式下载句柄 ----
//
// 说明：cgo 规则不允许 C 侧长期持有指向 Go 内存的指针，因此句柄
// 用标准库 runtime/cgo.Handle 管理（Go 1.17+ 官方的跨 cgo 句柄方案，
// 内部是全局 map + 自增 id）。句柄以 uintptr 数值跨 FFI 传递，
// 不经过 unsafe.Pointer 转换（go vet unsafeptr 会误报，且无必要）。
//
// 错误码约定：
//
//	ECHRead 返回 >0  读取的字节数
//	ECHRead 返回 0   EOF
//	ECHRead 返回 -1  body 读取错误（详情见日志）
//	ECHRead 返回 -2  无效句柄
type streamHandle struct {
	body io.ReadCloser
}

//export ECHFetchBegin
func ECHFetchBegin(urlStr, host, referer *C.char) uintptr {
	goURL := C.GoString(urlStr)
	goHost := C.GoString(host)
	goRef := C.GoString(referer)

	logMsg("ECHFetchBegin: " + goURL + " -> host " + goHost)

	outReq, err := buildFetchRequest(goURL, goHost, goRef)
	if err != nil {
		logMsg("ECHFetchBegin request error: " + err.Error())
		return 0
	}

	resp, err := cloudflare_ech.Do(outReq)
	if err != nil {
		logMsg("ECHFetchBegin Do error: " + err.Error())
		return 0
	}

	if resp.StatusCode != http.StatusOK {
		bodyPreview, _ := io.ReadAll(io.LimitReader(resp.Body, 200))
		bodyStr := string(bodyPreview)
		resp.Body.Close()
		detail := fmt.Sprintf("HTTP %d %s | URL=%s | body=%.200s", resp.StatusCode, http.StatusText(resp.StatusCode), goURL, bodyStr)
		logMsg("ECHFetchBegin failed: " + detail)
		return 0
	}

	h := cgo.NewHandle(&streamHandle{body: resp.Body})
	logMsg("ECHFetchBegin success: handle=" + fmt.Sprintf("%d", uintptr(h)))
	return uintptr(h)
}

//export ECHRead
func ECHRead(handle uintptr, buf unsafe.Pointer, max C.int) C.int {
	h := cgo.Handle(uintptr(handle))
	v, ok := h.Value().(*streamHandle)
	if !ok {
		return -2
	}
	if max <= 0 {
		return 0
	}
	readLen := int(max)
	// 单次读取上限 256KB，防止调用方传超大 buffer 时一次性占用过多
	if readLen > 256*1024 {
		readLen = 256 * 1024
	}
	n, err := v.body.Read(unsafe.Slice((*byte)(buf), readLen))
	if err != nil {
		if err == io.EOF {
			return 0
		}
		logMsg("ECHRead error: " + err.Error())
		return -1
	}
	return C.int(n)
}

//export ECHClose
func ECHClose(handle uintptr) {
	h := cgo.Handle(uintptr(handle))
	v, ok := h.Value().(*streamHandle)
	h.Delete()
	if ok && v.body != nil {
		v.body.Close()
		logMsg("ECHClose: handle " + fmt.Sprintf("%d", uintptr(h)))
	}
}

//export ECHFetch
func ECHFetch(urlStr, host, referer *C.char) *C.char {
	goURL := C.GoString(urlStr)
	goHost := C.GoString(host)
	goRef := C.GoString(referer)

	logMsg("ECHFetch: " + goURL + " -> host " + goHost)

	outReq, err := buildFetchRequest(goURL, goHost, goRef)
	if err != nil {
		logMsg("ECHFetch request error: " + err.Error())
		return C.CString("ERR: " + err.Error())
	}

	// 旧接口保留给图片等小文件：用 60s 总超时兜底，避免 header 挂死。
	// （大文件请走 ECHFetchBegin/ECHRead 流式接口，此超时会砍断大视频。）
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	outReq = outReq.WithContext(ctx)

	resp, err := cloudflare_ech.Do(outReq)
	if err != nil {
		logMsg("ECHFetch Do error: " + err.Error())
		return C.CString("ERR: " + err.Error())
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		bodyPreview, _ := io.ReadAll(io.LimitReader(resp.Body, 200))
		bodyStr := string(bodyPreview)
		detail := fmt.Sprintf("HTTP %d %s | URL=%s | body=%.200s", resp.StatusCode, http.StatusText(resp.StatusCode), goURL, bodyStr)
		logMsg("ECHFetch failed: " + detail)
		return C.CString("ERR: " + detail)
	}

	buf, err := io.ReadAll(resp.Body)
	if err != nil {
		logMsg("ECHFetch read error: " + err.Error())
		return C.CString("ERR: read body: " + err.Error())
	}

	encoded := base64.StdEncoding.EncodeToString(buf)
	logMsg("ECHFetch success: " + fmt.Sprintf("%d bytes -> %d base64", len(buf), len(encoded)))
	return C.CString(encoded)
}

//export ECHGetLogCount
func ECHGetLogCount() C.int {
	logMu.Lock()
	n := len(logBuf)
	logMu.Unlock()
	return C.int(n)
}

//export ECHGetLog
func ECHGetLog(i C.int) *C.char {
	logMu.Lock()
	defer logMu.Unlock()
	n := int(i)
	if n < 0 || n >= len(logBuf) {
		return nil
	}
	return C.CString(logBuf[n])
}

//export FreeCString
func FreeCString(s *C.char) {
	C.free(unsafe.Pointer(s))
}

func main() {}
