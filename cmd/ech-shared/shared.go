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

func logMsg(format string, args ...interface{}) {
	logMu.Lock()
	s := format
	if len(args) > 0 {
		// 参数真正格式化, 而不是静默丢弃 (旧实现形参名 fmt 遮蔽了
		// fmt 包, args 一直被忽略, 未来调用点传 %d 会悄悄丢参)。
		s = fmt.Sprintf(format, args...)
	}
	logBuf = append(logBuf, s)
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
			logMsg("ECHInit error: %v", err)
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
//	ECHRead 返回 -2  无效句柄（含已被 ECHClose 关闭）
//
// 坑（发现背景: 代码审阅）: cgo.Handle 对已 Delete 的句柄再调用
// Value()/Delete() 会直接 panic (runtime/cgo: misuse of an invalid
// Handle), C 侧重复 ECHClose 或 ECHRead 与 ECHClose 并发都会触发,
// 而 cgo 导出函数里的 panic 无法被 C 侧捕获, 会终止整个进程。
// 修复: 句柄包一层结构体, 用 closeOnce 保证 body 只关一次,
// closed 原子标记让 ECHRead 主动避开已关句柄, 再包 recover 兜底
// 吞掉任何 handle 层面的 panic。
type streamHandle struct {
	body io.ReadCloser
	// closed 置位后 ECHRead 不再访问 body; closeOnce 保证幂等。
	closed    atomic.Bool
	closeOnce sync.Once
}

func (sh *streamHandle) close() {
	sh.closeOnce.Do(func() {
		sh.closed.Store(true)
		if sh.body != nil {
			sh.body.Close()
		}
	})
}

//export ECHFetchBegin
func ECHFetchBegin(urlStr, host, referer *C.char) uintptr {
	goURL := C.GoString(urlStr)
	goHost := C.GoString(host)
	goRef := C.GoString(referer)

	logMsg("ECHFetchBegin: %s -> host %s", goURL, goHost)

	outReq, err := buildFetchRequest(goURL, goHost, goRef)
	if err != nil {
		logMsg("ECHFetchBegin request error: %v", err)
		return 0
	}

	resp, err := cloudflare_ech.Do(outReq)
	if err != nil {
		logMsg("ECHFetchBegin Do error: %v", err)
		return 0
	}

	if resp.StatusCode != http.StatusOK {
		bodyPreview, _ := io.ReadAll(io.LimitReader(resp.Body, 200))
		bodyStr := string(bodyPreview)
		resp.Body.Close()
		detail := fmt.Sprintf("HTTP %d %s | URL=%s | body=%.200s", resp.StatusCode, http.StatusText(resp.StatusCode), goURL, bodyStr)
		logMsg("ECHFetchBegin failed: %s", detail)
		return 0
	}

	h := cgo.NewHandle(&streamHandle{body: resp.Body})
	logMsg("ECHFetchBegin success: handle=%d", uintptr(h))
	return uintptr(h)
}

//export ECHRead
func ECHRead(handle uintptr, buf unsafe.Pointer, max C.int) (ret C.int) {
	// recover 兜底: 与 ECHClose 并发时 handle 可能已被 Delete,
	// Value() 会 panic; 吞掉并返回 -2 (无效句柄), 不能让它跨 cgo
	// 边界炸掉整个进程。
	defer func() {
		if recover() != nil {
			ret = -2
		}
	}()

	h := cgo.Handle(uintptr(handle))
	v, ok := h.Value().(*streamHandle)
	if !ok || v.closed.Load() {
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
	if n > 0 {
		// io.Reader 契约: n>0 时 err 可能同时为 io.EOF (网络 body
		// 常见)。必须先返回数据, EOF 留到下次调用再报, 否则丢数据
		// (发现背景: 代码审阅, 旧实现 n>0+EOF 直接 return 0)。
		return C.int(n)
	}
	if err == io.EOF {
		return 0
	}
	if err != nil {
		logMsg("ECHRead error: %v", err)
		return -1
	}
	return 0
}

//export ECHClose
func ECHClose(handle uintptr) {
	// recover 兜底: C 侧对同一 handle 调两次 ECHClose 时第二次
	// h.Value()/h.Delete() 会 panic, 必须吞掉 (见 streamHandle 注释)。
	defer func() { recover() }()

	h := cgo.Handle(uintptr(handle))
	v, ok := h.Value().(*streamHandle)
	if !ok {
		return
	}
	v.close()
	h.Delete()
	logMsg("ECHClose: handle %d", uintptr(h))
}

//export ECHFetch
func ECHFetch(urlStr, host, referer *C.char) *C.char {
	goURL := C.GoString(urlStr)
	goHost := C.GoString(host)
	goRef := C.GoString(referer)

	logMsg("ECHFetch: %s -> host %s", goURL, goHost)

	outReq, err := buildFetchRequest(goURL, goHost, goRef)
	if err != nil {
		logMsg("ECHFetch request error: %v", err)
		return C.CString("ERR: " + err.Error())
	}

	// 旧接口保留给图片等小文件：用 60s 总超时兜底，避免 header 挂死。
	// （大文件请走 ECHFetchBegin/ECHRead 流式接口，此超时会砍断大视频。）
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	outReq = outReq.WithContext(ctx)

	resp, err := cloudflare_ech.Do(outReq)
	if err != nil {
		logMsg("ECHFetch Do error: %v", err)
		return C.CString("ERR: " + err.Error())
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		bodyPreview, _ := io.ReadAll(io.LimitReader(resp.Body, 200))
		bodyStr := string(bodyPreview)
		detail := fmt.Sprintf("HTTP %d %s | URL=%s | body=%.200s", resp.StatusCode, http.StatusText(resp.StatusCode), goURL, bodyStr)
		logMsg("ECHFetch failed: %s", detail)
		return C.CString("ERR: " + detail)
	}

	buf, err := io.ReadAll(resp.Body)
	if err != nil {
		logMsg("ECHFetch read error: %v", err)
		return C.CString("ERR: read body: " + err.Error())
	}

	encoded := base64.StdEncoding.EncodeToString(buf)
	logMsg("ECHFetch success: %d bytes -> %d base64", len(buf), len(encoded))
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
