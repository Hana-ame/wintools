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

//export ECHFetch
func ECHFetch(urlStr, host, referer *C.char) *C.char {
	goURL := C.GoString(urlStr)
	goHost := C.GoString(host)
	goRef := C.GoString(referer)

	logMsg("ECHFetch: " + goURL)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	outReq, err := http.NewRequestWithContext(ctx, "GET", goURL, nil)
	if err != nil {
		logMsg("ECHFetch request error: " + err.Error())
		return C.CString("ERR: " + err.Error())
	}
	if goRef != "" {
		outReq.Header.Set("Referer", goRef)
	}
	outReq.Host = goHost

	resp, err := cloudflare_ech.Do(outReq)
	if err != nil {
		logMsg("ECHFetch Do error: " + err.Error())
		return C.CString("ERR: " + err.Error())
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		logMsg("ECHFetch HTTP " + http.StatusText(resp.StatusCode))
		return C.CString("ERR: HTTP " + http.StatusText(resp.StatusCode))
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
