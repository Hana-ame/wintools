package main

/*
#include <stdlib.h>
*/
import "C"
import (
	"encoding/base64"
	"io"
	"net/http"
	"sync"
	"sync/atomic"
	"unsafe"

	cloudflare_ech "github.com/Hana-ame/wintools/pkg/ech"
)

var (
	initOnce sync.Once
	initDone atomic.Bool
	initErr  atomic.Value
)

//export ECHSetDohURL
func ECHSetDohURL(url *C.char) {
	cloudflare_ech.SetDohURL(C.GoString(url))
}

//export ECHInit
func ECHInit() {
	initOnce.Do(func() {
		go func() {
			if err := cloudflare_ech.InitDefault(); err != nil {
				initErr.Store(err.Error())
				return
			}
			initDone.Store(true)
		}()
	})
}

//export ECHInitReady
func ECHInitReady() C.int {
	if initDone.Load() {
		return 1
	}
	if v := initErr.Load(); v != nil {
		return -1
	}
	return 0
}

//export ECHInitLastError
func ECHInitLastError() *C.char {
	if v := initErr.Load(); v != nil {
		return C.CString(v.(string))
	}
	return nil
}

//export ECHFetch
func ECHFetch(urlStr, host, referer *C.char) *C.char {
	goURL := C.GoString(urlStr)
	goHost := C.GoString(host)
	goRef := C.GoString(referer)

	outReq, err := http.NewRequest("GET", goURL, nil)
	if err != nil {
		return C.CString("ERR: " + err.Error())
	}
	if goRef != "" {
		outReq.Header.Set("Referer", goRef)
	}
	outReq.Host = goHost

	resp, err := cloudflare_ech.Do(outReq)
	if err != nil {
		return C.CString("ERR: " + err.Error())
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return C.CString("ERR: HTTP " + http.StatusText(resp.StatusCode))
	}

	buf, err := io.ReadAll(resp.Body)
	if err != nil {
		return C.CString("ERR: read body: " + err.Error())
	}

	encoded := base64.StdEncoding.EncodeToString(buf)
	return C.CString(encoded)
}

//export FreeCString
func FreeCString(s *C.char) {
	C.free(unsafe.Pointer(s))
}

func main() {}
