package main

/*
#include <stdlib.h>
*/
import "C"
import (
	"context"
	"io"
	"log"
	"net/http"
	"net/url"
	"sync"
	"sync/atomic"
	"unsafe"

	cloudflare_ech "github.com/Hana-ame/wintools/pkg/ech"
)

var (
	srv     atomic.Pointer[http.Server]
	startMu sync.Mutex
)

//export ECHProxyStart
func ECHProxyStart(addr, upstreamHost, upstreamReferer *C.char) *C.char {
	startMu.Lock()
	defer startMu.Unlock()

	if srv.Load() != nil {
		return nil
	}

	goAddr := C.GoString(addr)
	goHost := C.GoString(upstreamHost)
	goRef := C.GoString(upstreamReferer)

	go func() {
		if err := cloudflare_ech.InitDefault(); err != nil {
			log.Printf("ECH init error: %v", err)
			return
		}

		mux := http.NewServeMux()
		mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
			proxyRequest(w, r, goHost, goRef)
		})

		s := &http.Server{Addr: goAddr, Handler: mux}
		srv.Store(s)

		if err := s.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("ECH proxy server error: %v", err)
		}
	}()

	return nil
}

//export ECHProxyReady
func ECHProxyReady() C.int {
	if srv.Load() != nil {
		return 1
	}
	return 0
}

//export ECHProxyStop
func ECHProxyStop() {
	if s := srv.Load(); s != nil {
		s.Shutdown(context.Background())
		srv.Store(nil)
	}
}

//export FreeCString
func FreeCString(s *C.char) {
	C.free(unsafe.Pointer(s))
}

func proxyRequest(w http.ResponseWriter, r *http.Request, host, referer string) {
	upstreamURL := (&url.URL{
		Scheme:   "https",
		Host:     host,
		Path:     r.URL.Path,
		RawQuery: r.URL.RawQuery,
	}).String()

	outReq, err := http.NewRequest(r.Method, upstreamURL, r.Body)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	if r.Body != nil {
		defer r.Body.Close()
	}

	for k, vs := range r.Header {
		for _, v := range vs {
			outReq.Header.Add(k, v)
		}
	}
	if referer != "" {
		outReq.Header.Set("Referer", referer)
	}
	outReq.Host = host

	resp, err := cloudflare_ech.Do(outReq)
	if err != nil {
		w.WriteHeader(http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()

	for k, vs := range resp.Header {
		for _, v := range vs {
			w.Header().Add(k, v)
		}
	}
	w.WriteHeader(resp.StatusCode)
	io.Copy(w, resp.Body)
}

func main() {}
