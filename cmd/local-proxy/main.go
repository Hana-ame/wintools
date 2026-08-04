// Local Proxy in Go — simple Ollama-compatible (11434) forwarding proxy that
// round-robins to the bwh/vps/cloudcone zen proxies with failover.
// Replaces local_proxy.py.
package main

import (
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"time"
)

const (
	connectTimeout = 10 * time.Second
	streamTimeout  = 60 * time.Second
	attempts       = 3
)

var backends = []string{"bwh", "vps", "cloudcone"}

func makeClient() *http.Client {
	tr := &http.Transport{
		TLSClientConfig:       &tls.Config{InsecureSkipVerify: true}, // backends use self-signed certs
		ResponseHeaderTimeout: 30 * time.Second,
		IdleConnTimeout:       90 * time.Second,
		MaxIdleConnsPerHost:   8,
	}
	return &http.Client{Transport: tr}
}

func forward(w http.ResponseWriter, r *http.Request, method string) {
	client := makeClient()
	defer client.CloseIdleConnections()

	body, err := io.ReadAll(r.Body)
	if err != nil {
		writeJSON(w, 400, map[string]any{"error": "Bad Request"})
		return
	}

	isStream := false
	if len(body) > 0 {
		var payload map[string]any
		if err := jsonUnmarshal(body, &payload); err == nil {
			isStream, _ = payload["stream"].(bool)
		}
	}

	log.Printf("-> %s %s len=%d stream=%v", method, r.URL.Path, len(body), isStream)

	lastError := ""
	for _, host := range backends {
		for attempt := 0; attempt < attempts; attempt++ {
			log.Printf("-> %s attempt %d", host, attempt+1)
			url := fmt.Sprintf("https://%s.moonchan.xyz:8443/v1/chat/completions", host)
			if method == "GET" {
				url = fmt.Sprintf("https://%s.moonchan.xyz:8443/v1/models", host)
			}
			req, err := http.NewRequest(method, url, strings.NewReader(string(body)))
			if err != nil {
				continue
			}
			req.Header.Set("Content-Type", r.Header.Get("Content-Type"))
			if a := r.Header.Get("Authorization"); a != "" {
				req.Header.Set("Authorization", a)
			}

			resp, err := client.Do(req)
			if err != nil {
				log.Printf("%s error: %v", host, err)
				lastError = err.Error()
				break
			}

			if resp.StatusCode == 500 {
				lastError = fmt.Sprintf("%s 500", host)
				resp.Body.Close()
				continue
			}
			if resp.StatusCode != 200 {
				lastError = fmt.Sprintf("%s returned %d", host, resp.StatusCode)
				resp.Body.Close()
				break
			}

			t0 := time.Now()
			if isStream {
				ok := relayStream(w, resp.Body)
				resp.Body.Close()
				if ok {
					log.Printf("%s: stream done %s", host, time.Since(t0).Round(time.Millisecond))
					return
				}
				log.Printf("FAIL")
				return
			}
			data, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			h := w.Header()
			h.Set("Content-Type", "application/json")
			h.Set("Access-Control-Allow-Origin", "*")
			w.WriteHeader(200)
			w.Write(data)
			log.Printf("%s: 200 %s", host, time.Since(t0).Round(time.Millisecond))
			return
		}
	}

	writeJSON(w, 503, map[string]any{"error": fmt.Sprintf("All backends failed: %s", lastError)})
}

// relayStream copies the upstream body to the client with a sliding idle
// deadline. Returns false when the client disconnects or the stream stalls.
func relayStream(w http.ResponseWriter, body io.Reader) bool {
	flusher, _ := w.(http.Flusher)
	h := w.Header()
	h.Set("Content-Type", "text/event-stream")
	h.Set("Cache-Control", "no-cache")
	h.Set("Connection", "close")
	h.Set("Access-Control-Allow-Origin", "*")
	w.WriteHeader(200)
	if flusher != nil {
		flusher.Flush()
	}

	ch := make(chan []byte, 16)
	errCh := make(chan error, 1)
	go func() {
		buf := make([]byte, 16384)
		for {
			n, err := body.Read(buf)
			if n > 0 {
				cp := make([]byte, n)
				copy(cp, buf[:n])
				ch <- cp
			}
			if err != nil {
				errCh <- err
				return
			}
		}
	}()

	timer := time.NewTimer(streamTimeout)
	defer timer.Stop()
	for {
		select {
		case chunk := <-ch:
			timer.Reset(streamTimeout)
			if _, err := w.Write(chunk); err != nil {
				log.Printf("client disconnected")
				return false
			}
			if flusher != nil {
				flusher.Flush()
			}
		case err := <-errCh:
			return err == io.EOF
		case <-timer.C:
			log.Printf("stream idle timeout, aborting")
			return false
		}
	}
}

func jsonUnmarshal(b []byte, v any) error {
	return json.Unmarshal(b, v)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	h := w.Header()
	h.Set("Content-Type", "application/json")
	h.Set("Access-Control-Allow-Origin", "*")
	h.Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
	h.Set("Access-Control-Allow-Headers", "Content-Type, Authorization")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

func main() {
	host := "127.0.0.1"
	port := 11434
	if len(os.Args) > 1 {
		host = os.Args[1]
	}
	if len(os.Args) > 2 {
		fmt.Sscanf(os.Args[2], "%d", &port)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/v1/chat/completions", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "OPTIONS" {
			writeJSON(w, 204, map[string]any{})
			return
		}
		forward(w, r, r.Method)
	})
	mux.HandleFunc("/v1/models", func(w http.ResponseWriter, r *http.Request) {
		forward(w, r, "GET")
	})
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "OPTIONS" {
			writeJSON(w, 204, map[string]any{})
			return
		}
		w.Header().Set("Content-Type", "text/plain")
		w.WriteHeader(200)
		w.Write([]byte("Local proxy running\n"))
	})

	srv := &http.Server{
		Addr:              fmt.Sprintf("%s:%d", host, port),
		Handler:           mux,
		ReadHeaderTimeout: 15 * time.Second,
	}
	log.Printf("Local proxy on http://%s:%d/v1/chat/completions", host, port)
	log.Printf("Backends: %s", strings.Join(backends, ", "))
	if err := srv.ListenAndServe(); err != nil {
		log.Fatal(err)
	}
}
