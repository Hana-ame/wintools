package main

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Hana-ame/go-peerserver"
)

// Discovery background: 2026-09-20 deployed the public panel (packages/peerdrive-client/dist/panel.html)
// to GitHub Pages. Pages enforces HTTPS, and browsers block ws:// requests from HTTPS pages as
// mixed content, while the PeerJS side only shows "can't connect" with no hint at all. So self-hosted
// signaling must be able to provide wss:// -- that's why -tls-cert/-tls-key exist.

// writeSelfSigned generates a self-signed certificate, sufficient for testing (use Let's Encrypt / reverse proxy for production).
func writeSelfSigned(t *testing.T, dir string) (certPath, keyPath string) {
	t.Helper()
	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("failed to generate private key: %v", err)
	}
	tmpl := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "127.0.0.1"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
	}
	der, err := x509.CreateCertificate(rand.Reader, &tmpl, &tmpl, &priv.PublicKey, priv)
	if err != nil {
		t.Fatalf("failed to sign certificate: %v", err)
	}
	certPath = filepath.Join(dir, "cert.pem")
	keyPath = filepath.Join(dir, "key.pem")
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(priv)})
	if err := os.WriteFile(certPath, certPEM, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keyPath, keyPEM, 0o600); err != nil {
		t.Fatal(err)
	}
	return certPath, keyPath
}

// freeAddr gets a currently free address (Serve will listen on it by itself).
func freeAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	l.Close()
	return addr
}

func TestServe_TLSHalfConfig(t *testing.T) {
	// Providing only half must error: silently falling back to http would cause the HTTPS
	// panel to be blocked by mixed content, while the server logs look perfectly normal --
	// this kind of "half configuration" is the hardest to debug.
	mux := http.NewServeMux()
	mux.HandleFunc("/peerjs/id", func(w http.ResponseWriter, r *http.Request) { w.Write([]byte("x")) })

	if err := Serve(freeAddr(t), "cert.pem", "", mux); err == nil {
		t.Fatal("should error when only -tls-cert is provided without -tls-key")
	}
	if err := Serve(freeAddr(t), "", "key.pem", mux); err == nil {
		t.Fatal("should error when only -tls-key is provided without -tls-cert")
	}
}

func TestServe_WSS(t *testing.T) {
	certPath, keyPath := writeSelfSigned(t, t.TempDir())
	srv := signalserver.NewServer("peerjs")
	mux := http.NewServeMux()
	// The first thing the public panel does after connecting is GET /peerjs/id to get a temporary id, so use it as a probe
	mux.HandleFunc("/peerjs/id", srv.HandleID)

	addr := freeAddr(t)
	errCh := make(chan error, 1)
	go func() { errCh <- Serve(addr, certPath, keyPath, mux) }()

	client := &http.Client{
		Timeout: 3 * time.Second,
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true}, // self-signed certificate
		},
	}

	// Serve is blocking; wait for it to actually start listening (up to 3s)
	var resp *http.Response
	var err error
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		resp, err = client.Get("https://" + addr + "/peerjs/id")
		if err == nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if err != nil {
		t.Fatalf("wss endpoint unreachable: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status code = %d, expected 200", resp.StatusCode)
	}
	// The panel is on a different origin (Pages); cross-origin headers are a hard requirement --
	// without them the browser still can't get the id
	if got := resp.Header.Get("Access-Control-Allow-Origin"); got != "*" {
		t.Fatalf("Access-Control-Allow-Origin = %q, expected * (public panel cross-origin id fetching depends on it)", got)
	}

	// Reverse confirmation: accessing the same address with plain http cannot get the id (confirms TLS is serving).
	// Go's TLS server returns 400 directly for plain-text requests ("Client sent an HTTP request to an
	// HTTPS server"), so here we must accept both err and 400 -- only getting 200 means something is wrong.
	if plain, perr := (&http.Client{Timeout: 2 * time.Second}).Get("http://" + addr + "/peerjs/id"); perr == nil {
		plain.Body.Close()
		if plain.StatusCode == http.StatusOK {
			t.Fatal("with TLS enabled, plain http should not be able to get the id")
		}
	}

}
