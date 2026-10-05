// Peersignal: self-hosted PeerJS signaling server + built-in room discovery.
// Replaces public cloud signaling (0.peerjs.com) and public MQTT brokers—nodes just need to point
// PEERDRIVE_PEERJS_HOST/PORT at this server; discovery goes through the built-in HTTP API.
//
// Usage: peersignal [-addr :9000] [-key peerjs] [-tokens tok1,tok2] [-tls-cert c.pem -tls-key k.pem]
//
//	-tokens optional: signaling token whitelist (comma-separated). When set, the WS connection token
//	must be on the list, otherwise the upgrade is rejected (prevents arbitrary clients from impersonating nodes to receive signaling).
//	-tls-cert/-tls-key optional: serve over HTTPS/WSS when both are given.
package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"strings"

	"github.com/Hana-ame/go-peerserver"
)

func main() {
	addr := flag.String("addr", ":9000", "listen address")
	key := flag.String("key", "peerjs", "API key (client must match)")
	tokens := flag.String("tokens", "", "signaling token whitelist (comma-separated; empty = no restriction)")
	tlsCert := flag.String("tls-cert", "", "TLS certificate (PEM). When given together with -tls-key, serve over HTTPS/WSS")
	tlsKey := flag.String("tls-key", "", "TLS private key (PEM)")
	corsOrigin := flag.String("cors-origin", "",
		"comma-separated CORS allow-list for the panel-facing REST endpoints "+
			"(e.g. https://peerdrive.pages.dev,null). Empty = keep the historical wildcard '*'")
	flag.Parse()

	var opts []signalserver.Option
	if *tokens != "" {
		opts = append(opts, signalserver.WithTokenWhitelist(strings.Split(*tokens, ",")))
	}
	if *corsOrigin != "" {
		opts = append(opts, signalserver.WithCORSOrigins(strings.Split(*corsOrigin, ",")))
		log.Printf("cors: restricted to %s (file:// panels need an explicit \"null\")", *corsOrigin)
	}
	srv := signalserver.NewServer(*key, opts...)
	srv.Start() // background sweeper: clean up expired offline queues (H3)

	mux := http.NewServeMux()
	// PeerJS-compatible signaling endpoints
	mux.HandleFunc("/peerjs", srv.HandleWS)
	mux.HandleFunc("/peerjs/id", srv.HandleID)
	// Built-in room discovery (replaces MQTT)
	mux.HandleFunc("/discover/announce", srv.HandleAnnounce)
	mux.HandleFunc("/discover/leave", srv.HandleLeave)
	mux.HandleFunc("/discover/nodes", srv.HandleNodes)
	// Status API and dashboard (graph visualization)
	mux.HandleFunc("/status", srv.HandleStatus)
	mux.HandleFunc("/", srv.HandleDashboard)

	if err := Serve(*addr, *tlsCert, *tlsKey, mux); err != nil {
		log.Fatal(err)
	}
}

// Serve serves on addr: uses HTTPS/WSS when both certFile and keyFile are given, otherwise HTTP/WS.
//
// Why TLS support is needed: the public panel (packages/peerdrive-client/dist/panel.html) is deployed on
// GitHub Pages, which enforces HTTPS. Browsers treat ws:// requests from HTTPS pages as
// mixed content and block them outright (on the PeerJS side it just looks like a connection failure with no obvious cause), so self-hosted signaling
// must use wss:// to be reachable from the public panel—either TLS directly in this process, or a reverse proxy in front.
func Serve(addr, certFile, keyFile string, h http.Handler) error {
	if certFile == "" && keyFile == "" {
		log.Printf("peerserver listening on %s (ws)", addr)
		return http.ListenAndServe(addr, h)
	}
	// Giving only one half is a typical typo: silently falling back to HTTP would cause the remote HTTPS page to be blocked by mixed content
	// interception, while the server appears to have "started normally", making it extremely hard to debug. Better to fail loudly.
	if certFile == "" || keyFile == "" {
		return fmt.Errorf("TLS requires both -tls-cert and -tls-key (current cert=%q key=%q)", certFile, keyFile)
	}
	log.Printf("peerserver listening on %s (wss)", addr)
	return http.ListenAndServeTLS(addr, certFile, keyFile, h)
}
