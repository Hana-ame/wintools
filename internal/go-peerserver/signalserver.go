// Package signalserver is a self-hosted PeerJS signaling server (compatible with the peerjs-server protocol subset)
// + built-in room discovery (replaces public signaling + public MQTT brokers).
//
// Responsibilities:
//  1. Signaling: node registration (WS + token), OFFER/ANSWER/CANDIDATE/LEAVE forwarding by dst,
//     queuing when dst is offline (with expiry), heartbeat keep-alive, ID allocation
//  2. Discovery: nodes announce the collections they follow, `GET /discover/nodes?coll=` queries online nodes
//     ——after self-hosting, the server naturally knows all online nodes, no longer needing MQTT broadcast
//
// Protocol details aligned with peers/peerjs-server (src/services/webSocketServer, messageHandler):
//   - WS URL: /{path}peerjs?key=&id=&token=
//   - Messages {type, src, dst, payload}, server overwrites src
//   - Forward when dst is online; queue when offline (LEAVE/EXPIRE not queued)
//   - OPEN / ID-TAKEN / ERROR control messages
//   - Client sends HEARTBEAT every 5s for keep-alive
package signalserver

import (
	"crypto/rand"
	"embed"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
)

//go:embed dashboard.html
var dashboardFS embed.FS

type Server struct {
	key            string
	path           string
	queueTTL       time.Duration   // offline queue TTL (used for OFFER expiry)
	heartbeatTTL   time.Duration   // discovery heartbeat expiry time
	tokenWhitelist map[string]bool // allowed signaling tokens (nil/empty = unrestricted)
	// corsOrigins is the explicit CORS allow-list (nil/empty = wildcard "*", the historical behavior).
	// See WithCORSOrigins and allowCORS for why the default stays permissive.
	corsOrigins []string

	startedAt time.Time // server startup time
	msgCount  int64     // total forwarded message count (atomic access)

	mu        sync.Mutex
	clients   map[string]*client              // id → online connection
	queues    map[string][]queuedMsg          // dst → messages pending forwarding
	disc      map[string]map[string]time.Time // collection → peerId → lastSeen
	peerLinks map[string]map[string]time.Time // peerId → neighbor peerId → lastSeen (for graph)
	peerColls map[string][]string             // peerId -> collections
	peerStats map[string]*PeerStats           // peerId -> stats
}

// Option is a signaling server configuration option.
type Option func(*Server)

// WithCORSOrigins sets an explicit CORS allow-list for the panel-facing REST endpoints.
//
// Default (not called) stays `Access-Control-Allow-Origin: *`, which is what every existing
// deployment relies on — see the allowCORS comment for why the panel cannot be given an
// exact origin by default (file:// has Origin `null`, and the panel may be hosted anywhere).
//
// Once an operator supplies a list, the server echoes back the request's Origin when it
// matches instead of the wildcard. That is the tightening switch; it is opt-in so that
// turning it on is a deliberate deployment decision rather than a surprise after an upgrade.
//
// Practical note: `null` must be listed explicitly to keep file:// panels working:
//
//	-cors-origin "https://peerdrive.pages.dev,null"
func WithCORSOrigins(origins []string) Option {
	return func(s *Server) {
		cleaned := make([]string, 0, len(origins))
		for _, o := range origins {
			o = strings.TrimSpace(o)
			if o != "" {
				cleaned = append(cleaned, o)
			}
		}
		s.corsOrigins = cleaned
	}
}

// WithTokenWhitelist sets the signaling token whitelist: the WS connection token must be on the list,
// otherwise the upgrade is rejected ("Invalid token provided"). Empty list = unrestricted (default, compatible
// with current public deployments).
// Pitfall (2026-08-18 code review): tokens were originally just ID occupancy protection—any client could choose
// a token to connect, so an attacker could register any ID to impersonate an online node and receive signaling
// (combined with a self-chosen ID, this can send OFFERs to any node luring connections to the attacker).
// A whitelist lets self-hosted deployments trust only known nodes.
// Note: discovery endpoints (announce/nodes) remain public—discovery's purpose is to let anyone find
// nodes; the whitelist only constrains the signaling plane.
func WithTokenWhitelist(tokens []string) Option {
	return func(s *Server) {
		if len(tokens) == 0 {
			return
		}
		s.tokenWhitelist = make(map[string]bool, len(tokens))
		for _, t := range tokens {
			s.tokenWhitelist[t] = true
		}
	}
}

// Offline queue limit (H3 fix): each dst caches at most maxQueuedPerDst messages.
// Pitfall: the original implementation had no limit—when dst never connects, the queue grows unbounded (each malicious client can send OFFERs to any random ID
// to exhaust server memory). When the limit is exceeded, drop the oldest (signaling messages expire anyway, so dropping old is more reasonable than dropping new).
const maxQueuedPerDst = 100

// Start launches a background sweeper: periodically cleans up expired queue entries and empty queues.
// Background: expiry cleanup was originally only done in flushQueue (when dst comes online); if dst never connects, expired messages accumulate.
func (s *Server) Start() {
	go func() {
		t := time.NewTicker(30 * time.Second)
		defer t.Stop()
		for range t.C {
			s.sweepQueues()
			s.sweepDiscovery()
		}
	}()
}

func (s *Server) sweepQueues() {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	for dst, q := range s.queues {
		kept := q[:0]
		for _, qm := range q {
			if qm.expire.After(now) && s.clients[dst] == nil {
				kept = append(kept, qm)
			}
		}
		if len(kept) == 0 {
			delete(s.queues, dst)
		} else {
			s.queues[dst] = kept
		}
	}
}

// sweepDiscovery periodically cleans up discovery nodes with expired heartbeats, and synchronously cleans up graph/metadata to prevent memory and graph residue over long runs.
func (s *Server) sweepDiscovery() {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	cutoff := now.Add(-s.heartbeatTTL)

	active := make(map[string]bool)
	for coll, peers := range s.disc {
		for id, last := range peers {
			if last.Before(cutoff) {
				delete(peers, id)
				continue
			}
			active[id] = true
		}
		if len(peers) == 0 {
			delete(s.disc, coll)
		}
	}

	// Clean up graph origins of nodes that are no longer active
	for id := range s.peerLinks {
		if !active[id] {
			delete(s.peerLinks, id)
		}
	}
	// Clean up edges pointing to nodes that are no longer active
	for _, links := range s.peerLinks {
		for nid := range links {
			if !active[nid] {
				delete(links, nid)
			}
		}
	}
	// Clean up stats/collections of nodes that are no longer active
	for id := range s.peerStats {
		if !active[id] {
			delete(s.peerStats, id)
		}
	}
	for id := range s.peerColls {
		if !active[id] {
			delete(s.peerColls, id)
		}
	}
}

// client is an online signaling connection.
type client struct {
	id     string
	token  string
	conn   *websocket.Conn
	sendMu sync.Mutex // gorilla does not allow concurrent writes
	last   time.Time  // last heartbeat
}

// queuedMsg is an offline queue entry (with expiry time set at enqueue time).
type queuedMsg struct {
	msg    Message
	expire time.Time
}

// Message is a signaling message (consistent with the peerjs client protocol).
type Message struct {
	Type    string          `json:"type"`
	Src     string          `json:"src,omitempty"`
	Dst     string          `json:"dst,omitempty"`
	Payload json.RawMessage `json:"payload,omitempty"`
}

// NewServer creates a signaling server.
func NewServer(key string, opts ...Option) *Server {
	if key == "" {
		key = "peerjs"
	}
	s := &Server{
		key:          key,
		path:         "",
		queueTTL:     30 * time.Second,
		heartbeatTTL: 90 * time.Second,
		startedAt:    time.Now(),
		clients:      make(map[string]*client),
		queues:       make(map[string][]queuedMsg),
		disc:         make(map[string]map[string]time.Time),
		peerLinks:    make(map[string]map[string]time.Time),
		peerColls:    make(map[string][]string),
		peerStats:    make(map[string]*PeerStats),
	}
	for _, o := range opts {
		o(s)
	}
	return s
}

// allowCORS opens up cross-origin and short-circuits OPTIONS preflight.
//
// Discovery background: peerdrive's cloud storage UI is "a shared static panel (packages/peerdrive-client
// dist/panel.html, which can be opened via file:// double-click or hosted on any static space), connecting directly to nodes via PeerJS".
// The panel's first step is `GET /peerjs/id` to request a temporary id from the signaling server —— and the browser computes the
// origin of that page as `null` (file://) or the panel's own domain, **which is cross-origin from the signaling server**: responses without Access-Control-Allow-Origin
// are silently dropped by same-origin policy, and the client only shows a vague
// `server-error: Could not get an ID from the server`, with no indication it's CORS.
// These REST endpoints (fetch random id, discovery) are inherently public information, with no credentials to be stolen,
// so opening them up is safe. WS handshakes don't go through CORS and don't need handling.
func allowCORS(w http.ResponseWriter) {
	h := w.Header()
	h.Set("Access-Control-Allow-Origin", "*")
	h.Set("Access-Control-Allow-Headers", "Content-Type")
	h.Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
}

// allowCORSFor is allowCORS with the optional explicit allow-list applied (2026-10-04).
//
// No list configured -> wildcard, byte-identical to the historical behavior, so not
// configuring anything keeps every existing deployment working exactly as before.
// List configured -> echo the request Origin when it matches; when it does not match,
// deliberately emit **no** Allow-Origin header, which is exactly what makes the browser
// block the read. That is the point of the switch: a wildcard means "every site on the
// internet may read this", which is what turned the discovery endpoint into a one-fetch
// census of every node on the network.
func (s *Server) allowCORSFor(w http.ResponseWriter, r *http.Request) {
	if len(s.corsOrigins) == 0 {
		allowCORS(w)
		return
	}
	origin := r.Header.Get("Origin")
	for _, allowed := range s.corsOrigins {
		if allowed == "*" {
			allowCORS(w)
			return
		}
		if origin != "" && strings.EqualFold(origin, allowed) {
			h := w.Header()
			h.Set("Access-Control-Allow-Origin", origin)
			h.Set("Access-Control-Allow-Headers", "Content-Type")
			h.Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
			return
		}
	}
	// Not allow-listed: fall through with no Allow-Origin, so the browser blocks it.
}

// handleCORS writes cross-origin headers and handles preflight; returns true when the request is fully handled and the caller should return immediately.
func (s *Server) handleCORS(w http.ResponseWriter, r *http.Request) bool {
	s.allowCORSFor(w, r)
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusNoContent)
		return true
	}
	return false
}

// handleCORSPreflight is handleCORS **without** the wildcard header, for endpoints that
// must not be readable cross-origin.
//
// Why (2026-10-04): allowCORS sets `Access-Control-Allow-Origin: *`, which is load-bearing for
// the public panel — it is opened via file:// (Origin `null`) or from any static host, so all of
// its origins are cross-origin from the signaling server (see the allowCORS comment).
// But that same header on the **ops-facing** endpoints turns "which nodes exist on this
// network" into something any web page can read. So: panel endpoints keep the wildcard,
// ops endpoints keep only the preflight short-circuit and omit the allow-origin header —
// a cross-origin caller then gets an opaque response, a same-origin/curl caller is unaffected.
func handleCORSPreflight(w http.ResponseWriter, r *http.Request) bool {
	if r.Method == http.MethodOptions {
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		w.WriteHeader(http.StatusNoContent)
		return true
	}
	return false
}

// tokenFromRequest reads the signaling token from the query string or the
// `Authorization: Bearer` header.
//
// Two shapes accepted on purpose: the WebSocket upgrade can only carry a query param
// (see HandleWS), while curl/dashboard scripts naturally send a header.
func tokenFromRequest(r *http.Request) string {
	if t := r.URL.Query().Get("token"); t != "" {
		return t
	}
	if h := r.Header.Get("Authorization"); len(h) > 7 && strings.EqualFold(h[:7], "bearer ") {
		return strings.TrimSpace(h[7:])
	}
	return ""
}

// opsTokenOK reports whether this request may use ops-facing REST endpoints (/status and
// the full roster). Policy (2026-10-04):
//
//   - When a token whitelist is configured, a presented token must be on it.
//   - When **no** whitelist is configured (the default), any non-empty token passes.
//     This is deliberate: default deployments have no token to present, and hard-failing
//     would break every existing /status dashboard. The real fix is operators turning the
//     whitelist on (`-tokens`); this keeps that the single switch.
//
// An empty token never passes — that is what closes the "any web page reads the roster"
// hole for the default deployment, because a browser cannot conjure a token.
func (s *Server) opsTokenOK(r *http.Request) bool {
	t := tokenFromRequest(r)
	if t == "" {
		return false
	}
	if len(s.tokenWhitelist) == 0 {
		return true
	}
	return s.tokenWhitelist[t]
}

// HandleID GET /{path}{key}/id → random id (peerjs API compatible).
func (s *Server) HandleID(w http.ResponseWriter, r *http.Request) {
	if s.handleCORS(w, r) {
		return
	}
	w.Header().Set("Content-Type", "text/plain")
	fmt.Fprint(w, randomID())
}

// HandleWS handles signaling WebSocket upgrade and the message loop.
// Path is shaped like /{path}peerjs?key=&id=&token= ({path} is provided when mounted via gin routing).
func (s *Server) HandleWS(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	id, token, key := q.Get("id"), q.Get("token"), q.Get("key")
	if id == "" || token == "" || key == "" {
		s.wsError(w, "No id, token, or key supplied to websocket server")
		return
	}
	if key != s.key {
		s.wsError(w, "Invalid key provided")
		return
	}
	// Token whitelist: empty list = unrestricted (default); if non-empty, token must be on the list.
	// See the pitfall note in WithTokenWhitelist (any token can impersonate a node to receive signaling).
	if len(s.tokenWhitelist) > 0 && !s.tokenWhitelist[token] {
		s.wsError(w, "Invalid token provided")
		return
	}

	upgrader := websocket.Upgrader{
		CheckOrigin: func(*http.Request) bool { return true }, // self-hosted, whitelist configured by the caller
	}
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	// Read limit: signaling messages are small (SDP/ICE payloads), 40KB is sufficient; prevents malicious clients from stuffing huge payloads.
	// Also set a 60s read timeout as a fallback—readLoop refreshes it via HEARTBEAT, so disconnected clients don't hold resources.
	conn.SetReadLimit(40 << 10)
	_ = conn.SetReadDeadline(time.Now().Add(60 * time.Second))

	s.mu.Lock()
	// ID occupancy: if token matches, reuse the connection; otherwise reject
	if existing, ok := s.clients[id]; ok {
		if existing.token != token {
			s.mu.Unlock()
			_ = conn.WriteJSON(Message{Type: "ID-TAKEN", Payload: raw(`{"msg":"ID is taken"}`)})
			_ = conn.Close()
			return
		}
		existing.closeConn()
	}
	cl := &client{id: id, token: token, conn: conn, last: time.Now()}
	s.clients[id] = cl
	s.mu.Unlock()

	_ = cl.send(Message{Type: "OPEN"})
	s.flushQueue(cl)

	go s.readLoop(cl)
}

// readLoop reads client messages and routes them.
func (s *Server) readLoop(cl *client) {
	defer func() {
		s.removeClient(cl)
		cl.closeConn()
	}()
	for {
		var m Message
		if err := cl.conn.ReadJSON(&m); err != nil {
			return
		}
		m.Src = cl.id // server overwrites src
		s.mu.Lock()
		cl.last = time.Now()
		// Receiving a message counts as active; extend the read timeout (paired with SetReadDeadline in HandleWS)
		_ = cl.conn.SetReadDeadline(time.Now().Add(60 * time.Second))
		s.mu.Unlock()
		s.route(m)
	}
}

// route routes messages: forward if dst is online, queue if offline (except LEAVE/EXPIRE).
//
// Two fixes aligned with peers/peerjs-server (src/messageHandler/handlers/transmission):
//
//  1. send failures are no longer silently dropped. The old implementation `_ = dst.send(m)`: when the target socket is half-open but still
//     hasn't been removed from the clients table yet (peer crashed, NAT mapping expired, connection half-open without FIN),
//     OFFER/ANSWER/CANDIDATE messages are swallowed, leaving the initiator stuck waiting for handshake forever. Here we remove the dead connection
//     and send a supplementary LEAVE to the initiator so it stops retrying.
//  2. Release s.mu before send. WriteJSON inside send has a 10s write timeout; a slow/dead client
//     would hold the lock for 10s and freeze the entire signaling server's routing. So we unlock before writing.
func (s *Server) route(m Message) {
	s.mu.Lock()
	dst := s.clients[m.Dst]
	if dst != nil {
		s.mu.Unlock() // unlock before writing, to avoid blocking on s.mu held by a slow client
		if err := dst.send(m); err == nil {
			atomic.AddInt64(&s.msgCount, 1)
			return
		}
		s.handleDeadDst(dst, m)
		return
	}
	s.mu.Unlock()

	if m.Type == "LEAVE" || m.Type == "EXPIRE" || m.Dst == "" {
		return
	}
	// Enqueue: replay after the target comes online (OFFER/ANSWER/CANDIDATE)
	// H3: unbounded queue → OOM. When exceeding maxQueuedPerDst, drop the oldest.
	s.mu.Lock()
	defer s.mu.Unlock()
	q := append(s.queues[m.Dst], queuedMsg{msg: m, expire: time.Now().Add(s.queueTTL)})
	if len(q) > maxQueuedPerDst {
		q = q[len(q)-maxQueuedPerDst:]
	}
	s.queues[m.Dst] = q
	atomic.AddInt64(&s.msgCount, 1)
}

// handleDeadDst handles a target that's "in the clients table but failed to send": remove the entry, close the connection,
// broadcast LEAVE.
func (s *Server) handleDeadDst(dst *client, m Message) {
	s.mu.Lock()
	if cur, ok := s.clients[m.Dst]; !ok || cur != dst {
		s.mu.Unlock()
		return // already removed/replaced by another flow, leave cleanup to that process
	}
	delete(s.clients, m.Dst)
	var victims []*client
	for _, c := range s.clients {
		if c != dst {
			victims = append(victims, c)
		}
	}
	// Clean up the dead connection's residue in discovery/graph/metadata (removeClient won't come back to clean up since the client was already removed)
	for coll, peers := range s.disc {
		delete(peers, m.Dst)
		if len(peers) == 0 {
			delete(s.disc, coll)
		}
	}
	delete(s.peerLinks, m.Dst)
	for _, links := range s.peerLinks {
		delete(links, m.Dst)
	}
	delete(s.peerStats, m.Dst)
	delete(s.peerColls, m.Dst)
	s.mu.Unlock()

	dst.closeConn()
	leave := Message{Type: "LEAVE", Src: m.Dst}
	for _, c := range victims {
		_ = c.send(leave)
	}
	if m.Src == "" || m.Src == m.Dst {
		return
	}
	s.mu.Lock()
	src := s.clients[m.Src]
	s.mu.Unlock()
	if src != nil {
		_ = src.send(Message{Type: "LEAVE", Src: m.Dst, Dst: m.Src})
	}
}

// flushQueue replays the offline queue after a client comes online (with expiry cleanup).
func (s *Server) flushQueue(cl *client) {
	s.mu.Lock()
	q := s.queues[cl.id]
	delete(s.queues, cl.id)
	s.mu.Unlock()
	now := time.Now()
	for _, qm := range q {
		if qm.expire.After(now) {
			_ = cl.send(qm.msg)
		}
	}
}

// removeClient handles disconnect cleanup: notify other nodes with LEAVE, delete discovery records.
func (s *Server) removeClient(cl *client) {
	s.mu.Lock()
	if s.clients[cl.id] != cl {
		s.mu.Unlock()
		return
	}
	delete(s.clients, cl.id)
	leave := Message{Type: "LEAVE", Src: cl.id}
	var victims []*client
	for _, c := range s.clients {
		victims = append(victims, c)
	}
	for coll, peers := range s.disc {
		delete(peers, cl.id)
		if len(peers) == 0 {
			delete(s.disc, coll)
		}
	}
	delete(s.peerLinks, cl.id)
	for _, links := range s.peerLinks {
		delete(links, cl.id)
	}
	delete(s.peerStats, cl.id)
	delete(s.peerColls, cl.id)
	s.mu.Unlock()
	for _, c := range victims {
		_ = c.send(leave)
	}
}

// HandleAnnounce POST /discover/announce {peerId, collections[], nodeType, loadInfo, peers} node registration.
// Decoupled from signaling connections (nodes can report via any HTTP endpoint); lastSeen is refreshed by heartbeats.
// M15: unbounded decode risk—limit body size (8KB is sufficient: peerId + collections + peers + loadInfo)
// and the number of collections (a single node follows a limited number of rooms).
func (s *Server) HandleAnnounce(w http.ResponseWriter, r *http.Request) {
	if s.handleCORS(w, r) {
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, 8<<10)
	var body struct {
		PeerID      string         `json:"peerId"`
		Collections []string       `json:"collections"`
		NodeType    string         `json:"nodeType,omitempty"`
		LoadInfo    map[string]any `json:"loadInfo,omitempty"`
		Peers       []string       `json:"peers,omitempty"` // list of currently WebRTC-connected peer ids
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.PeerID == "" {
		http.Error(w, "peerId required", http.StatusBadRequest)
		return
	}
	const maxCollectionsPerAnnounce = 64
	if len(body.Collections) > maxCollectionsPerAnnounce {
		http.Error(w, "too many collections", http.StatusBadRequest)
		return
	}
	// Normalize the collection list: trim whitespace and remove empty strings; disc and peerColls use the same data.
	cleanColls := make([]string, 0, len(body.Collections))
	for _, coll := range body.Collections {
		coll = strings.TrimSpace(coll)
		if coll != "" {
			cleanColls = append(cleanColls, coll)
		}
	}

	now := time.Now()
	s.mu.Lock()
	collSet := make(map[string]bool, len(cleanColls))
	for _, coll := range cleanColls {
		collSet[coll] = true
		peers, ok := s.disc[coll]
		if !ok {
			peers = make(map[string]time.Time)
			s.disc[coll] = peers
		}
		peers[body.PeerID] = now
	}
	// Immediately remove this node from old collections (no need to wait for TTL after collections change)
	for coll, peers := range s.disc {
		if !collSet[coll] {
			delete(peers, body.PeerID)
			if len(peers) == 0 {
				delete(s.disc, coll)
			}
		}
	}
	// Update node metadata (nodeType/collections/loadInfo), shared by dashboard and discovery API.
	stats := s.peerStats[body.PeerID]
	if stats == nil {
		stats = &PeerStats{}
		s.peerStats[body.PeerID] = stats
	}
	stats.NodeType = body.NodeType
	stats.LastSeen = now
	stats.LoadInfo = body.LoadInfo
	// Always update collections (even clear old values when empty) to avoid stale collections remaining after a node clears its set.
	s.peerColls[body.PeerID] = cleanColls
	// Update graph edges: this node's currently directly connected peers.
	// Always update (even clear old edges when peers is empty/missing) to avoid stale connections lingering in the graph.
	links := make(map[string]time.Time, len(body.Peers))
	for _, nid := range body.Peers {
		nid = strings.TrimSpace(nid)
		if nid != "" && nid != body.PeerID {
			links[nid] = now
		}
	}
	s.peerLinks[body.PeerID] = links
	s.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write([]byte(`{"ok":true}`))
}

// HandleLeave POST /discover/leave graceful node shutdown.
func (s *Server) HandleLeave(w http.ResponseWriter, r *http.Request) {
	if s.handleCORS(w, r) {
		return
	}
	var body struct {
		PeerID string `json:"peerId"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.PeerID == "" {
		http.Error(w, "peerId required", http.StatusBadRequest)
		return
	}

	s.mu.Lock()
	// Remove from all collections
	for _, peers := range s.disc {
		delete(peers, body.PeerID)
	}
	delete(s.peerStats, body.PeerID)
	delete(s.peerColls, body.PeerID)
	delete(s.peerLinks, body.PeerID)
	// Also remove this node from other nodes' neighbor lists
	for _, links := range s.peerLinks {
		delete(links, body.PeerID)
	}
	s.mu.Unlock()

	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write([]byte(`{"ok":true}`))
}

// GraphLink is an edge in the graph (source ↔ target have established a WebRTC connection).
type GraphLink struct {
	Source   string `json:"source"`
	Target   string `json:"target"`
	LastSeen int64  `json:"lastSeen,omitempty"`
}

// HandleNodes GET /discover/nodes?coll=&type= → online node list (heartbeat-expired entries removed) + graph edges.
// Empty coll means return nodes from all collections; type can be used to filter node types.
func (s *Server) HandleNodes(w http.ResponseWriter, r *http.Request) {
	if s.handleCORS(w, r) {
		return
	}
	coll := r.URL.Query().Get("coll")
	nodeType := r.URL.Query().Get("type")
	cutoff := time.Now().Add(-s.heartbeatTTL)
	s.mu.Lock()
	out := make([]NodeInfo, 0)
	seen := make(map[string]bool)

	if coll != "" {
		// Specified collection: query only this room
		peers := s.disc[coll]
		out = make([]NodeInfo, 0, len(peers))
		for id, last := range peers {
			if last.After(cutoff) && !seen[id] {
				info := s.nodeInfo(id, last)
				// Don't mark as seen when type filter doesn't pass: allow this node to be checked again in other collections
				// while graph edges are only based on actually returned nodes.
				if nodeType != "" && info.NodeType != nodeType {
					continue
				}
				seen[id] = true
				out = append(out, info)
			}
		}
	} else {
		// 2026-10-04: `type` is an **ops filter** — the public panel never sends it
		// (panel/app.js discoverNow calls discoverNodes(sig) with neither coll nor type, to answer
		// "who is online at all"). The dashboard uses it to colour the node graph by role.
		// Gating on "no coll" was this change's first attempt and it was **wrong**: CI caught it —
		// the E2E panel step failed with "Discovery service returned HTTP 401" because the panel's
		// own auto-search IS a coll-less query. Keep the no-coll form public (it is the panel's
		// entry point) and require an ops token only for the inventory-style query.
		// Trade-off accepted for now: the roster is still enumerable without a token, because the
		// panel depends on it. What is closed here is /status (ops-only) and its cross-origin
		// readability; the roster proper waits for Round 3 panel token support.
		if nodeType != "" && !s.opsTokenOK(r) {
			s.mu.Unlock()
			http.Error(w, "ops token required for ?type= queries", http.StatusUnauthorized)
			return
		}
		// Empty coll: iterate all collections and return deduplicated online nodes
		for _, peers := range s.disc {
			for id, last := range peers {
				if last.After(cutoff) && !seen[id] {
					info := s.nodeInfo(id, last)
					if nodeType != "" && info.NodeType != nodeType {
						continue
					}
					seen[id] = true
					out = append(out, info)
				}
			}
		}
	}
	// Collect graph edges: only keep edges where both ends are still active, to avoid showing offline ghost nodes.
	links := make([]GraphLink, 0)
	linkSeen := make(map[string]bool)
	for src, neighbors := range s.peerLinks {
		if !seen[src] {
			continue
		}
		for dst, last := range neighbors {
			if !seen[dst] {
				continue
			}
			a, b := src, dst
			if a > b {
				a, b = b, a
			}
			key := a + "\x00" + b
			if linkSeen[key] {
				continue
			}
			linkSeen[key] = true
			links = append(links, GraphLink{Source: a, Target: b, LastSeen: last.Unix()})
		}
	}
	s.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"nodes": out, "links": links})
}

// PeerStats is node statistics and load information.
type PeerStats struct {
	NodeType string         `json:"nodeType,omitempty"`
	Uptime   int64          `json:"uptime,omitempty"`
	LoadInfo map[string]any `json:"loadInfo,omitempty"`
	LastSeen time.Time      `json:"-"`
}

// nodeInfo assembles a discovery response entry from peerStats/peerColls.
func (s *Server) nodeInfo(id string, last time.Time) NodeInfo {
	info := NodeInfo{PeerID: id, LastSeen: last.Unix()}
	if st := s.peerStats[id]; st != nil {
		info.NodeType = st.NodeType
		info.Uptime = st.Uptime
		info.LoadInfo = st.LoadInfo
	}
	if colls := s.peerColls[id]; len(colls) > 0 {
		info.Collections = colls
	}
	return info
}

// NodeInfo is a discovery response entry.
type NodeInfo struct {
	PeerID      string         `json:"peerId"`
	LastSeen    int64          `json:"lastSeen"`
	NodeType    string         `json:"nodeType,omitempty"`
	Collections []string       `json:"collections,omitempty"`
	Uptime      int64          `json:"uptime,omitempty"`
	LoadInfo    map[string]any `json:"loadInfo,omitempty"`
}

// HandleStatus GET /status → server status snapshot (for dashboard polling).
//
// 2026-10-04: now requires an ops token and drops the wildcard CORS header.
// /status embeds the full online-node roster (peer ids + share summary), so leaving it
// readable from any web page turned the roster into a one-fetch list of every node on the
// network. The bundled dashboard.html is served from this same origin, so it is unaffected;
// external tooling should pass `?token=` or an Authorization header.
func (s *Server) HandleStatus(w http.ResponseWriter, r *http.Request) {
	// CORS preflight first (Round 3), then the ops-token gate (Round 2). Both apply and
	// neither replaces the other: the preflight decides what the browser may read, the
	// token decides who may read it at all. Resolved conflict during the 2026-10-04 merge
	// of PR #4 and PR #5, which both touched this block.
	if handleCORSPreflight(w, r) {
		return
	}
	if !s.opsTokenOK(r) {
		http.Error(w, "ops token required", http.StatusUnauthorized)
		return
	}
	s.mu.Lock()
	clientCount := len(s.clients)
	queueCount := len(s.queues)
	totalQueued := 0
	for _, q := range s.queues {
		totalQueued += len(q)
	}
	// Collect active discovery nodes
	discoveredNodes := make([]NodeInfo, 0)
	seen := make(map[string]bool)
	cutoff := time.Now().Add(-s.heartbeatTTL)
	for _, peers := range s.disc {
		for id, last := range peers {
			if last.After(cutoff) && !seen[id] {
				seen[id] = true
				discoveredNodes = append(discoveredNodes, s.nodeInfo(id, last))
			}
		}
	}
	// Collect graph edges: only keep edges where both ends are still active, to avoid showing offline ghost nodes.
	links := make([]GraphLink, 0)
	linkSeen := make(map[string]bool)
	for src, neighbors := range s.peerLinks {
		if !seen[src] {
			continue
		}
		for dst, last := range neighbors {
			if !seen[dst] {
				continue
			}
			a, b := src, dst
			if a > b {
				a, b = b, a
			}
			key := a + "\x00" + b
			if linkSeen[key] {
				continue
			}
			linkSeen[key] = true
			links = append(links, GraphLink{Source: a, Target: b, LastSeen: last.Unix()})
		}
	}
	s.mu.Unlock()

	uptime := time.Since(s.startedAt).Seconds()
	resp := map[string]any{
		"key":         s.key,
		"uptimeSec":   int64(uptime),
		"uptimeStr":   formatDuration(uptime),
		"clients":     clientCount,
		"queues":      queueCount,
		"totalQueued": totalQueued,
		"discovered":  len(discoveredNodes),
		"nodes":       discoveredNodes,
		"links":       links,
		"msgCount":    atomic.LoadInt64(&s.msgCount),
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

// HandleDashboard GET / → dashboard HTML (embedded).
func (s *Server) HandleDashboard(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	b, err := dashboardFS.ReadFile("dashboard.html")
	if err != nil {
		http.Error(w, "dashboard not found", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "no-cache, no-store, must-revalidate")
	_, _ = w.Write(b)
}

// formatDuration converts seconds to a human-readable duration.
func formatDuration(seconds float64) string {
	d := time.Duration(seconds) * time.Second
	if d < time.Minute {
		return fmt.Sprintf("%.0fs", seconds)
	}
	if d < time.Hour {
		return fmt.Sprintf("%.1fm", seconds/60)
	}
	if d < 24*time.Hour {
		return fmt.Sprintf("%.1fh", seconds/3600)
	}
	return fmt.Sprintf("%.1fd", seconds/86400)
}

// wsError handles upgrade failure (HTTP layer).
func (s *Server) wsError(w http.ResponseWriter, msg string) {
	http.Error(w, msg, http.StatusBadRequest)
}

func (cl *client) send(m Message) error {
	cl.sendMu.Lock()
	defer cl.sendMu.Unlock()
	cl.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
	return cl.conn.WriteJSON(m)
}

func (cl *client) closeConn() {
	cl.sendMu.Lock()
	defer cl.sendMu.Unlock()
	_ = cl.conn.Close()
}

func raw(s string) json.RawMessage { return json.RawMessage(s) }

// randomID generates an alphanumeric id conforming to PeerJS rules.
func randomID() string {
	const chars = "abcdefghijklmnopqrstuvwxyz0123456789"
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	for i := range b {
		b[i] = chars[b[i]%byte(len(chars))]
	}
	return string(b)
}
