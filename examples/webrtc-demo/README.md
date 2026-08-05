# WebRTC Demo

This minimal example demonstrates a **WebRTC Peer‑to‑Peer** data channel using the existing **KV signalling server** (`cmd/api-server`).

## How it works

1. **Room creation** – the initiator (`p1`) generates a UUID and stores the SDP **offer** in the KV store at:
   ```
   POST /kv/room/<roomID>/offer
   ```
   The body is a JSON object `{"type":"offer","sdp":"..."}`.
2. The responder (`p2`) retrieves the offer via a long‑poll GET request:
   ```
   GET /kv/room/<roomID>/offer?wait=30
   ```
3. `p2` creates an SDP **answer**, stores it at:
   ```
   POST /kv/room/<roomID>/answer
   ```
4. `p1` retrieves the answer, sets it as the remote description and both peers finish ICE negotiation (non‑trickle, so all candidates are embedded in the SDP).
5. Once the **DataChannel** (`chat`) is open, each side sends a single test message (`ping` / `pong`).

## Build & Run

```bash
# Build the demo (requires Go 1.26+)
cd examples/webrtc-demo
go build -o webrtc-demo
```

### Initiator (p1)
```bash
./webrtc-demo -mode p1
```
It will print a generated room ID, post the offer, wait for the answer and then open the data channel. Keep the process running – it will listen for the reply.

### Responder (p2)
```bash
./webrtc-demo -mode p2 -room <roomID>
```
Replace `<roomID>` with the ID printed by the initiator. The responder fetches the offer, creates an answer, posts it back and sends a `pong` message.

## Signalling server

The demo talks to the KV server that is already running at:
```
http://bwh.moonchan.xyz:8080
```
If you run a local `cmd/api-server`, change the `-server` flag accordingly:
```bash
./webrtc-demo -server http://localhost:8080 -mode p1
```

## Expected output

**p1**
```
Created room ID: a1b2c3d4-...-...
Offer posted, waiting for answer...
Answer applied, connection should be ready
DataChannel open, sending ping...
[p1] received: pong from p2
```

**p2**
```
Joining room ID: a1b2c3d4-...-...
Answer posted, waiting for data channel...
DataChannel open, sending reply...
[p2] received: ping from p1
```

Both sides will keep running after the exchange, ready for further messages.

---

*Note*: This demo uses **non‑trickle ICE** (the SDP contains all ICE candidates) for simplicity. The signalling exchange is performed via the generic KV store; no custom `/room` endpoint is required.
