package localdns

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"golang.org/x/net/dns/dnsmessage"
)

// Run starts the local DNS server that forwards queries via DoH.
func Run(dohEndpoint string, port int) error {
	if port == 0 {
		port = 5353
	}
	addr := &net.UDPAddr{Port: port}
	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		return err
	}
	defer conn.Close()
	log.Printf("Listening on UDP :%d, forwarding to %s", addr.Port, dohEndpoint)

	// DoH 客户端复用，避免每次查询新建连接与 Transport。
	dohClient := &http.Client{Timeout: 10 * time.Second}

	// 4. Graceful shutdown handling
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		log.Println("Shutting down...")
		cancel()
		conn.Close()
	}()

	// 5. Main loop – handle each packet concurrently.
	// 512 字节只够经典 DNS；多数查询带 EDNS0 会更大，使用 4096 缓冲。
	// 超长报文直接丢弃，避免截断污染。
	const maxPacket = 4096
	buf := make([]byte, maxPacket)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		n, clientAddr, err := conn.ReadFromUDP(buf)
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return nil
			}
			log.Printf("Read error: %v", err)
			continue
		}
		if n == maxPacket {
			log.Printf("Dropping oversized DNS packet (%d bytes) from %v", n, clientAddr)
			continue
		}

		query := make([]byte, n)
		copy(query, buf[:n])

		go handleQuery(conn, clientAddr, query, dohClient, dohEndpoint)
	}
}

// handleQuery processes one DNS query: forward via DoH and send back the answer.
func handleQuery(conn *net.UDPConn, clientAddr *net.UDPAddr, query []byte, dohClient *http.Client, dohEndpoint string) {
	// 1. Parse the incoming DNS message (optional – we could forward blindly,
	//    but we need to keep the original ID and flags for the response)
	var msg dnsmessage.Message
	if err := msg.Unpack(query); err != nil {
		log.Printf("Failed to unpack DNS query from %v: %v", clientAddr, err)
		return
	}

	// 2. Send the raw query to the DoH endpoint (POST with application/dns-message)
	req, err := http.NewRequest("POST", dohEndpoint, bytes.NewReader(query))
	if err != nil {
		log.Printf("Failed to create DoH request: %v", err)
		return
	}
	req.Header.Set("Content-Type", "application/dns-message")
	req.Header.Set("Accept", "application/dns-message")

	resp, err := dohClient.Do(req)
	if err != nil {
		log.Printf("DoH request failed for %v: %v", clientAddr, err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		log.Printf("DoH returned non-200 status: %d", resp.StatusCode)
		return
	}

	// 3. Read the DoH response (binary DNS message)
	response, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Printf("Failed to read DoH response body: %v", err)
		return
	}

	// 4. (Optional) Verify the response matches the query ID – but the resolver
	//    will have used the same ID, so we can just send it back.

	// 5. Send the response back to the original client over UDP
	if _, err := conn.WriteToUDP(response, clientAddr); err != nil {
		log.Printf("Failed to send response to %v: %v", clientAddr, err)
	}
}
