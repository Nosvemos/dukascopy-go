package cli

import (
	"context"
	"crypto/sha1"
	"encoding/base64"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"
)

// wsHub is the WebSocket broadcast hub (zero external dependencies)
type wsHub struct {
	mu      sync.RWMutex
	clients map[chan []byte]struct{}
}

func newWSHub() *wsHub {
	return &wsHub{clients: make(map[chan []byte]struct{})}
}

func (h *wsHub) register(ch chan []byte) {
	h.mu.Lock()
	h.clients[ch] = struct{}{}
	h.mu.Unlock()
}

func (h *wsHub) unregister(ch chan []byte) {
	h.mu.Lock()
	delete(h.clients, ch)
	h.mu.Unlock()
	// drain channel so sender never blocks
	for len(ch) > 0 {
		<-ch
	}
}

func (h *wsHub) broadcast(msg []byte) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	for ch := range h.clients {
		select {
		case ch <- msg:
		default: // slow client — drop frame rather than block
		}
	}
}

func (h *wsHub) count() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.clients)
}

// wsHandshakeGUID is the fixed magic string from RFC 6455 §1.3.
const wsHandshakeGUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"

func wsComputeAcceptKey(clientKey string) string {
	h := sha1.New()
	h.Write([]byte(clientKey + wsHandshakeGUID))
	return base64.StdEncoding.EncodeToString(h.Sum(nil))
}

func wsHandler(hub *wsHub) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// Validate upgrade request
		if !strings.EqualFold(r.Header.Get("Upgrade"), "websocket") {
			http.Error(w, "not a websocket handshake", http.StatusBadRequest)
			return
		}
		clientKey := r.Header.Get("Sec-Websocket-Key")
		if clientKey == "" {
			http.Error(w, "missing Sec-WebSocket-Key", http.StatusBadRequest)
			return
		}

		acceptKey := wsComputeAcceptKey(clientKey)

		// Hijack the connection
		hj, ok := w.(http.Hijacker)
		if !ok {
			http.Error(w, "server does not support hijacking", http.StatusInternalServerError)
			return
		}
		conn, bufrw, err := hj.Hijack()
		if err != nil {
			return
		}

		// Send 101 Switching Protocols
		resp := "HTTP/1.1 101 Switching Protocols\r\n" +
			"Upgrade: websocket\r\n" +
			"Connection: Upgrade\r\n" +
			"Sec-WebSocket-Accept: " + acceptKey + "\r\n\r\n"
		_, _ = bufrw.WriteString(resp)
		_ = bufrw.Flush()

		// Register client channel
		ch := make(chan []byte, 64)
		hub.register(ch)
		defer func() {
			hub.unregister(ch)
			conn.Close()
		}()

		ctx, cancel := context.WithCancel(r.Context())
		defer cancel()

		// Start a read loop in a background goroutine to detect disconnects.
		// When the client disconnects or closes the connection, Read will error.
		// Calling cancel() and conn.Close() will unblock the writer loop select.
		go func() {
			buf := make([]byte, 256)
			for {
				if _, err := conn.Read(buf); err != nil {
					cancel()
					conn.Close()
					return
				}
			}
		}()

		// Write frames to client until context is cancelled or write fails
		for {
			select {
			case <-ctx.Done():
				return
			case msg, ok := <-ch:
				if !ok {
					return
				}
				if err := wsWriteTextFrame(conn, msg); err != nil {
					return
				}
			}
		}
	}
}

// wsWriteTextFrame writes a minimal RFC-6455 text frame (server→client, no masking).
func wsWriteTextFrame(conn net.Conn, payload []byte) error {
	l := len(payload)
	var header []byte
	switch {
	case l <= 125:
		header = []byte{0x81, byte(l)}
	case l <= 65535:
		header = []byte{0x81, 126, byte(l >> 8), byte(l & 0xff)}
	default:
		header = []byte{
			0x81, 127,
			0, 0, 0, 0,
			byte(l >> 24), byte((l >> 16) & 0xff), byte((l >> 8) & 0xff), byte(l & 0xff),
		}
	}
	_ = conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
	defer func() {
		_ = conn.SetWriteDeadline(time.Time{})
	}()
	if _, err := conn.Write(header); err != nil {
		return err
	}
	if _, err := conn.Write(payload); err != nil {
		return err
	}
	return nil
}
