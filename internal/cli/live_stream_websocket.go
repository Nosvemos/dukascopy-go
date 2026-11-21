package cli

import (
	"bufio"
	"context"
	"crypto/sha1"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
)

// WebSocket opcodes (RFC 6455 §5.2).
const (
	wsOpContinuation = 0x0
	wsOpText         = 0x1
	wsOpBinary       = 0x2
	wsOpClose        = 0x8
	wsOpPing         = 0x9
	wsOpPong         = 0xA
)

// WebSocket close status codes (RFC 6455 §7.4.1).
const (
	wsCloseNormal        = 1000
	wsCloseGoingAway     = 1001
	wsCloseProtocolError = 1002
	wsCloseTooBig        = 1009
)

const (
	// wsMaxControlPayload is the RFC 6455 limit for control frame payloads.
	wsMaxControlPayload = 125
	// wsMaxClientPayload bounds data frames sent by clients. The stream is
	// server-push only, so clients have no reason to send large messages.
	wsMaxClientPayload = 64 * 1024
	// wsWriteTimeout bounds every frame write to a client.
	wsWriteTimeout = 5 * time.Second
)

// wsReadHeaderTimeout bounds how long a client may take to send the HTTP
// handshake headers. It is a variable so tests can shorten it.
var wsReadHeaderTimeout = 10 * time.Second

// wsOptions configures the WebSocket handshake.
type wsOptions struct {
	// AllowedOrigins lists extra browser origins (e.g. "http://localhost:3000")
	// that may connect; "*" allows any origin. Requests without an Origin
	// header (non-browser clients) and same-origin requests are always allowed.
	AllowedOrigins []string
}

// wsHub is the WebSocket broadcast hub (zero external dependencies). It also
// tracks hijacked connections, which http.Server.Shutdown does not close.
type wsHub struct {
	mu      sync.RWMutex
	clients map[chan []byte]struct{}
	conns   map[*wsConn]struct{}
	closed  bool
}

func newWSHub() *wsHub {
	return &wsHub{
		clients: make(map[chan []byte]struct{}),
		conns:   make(map[*wsConn]struct{}),
	}
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

// trackConn records a hijacked connection. It returns false once the hub has
// been closed, in which case the caller must drop the connection.
func (h *wsHub) trackConn(c *wsConn) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return false
	}
	h.conns[c] = struct{}{}
	return true
}

func (h *wsHub) untrackConn(c *wsConn) {
	h.mu.Lock()
	delete(h.conns, c)
	h.mu.Unlock()
}

// closeAll sends a "going away" close frame to every connected client and
// closes the connections. Later handshakes are refused.
func (h *wsHub) closeAll() {
	h.mu.Lock()
	h.closed = true
	conns := make([]*wsConn, 0, len(h.conns))
	for c := range h.conns {
		conns = append(conns, c)
	}
	h.conns = make(map[*wsConn]struct{})
	h.mu.Unlock()

	for _, c := range conns {
		_ = c.writeClose(wsCloseGoingAway, "server shutting down")
		_ = c.conn.Close()
	}
}

// wsHandshakeGUID is the fixed magic string from RFC 6455 §1.3.
const wsHandshakeGUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"

func wsComputeAcceptKey(clientKey string) string {
	h := sha1.New()
	h.Write([]byte(clientKey + wsHandshakeGUID))
	return base64.StdEncoding.EncodeToString(h.Sum(nil))
}

// headerHasToken reports whether a comma-separated header contains token
// (case-insensitive), e.g. "keep-alive, Upgrade" contains "upgrade".
func headerHasToken(header http.Header, name string, token string) bool {
	for _, value := range header.Values(name) {
		for _, part := range strings.Split(value, ",") {
			if strings.EqualFold(strings.TrimSpace(part), token) {
				return true
			}
		}
	}
	return false
}

// wsValidateHandshake checks an upgrade request and returns the HTTP status
// and message to reject it with, or 0 when it is acceptable.
func wsValidateHandshake(r *http.Request, opts wsOptions) (int, string) {
	if r.Method != http.MethodGet {
		return http.StatusBadRequest, "websocket handshake requires GET"
	}
	if !headerHasToken(r.Header, "Connection", "upgrade") {
		return http.StatusBadRequest, "not a websocket handshake: missing Connection: Upgrade"
	}
	if !headerHasToken(r.Header, "Upgrade", "websocket") {
		return http.StatusBadRequest, "not a websocket handshake: missing Upgrade: websocket"
	}
	if strings.TrimSpace(r.Header.Get("Sec-WebSocket-Version")) != "13" {
		return http.StatusUpgradeRequired, "unsupported Sec-WebSocket-Version (only 13 is supported)"
	}
	key := strings.TrimSpace(r.Header.Get("Sec-WebSocket-Key"))
	if key == "" {
		return http.StatusBadRequest, "missing Sec-WebSocket-Key"
	}
	if decoded, err := base64.StdEncoding.DecodeString(key); err != nil || len(decoded) != 16 {
		return http.StatusBadRequest, "invalid Sec-WebSocket-Key"
	}
	if !wsOriginAllowed(r, opts) {
		return http.StatusForbidden, "cross-origin websocket connection refused (see --allow-origin)"
	}
	return 0, ""
}

// wsOriginAllowed allows requests without an Origin header (non-browser
// clients), same-origin requests (Origin host == Host header) and origins
// listed in opts.AllowedOrigins ("*" allows all).
func wsOriginAllowed(r *http.Request, opts wsOptions) bool {
	origin := strings.TrimSpace(r.Header.Get("Origin"))
	if origin == "" {
		return true
	}
	for _, allowed := range opts.AllowedOrigins {
		allowed = strings.TrimRight(strings.TrimSpace(allowed), "/")
		if allowed == "*" || (allowed != "" && strings.EqualFold(allowed, origin)) {
			return true
		}
	}
	parsed, err := url.Parse(origin)
	if err != nil || parsed.Host == "" {
		return false
	}
	return strings.EqualFold(parsed.Host, r.Host)
}

func wsHandler(hub *wsHub, opts wsOptions) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if status, message := wsValidateHandshake(r, opts); status != 0 {
			if status == http.StatusUpgradeRequired {
				w.Header().Set("Sec-WebSocket-Version", "13")
			}
			http.Error(w, message, status)
			return
		}
		acceptKey := wsComputeAcceptKey(strings.TrimSpace(r.Header.Get("Sec-WebSocket-Key")))

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
		// http.Server deadlines (read-header timeout etc.) stay armed on the
		// raw connection after Hijack; clear them or the client is dropped.
		_ = conn.SetDeadline(time.Time{})

		client := &wsConn{conn: conn, reader: bufrw.Reader}
		if !hub.trackConn(client) {
			_ = conn.Close()
			return
		}
		defer hub.untrackConn(client)

		// Send 101 Switching Protocols
		resp := "HTTP/1.1 101 Switching Protocols\r\n" +
			"Upgrade: websocket\r\n" +
			"Connection: Upgrade\r\n" +
			"Sec-WebSocket-Accept: " + acceptKey + "\r\n\r\n"
		if err := client.writeRaw([]byte(resp)); err != nil {
			_ = conn.Close()
			return
		}

		// Register client channel
		ch := make(chan []byte, 64)
		hub.register(ch)
		defer func() {
			hub.unregister(ch)
			conn.Close()
		}()

		ctx, cancel := context.WithCancel(r.Context())
		defer cancel()

		// The read loop parses client frames: it answers pings, completes the
		// close handshake and drops clients that violate the protocol. Any
		// read error cancels the writer below.
		go func() {
			defer cancel()
			client.readLoop()
			_ = conn.Close()
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
				if err := client.writeFrame(wsOpText, msg); err != nil {
					return
				}
			}
		}
	}
}

// wsConn is a hijacked WebSocket connection. Writes come from both the
// broadcast loop and the read loop (pong/close), so they are serialized.
type wsConn struct {
	conn   net.Conn
	reader *bufio.Reader
	wmu    sync.Mutex
}

func (c *wsConn) writeRaw(data []byte) error {
	c.wmu.Lock()
	defer c.wmu.Unlock()
	_ = c.conn.SetWriteDeadline(time.Now().Add(wsWriteTimeout))
	defer func() { _ = c.conn.SetWriteDeadline(time.Time{}) }()
	_, err := c.conn.Write(data)
	return err
}

func (c *wsConn) writeFrame(opcode byte, payload []byte) error {
	c.wmu.Lock()
	defer c.wmu.Unlock()
	return wsWriteFrame(c.conn, opcode, payload)
}

func (c *wsConn) writeClose(code int, reason string) error {
	payload := make([]byte, 2, 2+len(reason))
	binary.BigEndian.PutUint16(payload, uint16(code))
	payload = append(payload, reason...)
	if len(payload) > wsMaxControlPayload {
		payload = payload[:wsMaxControlPayload]
	}
	return c.writeFrame(wsOpClose, payload)
}

// wsFrameError is a protocol violation that ends the connection with the
// given close code.
type wsFrameError struct {
	code   int
	reason string
}

func (e *wsFrameError) Error() string { return e.reason }

// readLoop processes client frames until the connection fails, the client
// closes it, or the client violates the protocol.
func (c *wsConn) readLoop() {
	for {
		opcode, payload, err := wsReadClientFrame(c.reader)
		if err != nil {
			var frameErr *wsFrameError
			if errors.As(err, &frameErr) {
				_ = c.writeClose(frameErr.code, frameErr.reason)
			}
			return
		}
		switch opcode {
		case wsOpPing:
			if err := c.writeFrame(wsOpPong, payload); err != nil {
				return
			}
		case wsOpClose:
			// Echo the status code (if any) to complete the close handshake.
			reply := []byte{}
			if len(payload) >= 2 {
				reply = payload[:2]
			}
			_ = c.writeFrame(wsOpClose, reply)
			return
		case wsOpPong, wsOpText, wsOpBinary, wsOpContinuation:
			// Clients have nothing to tell the stream; ignore data and pongs.
		default:
			_ = c.writeClose(wsCloseProtocolError, "unknown opcode")
			return
		}
	}
}

// wsReadClientFrame reads one client frame (RFC 6455 §5.2). Client frames must
// be masked; control frames must be final and at most 125 bytes; the payload
// length is checked against wsMaxClientPayload before anything is allocated.
func wsReadClientFrame(r io.Reader) (byte, []byte, error) {
	var header [2]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		return 0, nil, err
	}
	fin := header[0]&0x80 != 0
	if header[0]&0x70 != 0 {
		return 0, nil, &wsFrameError{code: wsCloseProtocolError, reason: "reserved bits set"}
	}
	opcode := header[0] & 0x0f
	masked := header[1]&0x80 != 0
	if !masked {
		return 0, nil, &wsFrameError{code: wsCloseProtocolError, reason: "client frames must be masked"}
	}

	length := uint64(header[1] & 0x7f)
	switch length {
	case 126:
		var ext [2]byte
		if _, err := io.ReadFull(r, ext[:]); err != nil {
			return 0, nil, err
		}
		length = uint64(binary.BigEndian.Uint16(ext[:]))
	case 127:
		var ext [8]byte
		if _, err := io.ReadFull(r, ext[:]); err != nil {
			return 0, nil, err
		}
		length = binary.BigEndian.Uint64(ext[:])
		if length>>63 != 0 {
			return 0, nil, &wsFrameError{code: wsCloseProtocolError, reason: "invalid payload length"}
		}
	}

	isControl := opcode&0x08 != 0
	if isControl {
		if !fin {
			return 0, nil, &wsFrameError{code: wsCloseProtocolError, reason: "fragmented control frame"}
		}
		if length > wsMaxControlPayload {
			return 0, nil, &wsFrameError{code: wsCloseProtocolError, reason: "control frame too large"}
		}
	} else if length > wsMaxClientPayload {
		return 0, nil, &wsFrameError{code: wsCloseTooBig, reason: "message too big"}
	}

	var mask [4]byte
	if _, err := io.ReadFull(r, mask[:]); err != nil {
		return 0, nil, err
	}
	payload := make([]byte, int(length))
	if _, err := io.ReadFull(r, payload); err != nil {
		return 0, nil, err
	}
	for i := range payload {
		payload[i] ^= mask[i%4]
	}
	return opcode, payload, nil
}

// wsWriteFrame writes one unmasked server→client frame with FIN set.
func wsWriteFrame(conn net.Conn, opcode byte, payload []byte) error {
	l := len(payload)
	first := 0x80 | (opcode & 0x0f)
	var header []byte
	switch {
	case l <= 125:
		header = []byte{first, byte(l)}
	case l <= 65535:
		header = []byte{first, 126, byte(l >> 8), byte(l & 0xff)}
	default:
		header = make([]byte, 10)
		header[0] = first
		header[1] = 127
		binary.BigEndian.PutUint64(header[2:], uint64(l))
	}
	_ = conn.SetWriteDeadline(time.Now().Add(wsWriteTimeout))
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

// wsWriteTextFrame writes a minimal RFC-6455 text frame (server→client, no masking).
func wsWriteTextFrame(conn net.Conn, payload []byte) error {
	return wsWriteFrame(conn, wsOpText, payload)
}

// newLiveWSServer builds the HTTP server for the live WebSocket stream and
// binds it to host:port. It uses ReadHeaderTimeout rather than ReadTimeout so
// that no read deadline remains on long-lived WebSocket connections.
func newLiveWSServer(hub *wsHub, host string, port int, opts wsOptions, health http.HandlerFunc) (*http.Server, net.Listener, error) {
	mux := http.NewServeMux()
	mux.HandleFunc("/stream", wsHandler(hub, opts))
	if health != nil {
		mux.HandleFunc("/health", health)
	}
	addr := net.JoinHostPort(host, strconv.Itoa(port))
	server := &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: wsReadHeaderTimeout,
	}
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, nil, fmt.Errorf("cannot bind WebSocket server on %s: %w", addr, err)
	}
	return server, ln, nil
}
