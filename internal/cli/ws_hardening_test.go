package cli

import (
	"bufio"
	"encoding/binary"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

const testWSKey = "dGhlIHNhbXBsZSBub25jZQ=="

// wsTestDial performs a raw WebSocket handshake against addr and returns the
// connection, a reader positioned after the handshake response, and the
// response itself.
func wsTestDial(t *testing.T, addr string, extra map[string]string) (net.Conn, *bufio.Reader, *http.Response) {
	t.Helper()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { conn.Close() })

	headers := map[string]string{
		"Host":                  addr,
		"Upgrade":               "websocket",
		"Connection":            "Upgrade",
		"Sec-WebSocket-Version": "13",
		"Sec-WebSocket-Key":     testWSKey,
	}
	for k, v := range extra {
		if v == "" {
			delete(headers, k)
			continue
		}
		headers[k] = v
	}
	var b strings.Builder
	b.WriteString("GET /stream HTTP/1.1\r\n")
	for k, v := range headers {
		b.WriteString(k + ": " + v + "\r\n")
	}
	b.WriteString("\r\n")
	if _, err := conn.Write([]byte(b.String())); err != nil {
		t.Fatalf("write handshake: %v", err)
	}
	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	br := bufio.NewReader(conn)
	resp, err := http.ReadResponse(br, nil)
	if err != nil {
		t.Fatalf("read handshake response: %v", err)
	}
	_ = conn.SetReadDeadline(time.Time{})
	return conn, br, resp
}

// wsTestClientFrame encodes a client frame; masked=false produces an invalid
// (unmasked) client frame.
func wsTestClientFrame(opcode byte, payload []byte, masked bool) []byte {
	frame := []byte{0x80 | opcode}
	maskBit := byte(0)
	if masked {
		maskBit = 0x80
	}
	switch l := len(payload); {
	case l <= 125:
		frame = append(frame, maskBit|byte(l))
	case l <= 65535:
		frame = append(frame, maskBit|126, byte(l>>8), byte(l))
	default:
		ext := make([]byte, 8)
		binary.BigEndian.PutUint64(ext, uint64(l))
		frame = append(frame, maskBit|127)
		frame = append(frame, ext...)
	}
	if !masked {
		return append(frame, payload...)
	}
	mask := []byte{1, 2, 3, 4}
	frame = append(frame, mask...)
	for i, c := range payload {
		frame = append(frame, c^mask[i%4])
	}
	return frame
}

// wsTestReadServerFrame reads one (unmasked) server frame.
func wsTestReadServerFrame(t *testing.T, conn net.Conn, br *bufio.Reader) (byte, []byte) {
	t.Helper()
	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	defer conn.SetReadDeadline(time.Time{})
	var header [2]byte
	if _, err := io.ReadFull(br, header[:]); err != nil {
		t.Fatalf("read frame header: %v", err)
	}
	length := int(header[1] & 0x7f)
	switch length {
	case 126:
		var ext [2]byte
		if _, err := io.ReadFull(br, ext[:]); err != nil {
			t.Fatal(err)
		}
		length = int(binary.BigEndian.Uint16(ext[:]))
	case 127:
		var ext [8]byte
		if _, err := io.ReadFull(br, ext[:]); err != nil {
			t.Fatal(err)
		}
		length = int(binary.BigEndian.Uint64(ext[:]))
	}
	payload := make([]byte, length)
	if _, err := io.ReadFull(br, payload); err != nil {
		t.Fatalf("read frame payload: %v", err)
	}
	return header[0] & 0x0f, payload
}

func startTestLiveWSServer(t *testing.T, hub *wsHub, opts wsOptions) string {
	t.Helper()
	server, ln, err := newLiveWSServer(hub, defaultLiveHost, 0, opts, nil)
	if err != nil {
		t.Fatalf("newLiveWSServer: %v", err)
	}
	go func() { _ = server.Serve(ln) }()
	t.Cleanup(func() {
		_ = server.Close()
		hub.closeAll()
	})
	return ln.Addr().String()
}

func TestLiveWSServerKeepsClientPastHandshakeTimeout(t *testing.T) {
	previous := wsReadHeaderTimeout
	wsReadHeaderTimeout = 150 * time.Millisecond
	defer func() { wsReadHeaderTimeout = previous }()

	hub := newWSHub()
	addr := startTestLiveWSServer(t, hub, wsOptions{})
	conn, br, resp := wsTestDial(t, addr, nil)
	if resp.StatusCode != http.StatusSwitchingProtocols {
		t.Fatalf("expected 101, got %d", resp.StatusCode)
	}

	// Stay connected well past the server's header timeout, then make sure
	// the connection still works in both directions.
	time.Sleep(4 * wsReadHeaderTimeout)

	if _, err := conn.Write(wsTestClientFrame(wsOpPing, []byte("hi"), true)); err != nil {
		t.Fatalf("write ping: %v", err)
	}
	opcode, payload := wsTestReadServerFrame(t, conn, br)
	if opcode != wsOpPong || string(payload) != "hi" {
		t.Fatalf("expected pong \"hi\", got opcode %#x payload %q", opcode, payload)
	}

	deadline := time.Now().Add(2 * time.Second)
	for hub.count() == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	hub.broadcast([]byte(`{"n":1}` + "\n"))
	opcode, payload = wsTestReadServerFrame(t, conn, br)
	if opcode != wsOpText || !strings.Contains(string(payload), `"n":1`) {
		t.Fatalf("expected broadcast text frame, got opcode %#x payload %q", opcode, payload)
	}
}

func TestNewLiveWSServerBindsLoopbackByDefault(t *testing.T) {
	hub := newWSHub()
	server, ln, err := newLiveWSServer(hub, defaultLiveHost, 0, wsOptions{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	if server.ReadTimeout != 0 {
		t.Fatalf("ReadTimeout must be unset for long-lived websocket connections, got %s", server.ReadTimeout)
	}
	if server.ReadHeaderTimeout <= 0 {
		t.Fatal("expected a ReadHeaderTimeout")
	}
	tcpAddr, ok := ln.Addr().(*net.TCPAddr)
	if !ok || !tcpAddr.IP.IsLoopback() {
		t.Fatalf("expected loopback listener by default, got %v", ln.Addr())
	}
}

func TestWsHandshakeValidation(t *testing.T) {
	hub := newWSHub()
	ts := httptest.NewServer(wsHandler(hub, wsOptions{AllowedOrigins: []string{"http://allowed.example"}}))
	defer ts.Close()
	defer hub.closeAll()
	addr := ts.Listener.Addr().String()

	cases := []struct {
		name   string
		extra  map[string]string
		status int
	}{
		{"missing connection upgrade", map[string]string{"Connection": "keep-alive"}, http.StatusBadRequest},
		{"missing upgrade header", map[string]string{"Upgrade": ""}, http.StatusBadRequest},
		{"wrong version", map[string]string{"Sec-WebSocket-Version": "8"}, http.StatusUpgradeRequired},
		{"missing version", map[string]string{"Sec-WebSocket-Version": ""}, http.StatusUpgradeRequired},
		{"missing key", map[string]string{"Sec-WebSocket-Key": ""}, http.StatusBadRequest},
		{"malformed key", map[string]string{"Sec-WebSocket-Key": "not-base64!"}, http.StatusBadRequest},
		{"cross origin", map[string]string{"Origin": "http://evil.example"}, http.StatusForbidden},
		{"same origin", map[string]string{"Origin": "http://" + addr}, http.StatusSwitchingProtocols},
		{"allowed origin", map[string]string{"Origin": "http://allowed.example"}, http.StatusSwitchingProtocols},
		{"no origin", nil, http.StatusSwitchingProtocols},
		{"connection token list", map[string]string{"Connection": "keep-alive, Upgrade"}, http.StatusSwitchingProtocols},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, _, resp := wsTestDial(t, addr, tc.extra)
			if resp.StatusCode != tc.status {
				t.Fatalf("expected %d, got %d", tc.status, resp.StatusCode)
			}
			if tc.status == http.StatusUpgradeRequired && resp.Header.Get("Sec-WebSocket-Version") != "13" {
				t.Fatalf("426 response must advertise Sec-WebSocket-Version: 13")
			}
		})
	}

	resp, err := http.Post(ts.URL, "text/plain", strings.NewReader("x"))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400 for POST, got %d", resp.StatusCode)
	}
}

func TestWsOriginWildcard(t *testing.T) {
	r := httptest.NewRequest(http.MethodGet, "http://127.0.0.1:1/stream", nil)
	r.Header.Set("Origin", "http://anything.example")
	if !wsOriginAllowed(r, wsOptions{AllowedOrigins: []string{"*"}}) {
		t.Fatal("expected * to allow any origin")
	}
	if wsOriginAllowed(r, wsOptions{}) {
		t.Fatal("expected cross-origin request to be refused by default")
	}
}

func expectWSClose(t *testing.T, conn net.Conn, br *bufio.Reader, code int) {
	t.Helper()
	opcode, payload := wsTestReadServerFrame(t, conn, br)
	if opcode != wsOpClose {
		t.Fatalf("expected close frame, got opcode %#x", opcode)
	}
	if len(payload) < 2 || int(binary.BigEndian.Uint16(payload)) != code {
		t.Fatalf("expected close code %d, got payload %v", code, payload)
	}
}

func TestWsRejectsUnmaskedClientFrame(t *testing.T) {
	hub := newWSHub()
	addr := startTestLiveWSServer(t, hub, wsOptions{})
	conn, br, _ := wsTestDial(t, addr, nil)
	if _, err := conn.Write(wsTestClientFrame(wsOpText, []byte("hello"), false)); err != nil {
		t.Fatal(err)
	}
	expectWSClose(t, conn, br, wsCloseProtocolError)
}

func TestWsRejectsOversizedControlFrame(t *testing.T) {
	hub := newWSHub()
	addr := startTestLiveWSServer(t, hub, wsOptions{})
	conn, br, _ := wsTestDial(t, addr, nil)
	if _, err := conn.Write(wsTestClientFrame(wsOpPing, make([]byte, 126), true)); err != nil {
		t.Fatal(err)
	}
	expectWSClose(t, conn, br, wsCloseProtocolError)
}

func TestWsRejectsHugeDataFrameBeforeAllocating(t *testing.T) {
	// A header announcing an enormous payload must be refused from the length
	// alone, without trying to allocate or read it.
	header := []byte{0x80 | wsOpBinary, 0x80 | 127}
	ext := make([]byte, 8)
	binary.BigEndian.PutUint64(ext, 1<<40)
	frame := append(header, ext...)
	_, _, err := wsReadClientFrame(strings.NewReader(string(frame)))
	frameErr, ok := err.(*wsFrameError)
	if !ok || frameErr.code != wsCloseTooBig {
		t.Fatalf("expected message-too-big error, got %v", err)
	}
}

func TestWsAnswersCloseWithClose(t *testing.T) {
	hub := newWSHub()
	addr := startTestLiveWSServer(t, hub, wsOptions{})
	conn, br, _ := wsTestDial(t, addr, nil)
	payload := make([]byte, 2)
	binary.BigEndian.PutUint16(payload, wsCloseNormal)
	if _, err := conn.Write(wsTestClientFrame(wsOpClose, payload, true)); err != nil {
		t.Fatal(err)
	}
	expectWSClose(t, conn, br, wsCloseNormal)
}

func TestWsHubCloseAllClosesHijackedConnections(t *testing.T) {
	hub := newWSHub()
	server, ln, err := newLiveWSServer(hub, defaultLiveHost, 0, wsOptions{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = server.Serve(ln) }()
	defer server.Close()

	conn, br, resp := wsTestDial(t, ln.Addr().String(), nil)
	if resp.StatusCode != http.StatusSwitchingProtocols {
		t.Fatalf("expected 101, got %d", resp.StatusCode)
	}

	hub.closeAll()

	expectWSClose(t, conn, br, wsCloseGoingAway)
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, err := br.ReadByte(); err == nil {
		t.Fatal("expected the connection to be closed after closeAll")
	}

	// New handshakes are refused once the hub is closed.
	conn2, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn2.Close()
	handshake := "GET /stream HTTP/1.1\r\nHost: " + ln.Addr().String() +
		"\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: " + testWSKey + "\r\n\r\n"
	if _, err := conn2.Write([]byte(handshake)); err != nil {
		t.Fatal(err)
	}
	_ = conn2.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, err := http.ReadResponse(bufio.NewReader(conn2), nil); err == nil {
		t.Fatal("expected new connections to be dropped after closeAll")
	}
}
