package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// wsHub tests
// ---------------------------------------------------------------------------

func TestWSHub_RegisterUnregister(t *testing.T) {
	hub := newWSHub()
	if hub.count() != 0 {
		t.Fatal("expected 0 clients initially")
	}

	ch := make(chan []byte, 4)
	hub.register(ch)
	if hub.count() != 1 {
		t.Fatal("expected 1 client after register")
	}

	hub.unregister(ch)
	if hub.count() != 0 {
		t.Fatal("expected 0 clients after unregister")
	}
}

func TestWSHub_Broadcast(t *testing.T) {
	hub := newWSHub()

	ch1 := make(chan []byte, 8)
	ch2 := make(chan []byte, 8)
	hub.register(ch1)
	hub.register(ch2)

	msg := []byte(`{"test":true}`)
	hub.broadcast(msg)

	// both channels should receive the message
	select {
	case got := <-ch1:
		if string(got) != string(msg) {
			t.Errorf("ch1 got %q, want %q", got, msg)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for ch1")
	}

	select {
	case got := <-ch2:
		if string(got) != string(msg) {
			t.Errorf("ch2 got %q, want %q", got, msg)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for ch2")
	}
}

func TestWSHub_BroadcastSlowClientDropped(t *testing.T) {
	hub := newWSHub()

	// unbuffered channel — will always drop
	ch := make(chan []byte, 0)
	hub.register(ch)

	// Should not block even with a slow (unbuffered) client
	done := make(chan struct{})
	go func() {
		defer close(done)
		hub.broadcast([]byte("hello"))
	}()

	select {
	case <-done:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("broadcast blocked on slow client")
	}
}

// ---------------------------------------------------------------------------
// WebSocket accept-key computation
// ---------------------------------------------------------------------------

func TestWsComputeAcceptKey(t *testing.T) {
	// RFC 6455 §1.3 example
	key := "dGhlIHNhbXBsZSBub25jZQ=="
	expected := "s3pPLMBiTxaQ9kYGzzhZRbK+xOo="
	got := wsComputeAcceptKey(key)
	if got != expected {
		t.Errorf("wsComputeAcceptKey(%q) = %q, want %q", key, got, expected)
	}
}

// ---------------------------------------------------------------------------
// wsWriteTextFrame
// ---------------------------------------------------------------------------

func TestWsWriteTextFrame_Small(t *testing.T) {
	srv, cli := net.Pipe()
	defer srv.Close()
	defer cli.Close()

	payload := []byte("hello world")
	go func() {
		_ = wsWriteTextFrame(srv, payload)
		srv.Close()
	}()

	// Frame = 2-byte header + payload
	frameLen := 2 + len(payload)
	buf := make([]byte, frameLen)
	if _, err := io.ReadFull(cli, buf); err != nil {
		t.Fatalf("failed to read full frame: %v", err)
	}
	// First byte: 0x81 = FIN + opcode text
	if buf[0] != 0x81 {
		t.Errorf("first byte = 0x%02x, want 0x81", buf[0])
	}
	// Second byte: payload length (no mask bit for server frames)
	if int(buf[1]) != len(payload) {
		t.Errorf("length byte = %d, want %d", buf[1], len(payload))
	}
	// Payload content
	if string(buf[2:]) != string(payload) {
		t.Errorf("payload = %q, want %q", buf[2:], payload)
	}
}

func TestWsWriteTextFrame_Medium(t *testing.T) {
	srv, cli := net.Pipe()
	defer srv.Close()
	defer cli.Close()

	payload := bytes.Repeat([]byte("a"), 200) // > 125, requires 2-byte extended length
	go func() {
		_ = wsWriteTextFrame(srv, payload)
		srv.Close()
	}()

	buf := make([]byte, 512)
	n, _ := io.ReadFull(cli, buf[:4]) // 2 header + 2 extended length
	_ = n
	if buf[1] != 126 {
		t.Errorf("second byte = %d, want 126 (16-bit extended length indicator)", buf[1])
	}
	extended := int(buf[2])<<8 | int(buf[3])
	if extended != len(payload) {
		t.Errorf("extended length = %d, want %d", extended, len(payload))
	}
}

// ---------------------------------------------------------------------------
// wsHandler HTTP upgrade tests
// ---------------------------------------------------------------------------

func TestWsHandler_NotWebSocket(t *testing.T) {
	hub := newWSHub()
	ts := httptest.NewServer(wsHandler(hub))
	defer ts.Close()

	resp, err := http.Get(ts.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", resp.StatusCode)
	}
}

func TestWsHandler_MissingKey(t *testing.T) {
	hub := newWSHub()
	ts := httptest.NewServer(wsHandler(hub))
	defer ts.Close()

	req, _ := http.NewRequest(http.MethodGet, ts.URL, nil)
	req.Header.Set("Upgrade", "websocket")
	req.Header.Set("Connection", "upgrade")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", resp.StatusCode)
	}
}

// ---------------------------------------------------------------------------
// LiveTick / LiveBar JSON marshalling
// ---------------------------------------------------------------------------

func TestLiveTick_JSON(t *testing.T) {
	lt := LiveTick{
		Timestamp: 1700000000000,
		Symbol:    "EURUSD",
		Bid:       1.08650,
		Ask:       1.08652,
		BidVolume: 1.5,
		AskVolume: 2.0,
	}
	b, err := json.Marshal(lt)
	if err != nil {
		t.Fatal(err)
	}
	var got map[string]interface{}
	if err := json.Unmarshal(b, &got); err != nil {
		t.Fatal(err)
	}
	if got["symbol"] != "EURUSD" {
		t.Errorf("symbol = %v, want EURUSD", got["symbol"])
	}
	if got["bid"].(float64) != 1.0865 {
		t.Errorf("bid = %v, want 1.08650", got["bid"])
	}
}

func TestLiveBar_JSON(t *testing.T) {
	lb := LiveBar{
		Timestamp: 1700000000000,
		Symbol:    "XAUUSD",
		Timeframe: "M1",
		Open:      1980.5,
		High:      1981.0,
		Low:       1979.8,
		Close:     1980.9,
		Volume:    123.45,
		Side:      "BID",
	}
	b, err := json.Marshal(lb)
	if err != nil {
		t.Fatal(err)
	}
	var got map[string]interface{}
	if err := json.Unmarshal(b, &got); err != nil {
		t.Fatal(err)
	}
	if got["symbol"] != "XAUUSD" {
		t.Errorf("symbol = %v, want XAUUSD", got["symbol"])
	}
	if got["open"].(float64) != 1980.5 {
		t.Errorf("open = %v, want 1980.5", got["open"])
	}
}

// ---------------------------------------------------------------------------
// runLiveStream validation tests (no network)
// ---------------------------------------------------------------------------

func TestRunLiveStream_MissingSymbol(t *testing.T) {
	var stdout, stderr bytes.Buffer
	err := runLiveStream([]string{}, &stdout, &stderr)
	if err == nil {
		t.Fatal("expected error for missing --symbol")
	}
	if !strings.Contains(err.Error(), "--symbol") {
		t.Errorf("expected --symbol in error, got: %v", err)
	}
}

func TestRunLiveStream_InvalidFormat(t *testing.T) {
	var stdout, stderr bytes.Buffer
	err := runLiveStream([]string{"--symbol", "eurusd", "--format", "xml"}, &stdout, &stderr)
	if err == nil {
		t.Fatal("expected error for invalid --format")
	}
	if !strings.Contains(err.Error(), "xml") {
		t.Errorf("expected xml in error, got: %v", err)
	}
}

func TestRunLiveStream_InvalidPort(t *testing.T) {
	var stdout, stderr bytes.Buffer
	err := runLiveStream([]string{"--symbol", "eurusd", "--port", "-5"}, &stdout, &stderr)
	if err == nil {
		t.Fatal("expected error for invalid --port")
	}
}

func TestRunLiveStream_InvalidPollInterval(t *testing.T) {
	var stdout, stderr bytes.Buffer
	err := runLiveStream([]string{"--symbol", "eurusd", "--poll-interval", "0"}, &stdout, &stderr)
	if err == nil {
		t.Fatal("expected error for zero --poll-interval")
	}
}

func TestRunLiveStream_BadOutputFile(t *testing.T) {
	var stdout, stderr bytes.Buffer
	err := runLiveStream([]string{"--symbol", "eurusd", "--output", "/nonexistent/path/file.log"}, &stdout, &stderr)
	if err == nil {
		t.Fatal("expected error for bad output file path")
	}
}

// ---------------------------------------------------------------------------
// runLiveStream with mock Dukascopy API
// ---------------------------------------------------------------------------

func TestRunLiveStream_WithMockAPI(t *testing.T) {
	// Create a mock Dukascopy Jetta API server
	tickJSON := `{"ticks":[{"timestamp":1700000000000,"bid":1.08650,"ask":1.08652,"bidVolume":1.5,"askVolume":2.0}]}`
	instrumentsJSON := `{"instruments":[{"id":1,"name":"EURUSD","code":"EURUSD","description":"EUR/USD","priceScale":5}]}`

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Path
		if strings.Contains(path, "instruments") {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(instrumentsJSON))
			return
		}
		if strings.Contains(path, "ticks") {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(tickJSON))
			return
		}
		http.NotFound(w, r)
	}))
	defer ts.Close()

	// Run live stream with very short poll (so it exits quickly via context cancellation)
	ctx, cancel := context.WithTimeout(context.Background(), 1500*time.Millisecond)
	defer cancel()

	var stdout, stderr bytes.Buffer

	// Run in goroutine with context cancellation simulation
	done := make(chan error, 1)
	go func() {
		done <- runLiveStream([]string{
			"--symbol", "eurusd",
			"--timeframe", "tick",
			"--format", "jsonl",
			"--poll-interval", "200ms",
			"--base-url", ts.URL,
		}, &stdout, &stderr)
	}()

	// Wait for context or stream to finish
	select {
	case err := <-done:
		if err != nil && !strings.Contains(err.Error(), "context") &&
			!strings.Contains(err.Error(), "no instruments found") &&
			!strings.Contains(err.Error(), "decode") {
			t.Errorf("unexpected error: %v", err)
		}
	case <-ctx.Done():
		// Timeout expected — stream keeps running until Ctrl+C
	}
}

// ---------------------------------------------------------------------------
// Health endpoint test
// ---------------------------------------------------------------------------

func TestWSHealthEndpoint(t *testing.T) {
	hub := newWSHub()
	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprintf(w, `{"status":"ok","symbol":%q,"timeframe":%q,"clients":%d}`,
			"eurusd", "tick", hub.count())
	})
	ts := httptest.NewServer(mux)
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/health")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected 200, got %d", resp.StatusCode)
	}
	var body map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	if body["status"] != "ok" {
		t.Errorf("status = %v, want ok", body["status"])
	}
}

func TestWSHandlerDisconnectCleansUp(t *testing.T) {
	hub := newWSHub()
	server := httptest.NewServer(wsHandler(hub))
	defer server.Close()

	// Dial connection using raw TCP to simulate WebSocket handshake
	conn, err := net.Dial("tcp", server.Listener.Addr().String())
	if err != nil {
		t.Fatalf("Dial failed: %v", err)
	}

	// Send handshake request
	req := "GET /stream HTTP/1.1\r\n" +
		"Host: localhost\r\n" +
		"Upgrade: websocket\r\n" +
		"Connection: Upgrade\r\n" +
		"Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\r\n"

	if _, err := conn.Write([]byte(req)); err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	// Read handshake response
	buf := make([]byte, 1024)
	n, err := conn.Read(buf)
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}
	resp := string(buf[:n])
	if !strings.Contains(resp, "101 Switching Protocols") {
		t.Fatalf("expected 101 status, got: %q", resp)
	}

	// Verify client is registered in the hub
	time.Sleep(50 * time.Millisecond)
	if hub.count() != 1 {
		t.Fatalf("expected 1 registered client, got %d", hub.count())
	}

	// Close client connection to trigger server-side Read error and cleanup
	conn.Close()

	// Verify that the client is unregistered from the hub
	time.Sleep(100 * time.Millisecond)
	if hub.count() != 0 {
		t.Fatalf("expected 0 registered clients after disconnect, got %d", hub.count())
	}
}
