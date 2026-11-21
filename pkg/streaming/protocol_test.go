package streaming

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// readTestRESPCommand reads one RESP array-of-bulk-strings command, as sent
// by a client.
func readTestRESPCommand(r *bufio.Reader) ([]string, error) {
	line, err := r.ReadString('\n')
	if err != nil {
		return nil, err
	}
	line = strings.TrimRight(line, "\r\n")
	if !strings.HasPrefix(line, "*") {
		return nil, fmt.Errorf("expected array, got %q", line)
	}
	n, err := strconv.Atoi(line[1:])
	if err != nil {
		return nil, err
	}
	args := make([]string, 0, n)
	for i := 0; i < n; i++ {
		hdr, err := r.ReadString('\n')
		if err != nil {
			return nil, err
		}
		size, err := strconv.Atoi(strings.TrimRight(hdr, "\r\n")[1:])
		if err != nil {
			return nil, err
		}
		buf := make([]byte, size+2)
		if _, err := io.ReadFull(r, buf); err != nil {
			return nil, err
		}
		args = append(args, string(buf[:size]))
	}
	return args, nil
}

// startFakeRedis serves one connection, calling reply for every command and
// writing its result verbatim. Received commands are sent on the channel.
func startFakeRedis(t *testing.T, reply func(args []string) string) (addr string, cmds <-chan []string) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	ch := make(chan []string, 64)
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				r := bufio.NewReader(c)
				for {
					args, err := readTestRESPCommand(r)
					if err != nil {
						return
					}
					select {
					case ch <- args:
					default: // nobody is listening (stress tests)
					}
					if out := reply(args); out != "" {
						_, _ = c.Write([]byte(out))
					}
				}
			}(conn)
		}
	}()
	return ln.Addr().String(), ch
}

func TestRedisXADDBulkReplyKeepsRepliesInSync(t *testing.T) {
	xadds := 0
	addr, _ := startFakeRedis(t, func(args []string) string {
		if args[0] != "XADD" {
			return "+OK\r\n"
		}
		xadds++
		if xadds == 2 {
			return "-ERR OOM command not allowed when used memory > 'maxmemory'\r\n"
		}
		return "$15\r\n1700000000000-0\r\n"
	})

	pub, err := NewRedisPublisher("redis://" + addr + "/stream:ticks")
	if err != nil {
		t.Fatal(err)
	}
	defer pub.Close()

	if err := pub.Publish(context.Background(), []byte("one")); err != nil {
		t.Fatalf("first XADD: %v", err)
	}
	// Before the fix the client read only the "$15" line of the first reply,
	// so this call consumed the stale entry id and reported success.
	err = pub.Publish(context.Background(), []byte("two"))
	var redisErr RedisError
	if err == nil || !errors.As(err, &redisErr) || !strings.Contains(err.Error(), "OOM") {
		t.Fatalf("second XADD error = %v, want the server's OOM error", err)
	}
	// An error reply keeps the connection in sync and usable.
	if err := pub.Publish(context.Background(), []byte("three")); err != nil {
		t.Fatalf("third XADD: %v", err)
	}
}

func TestRedisSelectDBAndACLAuth(t *testing.T) {
	addr, cmds := startFakeRedis(t, func(args []string) string {
		if args[0] == "PUBLISH" {
			return ":0\r\n"
		}
		return "+OK\r\n"
	})

	pub, err := NewRedisPublisher("redis://alice:s3cret@" + addr + "/3/stream:eurusd")
	if err != nil {
		t.Fatal(err)
	}
	defer pub.Close()
	if pub.db != 3 || !pub.isStream || pub.streamKey != "eurusd" {
		t.Fatalf("parsed %+v", pub)
	}

	// Use a pub/sub publisher on db 2 for the round trip (PUBLISH -> integer).
	pub2, err := NewRedisPublisher("redis://alice:s3cret@" + addr + "/2")
	if err != nil {
		t.Fatal(err)
	}
	defer pub2.Close()
	if pub2.channel != "market_data" || pub2.db != 2 {
		t.Fatalf("parsed %+v", pub2)
	}
	if err := pub2.Publish(context.Background(), []byte("x")); err != nil {
		t.Fatalf("publish: %v", err)
	}
	want := []string{"AUTH alice s3cret", "SELECT 2", "PUBLISH market_data x"}
	for _, w := range want {
		select {
		case got := <-cmds:
			if strings.Join(got, " ") != w {
				t.Fatalf("command = %q, want %q", strings.Join(got, " "), w)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("timeout waiting for %q", w)
		}
	}
}

func TestReadRESPTypes(t *testing.T) {
	in := "+OK\r\n-ERR bad\r\n:42\r\n$3\r\nfoo\r\n$-1\r\n*2\r\n$1\r\na\r\n:7\r\n*-1\r\n"
	r := bufio.NewReader(strings.NewReader(in))
	var got []string
	for i := 0; i < 7; i++ {
		v, err := readRESP(r)
		if err != nil {
			t.Fatalf("reply %d: %v", i, err)
		}
		got = append(got, fmt.Sprintf("%T:%v", v, v))
	}
	want := []string{
		"string:OK",
		"streaming.RedisError:redis error: ERR bad",
		"int64:42",
		"[]uint8:[102 111 111]",
		"<nil>:<nil>",
		"[]interface {}:[[97] 7]",
		"<nil>:<nil>",
	}
	if strings.Join(got, "|") != strings.Join(want, "|") {
		t.Fatalf("got  %v\nwant %v", got, want)
	}
	for _, bad := range []string{"?x\r\n", ":nan\r\n", "$-5\r\n", "$3\r\nfooXY", "*x\r\n", "\r\n"} {
		if _, err := readRESP(bufio.NewReader(strings.NewReader(bad))); err == nil {
			t.Errorf("readRESP(%q) expected error", bad)
		}
	}
}

func TestRedisPublishHonoursContextDeadline(t *testing.T) {
	// Server accepts commands but never replies.
	addr, _ := startFakeRedis(t, func([]string) string { return "" })
	pub, err := NewRedisPublisher("redis://" + addr + "/chan")
	if err != nil {
		t.Fatal(err)
	}
	defer pub.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- pub.Publish(ctx, []byte("x")) }()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("expected timeout error from stalled server")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Publish blocked past the context deadline")
	}

	// A cancelled context returns immediately.
	cctx, ccancel := context.WithCancel(context.Background())
	ccancel()
	if err := pub.Publish(cctx, []byte("x")); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled ctx: got %v", err)
	}
}

// fakeNATS is a minimal NATS server for one connection at a time.
type fakeNATS struct {
	ln    net.Listener
	info  string
	lines chan string
	conns chan net.Conn
}

func startFakeNATS(t *testing.T, info string) *fakeNATS {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	f := &fakeNATS{ln: ln, info: info, lines: make(chan string, 256), conns: make(chan net.Conn, 8)}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			select {
			case f.conns <- c:
			default:
			}
			go func(c net.Conn) {
				defer c.Close()
				_, _ = c.Write([]byte("INFO " + f.info + "\r\n"))
				r := bufio.NewReader(c)
				for {
					line, err := r.ReadString('\n')
					if err != nil {
						return
					}
					line = strings.TrimRight(line, "\r\n")
					select {
					case f.lines <- line:
					default:
					}
					if line == "PING" {
						_, _ = c.Write([]byte("PONG\r\n"))
					}
				}
			}(c)
		}
	}()
	return f
}

func (f *fakeNATS) url(userinfo, subject string) string {
	return "nats://" + userinfo + f.ln.Addr().String() + "/" + subject
}

func (f *fakeNATS) waitLine(t *testing.T, pred func(string) bool) string {
	t.Helper()
	deadline := time.After(3 * time.Second)
	for {
		select {
		case l := <-f.lines:
			if pred(l) {
				return l
			}
		case <-deadline:
			t.Fatal("timeout waiting for client line")
			return ""
		}
	}
}

func TestNATSAnswersServerPing(t *testing.T) {
	f := startFakeNATS(t, `{"server_id":"test"}`)
	pub, err := NewNATSPublisher(f.url("", "market.eurusd"))
	if err != nil {
		t.Fatal(err)
	}
	defer pub.Close()
	if err := pub.Publish(context.Background(), []byte("hello")); err != nil {
		t.Fatal(err)
	}
	conn := <-f.conns
	f.waitLine(t, func(l string) bool { return strings.HasPrefix(l, "PUB market.eurusd 5") })

	// Server keep-alive: an unanswered PING gets the client dropped as stale.
	if _, err := conn.Write([]byte("PING\r\n")); err != nil {
		t.Fatal(err)
	}
	f.waitLine(t, func(l string) bool { return l == "PONG" })
}

func TestNATSReportsServerErrOnNextPublish(t *testing.T) {
	f := startFakeNATS(t, `{"server_id":"test"}`)
	pub, err := NewNATSPublisher(f.url("", "restricted.subject"))
	if err != nil {
		t.Fatal(err)
	}
	defer pub.Close()
	if err := pub.Publish(context.Background(), []byte("x")); err != nil {
		t.Fatal(err)
	}
	conn := <-f.conns
	_, _ = conn.Write([]byte("-ERR 'Permissions Violation for Publish to \"restricted.subject\"'\r\n"))

	deadline := time.Now().Add(3 * time.Second)
	for {
		err := pub.Publish(context.Background(), []byte("y"))
		if err != nil {
			if !strings.Contains(err.Error(), "Permissions Violation") {
				t.Fatalf("unexpected error: %v", err)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("-ERR from server was never reported by Publish")
		}
		time.Sleep(10 * time.Millisecond)
	}
	// The error is reported once; the (still open) connection keeps working.
	if err := pub.Publish(context.Background(), []byte("z")); err != nil {
		t.Fatalf("publish after reported error: %v", err)
	}
}

func TestNATSSendsCredentialsFromURL(t *testing.T) {
	f := startFakeNATS(t, `{"server_id":"test","auth_required":true}`)

	pub, err := NewNATSPublisher(f.url("alice:s3%22cret@", "a.b"))
	if err != nil {
		t.Fatal(err)
	}
	if err := pub.Publish(context.Background(), []byte("x")); err != nil {
		t.Fatal(err)
	}
	line := f.waitLine(t, func(l string) bool { return strings.HasPrefix(l, "CONNECT ") })
	if !strings.Contains(line, `"user":"alice"`) || !strings.Contains(line, `"pass":"s3\"cret"`) {
		t.Fatalf("CONNECT without user/pass: %s", line)
	}
	_ = pub.Close()

	tok, err := NewNATSPublisher(f.url("mytoken@", "a.b"))
	if err != nil {
		t.Fatal(err)
	}
	defer tok.Close()
	if err := tok.Publish(context.Background(), []byte("x")); err != nil {
		t.Fatal(err)
	}
	line = f.waitLine(t, func(l string) bool { return strings.HasPrefix(l, "CONNECT ") })
	if !strings.Contains(line, `"auth_token":"mytoken"`) {
		t.Fatalf("CONNECT without auth_token: %s", line)
	}
}

func TestNATSRejectsTLSRequiredServer(t *testing.T) {
	f := startFakeNATS(t, `{"server_id":"test","tls_required":true}`)
	pub, err := NewNATSPublisher(f.url("", "a.b"))
	if err != nil {
		t.Fatal(err)
	}
	defer pub.Close()
	err = pub.Publish(context.Background(), []byte("x"))
	if err == nil || !strings.Contains(err.Error(), "TLS") {
		t.Fatalf("expected TLS error, got %v", err)
	}
}

func TestNATSRejectsConnectErr(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go func() {
		c, err := ln.Accept()
		if err != nil {
			return
		}
		defer c.Close()
		_, _ = c.Write([]byte("INFO {}\r\n"))
		r := bufio.NewReader(c)
		for {
			l, err := r.ReadString('\n')
			if err != nil {
				return
			}
			if strings.HasPrefix(l, "PING") {
				_, _ = c.Write([]byte("-ERR 'Authorization Violation'\r\n"))
			}
		}
	}()
	pub, err := NewNATSPublisher("nats://bad:creds@" + ln.Addr().String() + "/a")
	if err != nil {
		t.Fatal(err)
	}
	defer pub.Close()
	if err := pub.Publish(context.Background(), []byte("x")); err == nil || !strings.Contains(err.Error(), "Authorization Violation") {
		t.Fatalf("expected authorization error, got %v", err)
	}
}

func TestNATSRejectsInjectedSubject(t *testing.T) {
	for _, raw := range []string{
		"nats://localhost:4222/market%0D%0APUB%20evil%201",
		"nats://localhost:4222/market%20eurusd",
		"nats://localhost:4222/market%09x",
		"nats://localhost:4222/market%00x",
	} {
		if _, err := NewNATSPublisher(raw); err == nil {
			t.Errorf("NewNATSPublisher(%q) accepted an unsafe subject", raw)
		}
	}
}

func TestNATSHandshakeHonoursContextDeadline(t *testing.T) {
	// Server accepts but never sends INFO.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go func() {
		c, err := ln.Accept()
		if err != nil {
			return
		}
		defer c.Close()
		_, _ = io.Copy(io.Discard, c)
	}()
	pub, err := NewNATSPublisher("nats://" + ln.Addr().String() + "/a")
	if err != nil {
		t.Fatal(err)
	}
	defer pub.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- pub.Publish(ctx, []byte("x")) }()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("expected handshake timeout")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Publish blocked past the context deadline")
	}
}

// TestConcurrentPublishAndClose exercises the old window between connect()
// releasing the mutex and Publish re-taking it, during which Close could nil
// the connection (nil pointer dereference). Run with -race.
func TestConcurrentPublishAndClose(t *testing.T) {
	f := startFakeNATS(t, `{"server_id":"test"}`)
	raddr, _ := startFakeRedis(t, func(args []string) string { return ":1\r\n" })

	np, err := NewNATSPublisher(f.url("", "a.b"))
	if err != nil {
		t.Fatal(err)
	}
	rp, err := NewRedisPublisher("redis://" + raddr + "/c")
	if err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	for g := 0; g < 4; g++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			for i := 0; i < 50; i++ {
				_ = np.Publish(context.Background(), []byte("x"))
				_ = rp.Publish(context.Background(), []byte("x"))
			}
		}()
		go func() {
			defer wg.Done()
			for i := 0; i < 50; i++ {
				_ = np.Close()
				_ = rp.Close()
			}
		}()
	}
	wg.Wait()
	_ = np.Close()
	_ = rp.Close()
}

func TestRedactURL(t *testing.T) {
	if got := redactURL("redis://u:secret@host:6379/x"); strings.Contains(got, "secret") {
		t.Errorf("password not redacted: %s", got)
	}
	if _, err := NewRedisPublisher("redis://u:secret@[bad/x"); err == nil || strings.Contains(err.Error(), "secret") {
		t.Errorf("error leaks password or is nil: %v", err)
	}
}
