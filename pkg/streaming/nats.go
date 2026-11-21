package streaming

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/url"
	"strings"
	"sync"
	"time"
	"unicode"
)

// defaultIOTimeout bounds every network round trip when the caller's context
// carries no deadline.
const defaultIOTimeout = 5 * time.Second

// maxProtocolLine bounds a single protocol line read from a broker.
const maxProtocolLine = 64 * 1024

// NATSPublisher publishes messages to NATS using the native NATS text protocol
// (core NATS, plain TCP). It has no third-party dependencies.
//
// Credentials are taken from the URL: nats://user:pass@host:4222/subject sends
// user/pass, nats://token@host:4222/subject sends auth_token. TLS is not
// supported; connecting to a server whose INFO advertises tls_required fails
// with an error.
//
// A background reader answers server PINGs and records asynchronous -ERR
// messages; a recorded -ERR is returned by the next Publish.
type NATSPublisher struct {
	addr    string
	subject string
	user    string
	pass    string
	token   string

	mu   sync.Mutex // guards conn and serializes Publish/Close
	conn *natsConn
}

// natsConn is one live connection plus its background reader state.
type natsConn struct {
	c   net.Conn
	wmu sync.Mutex // serializes writes (Publish and PONG replies)

	errMu   sync.Mutex
	srvErr  error // last -ERR from the server, not yet reported
	readErr error // set once the reader stops (connection unusable)
	done    chan struct{}
}

// NewNATSPublisher parses a NATS URL and returns a ready publisher.
// Examples:
// - nats://localhost:4222/market.eurusd
// - nats://127.0.0.1:4222/crypto.btcusd
// - nats://user:pass@localhost:4222/market.eurusd
// - nats://mytoken@localhost:4222/market.eurusd
func NewNATSPublisher(rawURL string) (*NATSPublisher, error) {
	if !strings.Contains(rawURL, "://") {
		rawURL = "nats://" + rawURL
	}
	u, err := url.Parse(rawURL)
	if err != nil {
		return nil, fmt.Errorf("invalid nats url %q: %w", redactURL(rawURL), urlParseCause(err))
	}

	host := u.Host
	if host == "" {
		host = "localhost:4222"
	}
	if u.Port() == "" && !strings.HasSuffix(host, ":") {
		host = net.JoinHostPort(strings.Trim(host, "[]"), "4222")
	}

	subject := strings.TrimPrefix(u.Path, "/")
	if subject == "" {
		subject = "market.data"
	}
	if err := validateNATSSubject(subject); err != nil {
		return nil, err
	}

	p := &NATSPublisher{addr: host, subject: subject}
	if u.User != nil {
		name := u.User.Username()
		if pass, ok := u.User.Password(); ok {
			p.user, p.pass = name, pass
		} else {
			p.token = name
		}
	}
	return p, nil
}

// validateNATSSubject rejects subjects that would break or inject into the
// line-based protocol (whitespace, CR/LF and other control characters).
func validateNATSSubject(subject string) error {
	for _, r := range subject {
		if unicode.IsSpace(r) || unicode.IsControl(r) {
			return fmt.Errorf("invalid nats subject %q: must not contain whitespace or control characters", subject)
		}
	}
	return nil
}

// ioDeadline returns the deadline for one network round trip.
func ioDeadline(ctx context.Context) time.Time {
	if d, ok := ctx.Deadline(); ok {
		return d
	}
	return time.Now().Add(defaultIOTimeout)
}

// watchCtx makes blocking I/O on c return as soon as ctx is cancelled, by
// moving the deadline (all deadlines, or only the write deadline when
// writeOnly is set) to now. The returned stop function must be called when
// the round trip is over.
func watchCtx(ctx context.Context, c net.Conn, writeOnly bool) (stop func() bool) {
	return context.AfterFunc(ctx, func() {
		if writeOnly {
			_ = c.SetWriteDeadline(time.Now())
			return
		}
		_ = c.SetDeadline(time.Now())
	})
}

// connectLocked dials and performs the NATS handshake. p.mu must be held.
func (p *NATSPublisher) connectLocked(ctx context.Context) error {
	if p.conn != nil {
		return nil
	}

	d := net.Dialer{Timeout: defaultIOTimeout}
	c, err := d.DialContext(ctx, "tcp", p.addr)
	if err != nil {
		return err
	}
	stop := watchCtx(ctx, c, false)
	defer stop()
	_ = c.SetDeadline(ioDeadline(ctx))

	r := bufio.NewReaderSize(c, 4096)
	fail := func(format string, args ...any) error {
		_ = c.Close()
		return fmt.Errorf(format, args...)
	}

	// Server greets with INFO {json}.
	line, err := readProtocolLine(r)
	if err != nil {
		return fail("nats handshake failed: %w", err)
	}
	if !strings.HasPrefix(line, "INFO") {
		return fail("nats handshake failed: expected INFO, got %q", truncate(line, 128))
	}
	var info struct {
		TLSRequired  bool `json:"tls_required"`
		AuthRequired bool `json:"auth_required"`
	}
	_ = json.Unmarshal([]byte(strings.TrimSpace(strings.TrimPrefix(line, "INFO"))), &info)
	if info.TLSRequired {
		return fail("nats server %s requires TLS (tls_required in INFO), which this publisher does not support", p.addr)
	}

	connect := map[string]any{
		"verbose":  false,
		"pedantic": false,
		"lang":     "go",
		"name":     "dukascopy-go",
	}
	if p.user != "" || p.pass != "" {
		connect["user"] = p.user
		connect["pass"] = p.pass
	}
	if p.token != "" {
		connect["auth_token"] = p.token
	}
	payload, err := json.Marshal(connect)
	if err != nil {
		return fail("nats connect message failed: %w", err)
	}
	// PING right after CONNECT: the PONG confirms the CONNECT was accepted
	// (authorization errors arrive as -ERR before it).
	if _, err := c.Write([]byte("CONNECT " + string(payload) + "\r\nPING\r\n")); err != nil {
		return fail("nats connect message failed: %w", err)
	}
	for {
		line, err := readProtocolLine(r)
		if err != nil {
			return fail("nats handshake failed: %w", err)
		}
		switch {
		case strings.HasPrefix(line, "PONG"):
			_ = c.SetDeadline(time.Time{})
			nc := &natsConn{c: c, done: make(chan struct{})}
			p.conn = nc
			go nc.readLoop(r)
			return nil
		case strings.HasPrefix(line, "-ERR"):
			return fail("nats server rejected connection: %s", strings.TrimSpace(strings.TrimPrefix(line, "-ERR")))
		case strings.HasPrefix(line, "PING"):
			if _, err := c.Write([]byte("PONG\r\n")); err != nil {
				return fail("nats handshake failed: %w", err)
			}
		default:
			// +OK, INFO updates: ignore.
		}
	}
}

// readLoop consumes server traffic: answers PING, records -ERR.
func (nc *natsConn) readLoop(r *bufio.Reader) {
	defer close(nc.done)
	for {
		line, err := readProtocolLine(r)
		if err != nil {
			nc.errMu.Lock()
			nc.readErr = err
			nc.errMu.Unlock()
			return
		}
		switch {
		case strings.HasPrefix(line, "PING"):
			if err := nc.write(context.Background(), []byte("PONG\r\n")); err != nil {
				nc.errMu.Lock()
				nc.readErr = err
				nc.errMu.Unlock()
				_ = nc.c.Close()
				return
			}
		case strings.HasPrefix(line, "-ERR"):
			msg := strings.Trim(strings.TrimSpace(strings.TrimPrefix(line, "-ERR")), "'")
			nc.errMu.Lock()
			nc.srvErr = fmt.Errorf("nats server error: %s", msg)
			nc.errMu.Unlock()
		default:
			// +OK, PONG, INFO: nothing to do.
		}
	}
}

func (nc *natsConn) write(ctx context.Context, b []byte) error {
	nc.wmu.Lock()
	defer nc.wmu.Unlock()
	stop := watchCtx(ctx, nc.c, true)
	defer stop()
	_ = nc.c.SetWriteDeadline(ioDeadline(ctx))
	_, err := nc.c.Write(b)
	return err
}

// state returns (and clears) a pending server error, and whether the
// connection is dead.
func (nc *natsConn) state() (srvErr error, dead bool) {
	nc.errMu.Lock()
	defer nc.errMu.Unlock()
	srvErr, nc.srvErr = nc.srvErr, nil
	return srvErr, nc.readErr != nil
}

func (nc *natsConn) close() error {
	err := nc.c.Close()
	<-nc.done
	return err
}

// Publish sends a message to the NATS subject. The round trip is bounded by
// ctx's deadline (or 5 s when ctx has none) and aborted when ctx is
// cancelled. An -ERR received from the server since the previous Publish is
// returned instead of publishing.
func (p *NATSPublisher) Publish(ctx context.Context, payload []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	if p.conn != nil {
		srvErr, dead := p.conn.state()
		if dead {
			_ = p.conn.close()
			p.conn = nil
		}
		if srvErr != nil {
			return srvErr
		}
	}
	if err := p.connectLocked(ctx); err != nil {
		return err
	}

	// PUB <subject> <len>\r\n<payload>\r\n
	pubHeader := fmt.Sprintf("PUB %s %d\r\n", p.subject, len(payload))
	msg := make([]byte, 0, len(pubHeader)+len(payload)+2)
	msg = append(msg, pubHeader...)
	msg = append(msg, payload...)
	msg = append(msg, '\r', '\n')

	if err := p.conn.write(ctx, msg); err != nil {
		_ = p.conn.close()
		p.conn = nil
		return err
	}
	return nil
}

// Close closes the connection (if any). It is safe to call concurrently with
// Publish and more than once.
func (p *NATSPublisher) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.conn != nil {
		err := p.conn.close()
		p.conn = nil
		if errors.Is(err, net.ErrClosed) {
			return nil
		}
		return err
	}
	return nil
}

// readProtocolLine reads one CRLF/LF-terminated line, bounded in length.
func readProtocolLine(r *bufio.Reader) (string, error) {
	var sb strings.Builder
	for {
		chunk, isPrefix, err := r.ReadLine()
		if err != nil {
			return "", err
		}
		sb.Write(chunk)
		if sb.Len() > maxProtocolLine {
			return "", fmt.Errorf("protocol line exceeds %d bytes", maxProtocolLine)
		}
		if !isPrefix {
			return sb.String(), nil
		}
	}
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}

// urlParseCause strips the *url.Error wrapper, whose message repeats the raw
// URL (including any password).
func urlParseCause(err error) error {
	var ue *url.Error
	if errors.As(err, &ue) {
		return ue.Err
	}
	return err
}

// redactURL hides a password embedded in a URL for error messages.
func redactURL(raw string) string {
	if u, err := url.Parse(raw); err == nil {
		return u.Redacted()
	}
	if at := strings.LastIndex(raw, "@"); at >= 0 {
		if scheme := strings.Index(raw, "://"); scheme >= 0 && scheme < at {
			return raw[:scheme+3] + "xxxxx" + raw[at:]
		}
	}
	return raw
}
