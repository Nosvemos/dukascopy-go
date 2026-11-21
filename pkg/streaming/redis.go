package streaming

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"strconv"
	"strings"
	"sync"
)

// maxBulkLen bounds a RESP bulk string / array length read from the server
// (Redis' own proto-max-bulk-len default is 512 MiB).
const maxBulkLen = 512 << 20

// RedisPublisher publishes messages to Redis via Pub/Sub or Streams (XADD) using native RESP.
// Zero third-party dependencies.
type RedisPublisher struct {
	addr      string
	username  string
	password  string
	db        int // selected with SELECT after connecting when > 0
	channel   string
	isStream  bool
	streamKey string

	mu   sync.Mutex // guards conn/r and serializes Publish/Close
	conn net.Conn
	r    *bufio.Reader
}

// RedisError is an error reply ("-ERR ...") returned by the Redis server.
type RedisError string

func (e RedisError) Error() string { return "redis error: " + string(e) }

// NewRedisPublisher parses a Redis URL and returns a ready publisher.
// Examples:
// - redis://localhost:6379/market_eurusd (Pub/Sub)
// - redis://:password@localhost:6379/stream:eurusd (Redis Stream XADD)
// - redis://user:password@localhost:6379/stream:eurusd (ACL user, AUTH user password)
// - redis://localhost:6379/2 (database 2, default channel "market_data")
// - redis://localhost:6379/2/stream:eurusd (database 2, stream "eurusd")
//
// A purely numeric first path segment is the database number (sent with
// SELECT); the remainder of the path is the channel or "stream:<key>".
// The database only affects stream keys; Pub/Sub channels are global.
func NewRedisPublisher(rawURL string) (*RedisPublisher, error) {
	if !strings.Contains(rawURL, "://") {
		rawURL = "redis://" + rawURL
	}
	u, err := url.Parse(rawURL)
	if err != nil {
		return nil, fmt.Errorf("invalid redis url %q: %w", redactURL(rawURL), urlParseCause(err))
	}

	host := u.Host
	if host == "" {
		host = "localhost:6379"
	}
	if u.Port() == "" && !strings.HasSuffix(host, ":") {
		host = net.JoinHostPort(strings.Trim(host, "[]"), "6379")
	}

	var user, pass string
	if u.User != nil {
		user = u.User.Username()
		pass, _ = u.User.Password()
	}

	path := strings.TrimPrefix(u.Path, "/")
	db := 0
	first, rest, _ := strings.Cut(path, "/")
	if first != "" && strings.Trim(first, "0123456789") == "" {
		n, err := strconv.Atoi(first)
		if err != nil || n < 0 {
			return nil, fmt.Errorf("invalid redis database number %q", first)
		}
		db = n
		path = rest
	}
	if path == "" {
		path = "market_data"
	}

	isStream := false
	streamKey := path
	if strings.HasPrefix(path, "stream:") {
		isStream = true
		streamKey = strings.TrimPrefix(path, "stream:")
	}

	return &RedisPublisher{
		addr:      host,
		username:  user,
		password:  pass,
		db:        db,
		channel:   path,
		isStream:  isStream,
		streamKey: streamKey,
	}, nil
}

// connectLocked dials and authenticates. p.mu must be held.
func (p *RedisPublisher) connectLocked(ctx context.Context) error {
	if p.conn != nil {
		return nil
	}

	d := net.Dialer{Timeout: defaultIOTimeout}
	conn, err := d.DialContext(ctx, "tcp", p.addr)
	if err != nil {
		return err
	}
	p.conn = conn
	p.r = bufio.NewReader(conn)

	fail := func(what string, err error) error {
		_ = p.conn.Close()
		p.conn, p.r = nil, nil
		return fmt.Errorf("%s: %w", what, err)
	}

	if p.password != "" {
		args := []string{"AUTH", p.password}
		if p.username != "" {
			args = []string{"AUTH", p.username, p.password}
		}
		if _, err := p.doLocked(ctx, args...); err != nil {
			return fail("redis auth failed", err)
		}
	}
	if p.db > 0 {
		if _, err := p.doLocked(ctx, "SELECT", strconv.Itoa(p.db)); err != nil {
			return fail("redis select db "+strconv.Itoa(p.db)+" failed", err)
		}
	}

	return nil
}

// Publish sends payload to the Redis channel or stream. The round trip is
// bounded by ctx's deadline (or 5 s when ctx has none) and aborted when ctx
// is cancelled.
func (p *RedisPublisher) Publish(ctx context.Context, payload []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	if err := p.connectLocked(ctx); err != nil {
		return err
	}

	var err error
	if p.isStream {
		// XADD streamKey * data <payload>  -> bulk string (entry id)
		_, err = p.doLocked(ctx, "XADD", p.streamKey, "*", "data", string(payload))
	} else {
		// PUBLISH channel <payload>  -> integer (receivers)
		_, err = p.doLocked(ctx, "PUBLISH", p.channel, string(payload))
	}

	if err != nil {
		var redisErr RedisError
		if !errors.As(err, &redisErr) {
			// I/O or protocol failure: the stream may be out of sync.
			_ = p.conn.Close()
			p.conn, p.r = nil, nil
		}
		return err
	}

	return nil
}

// doLocked sends one command and reads its complete reply. p.mu must be held.
func (p *RedisPublisher) doLocked(ctx context.Context, args ...string) (any, error) {
	stop := watchCtx(ctx, p.conn, false)
	defer stop()
	if err := p.conn.SetDeadline(ioDeadline(ctx)); err != nil {
		return nil, err
	}

	if _, err := p.conn.Write(encodeRESPCommand(args...)); err != nil {
		return nil, err
	}
	reply, err := readRESP(p.r)
	if err != nil {
		return nil, err
	}
	if e, ok := reply.(RedisError); ok {
		return nil, e
	}
	return reply, nil
}

func encodeRESPCommand(args ...string) []byte {
	var sb strings.Builder
	sb.WriteString("*" + strconv.Itoa(len(args)) + "\r\n")
	for _, arg := range args {
		sb.WriteString("$" + strconv.Itoa(len(arg)) + "\r\n" + arg + "\r\n")
	}
	return []byte(sb.String())
}

// readRESP reads one complete RESP2 reply. It returns string for simple
// strings (+), RedisError for errors (-), int64 for integers (:), []byte or
// nil for bulk strings ($), and []any or nil for arrays (*). Error replies are
// returned as values (not as the error result) so that a nested error inside
// an array does not desynchronize the stream; the error result is reserved
// for I/O and protocol failures.
func readRESP(r *bufio.Reader) (any, error) {
	line, err := readProtocolLine(r)
	if err != nil {
		return nil, err
	}
	if line == "" {
		return nil, errors.New("redis protocol error: empty reply line")
	}
	body := line[1:]
	switch line[0] {
	case '+':
		return body, nil
	case '-':
		return RedisError(body), nil
	case ':':
		n, err := strconv.ParseInt(body, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("redis protocol error: bad integer %q", body)
		}
		return n, nil
	case '$':
		n, err := strconv.Atoi(body)
		if err != nil || n < -1 || n > maxBulkLen {
			return nil, fmt.Errorf("redis protocol error: bad bulk length %q", body)
		}
		if n == -1 {
			return nil, nil
		}
		buf := make([]byte, n+2)
		if _, err := io.ReadFull(r, buf); err != nil {
			return nil, err
		}
		if buf[n] != '\r' || buf[n+1] != '\n' {
			return nil, errors.New("redis protocol error: bulk string not terminated by CRLF")
		}
		return buf[:n], nil
	case '*':
		n, err := strconv.Atoi(body)
		if err != nil || n < -1 || n > maxBulkLen {
			return nil, fmt.Errorf("redis protocol error: bad array length %q", body)
		}
		if n == -1 {
			return nil, nil
		}
		out := make([]any, 0, min(n, 1024))
		for i := 0; i < n; i++ {
			v, err := readRESP(r)
			if err != nil {
				return nil, err
			}
			out = append(out, v)
		}
		return out, nil
	default:
		return nil, fmt.Errorf("redis protocol error: unexpected reply %q", truncate(line, 64))
	}
}

// Close closes the connection (if any). It is safe to call concurrently with
// Publish and more than once.
func (p *RedisPublisher) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.conn != nil {
		err := p.conn.Close()
		p.conn, p.r = nil, nil
		return err
	}
	return nil
}
