package streaming

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
)

// RedisPublisher publishes messages to Redis via Pub/Sub or Streams (XADD) using native RESP.
// Zero third-party dependencies.
type RedisPublisher struct {
	addr     string
	password string
	db       int
	channel  string
	isStream bool
	streamKey string

	mu   sync.Mutex
	conn net.Conn
	r    *bufio.Reader
}

// NewRedisPublisher parses a Redis URL and returns a ready publisher.
// Examples:
// - redis://localhost:6379/market_eurusd (Pub/Sub)
// - redis://:password@localhost:6379/stream:eurusd (Redis Stream XADD)
func NewRedisPublisher(rawURL string) (*RedisPublisher, error) {
	if !strings.Contains(rawURL, "://") {
		rawURL = "redis://" + rawURL
	}
	u, err := url.Parse(rawURL)
	if err != nil {
		return nil, fmt.Errorf("invalid redis url %q: %w", rawURL, err)
	}

	host := u.Host
	if host == "" {
		host = "localhost:6379"
	}
	if !strings.Contains(host, ":") {
		host += ":6379"
	}

	pass, _ := u.User.Password()
	path := strings.TrimPrefix(u.Path, "/")
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
		password:  pass,
		channel:   path,
		isStream:  isStream,
		streamKey: streamKey,
	}, nil
}

func (p *RedisPublisher) connect() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.conn != nil {
		return nil
	}

	d := net.Dialer{Timeout: 5 * time.Second}
	conn, err := d.Dial("tcp", p.addr)
	if err != nil {
		return err
	}

	p.conn = conn
	p.r = bufio.NewReader(conn)

	// Auth if password provided
	if p.password != "" {
		if err := p.sendCommandLocked("AUTH", p.password); err != nil {
			p.conn.Close()
			p.conn = nil
			return fmt.Errorf("redis auth failed: %w", err)
		}
	}

	return nil
}

// Publish sends payload to the Redis channel or stream.
func (p *RedisPublisher) Publish(ctx context.Context, payload []byte) error {
	if err := p.connect(); err != nil {
		return err
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	var err error
	if p.isStream {
		// XADD streamKey * data <payload>
		err = p.sendCommandLocked("XADD", p.streamKey, "*", "data", string(payload))
	} else {
		// PUBLISH channel <payload>
		err = p.sendCommandLocked("PUBLISH", p.channel, string(payload))
	}

	if err != nil {
		if p.conn != nil {
			_ = p.conn.Close()
			p.conn = nil
		}
		return err
	}

	return nil
}

func (p *RedisPublisher) sendCommandLocked(args ...string) error {
	var sb strings.Builder
	sb.WriteString("*" + strconv.Itoa(len(args)) + "\r\n")
	for _, arg := range args {
		sb.WriteString("$" + strconv.Itoa(len(arg)) + "\r\n" + arg + "\r\n")
	}

	if _, err := p.conn.Write([]byte(sb.String())); err != nil {
		return err
	}

	// Read RESP response
	line, err := p.r.ReadString('\n')
	if err != nil {
		return err
	}

	if strings.HasPrefix(line, "-") {
		return fmt.Errorf("redis error: %s", strings.TrimSpace(line[1:]))
	}

	return nil
}

func (p *RedisPublisher) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.conn != nil {
		err := p.conn.Close()
		p.conn = nil
		return err
	}
	return nil
}
