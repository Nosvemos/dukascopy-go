package streaming

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"net/url"
	"strings"
	"sync"
	"time"
)

// NATSPublisher publishes messages to NATS using native NATS text protocol.
// Zero third-party dependencies.
type NATSPublisher struct {
	addr    string
	subject string

	mu   sync.Mutex
	conn net.Conn
	r    *bufio.Reader
}

// NewNATSPublisher parses a NATS URL and returns a ready publisher.
// Examples:
// - nats://localhost:4222/market.eurusd
// - nats://127.0.0.1:4222/crypto.btcusd
func NewNATSPublisher(rawURL string) (*NATSPublisher, error) {
	if !strings.Contains(rawURL, "://") {
		rawURL = "nats://" + rawURL
	}
	u, err := url.Parse(rawURL)
	if err != nil {
		return nil, fmt.Errorf("invalid nats url %q: %w", rawURL, err)
	}

	host := u.Host
	if host == "" {
		host = "localhost:4222"
	}
	if !strings.Contains(host, ":") {
		host += ":4222"
	}

	subject := strings.TrimPrefix(u.Path, "/")
	if subject == "" {
		subject = "market.data"
	}

	return &NATSPublisher{
		addr:    host,
		subject: subject,
	}, nil
}

func (p *NATSPublisher) connect() error {
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

	// Read initial INFO message from server
	_, err = p.r.ReadString('\n')
	if err != nil {
		conn.Close()
		p.conn = nil
		return fmt.Errorf("nats handshake failed: %w", err)
	}

	// Send CONNECT
	connectMsg := "CONNECT {\"verbose\":false,\"pedantic\":false}\r\n"
	if _, err := p.conn.Write([]byte(connectMsg)); err != nil {
		conn.Close()
		p.conn = nil
		return fmt.Errorf("nats connect message failed: %w", err)
	}

	return nil
}

// Publish sends a message to the NATS subject.
func (p *NATSPublisher) Publish(ctx context.Context, payload []byte) error {
	if err := p.connect(); err != nil {
		return err
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	// PUB <subject> <len>\r\n<payload>\r\n
	pubHeader := fmt.Sprintf("PUB %s %d\r\n", p.subject, len(payload))
	msg := make([]byte, 0, len(pubHeader)+len(payload)+2)
	msg = append(msg, []byte(pubHeader)...)
	msg = append(msg, payload...)
	msg = append(msg, '\r', '\n')

	if _, err := p.conn.Write(msg); err != nil {
		if p.conn != nil {
			_ = p.conn.Close()
			p.conn = nil
		}
		return err
	}

	return nil
}

func (p *NATSPublisher) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.conn != nil {
		err := p.conn.Close()
		p.conn = nil
		return err
	}
	return nil
}
