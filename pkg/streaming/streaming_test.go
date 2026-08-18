package streaming

import (
	"bufio"
	"context"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/Nosvemos/dukascopy-go/pkg/dukascopy"
)

func TestMicrostructureCalculation(t *testing.T) {
	t1 := dukascopy.Tick{
		Time:      time.Now().UTC(),
		Bid:       1.0800,
		Ask:       1.0802,
		BidVolume: 200,
		AskVolume: 100,
	}
	t2 := dukascopy.Tick{
		Time:      t1.Time.Add(500 * time.Millisecond),
		Bid:       1.0805,
		Ask:       1.0807,
		BidVolume: 150,
		AskVolume: 150,
	}

	m1 := ComputeMicrostructure(t1, nil)
	if m1.Spread < 0.00019 || m1.Spread > 0.00021 {
		t.Errorf("unexpected spread: %f", m1.Spread)
	}
	if m1.OrderFlowImbalance < 0.33 || m1.OrderFlowImbalance > 0.34 {
		t.Errorf("unexpected OFI: %f", m1.OrderFlowImbalance)
	}

	m2 := ComputeMicrostructure(t2, &t1)
	if m2.EffectiveSpread < 0.0009 || m2.EffectiveSpread > 0.0011 {
		t.Errorf("unexpected effective spread: %f", m2.EffectiveSpread)
	}
	if m2.PriceVelocity <= 0 {
		t.Errorf("expected positive price velocity, got %f", m2.PriceVelocity)
	}
}

func TestRedisMockPublish(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen failed: %v", err)
	}
	defer ln.Close()

	receivedChan := make(chan string, 10)
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()

		r := bufio.NewReader(conn)
		for {
			line, err := r.ReadString('\n')
			if err != nil {
				return
			}
			receivedChan <- line
			if strings.HasPrefix(line, "*") {
				_, _ = conn.Write([]byte(":1\r\n"))
			}
		}
	}()

	pub, err := NewRedisPublisher("redis://" + ln.Addr().String() + "/test_chan")
	if err != nil {
		t.Fatalf("NewRedisPublisher failed: %v", err)
	}
	defer pub.Close()

	if err := pub.Publish(context.Background(), []byte(`{"price":1.08}`)); err != nil {
		t.Fatalf("Publish failed: %v", err)
	}

	found := false
	timer := time.After(1 * time.Second)
	for !found {
		select {
		case line := <-receivedChan:
			if strings.Contains(line, "PUBLISH") {
				found = true
			}
		case <-timer:
			t.Fatal("timeout waiting for Redis PUBLISH command")
		}
	}
}

func TestNATSMockPublish(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen failed: %v", err)
	}
	defer ln.Close()

	receivedChan := make(chan string, 10)
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()

		// Send INFO
		_, _ = conn.Write([]byte("INFO {\"server_id\":\"test\"}\r\n"))

		r := bufio.NewReader(conn)
		for {
			line, err := r.ReadString('\n')
			if err != nil {
				return
			}
			receivedChan <- line
		}
	}()

	pub, err := NewNATSPublisher("nats://" + ln.Addr().String() + "/market.eurusd")
	if err != nil {
		t.Fatalf("NewNATSPublisher failed: %v", err)
	}
	defer pub.Close()

	if err := pub.Publish(context.Background(), []byte(`{"symbol":"EURUSD"}`)); err != nil {
		t.Fatalf("Publish failed: %v", err)
	}

	found := false
	timer := time.After(1 * time.Second)
	for !found {
		select {
		case line := <-receivedChan:
			if strings.Contains(line, "PUB market.eurusd") {
				found = true
			}
		case <-timer:
			t.Fatal("timeout waiting for NATS PUB command")
		}
	}
}

func TestRedisMockPublishStream(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen failed: %v", err)
	}
	defer ln.Close()

	receivedChan := make(chan string, 10)
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()

		r := bufio.NewReader(conn)
		for {
			line, err := r.ReadString('\n')
			if err != nil {
				return
			}
			receivedChan <- line
			if strings.HasPrefix(line, "*") {
				_, _ = conn.Write([]byte("+OK\r\n"))
			}
		}
	}()

	pub, err := NewRedisPublisher("redis://:secret@" + ln.Addr().String() + "/stream:market_ticks")
	if err != nil {
		t.Fatalf("NewRedisPublisher failed: %v", err)
	}
	defer pub.Close()

	if err := pub.Publish(context.Background(), []byte(`{"stream_test":true}`)); err != nil {
		t.Fatalf("Publish failed: %v", err)
	}

	hasAuth := false
	hasXadd := false
	timer := time.After(1 * time.Second)
	for !hasAuth || !hasXadd {
		select {
		case line := <-receivedChan:
			if strings.Contains(line, "AUTH") {
				hasAuth = true
			}
			if strings.Contains(line, "XADD") {
				hasXadd = true
			}
		case <-timer:
			t.Fatalf("timeout waiting for Redis commands (auth=%v, xadd=%v)", hasAuth, hasXadd)
		}
	}
}

func TestStreamingErrorBranches(t *testing.T) {
	// Defaults and URL parsing
	pDef, err := NewRedisPublisher("localhost")
	if err != nil || pDef.channel != "market_data" {
		t.Errorf("unexpected redis defaults: %+v", pDef)
	}
	_ = pDef.Close()

	nDef, err := NewNATSPublisher("localhost")
	if err != nil || nDef.subject != "market.data" {
		t.Errorf("unexpected nats defaults: %+v", nDef)
	}
	_ = nDef.Close()

	// Direct fail on down server
	p, err := NewRedisPublisher("redis://localhost:9999/chan")
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	defer p.Close()

	err = p.Publish(context.Background(), []byte("data"))
	if err == nil {
		t.Errorf("expected publish error on closed port")
	}

	np, err := NewNATSPublisher("nats://localhost:9999/market")
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	defer np.Close()

	err = np.Publish(context.Background(), []byte("data"))
	if err == nil {
		t.Errorf("expected nats publish error on closed port")
	}

	// Redis server returning -ERR response
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen failed: %v", err)
	}
	defer ln.Close()

	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()

		r := bufio.NewReader(conn)
		for {
			_, err := r.ReadString('\n')
			if err != nil {
				return
			}
			_, _ = conn.Write([]byte("-ERR custom redis error\r\n"))
		}
	}()

	errPub, err := NewRedisPublisher("redis://" + ln.Addr().String() + "/test")
	if err != nil {
		t.Fatalf("NewRedisPublisher failed: %v", err)
	}
	defer errPub.Close()

	if err := errPub.Publish(context.Background(), []byte("test")); err == nil {
		t.Errorf("expected error on -ERR response")
	}
}

func TestStreamingReconnectionAndAuth(t *testing.T) {
	// Mock server that accepts connection and responds to INFO and CONNECT
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen failed: %v", err)
	}
	defer ln.Close()

	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				_, _ = c.Write([]byte("INFO {\"server_id\":\"test\"}\r\n"))
				r := bufio.NewReader(c)
				for {
					line, err := r.ReadString('\n')
					if err != nil {
						return
					}
					if strings.HasPrefix(line, "PUB") {
						// close unexpectedly to test broken pipe
						return
					}
				}
			}(conn)
		}
	}()

	np, err := NewNATSPublisher("nats://" + ln.Addr().String() + "/market.eurusd")
	if err != nil {
		t.Fatalf("NewNATSPublisher failed: %v", err)
	}
	defer np.Close()

	// Initial publish
	_ = np.Publish(context.Background(), []byte("test 1"))
	// Second publish (connection reuse)
	_ = np.Publish(context.Background(), []byte("test 2"))
	_ = np.connect() // already connected branch

	// Invalid URLs
	if _, err := NewNATSPublisher("://invalid-url"); err == nil {
		t.Errorf("expected error for invalid nats url")
	}
	if _, err := NewRedisPublisher("://invalid-url"); err == nil {
		t.Errorf("expected error for invalid redis url")
	}

	// Server that immediately closes connection on connect (handshake fail)
	lnClose, err := net.Listen("tcp", "127.0.0.1:0")
	if err == nil {
		defer lnClose.Close()
		go func() {
			c, err := lnClose.Accept()
			if err == nil {
				c.Close() // immediately close
			}
		}()

		npFail, err := NewNATSPublisher("nats://" + lnClose.Addr().String() + "/test")
		if err == nil {
			defer npFail.Close()
			if err := npFail.Publish(context.Background(), []byte("test")); err == nil {
				t.Errorf("expected handshake error on closed socket")
			}
		}
	}

	// Redis already connected branch
	lnR, err := net.Listen("tcp", "127.0.0.1:0")
	if err == nil {
		defer lnR.Close()
		go func() {
			c, err := lnR.Accept()
			if err == nil {
				defer c.Close()
				r := bufio.NewReader(c)
				for {
					line, err := r.ReadString('\n')
					if err != nil {
						return
					}
					if strings.HasPrefix(line, "*") {
						_, _ = c.Write([]byte("+OK\r\n"))
					}
				}
			}
		}()

		rp, err := NewRedisPublisher("redis://" + lnR.Addr().String() + "/test")
		if err == nil {
			defer rp.Close()
			_ = rp.Publish(context.Background(), []byte("data1"))
			_ = rp.connect() // already connected branch
		}
	}
}
