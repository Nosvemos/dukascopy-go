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
