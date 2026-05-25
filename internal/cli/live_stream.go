package cli

import (
	"bufio"
	"context"
	"crypto/sha1"
	"encoding/base64"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"time"

	"github.com/Nosvemos/dukascopy-go/pkg/dukascopy"

	_ "time/tzdata"
)

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

// LiveTick is the JSON envelope streamed to stdout / WebSocket clients.
type LiveTick struct {
	Timestamp int64   `json:"timestamp"` // Unix milliseconds
	Symbol    string  `json:"symbol"`
	Bid       float64 `json:"bid"`
	Ask       float64 `json:"ask"`
	BidVolume float64 `json:"bid_volume,omitempty"`
	AskVolume float64 `json:"ask_volume,omitempty"`
}

// LiveBar is the JSON envelope for OHLCV bar streaming.
type LiveBar struct {
	Timestamp int64   `json:"timestamp"` // Unix milliseconds
	Symbol    string  `json:"symbol"`
	Timeframe string  `json:"timeframe"`
	Open      float64 `json:"open"`
	High      float64 `json:"high"`
	Low       float64 `json:"low"`
	Close     float64 `json:"close"`
	Volume    float64 `json:"volume"`
	Side      string  `json:"side"`
}

// ---------------------------------------------------------------------------
// WebSocket broadcast hub (zero external dependencies)
// ---------------------------------------------------------------------------

type wsHub struct {
	mu      sync.RWMutex
	clients map[chan []byte]struct{}
}

func newWSHub() *wsHub {
	return &wsHub{clients: make(map[chan []byte]struct{})}
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

// ---------------------------------------------------------------------------
// Minimal RFC-6455 WebSocket server (pure net/http + hijack)
// ---------------------------------------------------------------------------

// wsHandshakeGUID is the fixed magic string from RFC 6455 §1.3.
const wsHandshakeGUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"

func wsComputeAcceptKey(clientKey string) string {
	h := sha1.New()
	h.Write([]byte(clientKey + wsHandshakeGUID))
	return base64.StdEncoding.EncodeToString(h.Sum(nil))
}

func wsHandler(hub *wsHub) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// Validate upgrade request
		if !strings.EqualFold(r.Header.Get("Upgrade"), "websocket") {
			http.Error(w, "not a websocket handshake", http.StatusBadRequest)
			return
		}
		clientKey := r.Header.Get("Sec-Websocket-Key")
		if clientKey == "" {
			http.Error(w, "missing Sec-WebSocket-Key", http.StatusBadRequest)
			return
		}

		acceptKey := wsComputeAcceptKey(clientKey)

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

		// Send 101 Switching Protocols
		resp := "HTTP/1.1 101 Switching Protocols\r\n" +
			"Upgrade: websocket\r\n" +
			"Connection: Upgrade\r\n" +
			"Sec-WebSocket-Accept: " + acceptKey + "\r\n\r\n"
		_, _ = bufrw.WriteString(resp)
		_ = bufrw.Flush()

		// Register client channel
		ch := make(chan []byte, 64)
		hub.register(ch)
		defer func() {
			hub.unregister(ch)
			conn.Close()
		}()

		// Write frames to client until channel is closed or write fails
		for msg := range ch {
			if err := wsWriteTextFrame(conn, msg); err != nil {
				return
			}
		}
	}
}

// wsWriteTextFrame writes a minimal RFC-6455 text frame (server→client, no masking).
func wsWriteTextFrame(conn net.Conn, payload []byte) error {
	l := len(payload)
	var header []byte
	switch {
	case l <= 125:
		header = []byte{0x81, byte(l)}
	case l <= 65535:
		header = []byte{0x81, 126, byte(l >> 8), byte(l & 0xff)}
	default:
		header = []byte{
			0x81, 127,
			0, 0, 0, 0,
			byte(l >> 24), byte((l >> 16) & 0xff), byte((l >> 8) & 0xff), byte(l & 0xff),
		}
	}
	_ = conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
	if _, err := conn.Write(header); err != nil {
		return err
	}
	if _, err := conn.Write(payload); err != nil {
		return err
	}
	return nil
}

// ---------------------------------------------------------------------------
// runLiveStream — the main `live` command implementation
// ---------------------------------------------------------------------------

func runLiveStream(args []string, stdout io.Writer, stderr io.Writer) error {
	fs := flag.NewFlagSet("live", flag.ContinueOnError)
	fs.SetOutput(io.Discard)

	symbol       := fs.String("symbol", "", "instrument symbol such as eurusd or xauusd (required)")
	timeframe    := fs.String("timeframe", "tick", "tick, m1, m3, m5, m15, m30, h1, h4, d1")
	side         := fs.String("side", "bid", "bid or ask (used for bar streaming)")
	format       := fs.String("format", "jsonl", "output format: jsonl, csv, json")
	port         := fs.Int("port", 0, "if > 0, start a local WebSocket server on this port")
	pollInterval := fs.Duration("poll-interval", 1*time.Second, "polling interval for new ticks/bars")
	baseURL      := fs.String("base-url", readBaseURL(), "Dukascopy API base URL")
	output       := fs.String("output", "-", "output file path, - for stdout only")

	if err := fs.Parse(args); err != nil {
		return err
	}

	if strings.TrimSpace(*symbol) == "" {
		return errors.New("--symbol is required")
	}
	if *pollInterval <= 0 {
		return errors.New("--poll-interval must be greater than 0")
	}
	formatLower := strings.ToLower(strings.TrimSpace(*format))
	if formatLower != "jsonl" && formatLower != "csv" && formatLower != "json" {
		return fmt.Errorf("invalid --format %q (supported: jsonl, csv, json)", *format)
	}
	if *port < 0 || *port > 65535 {
		return fmt.Errorf("--port must be between 0 and 65535, got %d", *port)
	}

	// Determine output writer
	var out io.Writer = stdout
	outputPath := strings.TrimSpace(*output)
	if outputPath != "-" && outputPath != "" {
		f, err := os.OpenFile(outputPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
		if err != nil {
			return fmt.Errorf("cannot open output file %q: %w", outputPath, err)
		}
		defer f.Close()
		// write to both stdout and file
		out = io.MultiWriter(stdout, f)
	}

	normalizedTimeframe := dukascopy.NormalizeGranularity(dukascopy.Granularity(*timeframe))
	isTick := normalizedTimeframe == dukascopy.GranularityTick

	// Create WebSocket hub and optionally start server
	hub := newWSHub()
	var wsServer *http.Server
	if *port > 0 {
		mux := http.NewServeMux()
		mux.HandleFunc("/stream", wsHandler(hub))
		mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_, _ = fmt.Fprintf(w, `{"status":"ok","symbol":%q,"timeframe":%q,"clients":%d}`,
				*symbol, *timeframe, hub.count())
		})
		wsServer = &http.Server{
			Addr:        fmt.Sprintf(":%d", *port),
			Handler:     mux,
			ReadTimeout: 10 * time.Second,
		}
		ln, err := net.Listen("tcp", wsServer.Addr)
		if err != nil {
			return fmt.Errorf("cannot bind WebSocket server on port %d: %w", *port, err)
		}
		go func() { _ = wsServer.Serve(ln) }()
		fmt.Fprintf(stderr, "%slive%s WebSocket server listening on ws://localhost:%d/stream\n",
			colorize(colorCyan), colorize(colorReset), *port)
	}

	// Graceful shutdown context
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	client := dukascopy.NewClient(*baseURL, 30*time.Second)

	// Resolve instrument once up front
	instruments, err := client.ListInstruments(ctx)
	if err != nil {
		return fmt.Errorf("failed to load instruments: %w", err)
	}
	instrument, err := dukascopy.ResolveInstrument(instruments, *symbol)
	if err != nil {
		return err
	}

	fmt.Fprintf(stderr, "%slive%s streaming %s [%s] (poll every %s) — press Ctrl+C to stop\n",
		colorize(colorCyan), colorize(colorReset), instrument.Name, strings.ToUpper(*timeframe), *pollInterval)

	// Print CSV header once if format=csv
	if formatLower == "csv" {
		if isTick {
			_, _ = fmt.Fprintln(out, "timestamp,symbol,bid,ask,bid_volume,ask_volume")
		} else {
			_, _ = fmt.Fprintln(out, "timestamp,symbol,timeframe,side,open,high,low,close,volume")
		}
	}

	// Tracking state — last processed timestamp to deduplicate
	var (
		lastTickTime time.Time
		lastBarTime  time.Time
		stateMu      sync.Mutex
	)

	// emit writes a line to stdout/file and broadcasts to all WebSocket clients.
	emit := func(payload []byte) {
		line := make([]byte, len(payload)+1)
		copy(line, payload)
		line[len(payload)] = '\n'
		_, _ = out.Write(line)
		if *port > 0 {
			hub.broadcast(line)
		}
	}

	for {
		if err := ctx.Err(); err != nil {
			break
		}

		now := time.Now().UTC()

		if isTick {
			// Download ticks for the current hour window
			from := now.Truncate(time.Hour)
			to := now.Add(time.Nanosecond)
			req := dukascopy.DownloadRequest{
				Symbol:      *symbol,
				Granularity: dukascopy.GranularityTick,
				Side:        dukascopy.PriceSide(strings.ToUpper(*side)),
				From:        from,
				To:          to,
			}
			result, dlErr := client.Download(ctx, req)
			if dlErr == nil {
				stateMu.Lock()
				cutoff := lastTickTime
				stateMu.Unlock()

				for _, tick := range result.Ticks {
					if !tick.Time.After(cutoff) {
						continue
					}
					stateMu.Lock()
					lastTickTime = tick.Time
					stateMu.Unlock()
					cutoff = tick.Time

					lt := LiveTick{
						Timestamp: tick.Time.UnixMilli(),
						Symbol:    strings.ToUpper(instrument.Name),
						Bid:       tick.Bid,
						Ask:       tick.Ask,
						BidVolume: tick.BidVolume,
						AskVolume: tick.AskVolume,
					}
					switch formatLower {
					case "csv":
						emit([]byte(fmt.Sprintf("%d,%s,%.5f,%.5f,%.2f,%.2f",
							lt.Timestamp, lt.Symbol, lt.Bid, lt.Ask, lt.BidVolume, lt.AskVolume)))
					default: // jsonl / json
						b, _ := json.Marshal(lt)
						emit(b)
					}
				}
			}
		} else {
			// Bar streaming: emit the latest completed bar for the requested timeframe
			upper, upperErr := liveUpperInclusive(normalizedTimeframe, now)
			if upperErr != nil {
				if err2 := sleepWithContext(ctx, *pollInterval); err2 != nil {
					break
				}
				continue
			}

			stateMu.Lock()
			cutoff := lastBarTime
			stateMu.Unlock()

			if !upper.After(cutoff) {
				if err2 := sleepWithContext(ctx, *pollInterval); err2 != nil {
					break
				}
				continue
			}

			req := dukascopy.DownloadRequest{
				Symbol:      *symbol,
				Granularity: normalizedTimeframe,
				Side:        dukascopy.PriceSide(strings.ToUpper(*side)),
				From:        upper,
				To:          upper.Add(time.Nanosecond),
			}
			result, dlErr := client.Download(ctx, req)
			if dlErr == nil {
				stateMu.Lock()
				cutoff2 := lastBarTime
				stateMu.Unlock()

				for _, bar := range result.Bars {
					if !bar.Time.After(cutoff2) {
						continue
					}
					stateMu.Lock()
					lastBarTime = bar.Time
					stateMu.Unlock()
					cutoff2 = bar.Time

					lb := LiveBar{
						Timestamp: bar.Time.UnixMilli(),
						Symbol:    strings.ToUpper(instrument.Name),
						Timeframe: strings.ToUpper(*timeframe),
						Side:      strings.ToUpper(*side),
						Open:      bar.Open,
						High:      bar.High,
						Low:       bar.Low,
						Close:     bar.Close,
						Volume:    bar.Volume,
					}
					switch formatLower {
					case "csv":
						emit([]byte(fmt.Sprintf("%d,%s,%s,%s,%.5f,%.5f,%.5f,%.5f,%.2f",
							lb.Timestamp, lb.Symbol, lb.Timeframe, lb.Side,
							lb.Open, lb.High, lb.Low, lb.Close, lb.Volume)))
					default: // jsonl / json
						b, _ := json.Marshal(lb)
						emit(b)
					}
				}
			}
		}

		if err := sleepWithContext(ctx, *pollInterval); err != nil {
			break
		}
	}

	if wsServer != nil {
		shutCtx, shutCancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer shutCancel()
		_ = wsServer.Shutdown(shutCtx)
	}

	fmt.Fprintf(stderr, "%slive%s stopped\n", colorize(colorCyan), colorize(colorReset))
	return nil
}

// ---------------------------------------------------------------------------
// wsClientWriter — helper used in tests to drain a WebSocket client channel
// ---------------------------------------------------------------------------

func wsClientWriter(conn net.Conn, ch <-chan []byte) {
	bw := bufio.NewWriter(conn)
	for msg := range ch {
		if err := wsWriteTextFrame(conn, msg); err != nil {
			_ = bw.Flush()
			return
		}
	}
}
