package cli

import (
	"context"
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

// runLiveStream — the main `live` command implementation
func runLiveStream(args []string, stdout io.Writer, stderr io.Writer) error {
	fs := flag.NewFlagSet("live", flag.ContinueOnError)
	fs.SetOutput(stdout)
	fs.Usage = func() {
		fmt.Fprintf(stdout, "%slive:%s Stream real-time ticks/bars to stdout and optional WebSocket server\n\n", colorize(colorCyan), colorize(colorReset))
		fmt.Fprint(stdout, "Usage:\n  dukascopy-go live [options]\n\nOptions:\n")
		fs.PrintDefaults()
		fmt.Fprint(stdout, "\nExamples:\n  dukascopy-go live --symbol eurusd --timeframe tick --format jsonl\n  dukascopy-go live --symbol eurusd --timeframe m1 --side bid --port 8080\n")
	}

	symbol := fs.String("symbol", "", "instrument symbol such as eurusd or xauusd (required)")
	timeframe := fs.String("timeframe", "tick", "tick, m1, m3, m5, m15, m30, h1, h4, d1")
	side := fs.String("side", "bid", "bid or ask (used for bar streaming)")
	format := fs.String("format", "jsonl", "output format: jsonl, csv, json")
	port := fs.Int("port", 0, "if > 0, start a local WebSocket server on this port")
	pollInterval := fs.Duration("poll-interval", 1*time.Second, "polling interval for new ticks/bars")
	baseURL := fs.String("base-url", readBaseURL(), "Dukascopy API base URL")
	output := fs.String("output", "-", "output file path, - for stdout only")

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
