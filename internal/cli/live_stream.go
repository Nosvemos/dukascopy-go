package cli

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/Nosvemos/dukascopy-go/pkg/dukascopy"
	"github.com/Nosvemos/dukascopy-go/pkg/streaming"

	_ "time/tzdata"
)

// LiveTick is the JSON envelope streamed to stdout / WebSocket clients.
type LiveTick struct {
	Timestamp      int64                            `json:"timestamp"` // Unix milliseconds
	Symbol         string                           `json:"symbol"`
	Bid            float64                          `json:"bid"`
	Ask            float64                          `json:"ask"`
	BidVolume      float64                          `json:"bid_volume,omitempty"`
	AskVolume      float64                          `json:"ask_volume,omitempty"`
	Microstructure *streaming.MicrostructureMetrics `json:"microstructure,omitempty"`
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

// defaultLiveHost is the interface the live WebSocket server binds to unless
// --host says otherwise.
const defaultLiveHost = "127.0.0.1"

var (
	liveStreamNow   = func() time.Time { return time.Now().UTC() }
	liveStreamSleep = sleepWithContext
)

// liveStreamDownloader is the subset of *dukascopy.Client the stream needs.
type liveStreamDownloader interface {
	Download(ctx context.Context, request dukascopy.DownloadRequest) (dukascopy.DownloadResult, error)
}

// runLiveStream — the main `live` command implementation
func runLiveStream(args []string, stdout io.Writer, stderr io.Writer) error {
	fs := flag.NewFlagSet("live", flag.ContinueOnError)
	fs.SetOutput(stdout)
	fs.Usage = func() {
		fmt.Fprintf(stdout, "%slive:%s Stream real-time ticks/bars to stdout, WebSocket, Redis, or NATS\n\n", colorize(colorCyan), colorize(colorReset))
		fmt.Fprint(stdout, "Usage:\n  dukascopy-go live [options]\n\nOptions:\n")
		fs.PrintDefaults()
		fmt.Fprint(stdout, "\nExamples:\n  dukascopy-go live --symbol eurusd --timeframe tick --format jsonl\n  dukascopy-go live --symbol eurusd --timeframe m1 --side bid --port 8080\n  dukascopy-go live --symbol eurusd --port 8080 --host 0.0.0.0 --allow-origin http://localhost:3000\n  dukascopy-go live --symbol eurusd --redis redis://localhost:6379/market_eurusd\n  dukascopy-go live --symbol eurusd --nats nats://localhost:4222/market.eurusd\n")
	}

	symbol := fs.String("symbol", "", "instrument symbol such as eurusd or xauusd (required)")
	timeframe := fs.String("timeframe", "tick", "tick, m1, m3, m5, m15, m30, h1, h4, d1")
	side := fs.String("side", "bid", "bid or ask (used for bar streaming)")
	format := fs.String("format", "jsonl", "output format: jsonl, csv, json")
	port := fs.Int("port", 0, "if > 0, start a local WebSocket server on this port")
	host := fs.String("host", defaultLiveHost, "interface the WebSocket server binds to (use 0.0.0.0 to accept remote clients)")
	allowOrigin := fs.String("allow-origin", "", "comma-separated browser origins allowed to open the WebSocket in addition to same-origin pages (* allows any)")
	pollInterval := fs.Duration("poll-interval", 1*time.Second, "polling interval for new ticks/bars")
	baseURL := fs.String("base-url", readBaseURL(), "Dukascopy API base URL")
	output := fs.String("output", "-", "output file path, - for stdout only")
	redisURL := fs.String("redis", "", "optional Redis URL to publish live ticks/bars (e.g. redis://localhost:6379/market_eurusd)")
	natsURL := fs.String("nats", "", "optional NATS URL to publish live ticks/bars (e.g. nats://localhost:4222/market.eurusd)")
	microstructure := fs.Bool("microstructure", false, "include order flow & microstructure metrics in live tick stream (OFI, effective spread, velocity)")

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
	bindHost := strings.TrimSpace(*host)
	if bindHost == "" {
		bindHost = defaultLiveHost
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
		health := func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_, _ = fmt.Fprintf(w, `{"status":"ok","symbol":%q,"timeframe":%q,"clients":%d}`,
				*symbol, *timeframe, hub.count())
		}
		server, ln, err := newLiveWSServer(hub, bindHost, *port, wsOptions{AllowedOrigins: splitCommaList(*allowOrigin)}, health)
		if err != nil {
			return err
		}
		wsServer = server
		go func() { _ = wsServer.Serve(ln) }()
		fmt.Fprintf(stderr, "%slive%s WebSocket server listening on ws://%s/stream\n",
			colorize(colorCyan), colorize(colorReset), ln.Addr().String())
	}
	defer func() {
		if wsServer != nil {
			shutCtx, shutCancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer shutCancel()
			_ = wsServer.Shutdown(shutCtx)
			// Shutdown does not touch hijacked connections.
			hub.closeAll()
		}
	}()

	var publishers []livePublisher
	if strings.TrimSpace(*redisURL) != "" {
		rp, err := streaming.NewRedisPublisher(*redisURL)
		if err != nil {
			return err
		}
		defer rp.Close()
		publishers = append(publishers, livePublisher{name: "redis", publish: rp.Publish})
		fmt.Fprintf(stderr, "%slive%s publishing to Redis: %s\n", colorize(colorCyan), colorize(colorReset), redactURL(*redisURL))
	}

	if strings.TrimSpace(*natsURL) != "" {
		np, err := streaming.NewNATSPublisher(*natsURL)
		if err != nil {
			return err
		}
		defer np.Close()
		publishers = append(publishers, livePublisher{name: "nats", publish: np.Publish})
		fmt.Fprintf(stderr, "%slive%s publishing to NATS: %s\n", colorize(colorCyan), colorize(colorReset), redactURL(*natsURL))
	}

	// Graceful shutdown context
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	client, err := dukascopy.NewClient(*baseURL, 30*time.Second)
	if err != nil {
		return err
	}

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

	streamer := &liveStreamer{
		client:         client,
		out:            out,
		stderr:         stderr,
		publishers:     publishers,
		symbol:         *symbol,
		instrumentName: strings.ToUpper(instrument.Name),
		timeframeLabel: strings.ToUpper(*timeframe),
		side:           strings.ToUpper(*side),
		granularity:    normalizedTimeframe,
		isTick:         isTick,
		format:         formatLower,
		microstructure: *microstructure,
		pollInterval:   *pollInterval,
		errLog:         newRateLimitedLogger(stderr, liveErrorLogInterval),
	}
	if *port > 0 {
		streamer.hub = hub
	}

	if err := streamer.writeHeader(); err != nil {
		cancel()
		fmt.Fprintf(stderr, "%slive%s output closed (%v), stopping\n", colorize(colorCyan), colorize(colorReset), err)
		return nil
	}
	streamer.run(ctx)
	cancel()

	fmt.Fprintf(stderr, "%slive%s stopped\n", colorize(colorCyan), colorize(colorReset))
	return nil
}

func splitCommaList(value string) []string {
	var items []string
	for _, item := range strings.Split(value, ",") {
		if item = strings.TrimSpace(item); item != "" {
			items = append(items, item)
		}
	}
	return items
}

// liveErrorLogInterval rate-limits repeated error messages of the same kind.
const liveErrorLogInterval = 30 * time.Second

type livePublisher struct {
	name    string
	publish func(ctx context.Context, payload []byte) error
}

// liveOutputError marks a failed write to stdout / the output file. The
// stream stops cleanly on it (e.g. the reading end of a pipe went away).
type liveOutputError struct{ err error }

func (e *liveOutputError) Error() string { return "write output: " + e.err.Error() }
func (e *liveOutputError) Unwrap() error { return e.err }

// liveStreamer holds the polling state of `live`. All of its state is used
// from the single polling goroutine, so it needs no locking.
type liveStreamer struct {
	client     liveStreamDownloader
	out        io.Writer
	stderr     io.Writer
	hub        *wsHub
	publishers []livePublisher
	errLog     *rateLimitedLogger

	symbol         string
	instrumentName string
	timeframeLabel string
	side           string
	granularity    dukascopy.Granularity
	isTick         bool
	format         string
	microstructure bool
	pollInterval   time.Duration

	lastTickTime time.Time
	lastBarTime  time.Time
	prevTick     *dukascopy.Tick
	failures     int
}

// run polls until ctx is cancelled or the output can no longer be written.
func (s *liveStreamer) run(ctx context.Context) {
	for {
		if ctx.Err() != nil {
			return
		}

		var err error
		if s.isTick {
			err = s.pollTicks(ctx, liveStreamNow())
		} else {
			err = s.pollBars(ctx, liveStreamNow())
		}

		wait := s.pollInterval
		if err != nil {
			var outErr *liveOutputError
			if errors.As(err, &outErr) {
				fmt.Fprintf(s.stderr, "%slive%s output closed (%v), stopping\n", colorize(colorCyan), colorize(colorReset), outErr.err)
				return
			}
			if ctx.Err() != nil {
				return
			}
			s.failures++
			wait = liveFailureBackoff(s.pollInterval, s.failures)
			s.errLog.Printf("download", "%slive%s download failed (%d in a row), retrying in %s: %v",
				colorize(colorYellow), colorize(colorReset), s.failures, wait, err)
		} else {
			s.failures = 0
		}

		if err := liveStreamSleep(ctx, wait); err != nil {
			return
		}
	}
}

func (s *liveStreamer) writeHeader() error {
	if s.format != "csv" {
		return nil
	}
	header := "timestamp,symbol,timeframe,side,open,high,low,close,volume"
	if s.isTick {
		header = "timestamp,symbol,bid,ask,bid_volume,ask_volume"
		if s.microstructure {
			header = "timestamp,symbol,bid,ask,bid_volume,ask_volume,spread,mid,ofi,eff_spread,amihud,velocity"
		}
	}
	if _, err := io.WriteString(s.out, header+"\n"); err != nil {
		return &liveOutputError{err: err}
	}
	return nil
}

// pollTicks requests ticks from the last emitted tick onwards (the current
// hour start on the first poll), so ticks published late for the previous
// hour are not skipped, and emits the ones after the last emitted tick.
func (s *liveStreamer) pollTicks(ctx context.Context, now time.Time) error {
	from := now.Truncate(time.Hour)
	if !s.lastTickTime.IsZero() {
		from = s.lastTickTime
	}
	result, err := s.client.Download(ctx, dukascopy.DownloadRequest{
		Symbol:      s.symbol,
		Granularity: dukascopy.GranularityTick,
		Side:        dukascopy.PriceSide(s.side),
		From:        from,
		To:          now.Add(time.Nanosecond),
	})
	if err != nil {
		return err
	}

	ticks := append([]dukascopy.Tick(nil), result.Ticks...)
	sort.SliceStable(ticks, func(i, j int) bool { return ticks[i].Time.Before(ticks[j].Time) })

	for _, tick := range ticks {
		if !tick.Time.After(s.lastTickTime) {
			continue
		}
		s.lastTickTime = tick.Time

		var micro *streaming.MicrostructureMetrics
		if s.microstructure {
			m := streaming.ComputeMicrostructure(tick, s.prevTick)
			micro = &m
		}
		currTick := tick
		s.prevTick = &currTick

		lt := LiveTick{
			Timestamp:      tick.Time.UnixMilli(),
			Symbol:         s.instrumentName,
			Bid:            tick.Bid,
			Ask:            tick.Ask,
			BidVolume:      tick.BidVolume,
			AskVolume:      tick.AskVolume,
			Microstructure: micro,
		}
		var payload []byte
		if s.format == "csv" {
			payload = []byte(formatLiveTickCSV(lt))
		} else {
			payload, _ = json.Marshal(lt)
		}
		if err := s.emit(ctx, payload); err != nil {
			return err
		}
	}
	return nil
}

// pollBars requests every completed bar after the last emitted one through
// the latest completed bar, so a failed poll or publication lag does not skip
// bars. On the first poll only the latest completed bar is requested.
func (s *liveStreamer) pollBars(ctx context.Context, now time.Time) error {
	upper, err := liveUpperInclusive(s.granularity, now)
	if err != nil {
		return err
	}
	from := upper
	if !s.lastBarTime.IsZero() {
		next, err := nextLiveBarStart(s.granularity, s.lastBarTime)
		if err != nil {
			return err
		}
		from = next
	}
	if from.After(upper) {
		return nil
	}

	result, err := s.client.Download(ctx, dukascopy.DownloadRequest{
		Symbol:      s.symbol,
		Granularity: s.granularity,
		Side:        dukascopy.PriceSide(s.side),
		From:        from,
		To:          upper.Add(time.Nanosecond),
	})
	if err != nil {
		return err
	}

	bars := append([]dukascopy.Bar(nil), result.Bars...)
	sort.SliceStable(bars, func(i, j int) bool { return bars[i].Time.Before(bars[j].Time) })

	for _, bar := range bars {
		if !bar.Time.After(s.lastBarTime) || bar.Time.After(upper) {
			continue
		}
		s.lastBarTime = bar.Time

		lb := LiveBar{
			Timestamp: bar.Time.UnixMilli(),
			Symbol:    s.instrumentName,
			Timeframe: s.timeframeLabel,
			Side:      s.side,
			Open:      bar.Open,
			High:      bar.High,
			Low:       bar.Low,
			Close:     bar.Close,
			Volume:    bar.Volume,
		}
		var payload []byte
		if s.format == "csv" {
			payload = []byte(formatLiveBarCSV(lb))
		} else {
			payload, _ = json.Marshal(lb)
		}
		if err := s.emit(ctx, payload); err != nil {
			return err
		}
	}
	return nil
}

// emit writes a line to stdout/file and broadcasts it to WebSocket clients and
// the Redis/NATS publishers. Only an output write failure is returned;
// publisher failures are logged (rate-limited) and streaming continues.
func (s *liveStreamer) emit(ctx context.Context, payload []byte) error {
	line := make([]byte, len(payload)+1)
	copy(line, payload)
	line[len(payload)] = '\n'
	if _, err := s.out.Write(line); err != nil {
		return &liveOutputError{err: err}
	}
	if s.hub != nil {
		s.hub.broadcast(line)
	}
	for _, publisher := range s.publishers {
		if err := publisher.publish(ctx, payload); err != nil && ctx.Err() == nil {
			s.errLog.Printf(publisher.name, "%slive%s %s publish failed: %v", colorize(colorYellow), colorize(colorReset), publisher.name, err)
		}
	}
	return nil
}

func formatLiveFloat(v float64) string {
	return strconv.FormatFloat(v, 'f', -1, 64)
}

func formatLiveTickCSV(lt LiveTick) string {
	fields := []string{
		strconv.FormatInt(lt.Timestamp, 10),
		lt.Symbol,
		formatLiveFloat(lt.Bid),
		formatLiveFloat(lt.Ask),
		formatLiveFloat(lt.BidVolume),
		formatLiveFloat(lt.AskVolume),
	}
	if m := lt.Microstructure; m != nil {
		fields = append(fields,
			formatLiveFloat(m.Spread),
			formatLiveFloat(m.MidPrice),
			formatLiveFloat(m.OrderFlowImbalance),
			formatLiveFloat(m.EffectiveSpread),
			formatLiveFloat(m.AmihudIlliquidity),
			formatLiveFloat(m.PriceVelocity),
		)
	}
	return strings.Join(fields, ",")
}

func formatLiveBarCSV(lb LiveBar) string {
	return strings.Join([]string{
		strconv.FormatInt(lb.Timestamp, 10),
		lb.Symbol,
		lb.Timeframe,
		lb.Side,
		formatLiveFloat(lb.Open),
		formatLiveFloat(lb.High),
		formatLiveFloat(lb.Low),
		formatLiveFloat(lb.Close),
		formatLiveFloat(lb.Volume),
	}, ",")
}

// nextLiveBarStart returns the start of the bar following the one starting
// at t for the given granularity.
func nextLiveBarStart(granularity dukascopy.Granularity, t time.Time) (time.Time, error) {
	t = t.UTC()
	switch dukascopy.NormalizeGranularity(granularity) {
	case dukascopy.GranularityM1:
		return t.Add(time.Minute), nil
	case dukascopy.GranularityM3:
		return t.Add(3 * time.Minute), nil
	case dukascopy.GranularityM5:
		return t.Add(5 * time.Minute), nil
	case dukascopy.GranularityM15:
		return t.Add(15 * time.Minute), nil
	case dukascopy.GranularityM30:
		return t.Add(30 * time.Minute), nil
	case dukascopy.GranularityH1:
		return t.Add(time.Hour), nil
	case dukascopy.GranularityH4:
		return t.Add(4 * time.Hour), nil
	case dukascopy.GranularityD1:
		return t.AddDate(0, 0, 1), nil
	case dukascopy.GranularityW1:
		return t.AddDate(0, 0, 7), nil
	case dukascopy.GranularityMN1:
		return t.AddDate(0, 1, 0), nil
	default:
		return time.Time{}, fmt.Errorf("unsupported live bar timeframe %q", granularity)
	}
}

// rateLimitedLogger prints at most one message per key per interval and
// reports how many were suppressed in between. It is used from the single
// polling goroutine only.
type rateLimitedLogger struct {
	w          io.Writer
	interval   time.Duration
	now        func() time.Time
	last       map[string]time.Time
	suppressed map[string]int
}

func newRateLimitedLogger(w io.Writer, interval time.Duration) *rateLimitedLogger {
	return &rateLimitedLogger{
		w:          w,
		interval:   interval,
		now:        time.Now,
		last:       make(map[string]time.Time),
		suppressed: make(map[string]int),
	}
}

func (l *rateLimitedLogger) Printf(key string, format string, args ...any) {
	if l == nil || l.w == nil {
		return
	}
	now := l.now()
	if last, ok := l.last[key]; ok && now.Sub(last) < l.interval {
		l.suppressed[key]++
		return
	}
	l.last[key] = now
	message := fmt.Sprintf(format, args...)
	if n := l.suppressed[key]; n > 0 {
		message += fmt.Sprintf(" (%d similar messages suppressed)", n)
		l.suppressed[key] = 0
	}
	fmt.Fprintln(l.w, message)
}
