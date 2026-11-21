package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/Nosvemos/dukascopy-go/pkg/dukascopy"
	"github.com/Nosvemos/dukascopy-go/pkg/features"
)

type watchAlertPayload struct {
	Timestamp    int64   `json:"timestamp"`
	Symbol       string  `json:"symbol"`
	Condition    string  `json:"condition"`
	CurrentPrice float64 `json:"current_price"`
	Bid          float64 `json:"bid"`
	Ask          float64 `json:"ask"`
	Message      string  `json:"message"`
}

// watchSample is one evaluated point: a bid bar (bar timeframes) or a tick.
// For ticks Open/High/Low/Close all hold the bid. Ask is set when HasAsk.
type watchSample struct {
	Time   time.Time
	Open   float64
	High   float64
	Low    float64
	Close  float64
	Volume float64
	Bid    float64
	Ask    float64
	HasAsk bool
}

func samplesFromBars(bars []dukascopy.Bar) []watchSample {
	samples := make([]watchSample, len(bars))
	for i, bar := range bars {
		samples[i] = watchSample{Time: bar.Time, Open: bar.Open, High: bar.High, Low: bar.Low, Close: bar.Close, Volume: bar.Volume, Bid: bar.Close}
	}
	return samples
}

func samplesFromTicks(ticks []dukascopy.Tick) []watchSample {
	samples := make([]watchSample, len(ticks))
	for i, tick := range ticks {
		samples[i] = watchSample{Time: tick.Time, Open: tick.Bid, High: tick.Bid, Low: tick.Bid, Close: tick.Bid, Bid: tick.Bid, Ask: tick.Ask, HasAsk: true}
	}
	return samples
}

// watchFetcher downloads the samples in [from, to).
type watchFetcher func(ctx context.Context, from time.Time, to time.Time) ([]watchSample, error)

type watchOptions struct {
	symbol       string
	condition    string
	webhook      string
	granularity  dukascopy.Granularity
	pollInterval time.Duration
	cooldown     time.Duration
	once         bool
	maxFailures  int
}

func runWatch(args []string, stdout io.Writer, stderr io.Writer) error {
	fs := flag.NewFlagSet("watch", flag.ContinueOnError)
	fs.SetOutput(stdout)
	fs.Usage = func() {
		fmt.Fprintf(stdout, "%swatch:%s Monitor real-time prices & indicators and trigger webhooks/alerts\n\n", colorize(colorCyan), colorize(colorReset))
		fmt.Fprint(stdout, "Usage:\n  dukascopy-go watch [options]\n\nOptions:\n")
		fs.PrintDefaults()
		fmt.Fprint(stdout, "\nMetrics: price/close, open, high, low, volume, bid, ask, spread (ask - bid), rsi (period 14) or rsi:N.\nWith --timeframe tick only price/close, bid, ask, spread and rsi are available (computed from tick bids).\n")
		fmt.Fprint(stdout, "\nExamples:\n  dukascopy-go watch --symbol xauusd --condition \"price > 2500\" --webhook http://localhost:8080/alert\n  dukascopy-go watch --symbol eurusd --condition \"rsi < 30\" --timeframe m1 --webhook http://bot/signal --cooldown 5m\n  dukascopy-go watch --symbol eurusd --condition \"spread > 0.0002\" --timeframe tick\n")
	}

	symbol := fs.String("symbol", "", "instrument symbol such as eurusd or xauusd (required)")
	condition := fs.String("condition", "", "trigger condition: <metric> <op> <number>, e.g. price > 2500, rsi < 30, rsi:21 > 70, spread > 0.0002 (required)")
	webhook := fs.String("webhook", "", "webhook HTTP URL to POST alert payload on trigger")
	timeframe := fs.String("timeframe", "m1", "evaluation timeframe: tick, m1, m3, m5, m15, m30, h1, h4, d1, w1, mn1")
	pollInterval := fs.Duration("poll-interval", 2*time.Second, "polling interval (must be > 0)")
	cooldown := fs.Duration("cooldown", 1*time.Minute, "minimum delay between webhook alerts")
	once := fs.Bool("once", false, "exit immediately after first triggered alert")
	maxFailures := fs.Int("max-failures", 5, "stop after this many consecutive failed downloads")
	baseURL := fs.String("base-url", readBaseURL(), "Dukascopy API base URL")

	if err := fs.Parse(args); err != nil {
		return err
	}

	if strings.TrimSpace(*symbol) == "" {
		return errors.New("--symbol is required")
	}
	if strings.TrimSpace(*condition) == "" {
		return errors.New("--condition is required (e.g. \"price > 2500\" or \"rsi < 30\")")
	}
	if *pollInterval <= 0 {
		return errors.New("--poll-interval must be greater than 0")
	}
	if *cooldown < 0 {
		return errors.New("--cooldown must be 0 or greater")
	}
	if *maxFailures < 1 {
		return errors.New("--max-failures must be at least 1")
	}
	granularity, err := validateTimeframe(*timeframe)
	if err != nil {
		return err
	}

	evaluator, err := parseWatchCondition(*condition)
	if err != nil {
		return fmt.Errorf("invalid condition: %w", err)
	}
	if err := evaluator.validateFor(granularity); err != nil {
		return fmt.Errorf("invalid condition: %w", err)
	}

	client, err := dukascopy.NewClient(*baseURL, 15*time.Second)
	if err != nil {
		return err
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	opts := watchOptions{
		symbol:       *symbol,
		condition:    *condition,
		webhook:      *webhook,
		granularity:  granularity,
		pollInterval: *pollInterval,
		cooldown:     *cooldown,
		once:         *once,
		maxFailures:  *maxFailures,
	}
	return runWatchLoop(ctx, opts, evaluator, newWatchFetcher(client, opts.symbol, granularity, evaluator.needsAsk()), stdout, stderr)
}

// newWatchFetcher downloads bid bars (plus ask bars when the condition needs
// them) or ticks.
func newWatchFetcher(client *dukascopy.Client, symbol string, granularity dukascopy.Granularity, needsAsk bool) watchFetcher {
	return func(ctx context.Context, from time.Time, to time.Time) ([]watchSample, error) {
		request := dukascopy.DownloadRequest{Symbol: symbol, Granularity: granularity, Side: dukascopy.PriceSideBid, From: from, To: to}
		result, err := client.Download(ctx, request)
		if err != nil {
			return nil, err
		}
		if granularity == dukascopy.GranularityTick {
			return samplesFromTicks(result.Ticks), nil
		}
		samples := samplesFromBars(result.Bars)
		if !needsAsk || len(samples) == 0 {
			return samples, nil
		}
		_, askBars, err := client.DownloadBarsForSide(ctx, request, dukascopy.PriceSideAsk)
		if err != nil {
			return nil, err
		}
		askByTime := make(map[time.Time]float64, len(askBars))
		for _, bar := range askBars {
			askByTime[bar.Time.UTC()] = bar.Close
		}
		joined := samples[:0]
		for _, sample := range samples {
			if ask, ok := askByTime[sample.Time.UTC()]; ok {
				sample.Ask = ask
				sample.HasAsk = true
				joined = append(joined, sample)
			}
		}
		return joined, nil
	}
}

const (
	watchMinLookback = 2 * time.Hour
	watchMaxLookback = 400 * 24 * time.Hour
)

// initialWatchLookback is the first history window: enough bars for the
// metric (with slack), at least two hours. It widens automatically while the
// window holds too few bars (weekends, holidays).
func initialWatchLookback(granularity dukascopy.Granularity, need int) time.Duration {
	if granularity == dukascopy.GranularityTick {
		return time.Hour
	}
	interval := timeframeInterval(granularity)
	if granularity == dukascopy.GranularityMN1 {
		interval = 31 * 24 * time.Hour
	}
	lookback := 2 * interval * time.Duration(need+1)
	if lookback < watchMinLookback {
		lookback = watchMinLookback
	}
	if lookback > watchMaxLookback {
		lookback = watchMaxLookback
	}
	return lookback
}

// runWatchLoop polls fetch every opts.pollInterval and alerts when the
// condition holds. Download errors are reported on stderr; after
// opts.maxFailures consecutive failures it gives up with an error. While an
// indicator is still warming up (NaN) the poll is skipped, a note is printed
// once, and the history window is widened until enough bars arrive.
func runWatchLoop(ctx context.Context, opts watchOptions, evaluator *conditionEvaluator, fetch watchFetcher, stdout io.Writer, stderr io.Writer) error {
	fmt.Fprintf(stderr, "%swatch%s monitoring %s with condition %q (poll every %s)...\n",
		colorize(colorCyan), colorize(colorReset), strings.ToUpper(opts.symbol), opts.condition, opts.pollInterval)

	var lastAlertTime time.Time
	httpClient := &http.Client{Timeout: 5 * time.Second}
	need := evaluator.requiredSamples()
	lookback := initialWatchLookback(opts.granularity, need)
	failures := 0
	warmupNoted := false

	for {
		if ctx.Err() != nil {
			return nil
		}

		now := time.Now().UTC()
		samples, err := fetch(ctx, now.Add(-lookback), now.Add(time.Nanosecond))
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			failures++
			fmt.Fprintf(stderr, "%swatch%s download failed (%d/%d): %v\n", colorize(colorRed), colorize(colorReset), failures, opts.maxFailures, err)
			if failures >= opts.maxFailures {
				return fmt.Errorf("giving up after %d consecutive download failures: %w", failures, err)
			}
		} else {
			failures = 0
			result := evaluator.Evaluate(samples)
			if !result.ready {
				if !warmupNoted {
					fmt.Fprintf(stderr, "%swatch%s warming up: %s needs %d %s, have %d; waiting for more history\n",
						colorize(colorYellow), colorize(colorReset), evaluator.metric, need, sampleLabel(opts.granularity), len(samples))
					warmupNoted = true
				}
				if len(samples) < need && lookback < watchMaxLookback {
					lookback *= 2
					if lookback > watchMaxLookback {
						lookback = watchMaxLookback
					}
				}
			} else if result.triggered && time.Since(lastAlertTime) >= opts.cooldown {
				lastAlertTime = time.Now()
				last := samples[len(samples)-1]
				ask := last.Close
				if last.HasAsk {
					ask = last.Ask
				}
				alert := watchAlertPayload{
					Timestamp:    last.Time.UnixMilli(),
					Symbol:       strings.ToUpper(opts.symbol),
					Condition:    opts.condition,
					CurrentPrice: last.Close,
					Bid:          last.Bid,
					Ask:          ask,
					Message:      result.message,
				}

				fmt.Fprintf(stdout, "%sALERT TRIGGERED%s [%s] %s (value: %.5f)\n",
					colorize(colorGreen), colorize(colorReset), last.Time.Format(time.RFC3339), result.message, result.value)

				if strings.TrimSpace(opts.webhook) != "" {
					payloadBytes, _ := json.Marshal(alert)
					resp, postErr := httpClient.Post(opts.webhook, "application/json", bytes.NewReader(payloadBytes))
					if postErr != nil {
						fmt.Fprintf(stderr, "%swatch%s webhook error: %v\n", colorize(colorRed), colorize(colorReset), postErr)
					} else {
						_ = resp.Body.Close()
						fmt.Fprintf(stderr, "%swatch%s webhook delivered to %s (status: %d)\n",
							colorize(colorGreen), colorize(colorReset), opts.webhook, resp.StatusCode)
					}
				}

				if opts.once {
					return nil
				}
			}
		}

		if err := sleepWithContext(ctx, opts.pollInterval); err != nil {
			return nil
		}
	}
}

func sampleLabel(granularity dukascopy.Granularity) string {
	if granularity == dukascopy.GranularityTick {
		return "ticks"
	}
	return "bars"
}

type conditionEvaluator struct {
	metric    string
	operator  string
	target    float64
	rsiPeriod int
}

// watchMetrics are the supported condition metrics (rsi also accepts
// rsi_N / rsi:N).
var watchMetrics = map[string]bool{
	"price": true, "close": true, "open": true, "high": true, "low": true,
	"volume": true, "bid": true, "ask": true, "spread": true, "rsi": true,
}

func parseWatchCondition(cond string) (*conditionEvaluator, error) {
	cond = strings.TrimSpace(cond)
	var op string
	switch {
	case strings.Contains(cond, ">="):
		op = ">="
	case strings.Contains(cond, "<="):
		op = "<="
	case strings.Contains(cond, ">"):
		op = ">"
	case strings.Contains(cond, "<"):
		op = "<"
	case strings.Contains(cond, "=="):
		op = "=="
	default:
		return nil, fmt.Errorf("no valid comparison operator (>, <, >=, <=, ==) in %q", cond)
	}

	parts := strings.Split(cond, op)
	if len(parts) != 2 {
		return nil, fmt.Errorf("invalid condition format in %q", cond)
	}

	metric := strings.ToLower(strings.TrimSpace(parts[0]))
	valStr := strings.TrimSpace(parts[1])
	target, err := strconv.ParseFloat(valStr, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid target number %q in %q: %w", valStr, cond, err)
	}

	evaluator := &conditionEvaluator{metric: metric, operator: op, target: target}
	if strings.HasPrefix(metric, "rsi") {
		period := 14
		if rest := strings.TrimLeft(strings.TrimPrefix(metric, "rsi"), ":_"); rest != "" {
			if rest == strings.TrimPrefix(metric, "rsi") {
				return nil, fmt.Errorf("unknown metric %q", metric)
			}
			period, err = strconv.Atoi(rest)
			if err != nil || period < 1 {
				return nil, fmt.Errorf("invalid rsi period in %q", metric)
			}
		}
		evaluator.metric = "rsi"
		evaluator.rsiPeriod = period
		return evaluator, nil
	}
	if !watchMetrics[metric] {
		return nil, fmt.Errorf("unknown metric %q (supported: price, close, open, high, low, volume, bid, ask, spread, rsi, rsi:N)", metric)
	}
	return evaluator, nil
}

// validateFor rejects metrics that the timeframe cannot provide.
func (e *conditionEvaluator) validateFor(granularity dukascopy.Granularity) error {
	if granularity != dukascopy.GranularityTick {
		return nil
	}
	switch e.metric {
	case "open", "high", "low", "volume":
		return fmt.Errorf("metric %q is not available with --timeframe tick (use price, bid, ask, spread or rsi)", e.metric)
	}
	return nil
}

func (e *conditionEvaluator) needsAsk() bool {
	return e.metric == "spread" || e.metric == "ask"
}

// requiredSamples is how many samples the metric needs to produce a value.
func (e *conditionEvaluator) requiredSamples() int {
	if e.metric == "rsi" {
		return e.rsiPeriod + 1
	}
	return 1
}

// watchEvaluation is the outcome of one evaluation. ready is false when the
// metric has no value yet (no samples, or an indicator still warming up).
type watchEvaluation struct {
	ready     bool
	triggered bool
	value     float64
	message   string
}

func (e *conditionEvaluator) Evaluate(samples []watchSample) watchEvaluation {
	if len(samples) == 0 {
		return watchEvaluation{}
	}

	last := samples[len(samples)-1]
	var currentVal float64

	switch e.metric {
	case "price", "close":
		currentVal = last.Close
	case "open":
		currentVal = last.Open
	case "high":
		currentVal = last.High
	case "low":
		currentVal = last.Low
	case "volume":
		currentVal = last.Volume
	case "bid":
		currentVal = last.Bid
	case "ask":
		if !last.HasAsk {
			return watchEvaluation{}
		}
		currentVal = last.Ask
	case "spread":
		if !last.HasAsk {
			return watchEvaluation{}
		}
		currentVal = last.Ask - last.Bid
	case "rsi":
		closes := make([]float64, len(samples))
		for i, sample := range samples {
			closes[i] = sample.Close
		}
		rsiValues := features.ComputeRSI(closes, e.rsiPeriod)
		if len(rsiValues) == 0 {
			return watchEvaluation{}
		}
		currentVal = rsiValues[len(rsiValues)-1]
	default:
		return watchEvaluation{}
	}
	if math.IsNaN(currentVal) || math.IsInf(currentVal, 0) {
		return watchEvaluation{}
	}

	triggered := false
	switch e.operator {
	case ">":
		triggered = currentVal > e.target
	case "<":
		triggered = currentVal < e.target
	case ">=":
		triggered = currentVal >= e.target
	case "<=":
		triggered = currentVal <= e.target
	case "==":
		triggered = currentVal == e.target
	}

	label := e.metric
	if e.metric == "rsi" {
		label = fmt.Sprintf("rsi(%d)", e.rsiPeriod)
	}
	msg := fmt.Sprintf("%s %s %.5f (current: %.5f)", label, e.operator, e.target, currentVal)
	return watchEvaluation{ready: true, triggered: triggered, value: currentVal, message: msg}
}
