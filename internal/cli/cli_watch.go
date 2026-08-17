package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
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

func runWatch(args []string, stdout io.Writer, stderr io.Writer) error {
	fs := flag.NewFlagSet("watch", flag.ContinueOnError)
	fs.SetOutput(stdout)
	fs.Usage = func() {
		fmt.Fprintf(stdout, "%swatch:%s Monitor real-time prices & indicators and trigger webhooks/alerts\n\n", colorize(colorCyan), colorize(colorReset))
		fmt.Fprint(stdout, "Usage:\n  dukascopy-go watch [options]\n\nOptions:\n")
		fs.PrintDefaults()
		fmt.Fprint(stdout, "\nExamples:\n  dukascopy-go watch --symbol xauusd --condition \"price > 2500\" --webhook http://localhost:8080/alert\n  dukascopy-go watch --symbol eurusd --condition \"rsi < 30\" --timeframe m1 --webhook http://bot/signal --cooldown 5m\n")
	}

	symbol := fs.String("symbol", "", "instrument symbol such as eurusd or xauusd (required)")
	condition := fs.String("condition", "", "trigger condition: price > X, price < X, rsi < X, spread > X (required)")
	webhook := fs.String("webhook", "", "webhook HTTP URL to POST alert payload on trigger")
	timeframe := fs.String("timeframe", "m1", "evaluation timeframe: tick, m1, m5, h1")
	pollInterval := fs.Duration("poll-interval", 2*time.Second, "polling interval")
	cooldown := fs.Duration("cooldown", 1*time.Minute, "minimum delay between webhook alerts")
	once := fs.Bool("once", false, "exit immediately after first triggered alert")
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

	evaluator, err := parseWatchCondition(*condition)
	if err != nil {
		return fmt.Errorf("invalid condition: %w", err)
	}

	client, err := dukascopy.NewClient(*baseURL, 15*time.Second)
	if err != nil {
		return err
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	fmt.Fprintf(stderr, "%swatch%s monitoring %s with condition %q (poll every %s)...\n",
		colorize(colorCyan), colorize(colorReset), strings.ToUpper(*symbol), *condition, *pollInterval)

	var lastAlertTime time.Time
	httpClient := &http.Client{Timeout: 5 * time.Second}

	for {
		if err := ctx.Err(); err != nil {
			break
		}

		now := time.Now().UTC()
		req := dukascopy.DownloadRequest{
			Symbol:      *symbol,
			Granularity: dukascopy.NormalizeGranularity(dukascopy.Granularity(*timeframe)),
			Side:        dukascopy.PriceSideBid,
			From:        now.Add(-2 * time.Hour),
			To:          now.Add(time.Nanosecond),
		}

		res, err := client.Download(ctx, req)
		if err == nil && len(res.Bars) > 0 {
			lastBar := res.Bars[len(res.Bars)-1]
			triggered, currentVal, msg := evaluator.Evaluate(res.Bars)
			if triggered {
				if time.Since(lastAlertTime) >= *cooldown {
					lastAlertTime = time.Now()
					alert := watchAlertPayload{
						Timestamp:    lastBar.Time.UnixMilli(),
						Symbol:       strings.ToUpper(*symbol),
						Condition:    *condition,
						CurrentPrice: lastBar.Close,
						Bid:          lastBar.Close,
						Ask:          lastBar.Close,
						Message:      msg,
					}

					fmt.Fprintf(stdout, "%sALERT TRIGGERED%s [%s] %s (value: %.5f)\n",
						colorize(colorGreen), colorize(colorReset), lastBar.Time.Format(time.RFC3339), msg, currentVal)

					if strings.TrimSpace(*webhook) != "" {
						payloadBytes, _ := json.Marshal(alert)
						resp, postErr := httpClient.Post(*webhook, "application/json", bytes.NewReader(payloadBytes))
						if postErr != nil {
							fmt.Fprintf(stderr, "%swatch%s webhook error: %v\n", colorize(colorRed), colorize(colorReset), postErr)
						} else {
							_ = resp.Body.Close()
							fmt.Fprintf(stderr, "%swatch%s webhook delivered to %s (status: %d)\n",
								colorize(colorGreen), colorize(colorReset), *webhook, resp.StatusCode)
						}
					}

					if *once {
						return nil
					}
				}
			}
		}

		if err := sleepWithContext(ctx, *pollInterval); err != nil {
			break
		}
	}

	return nil
}

type conditionEvaluator struct {
	metric   string
	operator string
	target   float64
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

	return &conditionEvaluator{
		metric:   metric,
		operator: op,
		target:   target,
	}, nil
}

func (e *conditionEvaluator) Evaluate(bars []dukascopy.Bar) (bool, float64, string) {
	if len(bars) == 0 {
		return false, 0, ""
	}

	last := bars[len(bars)-1]
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
	case "rsi", "rsi_14", "rsi:14":
		closes := make([]float64, len(bars))
		for i, b := range bars {
			closes[i] = b.Close
		}
		rsiValues := features.ComputeRSI(closes, 14)
		currentVal = rsiValues[len(rsiValues)-1]
	default:
		currentVal = last.Close
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

	msg := fmt.Sprintf("%s %s %.5f (current: %.5f)", e.metric, e.operator, e.target, currentVal)
	return triggered, currentVal, msg
}
