package csvout

import (
	"fmt"
	"math"
	"strconv"
	"time"

	"github.com/Nosvemos/dukascopy-go/pkg/dukascopy"
)

func (c *Config) formatTime(t time.Time) string {
	loc := c.Location
	if loc == nil {
		loc = time.UTC
	}
	layout := c.TimestampFormat
	if layout == "" {
		layout = time.RFC3339Nano
	}
	return t.In(loc).Format(layout)
}

func formatTime(t time.Time) string {
	return DefaultConfig().formatTime(t)
}

func (c *Config) formatPrimaryBarColumn(column string, scale int, bar dukascopy.Bar) (string, error) {
	switch column {
	case "timestamp":
		return c.formatTime(bar.Time), nil
	case "open":
		return formatPrice(bar.Open, scale), nil
	case "high":
		return formatPrice(bar.High, scale), nil
	case "low":
		return formatPrice(bar.Low, scale), nil
	case "close":
		return formatPrice(bar.Close, scale), nil
	case "mid_open":
		return formatPrice(bar.Open, scale), nil
	case "mid_high":
		return formatPrice(bar.High, scale), nil
	case "mid_low":
		return formatPrice(bar.Low, scale), nil
	case "mid_close":
		return formatPrice(bar.Close, scale), nil
	case "volume":
		return formatVolume(bar.Volume), nil
	default:
		return "", fmt.Errorf("column %q requires bid/ask data or is unsupported for simple bars", column)
	}
}

func formatPrimaryBarColumn(column string, scale int, bar dukascopy.Bar) (string, error) {
	return DefaultConfig().formatPrimaryBarColumn(column, scale, bar)
}

func (c *Config) formatBarColumn(column string, scale int, bid dukascopy.Bar, ask dukascopy.Bar) (string, error) {
	roundedBidOpen := roundToScale(bid.Open, scale)
	roundedBidHigh := roundToScale(bid.High, scale)
	roundedBidLow := roundToScale(bid.Low, scale)
	roundedBidClose := roundToScale(bid.Close, scale)
	roundedAskOpen := roundToScale(ask.Open, scale)
	roundedAskHigh := roundToScale(ask.High, scale)
	roundedAskLow := roundToScale(ask.Low, scale)
	roundedAskClose := roundToScale(ask.Close, scale)

	switch column {
	case "timestamp":
		return c.formatTime(bid.Time), nil
	case "open":
		return formatMidPrice(midpoint(roundedBidOpen, roundedAskOpen), scale), nil
	case "high":
		return formatMidPrice(midpoint(roundedBidHigh, roundedAskHigh), scale), nil
	case "low":
		return formatMidPrice(midpoint(roundedBidLow, roundedAskLow), scale), nil
	case "close":
		return formatMidPrice(midpoint(roundedBidClose, roundedAskClose), scale), nil
	case "mid_open":
		return formatMidPrice(midpoint(roundedBidOpen, roundedAskOpen), scale), nil
	case "mid_high":
		return formatMidPrice(midpoint(roundedBidHigh, roundedAskHigh), scale), nil
	case "mid_low":
		return formatMidPrice(midpoint(roundedBidLow, roundedAskLow), scale), nil
	case "mid_close":
		return formatMidPrice(midpoint(roundedBidClose, roundedAskClose), scale), nil
	case "spread":
		return formatPrice(roundedAskClose-roundedBidClose, scale), nil
	case "volume":
		return formatVolume(bid.Volume), nil
	case "bid_open":
		return formatPrice(roundedBidOpen, scale), nil
	case "bid_high":
		return formatPrice(roundedBidHigh, scale), nil
	case "bid_low":
		return formatPrice(roundedBidLow, scale), nil
	case "bid_close":
		return formatPrice(roundedBidClose, scale), nil
	case "ask_open":
		return formatPrice(roundedAskOpen, scale), nil
	case "ask_high":
		return formatPrice(roundedAskHigh, scale), nil
	case "ask_low":
		return formatPrice(roundedAskLow, scale), nil
	case "ask_close":
		return formatPrice(roundedAskClose, scale), nil
	default:
		return "", fmt.Errorf("unsupported bar column %q", column)
	}
}

func formatBarColumn(column string, scale int, bid dukascopy.Bar, ask dukascopy.Bar) (string, error) {
	return DefaultConfig().formatBarColumn(column, scale, bid, ask)
}

func (c *Config) formatTickColumn(column string, scale int, tick dukascopy.Tick) (string, error) {
	switch column {
	case "timestamp":
		return c.formatTime(tick.Time), nil
	case "bid":
		return formatPrice(tick.Bid, scale), nil
	case "ask":
		return formatPrice(tick.Ask, scale), nil
	case "spread":
		return formatPrice(tick.Ask-tick.Bid, scale), nil
	case "bid_volume":
		return formatVolume(tick.BidVolume), nil
	case "ask_volume":
		return formatVolume(tick.AskVolume), nil
	default:
		return "", fmt.Errorf("unsupported tick column %q", column)
	}
}

func formatTickColumn(column string, scale int, tick dukascopy.Tick) (string, error) {
	return DefaultConfig().formatTickColumn(column, scale, tick)
}

func formatPrice(value float64, scale int) string {
	if scale <= 0 {
		return strconv.FormatFloat(value, 'f', -1, 64)
	}
	return strconv.FormatFloat(value, 'f', scale, 64)
}

func formatMidPrice(value float64, scale int) string {
	precision := scale + 1
	if precision < 0 {
		precision = -1
	}
	factor := math.Pow10(precision)
	rounded := math.Round(value*factor) / factor
	return strconv.FormatFloat(rounded, 'f', -1, 64)
}

func formatVolume(value float64) string {
	return strconv.FormatFloat(value, 'f', -1, 64)
}

func midpoint(a float64, b float64) float64 {
	return (a + b) / 2
}

func roundToScale(value float64, scale int) float64 {
	if scale < 0 {
		return value
	}
	factor := math.Pow10(scale)
	return math.Round(value*factor) / factor
}

type combinedBarRow struct {
	Time time.Time
	Bid  dukascopy.Bar
	Ask  dukascopy.Bar
}

func combineBarRows(bidBars []dukascopy.Bar, askBars []dukascopy.Bar) ([]combinedBarRow, error) {
	if len(bidBars) != len(askBars) {
		return nil, fmt.Errorf("bid/ask bar length mismatch: %d vs %d", len(bidBars), len(askBars))
	}

	rows := make([]combinedBarRow, 0, len(bidBars))
	for index := range bidBars {
		if !bidBars[index].Time.Equal(askBars[index].Time) {
			return nil, fmt.Errorf("bid/ask timestamp mismatch at row %d: %s vs %s", index, bidBars[index].Time.UTC().Format(timestampLayout), askBars[index].Time.UTC().Format(timestampLayout))
		}
		rows = append(rows, combinedBarRow{
			Time: bidBars[index].Time,
			Bid:  bidBars[index],
			Ask:  askBars[index],
		})
	}

	return rows, nil
}
