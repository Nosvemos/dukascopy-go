package csvout

import (
	"encoding/csv"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"dukascopy-data-downloader/internal/dukascopy"
)

const timestampLayout = time.RFC3339Nano

type Profile string

const (
	ProfileSimple Profile = "simple"
	ProfileFull   Profile = "full"
)

var simpleBarColumns = []string{"timestamp", "open", "high", "low", "close", "volume"}
var fullBarColumns = []string{"timestamp", "mid_open", "mid_high", "mid_low", "mid_close", "spread", "volume", "bid_open", "bid_high", "bid_low", "bid_close", "ask_open", "ask_high", "ask_low", "ask_close"}
var simpleTickColumns = []string{"timestamp", "bid", "ask"}
var fullTickColumns = []string{"timestamp", "bid", "ask", "bid_volume", "ask_volume"}

func BarColumnsForProfile(profile Profile) []string {
	switch profile {
	case ProfileSimple:
		return cloneColumns(simpleBarColumns)
	case ProfileFull:
		return cloneColumns(fullBarColumns)
	default:
		return nil
	}
}

func TickColumnsForProfile(profile Profile) []string {
	switch profile {
	case ProfileSimple:
		return cloneColumns(simpleTickColumns)
	case ProfileFull:
		return cloneColumns(fullTickColumns)
	default:
		return nil
	}
}

func ParseBarColumns(value string) ([]string, error) {
	return parseColumns(value, map[string]struct{}{
		"timestamp": {},
		"open":      {},
		"high":      {},
		"low":       {},
		"close":     {},
		"mid_open":  {},
		"mid_high":  {},
		"mid_low":   {},
		"mid_close": {},
		"spread":    {},
		"volume":    {},
		"bid_open":  {},
		"bid_high":  {},
		"bid_low":   {},
		"bid_close": {},
		"ask_open":  {},
		"ask_high":  {},
		"ask_low":   {},
		"ask_close": {},
	})
}

func ParseTickColumns(value string) ([]string, error) {
	return parseColumns(value, map[string]struct{}{
		"timestamp":  {},
		"bid":        {},
		"ask":        {},
		"bid_volume": {},
		"ask_volume": {},
	})
}

func BarColumnsNeedBidAsk(columns []string) bool {
	for _, column := range columns {
		if strings.HasPrefix(column, "bid_") || strings.HasPrefix(column, "ask_") || strings.HasPrefix(column, "mid_") || column == "spread" {
			return true
		}
	}
	return false
}

func WriteBars(outputPath string, instrument dukascopy.Instrument, columns []string, primaryBars []dukascopy.Bar, bidBars []dukascopy.Bar, askBars []dukascopy.Bar) error {
	if err := ensureParentDir(outputPath); err != nil {
		return err
	}

	file, err := os.Create(outputPath)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	if err := writer.Write(columns); err != nil {
		return err
	}

	if BarColumnsNeedBidAsk(columns) {
		rows, err := combineBarRows(bidBars, askBars)
		if err != nil {
			return err
		}

		for _, row := range rows {
			record := make([]string, 0, len(columns))
			for _, column := range columns {
				value, valueErr := formatBarColumn(column, instrument.PriceScale, row.Bid, row.Ask)
				if valueErr != nil {
					return valueErr
				}
				record = append(record, value)
			}
			if err := writer.Write(record); err != nil {
				return err
			}
		}

		return writer.Error()
	}

	for _, bar := range primaryBars {
		record := make([]string, 0, len(columns))
		for _, column := range columns {
			value, err := formatPrimaryBarColumn(column, instrument.PriceScale, bar)
			if err != nil {
				return err
			}
			record = append(record, value)
		}
		if err := writer.Write(record); err != nil {
			return err
		}
	}

	return writer.Error()
}

func WriteTicks(outputPath string, instrument dukascopy.Instrument, columns []string, ticks []dukascopy.Tick) error {
	if err := ensureParentDir(outputPath); err != nil {
		return err
	}

	file, err := os.Create(outputPath)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	if err := writer.Write(columns); err != nil {
		return err
	}

	for _, tick := range ticks {
		record := make([]string, 0, len(columns))
		for _, column := range columns {
			value, valueErr := formatTickColumn(column, instrument.PriceScale, tick)
			if valueErr != nil {
				return valueErr
			}
			record = append(record, value)
		}
		if err := writer.Write(record); err != nil {
			return err
		}
	}

	return writer.Error()
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

func formatPrimaryBarColumn(column string, scale int, bar dukascopy.Bar) (string, error) {
	switch column {
	case "timestamp":
		return bar.Time.UTC().Format(timestampLayout), nil
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

func formatBarColumn(column string, scale int, bid dukascopy.Bar, ask dukascopy.Bar) (string, error) {
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
		return bid.Time.UTC().Format(timestampLayout), nil
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

func formatTickColumn(column string, scale int, tick dukascopy.Tick) (string, error) {
	switch column {
	case "timestamp":
		return tick.Time.UTC().Format(timestampLayout), nil
	case "bid":
		return formatPrice(tick.Bid, scale), nil
	case "ask":
		return formatPrice(tick.Ask, scale), nil
	case "bid_volume":
		return formatVolume(tick.BidVolume), nil
	case "ask_volume":
		return formatVolume(tick.AskVolume), nil
	default:
		return "", fmt.Errorf("unsupported tick column %q", column)
	}
}

func parseColumns(value string, allowed map[string]struct{}) ([]string, error) {
	parts := strings.Split(value, ",")
	columns := make([]string, 0, len(parts))
	for _, part := range parts {
		column := strings.TrimSpace(strings.ToLower(part))
		if column == "" {
			continue
		}
		if _, ok := allowed[column]; !ok {
			return nil, fmt.Errorf("unsupported column %q", column)
		}
		columns = append(columns, column)
	}
	if len(columns) == 0 {
		return nil, fmt.Errorf("at least one column must be provided")
	}
	return columns, nil
}

func cloneColumns(columns []string) []string {
	cloned := make([]string, len(columns))
	copy(cloned, columns)
	return cloned
}

func ensureParentDir(outputPath string) error {
	dir := filepath.Dir(outputPath)
	if dir == "." || dir == "" {
		return nil
	}
	return os.MkdirAll(dir, 0o755)
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
