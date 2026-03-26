package csvout

import (
	"encoding/csv"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"dukascopy-data-downloader/internal/dukascopy"
)

const timestampLayout = time.RFC3339Nano

type Profile string

const (
	ProfileSimple Profile = "simple"
	ProfileFull   Profile = "full"
)

func WriteBars(outputPath string, instrument dukascopy.Instrument, profile Profile, bars []dukascopy.Bar, bidBars []dukascopy.Bar, askBars []dukascopy.Bar) error {
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

	switch profile {
	case ProfileSimple:
		if err := writer.Write([]string{"timestamp", "open", "high", "low", "close", "volume"}); err != nil {
			return err
		}

		for _, bar := range bars {
			row := []string{
				bar.Time.UTC().Format(timestampLayout),
				formatPrice(bar.Open, instrument.PriceScale),
				formatPrice(bar.High, instrument.PriceScale),
				formatPrice(bar.Low, instrument.PriceScale),
				formatPrice(bar.Close, instrument.PriceScale),
				formatVolume(bar.Volume),
			}
			if err := writer.Write(row); err != nil {
				return err
			}
		}
	case ProfileFull:
		if err := writer.Write([]string{
			"timestamp",
			"open",
			"high",
			"low",
			"close",
			"volume",
			"bid_open",
			"bid_high",
			"bid_low",
			"bid_close",
			"ask_open",
			"ask_high",
			"ask_low",
			"ask_close",
		}); err != nil {
			return err
		}

		rows, err := combineBarRows(bidBars, askBars)
		if err != nil {
			return err
		}

		for _, row := range rows {
			record := []string{
				row.Time.UTC().Format(timestampLayout),
				formatPrice(midpoint(row.Bid.Open, row.Ask.Open), instrument.PriceScale),
				formatPrice(midpoint(row.Bid.High, row.Ask.High), instrument.PriceScale),
				formatPrice(midpoint(row.Bid.Low, row.Ask.Low), instrument.PriceScale),
				formatPrice(midpoint(row.Bid.Close, row.Ask.Close), instrument.PriceScale),
				formatVolume(row.Bid.Volume),
				formatPrice(row.Bid.Open, instrument.PriceScale),
				formatPrice(row.Bid.High, instrument.PriceScale),
				formatPrice(row.Bid.Low, instrument.PriceScale),
				formatPrice(row.Bid.Close, instrument.PriceScale),
				formatPrice(row.Ask.Open, instrument.PriceScale),
				formatPrice(row.Ask.High, instrument.PriceScale),
				formatPrice(row.Ask.Low, instrument.PriceScale),
				formatPrice(row.Ask.Close, instrument.PriceScale),
			}
			if err := writer.Write(record); err != nil {
				return err
			}
		}
	default:
		return fmt.Errorf("unsupported csv profile %q", profile)
	}

	return writer.Error()
}

func WriteTicks(outputPath string, instrument dukascopy.Instrument, profile Profile, ticks []dukascopy.Tick) error {
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

	switch profile {
	case ProfileSimple:
		if err := writer.Write([]string{"timestamp", "bid", "ask"}); err != nil {
			return err
		}

		for _, tick := range ticks {
			row := []string{
				tick.Time.UTC().Format(timestampLayout),
				formatPrice(tick.Bid, instrument.PriceScale),
				formatPrice(tick.Ask, instrument.PriceScale),
			}
			if err := writer.Write(row); err != nil {
				return err
			}
		}
	case ProfileFull:
		if err := writer.Write([]string{"timestamp", "bid", "ask", "bid_volume", "ask_volume"}); err != nil {
			return err
		}

		for _, tick := range ticks {
			row := []string{
				tick.Time.UTC().Format(timestampLayout),
				formatPrice(tick.Bid, instrument.PriceScale),
				formatPrice(tick.Ask, instrument.PriceScale),
				formatVolume(tick.BidVolume),
				formatVolume(tick.AskVolume),
			}
			if err := writer.Write(row); err != nil {
				return err
			}
		}
	default:
		return fmt.Errorf("unsupported csv profile %q", profile)
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

func formatVolume(value float64) string {
	return strconv.FormatFloat(value, 'f', -1, 64)
}

func midpoint(a float64, b float64) float64 {
	return (a + b) / 2
}
