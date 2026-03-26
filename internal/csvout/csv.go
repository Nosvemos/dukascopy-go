package csvout

import (
	"encoding/csv"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"dukascopy-data-downloader/internal/dukascopy"
)

const timestampLayout = time.RFC3339Nano

func WriteBars(outputPath string, instrument dukascopy.Instrument, bars []dukascopy.Bar) error {
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

	if err := writer.Write([]string{"instrument", "time", "open", "high", "low", "close", "volume"}); err != nil {
		return err
	}

	for _, bar := range bars {
		row := []string{
			instrument.Code,
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

	return writer.Error()
}

func WriteTicks(outputPath string, instrument dukascopy.Instrument, ticks []dukascopy.Tick) error {
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

	if err := writer.Write([]string{"instrument", "time", "ask", "bid", "ask_volume", "bid_volume"}); err != nil {
		return err
	}

	for _, tick := range ticks {
		row := []string{
			instrument.Code,
			tick.Time.UTC().Format(timestampLayout),
			formatPrice(tick.Ask, instrument.PriceScale),
			formatPrice(tick.Bid, instrument.PriceScale),
			formatVolume(tick.AskVolume),
			formatVolume(tick.BidVolume),
		}
		if err := writer.Write(row); err != nil {
			return err
		}
	}

	return writer.Error()
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
