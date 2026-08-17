package csvout

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/Nosvemos/dukascopy-go/pkg/dukascopy"
	"github.com/Nosvemos/dukascopy-go/pkg/features"
)

func TestWriteBarsWithFeaturesAllFormats(t *testing.T) {
	now := time.Now().UTC()
	bars := make([]dukascopy.Bar, 30)
	for i := 0; i < 30; i++ {
		bars[i] = dukascopy.Bar{
			Time:   now.Add(time.Duration(i) * time.Minute),
			Open:   1.0800 + float64(i)*0.0005,
			High:   1.0810 + float64(i)*0.0005,
			Low:    1.0790 + float64(i)*0.0005,
			Close:  1.0805 + float64(i)*0.0005,
			Volume: 500,
		}
	}

	specs, err := features.ParseFeatureSpecs("returns:log,rsi:14,ema:10,vwap")
	if err != nil {
		t.Fatalf("ParseFeatureSpecs: %v", err)
	}

	featNames, featRows, err := features.ComputeBarFeatures(bars, specs)
	if err != nil {
		t.Fatalf("ComputeBarFeatures: %v", err)
	}

	instrument := dukascopy.Instrument{
		Name:       "EURUSD",
		PriceScale: 5,
	}
	baseCols := []string{"timestamp", "open", "high", "low", "close", "volume"}

	dir := t.TempDir()

	// 1. CSV
	csvPath := filepath.Join(dir, "out.csv")
	if err := WriteBarsWithFeatures(csvPath, instrument, baseCols, bars, featNames, featRows); err != nil {
		t.Fatalf("WriteBarsWithFeatures CSV failed: %v", err)
	}
	readBars, header, err := ReadBarsFromCSV(csvPath)
	if err != nil {
		t.Fatalf("ReadBarsFromCSV failed: %v", err)
	}
	if len(readBars) != 30 {
		t.Errorf("expected 30 bars, got %d", len(readBars))
	}
	if len(header) != len(baseCols)+len(featNames) {
		t.Errorf("expected %d headers, got %d", len(baseCols)+len(featNames), len(header))
	}

	// 2. Parquet
	parquetPath := filepath.Join(dir, "out.parquet")
	if err := WriteBarsWithFeatures(parquetPath, instrument, baseCols, bars, featNames, featRows); err != nil {
		t.Fatalf("WriteBarsWithFeatures Parquet failed: %v", err)
	}

	// 3. Arrow
	arrowPath := filepath.Join(dir, "out.arrow")
	if err := WriteBarsWithFeatures(arrowPath, instrument, baseCols, bars, featNames, featRows); err != nil {
		t.Fatalf("WriteBarsWithFeatures Arrow failed: %v", err)
	}

	// 4. JSONL
	jsonlPath := filepath.Join(dir, "out.jsonl")
	if err := WriteBarsWithFeatures(jsonlPath, instrument, baseCols, bars, featNames, featRows); err != nil {
		t.Fatalf("WriteBarsWithFeatures JSONL failed: %v", err)
	}
}
