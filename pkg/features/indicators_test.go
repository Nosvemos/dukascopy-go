package features

import (
	"math"
	"testing"
	"time"

	"github.com/Nosvemos/dukascopy-go/pkg/dukascopy"
)

func TestParseFeatureSpecs(t *testing.T) {
	raw := "returns:log,returns:pct,sma:20,ema:50,rsi:14,atr:14,vwap,obv,volatility:garman-klass:14,volatility:parkinson:14,macd:12:26:9,bollinger:20:2.0"
	specs, err := ParseFeatureSpecs(raw)
	if err != nil {
		t.Fatalf("ParseFeatureSpecs failed: %v", err)
	}

	expectedNames := []string{
		"log_returns",
		"pct_returns",
		"sma_20",
		"ema_50",
		"rsi_14",
		"atr_14",
		"vwap",
		"obv",
		"vol_garman_klass_14",
		"vol_parkinson_14",
		"macd_12_26",
		"macd_signal_9",
		"macd_hist_12_26_9",
		"bb_upper_20",
		"bb_mid_20",
		"bb_lower_20",
	}

	if len(specs) != len(expectedNames) {
		t.Fatalf("expected %d specs, got %d", len(expectedNames), len(specs))
	}

	for i, exp := range expectedNames {
		if specs[i].Name != exp {
			t.Errorf("spec[%d] expected name %s, got %s", i, exp, specs[i].Name)
		}
	}
}

func TestComputeBarFeatures(t *testing.T) {
	now := time.Now().UTC()
	bars := make([]dukascopy.Bar, 50)
	price := 100.0
	for i := 0; i < 50; i++ {
		price += math.Sin(float64(i)) * 1.5
		bars[i] = dukascopy.Bar{
			Time:   now.Add(time.Duration(i) * time.Minute),
			Open:   price - 0.5,
			High:   price + 1.0,
			Low:    price - 1.0,
			Close:  price,
			Volume: 1000 + float64(i*10),
		}
	}

	specs, err := ParseFeatureSpecs("returns:log,returns:pct,sma:10,ema:10,rsi:14,atr:14,vwap,obv,volatility:garman-klass:14,macd:12:26:9,bollinger:20:2")
	if err != nil {
		t.Fatalf("ParseFeatureSpecs: %v", err)
	}

	colNames, rows, err := ComputeBarFeatures(bars, specs)
	if err != nil {
		t.Fatalf("ComputeBarFeatures: %v", err)
	}

	if len(rows) != 50 {
		t.Fatalf("expected 50 rows, got %d", len(rows))
	}
	if len(colNames) != len(rows[0]) {
		t.Fatalf("expected %d columns in row, got %d", len(colNames), len(rows[0]))
	}

	// Verify values are not NaN or Inf
	for r, row := range rows {
		for c, val := range row {
			if math.IsNaN(val) || math.IsInf(val, 0) {
				t.Errorf("row %d col %s is NaN/Inf", r, colNames[c])
			}
		}
	}
}

func TestShortSeriesIndicators(t *testing.T) {
	// Test very short series (fewer bars than period)
	closes := []float64{1.0800, 1.0805, 1.0810}
	highs := []float64{1.0810, 1.0815, 1.0820}
	lows := []float64{1.0790, 1.0795, 1.0800}

	rsi := ComputeRSI(closes, 14)
	if len(rsi) != 3 {
		t.Fatalf("expected 3 rsi values, got %d", len(rsi))
	}

	atr := ComputeATR(highs, lows, closes, 14)
	if len(atr) != 3 {
		t.Fatalf("expected 3 atr values, got %d", len(atr))
	}
}
