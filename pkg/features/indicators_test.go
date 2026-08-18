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

	park := ComputeParkinsonVolatility(highs, lows, 14)
	if len(park) != 3 {
		t.Fatalf("expected 3 parkinson values, got %d", len(park))
	}
}

func TestIndicatorsEdgeCasesAndErrors(t *testing.T) {
	// Empty inputs
	if len(ComputeLogReturns(nil)) != 0 {
		t.Errorf("expected empty slice")
	}
	if len(ComputePctReturns(nil)) != 0 {
		t.Errorf("expected empty slice")
	}
	if len(ComputeSMA(nil, 10)) != 0 || len(ComputeSMA([]float64{1}, 0)) != 1 {
		t.Errorf("unexpected sma empty/invalid period result")
	}
	if len(ComputeEMA(nil, 10)) != 0 || len(ComputeEMA([]float64{1}, 0)) != 1 {
		t.Errorf("unexpected ema empty/invalid period result")
	}
	if len(ComputeRSI(nil, 14)) != 0 || len(ComputeRSI([]float64{1}, 0)) != 1 {
		t.Errorf("unexpected rsi empty/invalid period result")
	}
	if len(ComputeATR(nil, nil, nil, 14)) != 0 || len(ComputeATR([]float64{1}, []float64{1}, []float64{1}, 0)) != 1 {
		t.Errorf("unexpected atr empty/invalid period result")
	}
	if len(ComputeVWAP(nil, nil, nil, nil)) != 0 {
		t.Errorf("unexpected vwap empty result")
	}
	if len(ComputeOBV(nil, nil)) != 0 {
		t.Errorf("unexpected obv empty result")
	}
	if len(ComputeParkinsonVolatility(nil, nil, 14)) != 0 || len(ComputeParkinsonVolatility([]float64{1}, []float64{1}, 0)) != 1 {
		t.Errorf("unexpected parkinson empty result")
	}

	// Parkinson Volatility with valid values
	highs := []float64{105, 106, 108, 107, 109, 110, 112, 111, 113, 115, 114, 116, 118, 117, 119}
	lows := []float64{95, 96, 98, 97, 99, 100, 102, 101, 103, 105, 104, 106, 108, 107, 109}
	pv := ComputeParkinsonVolatility(highs, lows, 5)
	if len(pv) != 15 || pv[14] <= 0 {
		t.Errorf("expected positive parkinson volatility, got %v", pv)
	}

	// OBV with up/down/flat closes
	closes := []float64{100, 105, 102, 102, 108}
	vols := []float64{1000, 2000, 1500, 1200, 3000}
	obv := ComputeOBV(closes, vols)
	if len(obv) != 5 || obv[1] != 2000 || obv[2] != 500 {
		t.Errorf("unexpected OBV calculation: %v", obv)
	}

	// VWAP with zero volume
	zeroVols := []float64{0, 0, 0}
	vwap := ComputeVWAP([]float64{10, 10, 10}, []float64{8, 8, 8}, []float64{9, 9, 9}, zeroVols)
	if len(vwap) != 3 || vwap[0] != 9 {
		t.Errorf("unexpected zero volume VWAP: %v", vwap)
	}

	// ParseFeatureSpecs error branches
	invalidSpecs := []string{
		"unknown_feature",
		"sma:abc",
		"ema:invalid",
		"rsi:not_a_num",
		"atr:-5",
		"volatility:garman-klass:not_num",
		"volatility:parkinson:not_num",
		"volatility:unknown",
		"macd:12:invalid:9",
		"bollinger:20:invalid",
	}
	for _, raw := range invalidSpecs {
		if _, err := ParseFeatureSpecs(raw); err == nil {
			t.Errorf("expected error for invalid spec %q", raw)
		}
	}
}
