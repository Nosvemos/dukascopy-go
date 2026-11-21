package features

import (
	"math"
	"math/rand"
	"strings"
	"testing"
	"time"

	"github.com/Nosvemos/dukascopy-go/pkg/dukascopy"
)

// Reference series used by the golden tests. All expected values below were
// derived by hand (and cross-checked with exact rational arithmetic) from
// these inputs; the derivations are in the comments next to each value.
//
//	index:  0   1   2   3   4   5   6
//	close: 10  11  10  12  13  12  14
//	high:  11  12  11  13  14  13  15
//	low:    9  10   9  11  12  11  13
var (
	goldenCloses = []float64{10, 11, 10, 12, 13, 12, 14}
	goldenHighs  = []float64{11, 12, 11, 13, 14, 13, 15}
	goldenLows   = []float64{9, 10, 9, 11, 12, 11, 13}
)

const goldenTol = 1e-12

func nan() float64 { return math.NaN() }

func assertSeries(t *testing.T, name string, got, want []float64) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("%s: length %d, want %d", name, len(got), len(want))
	}
	for i := range want {
		switch {
		case math.IsNaN(want[i]):
			if !math.IsNaN(got[i]) {
				t.Errorf("%s[%d] = %v, want NaN (warm-up)", name, i, got[i])
			}
		case math.IsNaN(got[i]) || math.Abs(got[i]-want[i]) > goldenTol*math.Max(1, math.Abs(want[i])):
			t.Errorf("%s[%d] = %.15g, want %.15g", name, i, got[i], want[i])
		}
	}
}

func TestGoldenSMA(t *testing.T) {
	// SMA(3): mean of the last three closes, NaN until a full window exists.
	//   [2] (10+11+10)/3 = 31/3
	//   [3] (11+10+12)/3 = 11
	//   [4] (10+12+13)/3 = 35/3
	//   [5] (12+13+12)/3 = 37/3
	//   [6] (13+12+14)/3 = 13
	want := []float64{nan(), nan(), 31.0 / 3, 11, 35.0 / 3, 37.0 / 3, 13}
	assertSeries(t, "sma3", ComputeSMA(goldenCloses, 3), want)
}

func TestGoldenEMA(t *testing.T) {
	// EMA(3): alpha = 2/(3+1) = 1/2, seeded with SMA(3) at index 2.
	//   [2] seed = 31/3
	//   [3] 12/2 + (31/3)/2   = 67/6
	//   [4] 13/2 + (67/6)/2   = 145/12
	//   [5] 12/2 + (145/12)/2 = 289/24
	//   [6] 14/2 + (289/24)/2 = 625/48
	want := []float64{nan(), nan(), 31.0 / 3, 67.0 / 6, 145.0 / 12, 289.0 / 24, 625.0 / 48}
	assertSeries(t, "ema3", ComputeEMA(goldenCloses, 3), want)
}

func TestGoldenRSIWilder(t *testing.T) {
	// RSI(3), Wilder. Close changes: +1, -1, +2, +1, -1, +2.
	//   [3] avgGain = (1+0+2)/3 = 1,     avgLoss = (0+1+0)/3 = 1/3
	//       RS = 3            -> RSI = 100 - 100/4      = 75
	//   [4] change +1: avgGain = (1*2+1)/3 = 1, avgLoss = (1/3*2+0)/3 = 2/9
	//       RS = 9/2          -> RSI = 100 - 100/(11/2) = 900/11
	//   [5] change -1: avgGain = (1*2+0)/3 = 2/3, avgLoss = (2/9*2+1)/3 = 13/27
	//       RS = 18/13        -> RSI = 100 - 100/(31/13) = 1800/31
	//   [6] change +2: avgGain = (2/3*2+2)/3 = 10/9, avgLoss = (13/27*2)/3 = 26/81
	//       RS = 45/13        -> RSI = 100 - 100/(58/13) = 2250/29
	want := []float64{nan(), nan(), nan(), 75, 900.0 / 11, 1800.0 / 31, 2250.0 / 29}
	assertSeries(t, "rsi3", ComputeRSI(goldenCloses, 3), want)

	// Fewer than period+1 closes: no full RSI exists, so everything is NaN
	// (previously these rows were back-filled from a partial computation).
	assertSeries(t, "rsi14-short", ComputeRSI([]float64{1, 2, 3}, 14), []float64{nan(), nan(), nan()})
}

func TestGoldenATRWilder(t *testing.T) {
	// True range: TR0 = H-L = 2; then max(H-L, |H-prevC|, |L-prevC|):
	//   TR = 2, 2, 2, 3, 2, 2, 3
	// ATR(3): first value at index 2 = (2+2+2)/3 = 2, then Wilder smoothing:
	//   [3] (2*2+3)/3      = 7/3
	//   [4] (7/3*2+2)/3    = 20/9
	//   [5] (20/9*2+2)/3   = 58/27
	//   [6] (58/27*2+3)/3  = 197/81
	want := []float64{nan(), nan(), 2, 7.0 / 3, 20.0 / 9, 58.0 / 27, 197.0 / 81}
	assertSeries(t, "atr3", ComputeATR(goldenHighs, goldenLows, goldenCloses, 3), want)
}

func TestGoldenMACD(t *testing.T) {
	// MACD(fast=2, slow=3, signal=2).
	// EMA(2): alpha = 2/3, seed at [1] = (10+11)/2 = 21/2
	//   [2] 61/6, [3] 205/18, [4] 673/54, [5] 1969/162, [6] 6505/486
	// EMA(3): see TestGoldenEMA: [2] 31/3, [3] 67/6, [4] 145/12, [5] 289/24, [6] 625/48
	// MACD = EMA2 - EMA3:
	//   [2] -1/6, [3] 2/9, [4] 41/108, [5] 73/648, [6] 1415/3888
	// Signal = EMA(2) of MACD, seeded at [3] with (-1/6 + 2/9)/2 = 1/36:
	//   [4] 1/36 + (41/108 - 1/36)*2/3    = 85/324
	//   [5] 85/324 + (73/648 - 85/324)*2/3 = 79/486
	//   [6] 79/486 + (1415/3888 - 79/486)*2/3 = 577/1944
	// Histogram = MACD - Signal:
	//   [3] 7/36, [4] 19/162, [5] -97/1944, [6] 29/432
	macd, signal, hist := ComputeMACD(goldenCloses, 2, 3, 2)
	assertSeries(t, "macd", macd, []float64{nan(), nan(), -1.0 / 6, 2.0 / 9, 41.0 / 108, 73.0 / 648, 1415.0 / 3888})
	assertSeries(t, "signal", signal, []float64{nan(), nan(), nan(), 1.0 / 36, 85.0 / 324, 79.0 / 486, 577.0 / 1944})
	assertSeries(t, "hist", hist, []float64{nan(), nan(), nan(), 7.0 / 36, 19.0 / 162, -97.0 / 1944, 29.0 / 432})
}

func TestGoldenBollingerPopulationStdDev(t *testing.T) {
	// Bollinger(3, k=2): mid = SMA(3); sigma = POPULATION std dev (divide by 3).
	//   [2] window 10,11,10: mean 31/3, var = (1/9+4/9+1/9)/3 = 2/9
	//   [3] window 11,10,12: mean 11,   var = (0+1+1)/3       = 2/3
	//   [4] window 10,12,13: mean 35/3, var = (25/9+1/9+16/9)/3 = 14/9
	//   [5] window 12,13,12: mean 37/3, var = 2/9
	//   [6] window 13,12,14: mean 13,   var = 2/3
	means := []float64{31.0 / 3, 11, 35.0 / 3, 37.0 / 3, 13}
	vars := []float64{2.0 / 9, 2.0 / 3, 14.0 / 9, 2.0 / 9, 2.0 / 3}
	wantU := []float64{nan(), nan()}
	wantM := []float64{nan(), nan()}
	wantL := []float64{nan(), nan()}
	for i := range means {
		sd := math.Sqrt(vars[i])
		wantU = append(wantU, means[i]+2*sd)
		wantM = append(wantM, means[i])
		wantL = append(wantL, means[i]-2*sd)
	}
	upper, mid, lower := ComputeBollingerBands(goldenCloses, 3, 2)
	assertSeries(t, "bb_upper", upper, wantU)
	assertSeries(t, "bb_mid", mid, wantM)
	assertSeries(t, "bb_lower", lower, wantL)
}

// naiveBollinger is the previous O(n*period) two-pass implementation (for full
// windows), kept here as a reference for the rolling version.
func naiveBollinger(closes []float64, period int, k float64) ([]float64, []float64) {
	n := len(closes)
	up, lo := make([]float64, n), make([]float64, n)
	for i := range closes {
		if i < period-1 {
			up[i], lo[i] = math.NaN(), math.NaN()
			continue
		}
		w := closes[i-period+1 : i+1]
		var sum float64
		for _, v := range w {
			sum += v
		}
		mean := sum / float64(period)
		var ss float64
		for _, v := range w {
			ss += (v - mean) * (v - mean)
		}
		sd := math.Sqrt(ss / float64(period))
		up[i], lo[i] = mean+k*sd, mean-k*sd
	}
	return up, lo
}

func TestBollingerRollingMatchesTwoPass(t *testing.T) {
	// Long FX-like series (small variance around ~1.08) crossing several
	// exact-resync boundaries of the rolling variance.
	rng := rand.New(rand.NewSource(42))
	closes := make([]float64, 5000)
	p := 1.08
	for i := range closes {
		p += (rng.Float64() - 0.5) * 0.0004
		closes[i] = p
	}
	for _, period := range []int{1, 2, 20, 1500} {
		upper, _, lower := ComputeBollingerBands(closes, period, 2)
		wantU, wantL := naiveBollinger(closes, period, 2)
		for i := range closes {
			if math.IsNaN(wantU[i]) {
				if !math.IsNaN(upper[i]) || !math.IsNaN(lower[i]) {
					t.Fatalf("period %d row %d: expected NaN warm-up", period, i)
				}
				continue
			}
			if math.Abs(upper[i]-wantU[i]) > 1e-9 || math.Abs(lower[i]-wantL[i]) > 1e-9 {
				t.Fatalf("period %d row %d: rolling (%v,%v) vs two-pass (%v,%v)", period, i, upper[i], lower[i], wantU[i], wantL[i])
			}
		}
	}
}

// TestNoLookAhead verifies that changing future bars never changes an
// earlier indicator value (the old back-fill made warm-up rows depend on
// later bars).
func TestNoLookAhead(t *testing.T) {
	base := []float64{10, 11, 10, 12, 13, 12, 14, 13, 15, 16, 14, 15, 17, 18, 16, 15, 17, 19, 18, 20}
	changed := append([]float64(nil), base...)
	for i := 10; i < len(changed); i++ {
		changed[i] *= 3
	}
	highs := func(c []float64) []float64 {
		out := make([]float64, len(c))
		for i, v := range c {
			out[i] = v + 1
		}
		return out
	}
	lows := func(c []float64) []float64 {
		out := make([]float64, len(c))
		for i, v := range c {
			out[i] = v - 1
		}
		return out
	}
	type fn func(c []float64) []float64
	cases := map[string]fn{
		"sma5": func(c []float64) []float64 { return ComputeSMA(c, 5) },
		"ema5": func(c []float64) []float64 { return ComputeEMA(c, 5) },
		"rsi5": func(c []float64) []float64 { return ComputeRSI(c, 5) },
		"atr5": func(c []float64) []float64 { return ComputeATR(highs(c), lows(c), c, 5) },
		"macd": func(c []float64) []float64 { m, _, _ := ComputeMACD(c, 3, 6, 3); return m },
		"sig":  func(c []float64) []float64 { _, s, _ := ComputeMACD(c, 3, 6, 3); return s },
		"bbu":  func(c []float64) []float64 { u, _, _ := ComputeBollingerBands(c, 5, 2); return u },
	}
	for name, f := range cases {
		a, b := f(base), f(changed)
		for i := 0; i < 10; i++ {
			if !(math.IsNaN(a[i]) && math.IsNaN(b[i])) && a[i] != b[i] {
				t.Errorf("%s[%d] depends on future bars: %v vs %v", name, i, a[i], b[i])
			}
		}
	}
}

func TestComputeBarFeaturesSingleMACDOrBollingerSpec(t *testing.T) {
	bars := make([]dukascopy.Bar, len(goldenCloses))
	start := time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC)
	for i, c := range goldenCloses {
		bars[i] = dukascopy.Bar{Time: start.Add(time.Duration(i) * time.Minute), Open: c, High: goldenHighs[i], Low: goldenLows[i], Close: c, Volume: 1}
	}

	// A lone MACD spec used to panic with index out of range (specs[i+1]).
	names, rows, err := ComputeBarFeatures(bars, []FeatureSpec{{Type: FeatureMACD, Param1: 2, Param2: 3, Param3: 2, Name: "my_macd"}})
	if err != nil {
		t.Fatalf("single MACD spec: %v", err)
	}
	if strings.Join(names, ",") != "my_macd,macd_signal_2,macd_hist_2_3_2" {
		t.Fatalf("unexpected MACD names %v", names)
	}
	if got := rows[6][0]; math.Abs(got-1415.0/3888) > goldenTol {
		t.Errorf("macd[6] = %v", got)
	}

	// A lone Bollinger spec followed by an unrelated spec must not swallow it.
	names, _, err = ComputeBarFeatures(bars, []FeatureSpec{
		{Type: FeatureBollinger, Param1: 3, ParamF: 2, Name: "bb_upper_3"},
		{Type: FeatureSMA, Param1: 3, Name: "sma_3"},
	})
	if err != nil {
		t.Fatalf("single Bollinger spec: %v", err)
	}
	if strings.Join(names, ",") != "bb_upper_3,bb_mid_3,bb_lower_3,sma_3" {
		t.Fatalf("unexpected Bollinger names %v", names)
	}

	// Duplicate column names from hand-built specs are rejected.
	if _, _, err := ComputeBarFeatures(bars, []FeatureSpec{
		{Type: FeatureSMA, Param1: 3, Name: "x"},
		{Type: FeatureEMA, Param1: 3, Name: "x"},
	}); err == nil {
		t.Errorf("expected duplicate column error")
	}
}

func TestParseFeatureSpecsRejectsDuplicateColumns(t *testing.T) {
	for _, raw := range []string{
		"macd:12:26:9,macd:5:35:9",      // both produce macd_signal_9
		"bollinger:20:2,bollinger:20:3", // same bb_*_20 columns
		"rsi,rsi:14",                    // both rsi_14
		"sma:20,sma:20",                 // exact duplicate
		"returns,returns:log",           // both log_returns
	} {
		_, err := ParseFeatureSpecs(raw)
		if err == nil || !strings.Contains(err.Error(), "column") {
			t.Errorf("ParseFeatureSpecs(%q) error = %v, want duplicate column error", raw, err)
		}
	}

	// Non-colliding combinations keep their existing column names.
	specs, err := ParseFeatureSpecs("macd:12:26:9,macd:5:35:10,bollinger:20:2,bollinger:10:2,sma:20,ema:20")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	var names []string
	for _, s := range specs {
		names = append(names, s.Name)
	}
	want := "macd_12_26,macd_signal_9,macd_hist_12_26_9,macd_5_35,macd_signal_10,macd_hist_5_35_10," +
		"bb_upper_20,bb_mid_20,bb_lower_20,bb_upper_10,bb_mid_10,bb_lower_10,sma_20,ema_20"
	if strings.Join(names, ",") != want {
		t.Errorf("names = %v", names)
	}
}
