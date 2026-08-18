package features

import (
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/Nosvemos/dukascopy-go/pkg/dukascopy"
)

// FeatureType defines the category/algorithm of technical feature or indicator.
type FeatureType string

const (
	FeatureLogReturns     FeatureType = "returns_log"
	FeaturePctReturns     FeatureType = "returns_pct"
	FeatureSMA            FeatureType = "sma"
	FeatureEMA            FeatureType = "ema"
	FeatureRSI            FeatureType = "rsi"
	FeatureATR            FeatureType = "atr"
	FeatureMACD           FeatureType = "macd"
	FeatureBollinger      FeatureType = "bollinger"
	FeatureVWAP           FeatureType = "vwap"
	FeatureOBV            FeatureType = "obv"
	FeatureGarmanKlassVol FeatureType = "volatility_garman_klass"
	FeatureParkinsonVol   FeatureType = "volatility_parkinson"
)

// FeatureSpec represents a parsed requested feature with its parameters.
type FeatureSpec struct {
	Type   FeatureType
	Param1 int     // e.g. period (14, 20)
	Param2 int     // e.g. slow period (26)
	Param3 int     // e.g. signal period (9)
	ParamF float64 // e.g. std dev (2.0)
	Name   string  // Output column name (e.g. "rsi_14", "ema_50", "log_returns")
}

// ParseFeatureSpecs parses a comma-separated feature string.
// Examples:
// - "returns:log,rsi:14,ema:20,ema:50,atr:14,volatility:garman-klass,vwap"
// - "sma:20,bollinger:20:2,macd:12:26:9,obv"
func ParseFeatureSpecs(raw string) ([]FeatureSpec, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, nil
	}

	tokens := strings.Split(raw, ",")
	specs := make([]FeatureSpec, 0, len(tokens))

	for _, token := range tokens {
		token = strings.TrimSpace(token)
		if token == "" {
			continue
		}

		parts := strings.Split(token, ":")
		key := strings.ToLower(strings.TrimSpace(parts[0]))

		switch key {
		case "returns", "return":
			mode := "log"
			if len(parts) > 1 {
				mode = strings.ToLower(strings.TrimSpace(parts[1]))
			}
			if mode == "pct" || mode == "percent" {
				specs = append(specs, FeatureSpec{Type: FeaturePctReturns, Name: "pct_returns"})
			} else {
				specs = append(specs, FeatureSpec{Type: FeatureLogReturns, Name: "log_returns"})
			}

		case "sma":
			period := 14
			if len(parts) > 1 {
				p, err := strconv.Atoi(strings.TrimSpace(parts[1]))
				if err != nil || p <= 0 {
					return nil, fmt.Errorf("invalid SMA period in %q", token)
				}
				period = p
			}
			specs = append(specs, FeatureSpec{Type: FeatureSMA, Param1: period, Name: fmt.Sprintf("sma_%d", period)})

		case "ema":
			period := 14
			if len(parts) > 1 {
				p, err := strconv.Atoi(strings.TrimSpace(parts[1]))
				if err != nil || p <= 0 {
					return nil, fmt.Errorf("invalid EMA period in %q", token)
				}
				period = p
			}
			specs = append(specs, FeatureSpec{Type: FeatureEMA, Param1: period, Name: fmt.Sprintf("ema_%d", period)})

		case "rsi":
			period := 14
			if len(parts) > 1 {
				p, err := strconv.Atoi(strings.TrimSpace(parts[1]))
				if err != nil || p <= 0 {
					return nil, fmt.Errorf("invalid RSI period in %q", token)
				}
				period = p
			}
			specs = append(specs, FeatureSpec{Type: FeatureRSI, Param1: period, Name: fmt.Sprintf("rsi_%d", period)})

		case "atr":
			period := 14
			if len(parts) > 1 {
				p, err := strconv.Atoi(strings.TrimSpace(parts[1]))
				if err != nil || p <= 0 {
					return nil, fmt.Errorf("invalid ATR period in %q", token)
				}
				period = p
			}
			specs = append(specs, FeatureSpec{Type: FeatureATR, Param1: period, Name: fmt.Sprintf("atr_%d", period)})

		case "vwap":
			specs = append(specs, FeatureSpec{Type: FeatureVWAP, Name: "vwap"})

		case "obv":
			specs = append(specs, FeatureSpec{Type: FeatureOBV, Name: "obv"})

		case "volatility", "vol":
			algo := "garman-klass"
			window := 14
			if len(parts) > 1 {
				algo = strings.ToLower(strings.TrimSpace(parts[1]))
				if !strings.Contains(algo, "parkinson") && !strings.Contains(algo, "garman") {
					return nil, fmt.Errorf("unknown volatility algorithm %q (supported: garman-klass, parkinson)", parts[1])
				}
			}
			if len(parts) > 2 {
				w, err := strconv.Atoi(strings.TrimSpace(parts[2]))
				if err != nil || w <= 0 {
					return nil, fmt.Errorf("invalid volatility window %q in %q", parts[2], token)
				}
				window = w
			}

			if strings.Contains(algo, "parkinson") {
				specs = append(specs, FeatureSpec{Type: FeatureParkinsonVol, Param1: window, Name: fmt.Sprintf("vol_parkinson_%d", window)})
			} else {
				specs = append(specs, FeatureSpec{Type: FeatureGarmanKlassVol, Param1: window, Name: fmt.Sprintf("vol_garman_klass_%d", window)})
			}

		case "macd":
			fast := 12
			slow := 26
			signal := 9
			if len(parts) > 1 {
				f, err := strconv.Atoi(strings.TrimSpace(parts[1]))
				if err != nil || f <= 0 {
					return nil, fmt.Errorf("invalid macd fast period %q in %q", parts[1], token)
				}
				fast = f
			}
			if len(parts) > 2 {
				s, err := strconv.Atoi(strings.TrimSpace(parts[2]))
				if err != nil || s <= 0 {
					return nil, fmt.Errorf("invalid macd slow period %q in %q", parts[2], token)
				}
				slow = s
			}
			if len(parts) > 3 {
				sig, err := strconv.Atoi(strings.TrimSpace(parts[3]))
				if err != nil || sig <= 0 {
					return nil, fmt.Errorf("invalid macd signal period %q in %q", parts[3], token)
				}
				signal = sig
			}
			specs = append(specs,
				FeatureSpec{Type: FeatureMACD, Param1: fast, Param2: slow, Param3: signal, Name: fmt.Sprintf("macd_%d_%d", fast, slow)},
				FeatureSpec{Type: FeatureMACD, Param1: fast, Param2: slow, Param3: signal, Name: fmt.Sprintf("macd_signal_%d", signal)},
				FeatureSpec{Type: FeatureMACD, Param1: fast, Param2: slow, Param3: signal, Name: fmt.Sprintf("macd_hist_%d_%d_%d", fast, slow, signal)},
			)

		case "bollinger", "bb":
			period := 20
			stdDev := 2.0
			if len(parts) > 1 {
				p, err := strconv.Atoi(strings.TrimSpace(parts[1]))
				if err != nil || p <= 0 {
					return nil, fmt.Errorf("invalid bollinger period %q in %q", parts[1], token)
				}
				period = p
			}
			if len(parts) > 2 {
				s, err := strconv.ParseFloat(strings.TrimSpace(parts[2]), 64)
				if err != nil || s <= 0 {
					return nil, fmt.Errorf("invalid bollinger std_dev %q in %q", parts[2], token)
				}
				stdDev = s
			}
			specs = append(specs,
				FeatureSpec{Type: FeatureBollinger, Param1: period, ParamF: stdDev, Name: fmt.Sprintf("bb_upper_%d", period)},
				FeatureSpec{Type: FeatureBollinger, Param1: period, ParamF: stdDev, Name: fmt.Sprintf("bb_mid_%d", period)},
				FeatureSpec{Type: FeatureBollinger, Param1: period, ParamF: stdDev, Name: fmt.Sprintf("bb_lower_%d", period)},
			)

		default:
			return nil, fmt.Errorf("unknown feature/indicator %q", token)
		}
	}

	return specs, nil
}

// ComputeBarFeatures computes all requested features across the bars slice and returns
// the column names and a slice of feature rows corresponding to each bar.
func ComputeBarFeatures(bars []dukascopy.Bar, specs []FeatureSpec) ([]string, [][]float64, error) {
	n := len(bars)
	if n == 0 || len(specs) == 0 {
		return nil, nil, nil
	}

	opens := make([]float64, n)
	highs := make([]float64, n)
	lows := make([]float64, n)
	closes := make([]float64, n)
	volumes := make([]float64, n)

	for i := 0; i < n; i++ {
		opens[i] = bars[i].Open
		highs[i] = bars[i].High
		lows[i] = bars[i].Low
		closes[i] = bars[i].Close
		volumes[i] = bars[i].Volume
	}

	colNames := make([]string, 0, len(specs))
	colData := make([][]float64, 0, len(specs))

	for i := 0; i < len(specs); i++ {
		spec := specs[i]
		switch spec.Type {
		case FeatureLogReturns:
			colNames = append(colNames, spec.Name)
			colData = append(colData, ComputeLogReturns(closes))

		case FeaturePctReturns:
			colNames = append(colNames, spec.Name)
			colData = append(colData, ComputePctReturns(closes))

		case FeatureSMA:
			colNames = append(colNames, spec.Name)
			colData = append(colData, ComputeSMA(closes, spec.Param1))

		case FeatureEMA:
			colNames = append(colNames, spec.Name)
			colData = append(colData, ComputeEMA(closes, spec.Param1))

		case FeatureRSI:
			colNames = append(colNames, spec.Name)
			colData = append(colData, ComputeRSI(closes, spec.Param1))

		case FeatureATR:
			colNames = append(colNames, spec.Name)
			colData = append(colData, ComputeATR(highs, lows, closes, spec.Param1))

		case FeatureVWAP:
			colNames = append(colNames, spec.Name)
			colData = append(colData, ComputeVWAP(highs, lows, closes, volumes))

		case FeatureOBV:
			colNames = append(colNames, spec.Name)
			colData = append(colData, ComputeOBV(closes, volumes))

		case FeatureGarmanKlassVol:
			colNames = append(colNames, spec.Name)
			colData = append(colData, ComputeGarmanKlassVolatility(opens, highs, lows, closes, spec.Param1))

		case FeatureParkinsonVol:
			colNames = append(colNames, spec.Name)
			colData = append(colData, ComputeParkinsonVolatility(highs, lows, spec.Param1))

		case FeatureMACD:
			macd, signal, hist := ComputeMACD(closes, spec.Param1, spec.Param2, spec.Param3)
			colNames = append(colNames, spec.Name, specs[i+1].Name, specs[i+2].Name)
			colData = append(colData, macd, signal, hist)
			i += 2 // skip signal and hist specs

		case FeatureBollinger:
			upper, mid, lower := ComputeBollingerBands(closes, spec.Param1, spec.ParamF)
			colNames = append(colNames, spec.Name, specs[i+1].Name, specs[i+2].Name)
			colData = append(colData, upper, mid, lower)
			i += 2 // skip mid and lower specs
		}
	}

	// Transpose colData into rows (n rows × len(colNames) columns)
	rows := make([][]float64, n)
	numCols := len(colNames)
	for r := 0; r < n; r++ {
		row := make([]float64, numCols)
		for c := 0; c < numCols; c++ {
			row[c] = colData[c][r]
		}
		rows[r] = row
	}

	return colNames, rows, nil
}

// ComputeLogReturns calculates log returns: ln(Close_t / Close_{t-1}).
func ComputeLogReturns(closes []float64) []float64 {
	n := len(closes)
	out := make([]float64, n)
	if n == 0 {
		return out
	}
	out[0] = 0
	for i := 1; i < n; i++ {
		if closes[i-1] > 0 && closes[i] > 0 {
			out[i] = math.Log(closes[i] / closes[i-1])
		}
	}
	return out
}

// ComputePctReturns calculates percent returns: (Close_t - Close_{t-1}) / Close_{t-1}.
func ComputePctReturns(closes []float64) []float64 {
	n := len(closes)
	out := make([]float64, n)
	if n == 0 {
		return out
	}
	out[0] = 0
	for i := 1; i < n; i++ {
		if closes[i-1] != 0 {
			out[i] = (closes[i] - closes[i-1]) / closes[i-1]
		}
	}
	return out
}

// ComputeSMA calculates Simple Moving Average over period.
func ComputeSMA(values []float64, period int) []float64 {
	n := len(values)
	out := make([]float64, n)
	if n == 0 || period <= 0 {
		return out
	}

	var sum float64
	for i := 0; i < n; i++ {
		sum += values[i]
		if i >= period {
			sum -= values[i-period]
		}
		if i >= period-1 {
			out[i] = sum / float64(period)
		} else {
			out[i] = sum / float64(i+1)
		}
	}
	return out
}

// ComputeEMA calculates Exponential Moving Average over period.
func ComputeEMA(values []float64, period int) []float64 {
	n := len(values)
	out := make([]float64, n)
	if n == 0 || period <= 0 {
		return out
	}

	multiplier := 2.0 / float64(period+1)
	out[0] = values[0]
	for i := 1; i < n; i++ {
		out[i] = (values[i]-out[i-1])*multiplier + out[i-1]
	}
	return out
}

// ComputeRSI calculates Relative Strength Index over period.
func ComputeRSI(closes []float64, period int) []float64 {
	n := len(closes)
	out := make([]float64, n)
	if n < 2 || period <= 0 {
		return out
	}

	limit := period
	if limit >= n {
		limit = n - 1
	}

	var avgGain, avgLoss float64
	for i := 1; i <= limit; i++ {
		diff := closes[i] - closes[i-1]
		if diff > 0 {
			avgGain += diff
		} else {
			avgLoss -= diff
		}
	}
	avgGain /= float64(limit)
	avgLoss /= float64(limit)

	var baseRSI float64
	if avgLoss == 0 {
		baseRSI = 100
	} else {
		rs := avgGain / avgLoss
		baseRSI = 100 - (100 / (1 + rs))
	}

	if n <= period {
		for i := 0; i < n; i++ {
			out[i] = baseRSI
		}
		return out
	}

	out[period] = baseRSI

	for i := period + 1; i < n; i++ {
		diff := closes[i] - closes[i-1]
		var gain, loss float64
		if diff > 0 {
			gain = diff
		} else {
			loss = -diff
		}

		avgGain = (avgGain*float64(period-1) + gain) / float64(period)
		avgLoss = (avgLoss*float64(period-1) + loss) / float64(period)

		if avgLoss == 0 {
			out[i] = 100
		} else {
			rs := avgGain / avgLoss
			out[i] = 100 - (100 / (1 + rs))
		}
	}

	// For first period bars, fill with initial RSI
	for i := 0; i < period; i++ {
		out[i] = out[period]
	}

	return out
}

// ComputeATR calculates Average True Range over period.
func ComputeATR(highs, lows, closes []float64, period int) []float64 {
	n := len(closes)
	out := make([]float64, n)
	if n == 0 || period <= 0 {
		return out
	}

	tr := make([]float64, n)
	tr[0] = highs[0] - lows[0]
	for i := 1; i < n; i++ {
		hl := highs[i] - lows[i]
		hc := math.Abs(highs[i] - closes[i-1])
		lc := math.Abs(lows[i] - closes[i-1])
		tr[i] = math.Max(hl, math.Max(hc, lc))
	}

	limit := period
	if limit > n {
		limit = n
	}

	var sum float64
	for i := 0; i < limit; i++ {
		sum += tr[i]
	}
	atr := sum / float64(limit)

	if n <= period {
		for i := 0; i < n; i++ {
			out[i] = atr
		}
		return out
	}

	out[period-1] = atr

	for i := period; i < n; i++ {
		atr = (atr*float64(period-1) + tr[i]) / float64(period)
		out[i] = atr
	}

	for i := 0; i < period-1; i++ {
		out[i] = out[period-1]
	}

	return out
}

// ComputeMACD calculates MACD line, signal line, and histogram.
func ComputeMACD(closes []float64, fast, slow, signal int) ([]float64, []float64, []float64) {
	fastEMA := ComputeEMA(closes, fast)
	slowEMA := ComputeEMA(closes, slow)

	n := len(closes)
	macd := make([]float64, n)
	for i := 0; i < n; i++ {
		macd[i] = fastEMA[i] - slowEMA[i]
	}

	signalLine := ComputeEMA(macd, signal)
	hist := make([]float64, n)
	for i := 0; i < n; i++ {
		hist[i] = macd[i] - signalLine[i]
	}

	return macd, signalLine, hist
}

// ComputeBollingerBands calculates Upper, Middle (SMA), and Lower Bollinger Bands.
func ComputeBollingerBands(closes []float64, period int, numStd float64) ([]float64, []float64, []float64) {
	n := len(closes)
	upper := make([]float64, n)
	mid := ComputeSMA(closes, period)
	lower := make([]float64, n)

	for i := 0; i < n; i++ {
		start := i - period + 1
		if start < 0 {
			start = 0
		}
		window := closes[start : i+1]
		mean := mid[i]
		var variance float64
		for _, val := range window {
			diff := val - mean
			variance += diff * diff
		}
		stdDev := math.Sqrt(variance / float64(len(window)))
		upper[i] = mean + numStd*stdDev
		lower[i] = mean - numStd*stdDev
	}

	return upper, mid, lower
}

// ComputeVWAP calculates Volume Weighted Average Price: sum(typical_price * volume) / sum(volume).
func ComputeVWAP(highs, lows, closes, volumes []float64) []float64 {
	n := len(closes)
	out := make([]float64, n)
	var cumVol, cumPV float64

	for i := 0; i < n; i++ {
		typical := (highs[i] + lows[i] + closes[i]) / 3.0
		cumPV += typical * volumes[i]
		cumVol += volumes[i]
		if cumVol > 0 {
			out[i] = cumPV / cumVol
		} else {
			out[i] = typical
		}
	}
	return out
}

// ComputeOBV calculates On-Balance Volume.
func ComputeOBV(closes, volumes []float64) []float64 {
	n := len(closes)
	out := make([]float64, n)
	if n == 0 {
		return out
	}

	var obv float64
	for i := 1; i < n; i++ {
		if closes[i] > closes[i-1] {
			obv += volumes[i]
		} else if closes[i] < closes[i-1] {
			obv -= volumes[i]
		}
		out[i] = obv
	}
	return out
}

// ComputeGarmanKlassVolatility calculates the Garman-Klass volatility estimator over a rolling window.
// $\sigma^2 = \frac{1}{2} (\ln(H/L))^2 - (2\ln 2 - 1) (\ln(C/O))^2$
func ComputeGarmanKlassVolatility(opens, highs, lows, closes []float64, window int) []float64 {
	n := len(closes)
	out := make([]float64, n)
	if n == 0 || window <= 0 {
		return out
	}

	gkValues := make([]float64, n)
	const c = 2.0*math.Ln2 - 1.0 // ~0.386

	for i := 0; i < n; i++ {
		if opens[i] > 0 && highs[i] > 0 && lows[i] > 0 && closes[i] > 0 {
			hlRatio := math.Log(highs[i] / lows[i])
			coRatio := math.Log(closes[i] / opens[i])
			gkValues[i] = 0.5*hlRatio*hlRatio - c*coRatio*coRatio
		}
	}

	var sum float64
	for i := 0; i < n; i++ {
		sum += gkValues[i]
		if i >= window {
			sum -= gkValues[i-window]
		}
		count := i + 1
		if count > window {
			count = window
		}
		avg := sum / float64(count)
		if avg > 0 {
			out[i] = math.Sqrt(avg)
		}
	}

	return out
}

// ComputeParkinsonVolatility calculates the Parkinson volatility estimator over a rolling window.
// $\sigma^2 = \frac{(\ln(H/L))^2}{4 \ln 2}$
func ComputeParkinsonVolatility(highs, lows []float64, window int) []float64 {
	n := len(highs)
	out := make([]float64, n)
	if n == 0 || window <= 0 {
		return out
	}

	pvValues := make([]float64, n)
	const factor = 4.0 * math.Ln2 // ~2.7725887

	for i := 0; i < n; i++ {
		if highs[i] > 0 && lows[i] > 0 {
			hlRatio := math.Log(highs[i] / lows[i])
			pvValues[i] = (hlRatio * hlRatio) / factor
		}
	}

	var sum float64
	for i := 0; i < n; i++ {
		sum += pvValues[i]
		if i >= window {
			sum -= pvValues[i-window]
		}
		count := i + 1
		if count > window {
			count = window
		}
		avg := sum / float64(count)
		if avg > 0 {
			out[i] = math.Sqrt(avg)
		}
	}

	return out
}
