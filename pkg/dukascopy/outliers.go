package dukascopy

import (
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
)

// OutlierConfig holds options for outlier scrubbing.
type OutlierConfig struct {
	Enabled   bool
	Window    int     // Rolling window size (e.g. 7)
	Threshold float64 // Number of MAD / standard deviations (e.g. 5.0)
}

// ParseOutlierConfig parses an outlier flag string such as:
// - "auto" or "true" -> default window=7, threshold=5.0
// - "median:7:5.0" -> window=7, threshold=5.0
// - "none" or "false" -> disabled
func ParseOutlierConfig(raw string) (OutlierConfig, error) {
	raw = strings.ToLower(strings.TrimSpace(raw))
	if raw == "" || raw == "none" || raw == "false" || raw == "off" {
		return OutlierConfig{Enabled: false}, nil
	}

	if raw == "auto" || raw == "true" || raw == "on" {
		return OutlierConfig{Enabled: true, Window: 7, Threshold: 5.0}, nil
	}

	parts := strings.Split(raw, ":")
	strategy := parts[0]
	if strategy != "median" && strategy != "mad" {
		return OutlierConfig{}, fmt.Errorf("unsupported outlier strategy %q (supported: median, mad)", strategy)
	}
	window := 7
	threshold := 5.0

	if len(parts) > 1 {
		w, err := strconv.Atoi(parts[1])
		if err != nil || w < 3 {
			return OutlierConfig{}, fmt.Errorf("invalid outlier window size %q (must be >= 3)", parts[1])
		}
		window = w
	}
	if len(parts) > 2 {
		th, err := strconv.ParseFloat(parts[2], 64)
		if err != nil || th <= 0 {
			return OutlierConfig{}, fmt.Errorf("invalid outlier threshold %q (must be > 0)", parts[2])
		}
		threshold = th
	}

	return OutlierConfig{
		Enabled:   true,
		Window:    window,
		Threshold: threshold,
	}, nil
}

// CleanTickOutliers removes bad ticks / flash spikes that deviate from rolling median.
// Uses Median Absolute Deviation (MAD) which is robust against extreme outliers.
func CleanTickOutliers(ticks []Tick, window int, threshold float64) []Tick {
	n := len(ticks)
	if n < window || window < 3 {
		return ticks
	}

	cleaned := make([]Tick, 0, n)
	halfWin := window / 2
	scratch := newMedianScratch(2*halfWin + 1)

	for i := 0; i < n; i++ {
		start := i - halfWin
		if start < 0 {
			start = 0
		}
		end := i + halfWin + 1
		if end > n {
			end = n
		}

		prices := scratch.values[:0]
		for _, t := range ticks[start:end] {
			prices = append(prices, (t.Bid+t.Ask)/2.0)
		}

		med, mad := scratch.medianAndMAD(prices)

		currentMid := (ticks[i].Bid + ticks[i].Ask) / 2.0
		diff := math.Abs(currentMid - med)

		// If MAD is zero, use a small relative threshold
		cutoff := threshold * mad
		if cutoff == 0 {
			cutoff = med * 0.01 // 1% price spike threshold
		}

		if diff > cutoff {
			// Skip this outlier tick!
			continue
		}

		cleaned = append(cleaned, ticks[i])
	}

	return cleaned
}

// CleanBarOutliers cleans impossible candle spikes from bar data.
func CleanBarOutliers(bars []Bar, window int, threshold float64) []Bar {
	n := len(bars)
	if n < window || window < 3 {
		return bars
	}

	cleaned := make([]Bar, 0, n)
	halfWin := window / 2
	scratch := newMedianScratch(2*halfWin + 1)

	for i := 0; i < n; i++ {
		start := i - halfWin
		if start < 0 {
			start = 0
		}
		end := i + halfWin + 1
		if end > n {
			end = n
		}

		closes := scratch.values[:0]
		for _, b := range bars[start:end] {
			closes = append(closes, b.Close)
		}

		med, mad := scratch.medianAndMAD(closes)

		b := bars[i]
		cutoff := threshold * mad
		if cutoff == 0 {
			cutoff = med * 0.02
		}

		// Check if High or Low are anomalous
		if math.Abs(b.High-med) > cutoff*2 || math.Abs(b.Low-med) > cutoff*2 || math.Abs(b.Close-med) > cutoff {
			// Clamp anomalous High/Low to reasonable range
			if b.High > med+cutoff {
				b.High = math.Max(b.Open, b.Close)
			}
			if b.Low < med-cutoff && med-cutoff > 0 {
				b.Low = math.Min(b.Open, b.Close)
			}
			if math.Abs(b.Close-med) > cutoff {
				continue // skip completely corrupted bar
			}
		}

		cleaned = append(cleaned, b)
	}

	return cleaned
}

// medianScratch holds reusable buffers for rolling median/MAD computation so
// the outlier cleaners do not allocate per element.
type medianScratch struct {
	values []float64
	sorted []float64
}

func newMedianScratch(size int) *medianScratch {
	return &medianScratch{
		values: make([]float64, 0, size),
		sorted: make([]float64, 0, size),
	}
}

// medianAndMAD returns median(values) and
// medianAbsoluteDeviation(values, median(values)), computing exactly the same
// float64 results without allocating. values is not modified.
func (s *medianScratch) medianAndMAD(values []float64) (float64, float64) {
	if len(values) == 0 {
		return 0, 0
	}
	s.sorted = append(s.sorted[:0], values...)
	sort.Float64s(s.sorted)
	med := sortedMedian(s.sorted)

	// The absolute deviations form the same multiset whether computed from
	// values or from its sorted copy, so their median is identical.
	for i, v := range s.sorted {
		s.sorted[i] = math.Abs(v - med)
	}
	sort.Float64s(s.sorted)
	return med, sortedMedian(s.sorted)
}

func sortedMedian(sorted []float64) float64 {
	mid := len(sorted) / 2
	if len(sorted)%2 == 0 {
		return (sorted[mid-1] + sorted[mid]) / 2.0
	}
	return sorted[mid]
}
