package dukascopy

import (
	"fmt"
	"sort"
	"strings"
	"time"
)

const tickVolumeDivider = 1_000_000

type candlePayload struct {
	Timestamp  int64     `json:"timestamp"`
	Multiplier float64   `json:"multiplier"`
	Open       float64   `json:"open"`
	High       float64   `json:"high"`
	Low        float64   `json:"low"`
	Close      float64   `json:"close"`
	Shift      int64     `json:"shift"`
	Times      []int64   `json:"times"`
	Opens      []float64 `json:"opens"`
	Highs      []float64 `json:"highs"`
	Lows       []float64 `json:"lows"`
	Closes     []float64 `json:"closes"`
	Volumes    []float64 `json:"volumes"`
}

type tickPayload struct {
	Timestamp  int64     `json:"timestamp"`
	Multiplier float64   `json:"multiplier"`
	Ask        float64   `json:"ask"`
	Bid        float64   `json:"bid"`
	Times      []int64   `json:"times"`
	Asks       []float64 `json:"asks"`
	Bids       []float64 `json:"bids"`
	AskVolumes []float64 `json:"askVolumes"`
	BidVolumes []float64 `json:"bidVolumes"`
}

func ResolveInstrument(instruments []Instrument, raw string) (Instrument, error) {
	needle := compactSymbol(raw)
	if needle == "" {
		return Instrument{}, fmt.Errorf("symbol cannot be empty")
	}

	type candidate struct {
		instrument Instrument
		score      int
	}

	var candidates []candidate
	for _, instrument := range instruments {
		score := scoreInstrument(instrument, raw, needle)
		if score > 0 {
			candidates = append(candidates, candidate{instrument: instrument, score: score})
		}
	}

	if len(candidates) == 0 {
		return Instrument{}, fmt.Errorf("could not resolve instrument %q", raw)
	}

	sort.SliceStable(candidates, func(i, j int) bool {
		if candidates[i].score == candidates[j].score {
			return candidates[i].instrument.Name < candidates[j].instrument.Name
		}
		return candidates[i].score > candidates[j].score
	})

	return candidates[0].instrument, nil
}

func FilterInstruments(instruments []Instrument, raw string, limit int) []Instrument {
	needle := compactSymbol(raw)
	if needle == "" {
		return nil
	}

	type candidate struct {
		instrument Instrument
		score      int
	}

	var candidates []candidate
	for _, instrument := range instruments {
		score := scoreInstrument(instrument, raw, needle)
		if score > 0 {
			candidates = append(candidates, candidate{instrument: instrument, score: score})
		}
	}

	sort.SliceStable(candidates, func(i, j int) bool {
		if candidates[i].score == candidates[j].score {
			return candidates[i].instrument.Name < candidates[j].instrument.Name
		}
		return candidates[i].score > candidates[j].score
	})

	if limit > len(candidates) {
		limit = len(candidates)
	}

	filtered := make([]Instrument, 0, limit)
	for i := 0; i < limit; i++ {
		filtered = append(filtered, candidates[i].instrument)
	}
	return filtered
}

func normalizeGranularity(value Granularity) Granularity {
	switch strings.ToLower(strings.TrimSpace(string(value))) {
	case "tick":
		return GranularityTick
	case "minute", "min":
		return GranularityMinute
	case "hour", "hr":
		return GranularityHour
	case "day":
		return GranularityDay
	default:
		return Granularity(strings.ToLower(strings.TrimSpace(string(value))))
	}
}

func normalizeSide(side PriceSide) (PriceSide, error) {
	switch strings.ToUpper(strings.TrimSpace(string(side))) {
	case "BID":
		return PriceSideBid, nil
	case "ASK":
		return PriceSideAsk, nil
	default:
		return "", fmt.Errorf("unsupported side %q", side)
	}
}

func compactSymbol(value string) string {
	value = strings.ToUpper(strings.TrimSpace(value))
	replacer := strings.NewReplacer("/", "", "-", "", "_", "", " ", "", ".", "")
	return replacer.Replace(value)
}

func scoreInstrument(instrument Instrument, raw string, needle string) int {
	rawUpper := strings.ToUpper(strings.TrimSpace(raw))
	codeUpper := strings.ToUpper(instrument.Code)
	nameUpper := strings.ToUpper(instrument.Name)
	descriptionUpper := strings.ToUpper(instrument.Description)
	codeCompact := compactSymbol(instrument.Code)
	nameCompact := compactSymbol(instrument.Name)

	switch {
	case rawUpper == codeUpper:
		return 100
	case rawUpper == nameUpper:
		return 95
	case needle == codeCompact:
		return 90
	case needle == nameCompact:
		return 85
	case strings.Contains(codeCompact, needle):
		return 70
	case strings.Contains(nameCompact, needle):
		return 65
	case strings.Contains(strings.ToUpper(descriptionUpper), rawUpper):
		return 50
	default:
		return 0
	}
}

func decodeBars(payload candlePayload) []Bar {
	size := minLength(
		len(payload.Times),
		len(payload.Opens),
		len(payload.Highs),
		len(payload.Lows),
		len(payload.Closes),
		len(payload.Volumes),
	)
	if size == 0 {
		return nil
	}

	bars := make([]Bar, 0, size)
	currentTime := payload.Timestamp
	currentOpen := payload.Open
	currentHigh := payload.High
	currentLow := payload.Low
	currentClose := payload.Close

	for i := 0; i < size; i++ {
		currentTime += payload.Shift * payload.Times[i]
		currentOpen += payload.Opens[i] * payload.Multiplier
		currentHigh += payload.Highs[i] * payload.Multiplier
		currentLow += payload.Lows[i] * payload.Multiplier
		currentClose += payload.Closes[i] * payload.Multiplier

		bars = append(bars, Bar{
			Time:   time.UnixMilli(currentTime).UTC(),
			Open:   currentOpen,
			High:   currentHigh,
			Low:    currentLow,
			Close:  currentClose,
			Volume: payload.Volumes[i],
		})
	}

	return bars
}

func decodeTicks(payload tickPayload) []Tick {
	size := minLength(
		len(payload.Times),
		len(payload.Asks),
		len(payload.Bids),
		len(payload.AskVolumes),
		len(payload.BidVolumes),
	)
	if size == 0 {
		return nil
	}

	ticks := make([]Tick, 0, size)
	currentTime := payload.Timestamp
	currentAsk := payload.Ask
	currentBid := payload.Bid

	for i := 0; i < size; i++ {
		currentTime += payload.Times[i]
		currentAsk += payload.Asks[i] * payload.Multiplier
		currentBid += payload.Bids[i] * payload.Multiplier

		ticks = append(ticks, Tick{
			Time:      time.UnixMilli(currentTime).UTC(),
			Ask:       currentAsk,
			Bid:       currentBid,
			AskVolume: payload.AskVolumes[i] / tickVolumeDivider,
			BidVolume: payload.BidVolumes[i] / tickVolumeDivider,
		})
	}

	return ticks
}

func filterBars(bars []Bar, from time.Time, to time.Time) []Bar {
	filtered := make([]Bar, 0, len(bars))
	for _, bar := range bars {
		if !bar.Time.Before(from) && bar.Time.Before(to) {
			filtered = append(filtered, bar)
		}
	}
	return filtered
}

func filterTicks(ticks []Tick, from time.Time, to time.Time) []Tick {
	filtered := make([]Tick, 0, len(ticks))
	for _, tick := range ticks {
		if !tick.Time.Before(from) && tick.Time.Before(to) {
			filtered = append(filtered, tick)
		}
	}
	return filtered
}

func midnightUTC(value time.Time) time.Time {
	value = value.UTC()
	return time.Date(value.Year(), value.Month(), value.Day(), 0, 0, 0, 0, time.UTC)
}

func monthStartUTC(value time.Time) time.Time {
	value = value.UTC()
	return time.Date(value.Year(), value.Month(), 1, 0, 0, 0, 0, time.UTC)
}

func hourStartUTC(value time.Time) time.Time {
	value = value.UTC()
	return value.Truncate(time.Hour)
}

func minLength(lengths ...int) int {
	if len(lengths) == 0 {
		return 0
	}
	min := lengths[0]
	for _, length := range lengths[1:] {
		if length < min {
			min = length
		}
	}
	return min
}
