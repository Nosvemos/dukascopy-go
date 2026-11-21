package dukascopy

import (
	"fmt"
	"math"
	"strings"
	"time"
)

// BarType defines how bars/candlesticks are sampled.
type BarType string

const (
	BarTypeTime   BarType = "time"
	BarTypeTick   BarType = "tick"
	BarTypeVolume BarType = "volume"
	BarTypeDollar BarType = "dollar"
)

// ParseBarType parses string to BarType.
func ParseBarType(value string) (BarType, error) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "", "time", "standard":
		return BarTypeTime, nil
	case "tick", "ticks", "count":
		return BarTypeTick, nil
	case "volume", "vol":
		return BarTypeVolume, nil
	case "dollar", "notional", "value":
		return BarTypeDollar, nil
	default:
		return "", fmt.Errorf("unsupported bar-type %q (supported: time, tick, volume, dollar)", value)
	}
}

// AggregateTicksToTickBars aggregates a stream of ticks into Tick Count bars.
// A new bar is created every ticksPerBar ticks.
//
// side is normalized ("ask", " Ask " select ask prices); an unrecognized side
// selects bid prices. It returns nil when ticks is empty or ticksPerBar < 1;
// use SampleTicksToCustomBars to get an error for invalid sizes or sides.
func AggregateTicksToTickBars(ticks []Tick, ticksPerBar int, side PriceSide) []Bar {
	if len(ticks) == 0 || ticksPerBar <= 0 {
		return nil
	}
	side = samplingSide(side)

	capacity := len(ticks)/ticksPerBar + 1
	bars := make([]Bar, 0, capacity)

	var currentBar *Bar
	var tickCount int

	for _, tick := range ticks {
		price := tick.Bid
		vol := tick.BidVolume
		if side == PriceSideAsk {
			price = tick.Ask
			vol = tick.AskVolume
		}

		if currentBar == nil {
			currentBar = &Bar{
				Time:   tick.Time,
				Open:   price,
				High:   price,
				Low:    price,
				Close:  price,
				Volume: vol,
			}
			tickCount = 1
		} else {
			if price > currentBar.High {
				currentBar.High = price
			}
			if price < currentBar.Low {
				currentBar.Low = price
			}
			currentBar.Close = price
			currentBar.Volume += vol
			tickCount++
		}

		if tickCount >= ticksPerBar {
			bars = append(bars, *currentBar)
			currentBar = nil
			tickCount = 0
		}
	}

	if currentBar != nil {
		bars = append(bars, *currentBar)
	}

	return bars
}

// AggregateTicksToVolumeBars aggregates a stream of ticks into Volume bars.
// A new bar is formed when cumulative volume reaches or exceeds volumePerBar.
//
// side is normalized as in AggregateTicksToTickBars. It returns nil when
// ticks is empty or volumePerBar <= 0.
func AggregateTicksToVolumeBars(ticks []Tick, volumePerBar float64, side PriceSide) []Bar {
	if len(ticks) == 0 || volumePerBar <= 0 {
		return nil
	}
	side = samplingSide(side)

	bars := make([]Bar, 0, len(ticks)/100+1)
	var currentBar *Bar
	var cumVol float64

	for _, tick := range ticks {
		price := tick.Bid
		vol := tick.BidVolume
		if side == PriceSideAsk {
			price = tick.Ask
			vol = tick.AskVolume
		}

		if currentBar == nil {
			currentBar = &Bar{
				Time:   tick.Time,
				Open:   price,
				High:   price,
				Low:    price,
				Close:  price,
				Volume: vol,
			}
			cumVol = vol
		} else {
			if price > currentBar.High {
				currentBar.High = price
			}
			if price < currentBar.Low {
				currentBar.Low = price
			}
			currentBar.Close = price
			currentBar.Volume += vol
			cumVol += vol
		}

		if cumVol >= volumePerBar {
			bars = append(bars, *currentBar)
			currentBar = nil
			cumVol = 0
		}
	}

	if currentBar != nil {
		bars = append(bars, *currentBar)
	}

	return bars
}

// AggregateTicksToDollarBars aggregates a stream of ticks into Dollar/Notional value bars.
// A new bar is formed when cumulative notional trade value (Price * Volume) reaches dollarPerBar.
//
// side is normalized as in AggregateTicksToTickBars. It returns nil when
// ticks is empty or dollarPerBar <= 0.
func AggregateTicksToDollarBars(ticks []Tick, dollarPerBar float64, side PriceSide) []Bar {
	if len(ticks) == 0 || dollarPerBar <= 0 {
		return nil
	}
	side = samplingSide(side)

	bars := make([]Bar, 0, len(ticks)/100+1)
	var currentBar *Bar
	var cumDollar float64

	for _, tick := range ticks {
		price := tick.Bid
		vol := tick.BidVolume
		if side == PriceSideAsk {
			price = tick.Ask
			vol = tick.AskVolume
		}
		notional := price * vol

		if currentBar == nil {
			currentBar = &Bar{
				Time:   tick.Time,
				Open:   price,
				High:   price,
				Low:    price,
				Close:  price,
				Volume: vol,
			}
			cumDollar = notional
		} else {
			if price > currentBar.High {
				currentBar.High = price
			}
			if price < currentBar.Low {
				currentBar.Low = price
			}
			currentBar.Close = price
			currentBar.Volume += vol
			cumDollar += notional
		}

		if cumDollar >= dollarPerBar {
			bars = append(bars, *currentBar)
			currentBar = nil
			cumDollar = 0
		}
	}

	if currentBar != nil {
		bars = append(bars, *currentBar)
	}

	return bars
}

// SampleTicksToCustomBars samples ticks into the specified BarType (tick, volume, dollar).
//
// side is normalized (any casing, surrounding whitespace) and must be bid or
// ask. barSize must be > 0; for BarTypeTick it is a tick count, must be at
// least 1, and is truncated to an integer (2.7 means 2 ticks per bar).
func SampleTicksToCustomBars(ticks []Tick, barType BarType, barSize float64, side PriceSide) ([]Bar, error) {
	if !(barSize > 0) {
		return nil, fmt.Errorf("bar size must be greater than 0, got %f", barSize)
	}
	normalizedSide, err := normalizeSide(side)
	if err != nil {
		return nil, err
	}
	side = normalizedSide

	switch barType {
	case BarTypeTick:
		if barSize < 1 || barSize > float64(math.MaxInt32) {
			return nil, fmt.Errorf("tick bar size must be a tick count between 1 and %d, got %g", math.MaxInt32, barSize)
		}
		return AggregateTicksToTickBars(ticks, int(barSize), side), nil
	case BarTypeVolume:
		return AggregateTicksToVolumeBars(ticks, barSize, side), nil
	case BarTypeDollar:
		return AggregateTicksToDollarBars(ticks, barSize, side), nil
	default:
		return nil, fmt.Errorf("unsupported sampling bar type: %s", barType)
	}
}

// samplingSide normalizes side for the non-error-returning aggregators,
// falling back to bid (their historical behavior) for unknown values.
func samplingSide(side PriceSide) PriceSide {
	if normalized, err := normalizeSide(side); err == nil {
		return normalized
	}
	return PriceSideBid
}

// EnsureBarOrder sorts and ensures strictly chronological order of sampled bars.
func EnsureBarOrder(bars []Bar) {
	for i := 1; i < len(bars); i++ {
		if bars[i].Time.Before(bars[i-1].Time) {
			bars[i].Time = bars[i-1].Time.Add(time.Millisecond)
		}
	}
}
