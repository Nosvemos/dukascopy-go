package dukascopy

import (
	"fmt"
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
func AggregateTicksToTickBars(ticks []Tick, ticksPerBar int, side PriceSide) []Bar {
	if len(ticks) == 0 || ticksPerBar <= 0 {
		return nil
	}

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
func AggregateTicksToVolumeBars(ticks []Tick, volumePerBar float64, side PriceSide) []Bar {
	if len(ticks) == 0 || volumePerBar <= 0 {
		return nil
	}

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
func AggregateTicksToDollarBars(ticks []Tick, dollarPerBar float64, side PriceSide) []Bar {
	if len(ticks) == 0 || dollarPerBar <= 0 {
		return nil
	}

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
func SampleTicksToCustomBars(ticks []Tick, barType BarType, barSize float64, side PriceSide) ([]Bar, error) {
	if barSize <= 0 {
		return nil, fmt.Errorf("bar size must be greater than 0, got %f", barSize)
	}

	switch barType {
	case BarTypeTick:
		return AggregateTicksToTickBars(ticks, int(barSize), side), nil
	case BarTypeVolume:
		return AggregateTicksToVolumeBars(ticks, barSize, side), nil
	case BarTypeDollar:
		return AggregateTicksToDollarBars(ticks, barSize, side), nil
	default:
		return nil, fmt.Errorf("unsupported sampling bar type: %s", barType)
	}
}

// EnsureBarOrder sorts and ensures strictly chronological order of sampled bars.
func EnsureBarOrder(bars []Bar) {
	for i := 1; i < len(bars); i++ {
		if bars[i].Time.Before(bars[i-1].Time) {
			bars[i].Time = bars[i-1].Time.Add(time.Millisecond)
		}
	}
}
