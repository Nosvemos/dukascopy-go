package dukascopy

import (
	"fmt"
	"sort"
	"time"
)

func AggregateTicksToBars(ticks []Tick, granularity Granularity, side PriceSide, from time.Time, to time.Time) ([]Bar, error) {
	normalizedSide, err := normalizeSide(side)
	if err != nil {
		return nil, err
	}

	normalizedGranularity := normalizeGranularity(granularity)
	if normalizedGranularity != GranularityMinute && normalizedGranularity != GranularityHour && normalizedGranularity != GranularityDay {
		return nil, fmt.Errorf("tick aggregation does not support granularity %q", granularity)
	}

	type bucketState struct {
		bar         Bar
		initialized bool
	}

	buckets := make(map[time.Time]*bucketState)
	keys := make([]time.Time, 0)

	for _, tick := range ticks {
		bucketTime := bucketStart(tick.Time.UTC(), normalizedGranularity)
		state, exists := buckets[bucketTime]
		if !exists {
			state = &bucketState{}
			buckets[bucketTime] = state
			keys = append(keys, bucketTime)
		}

		price, volume := tickSideValue(tick, normalizedSide)
		if !state.initialized {
			state.bar = Bar{
				Time:   bucketTime,
				Open:   price,
				High:   price,
				Low:    price,
				Close:  price,
				Volume: volume,
			}
			state.initialized = true
			continue
		}

		if price > state.bar.High {
			state.bar.High = price
		}
		if price < state.bar.Low {
			state.bar.Low = price
		}
		state.bar.Close = price
		state.bar.Volume += volume
	}

	sort.Slice(keys, func(i, j int) bool {
		return keys[i].Before(keys[j])
	})

	bars := make([]Bar, 0, len(keys))
	for _, key := range keys {
		bar := buckets[key].bar
		if !bar.Time.Before(from) && bar.Time.Before(to) {
			bars = append(bars, bar)
		}
	}

	return bars, nil
}

func bucketStart(value time.Time, granularity Granularity) time.Time {
	value = value.UTC()
	switch granularity {
	case GranularityMinute:
		return value.Truncate(time.Minute)
	case GranularityHour:
		return value.Truncate(time.Hour)
	case GranularityDay:
		return time.Date(value.Year(), value.Month(), value.Day(), 0, 0, 0, 0, time.UTC)
	default:
		return value
	}
}

func tickSideValue(tick Tick, side PriceSide) (float64, float64) {
	if side == PriceSideAsk {
		return tick.Ask, tick.AskVolume
	}
	return tick.Bid, tick.BidVolume
}
