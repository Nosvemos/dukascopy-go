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

	if !isBarTimeframe(granularity) {
		return nil, fmt.Errorf("tick aggregation does not support granularity %q", granularity)
	}

	type bucketState struct {
		bar         Bar
		initialized bool
	}

	buckets := make(map[time.Time]*bucketState)
	keys := make([]time.Time, 0)

	for _, tick := range ticks {
		bucketTime, err := bucketStart(tick.Time.UTC(), granularity)
		if err != nil {
			return nil, err
		}
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

func AggregateBars(sourceBars []Bar, granularity Granularity, from time.Time, to time.Time) ([]Bar, error) {
	normalized := normalizeGranularity(granularity)
	if normalized == GranularityM1 || normalized == GranularityH1 || normalized == GranularityD1 {
		return filterBars(sourceBars, from, to), nil
	}
	if !isBarTimeframe(granularity) {
		return nil, fmt.Errorf("bar aggregation does not support granularity %q", granularity)
	}

	type bucketState struct {
		bar         Bar
		initialized bool
	}

	buckets := make(map[time.Time]*bucketState)
	keys := make([]time.Time, 0)

	for _, source := range sourceBars {
		bucketTime, err := bucketStart(source.Time.UTC(), granularity)
		if err != nil {
			return nil, err
		}
		state, exists := buckets[bucketTime]
		if !exists {
			state = &bucketState{}
			buckets[bucketTime] = state
			keys = append(keys, bucketTime)
		}

		if !state.initialized {
			state.bar = Bar{
				Time:   bucketTime,
				Open:   source.Open,
				High:   source.High,
				Low:    source.Low,
				Close:  source.Close,
				Volume: source.Volume,
			}
			state.initialized = true
			continue
		}

		if source.High > state.bar.High {
			state.bar.High = source.High
		}
		if source.Low < state.bar.Low {
			state.bar.Low = source.Low
		}
		state.bar.Close = source.Close
		state.bar.Volume += source.Volume
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

func bucketStart(value time.Time, granularity Granularity) (time.Time, error) {
	value = value.UTC()
	switch normalizeGranularity(granularity) {
	case GranularityM1:
		return value.Truncate(time.Minute), nil
	case GranularityM3:
		return value.Truncate(3 * time.Minute), nil
	case GranularityM5:
		return value.Truncate(5 * time.Minute), nil
	case GranularityM15:
		return value.Truncate(15 * time.Minute), nil
	case GranularityM30:
		return value.Truncate(30 * time.Minute), nil
	case GranularityH1:
		return value.Truncate(time.Hour), nil
	case GranularityH4:
		return value.Truncate(4 * time.Hour), nil
	case GranularityD1:
		return time.Date(value.Year(), value.Month(), value.Day(), 0, 0, 0, 0, time.UTC), nil
	case GranularityW1:
		return weekStartUTC(value), nil
	case GranularityMN1:
		return monthStartUTC(value), nil
	default:
		return time.Time{}, fmt.Errorf("unsupported bucket granularity %q", granularity)
	}
}

func weekStartUTC(value time.Time) time.Time {
	value = time.Date(value.Year(), value.Month(), value.Day(), 0, 0, 0, 0, time.UTC)
	offset := (int(value.Weekday()) + 6) % 7
	return value.AddDate(0, 0, -offset)
}

func isBarTimeframe(granularity Granularity) bool {
	switch normalizeGranularity(granularity) {
	case GranularityM1, GranularityM3, GranularityM5, GranularityM15, GranularityM30, GranularityH1, GranularityH4, GranularityD1, GranularityW1, GranularityMN1:
		return true
	default:
		return false
	}
}

func monthStartUTC(value time.Time) time.Time {
	value = value.UTC()
	return time.Date(value.Year(), value.Month(), 1, 0, 0, 0, 0, time.UTC)
}

func tickSideValue(tick Tick, side PriceSide) (float64, float64) {
	if side == PriceSideAsk {
		return tick.Ask, tick.AskVolume
	}
	return tick.Bid, tick.BidVolume
}
