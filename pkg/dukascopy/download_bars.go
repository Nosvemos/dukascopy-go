package dukascopy

import (
	"bytes"
	"context"
	"fmt"
	"strconv"
	"time"
)

func (c *Client) downloadBars(ctx context.Context, instrument Instrument, side PriceSide, granularity Granularity, from time.Time, to time.Time) ([]Bar, error) {
	switch normalizeGranularity(granularity) {
	case GranularityM1:
		return c.downloadMinuteBars(ctx, instrument, side, from, to)
	case GranularityM3, GranularityM5, GranularityM15, GranularityM30:
		minuteBars, err := c.downloadMinuteBars(ctx, instrument, side, from, to)
		if err != nil {
			return nil, err
		}
		return AggregateBars(minuteBars, granularity, from, to)
	case GranularityH1:
		return c.downloadHourlyBars(ctx, instrument, side, from, to)
	case GranularityH4:
		hourlyBars, err := c.downloadHourlyBars(ctx, instrument, side, from, to)
		if err != nil {
			return nil, err
		}
		return AggregateBars(hourlyBars, granularity, from, to)
	case GranularityD1:
		return c.downloadDailyBars(ctx, instrument, side, from, to)
	case GranularityW1, GranularityMN1:
		dailyBars, err := c.downloadDailyBars(ctx, instrument, side, from, to)
		if err != nil {
			return nil, err
		}
		return AggregateBars(dailyBars, granularity, from, to)
	default:
		return nil, fmt.Errorf("unsupported bar granularity %q", granularity)
	}
}

func (c *Client) downloadMinuteBars(ctx context.Context, instrument Instrument, side PriceSide, from time.Time, to time.Time) ([]Bar, error) {
	days := make([]time.Time, 0)
	for current := midnightUTC(from); current.Before(to); current = current.AddDate(0, 0, 1) {
		if !IsCryptoSymbol(instrument.Code) {
			weekday := current.UTC().Weekday()
			if weekday == time.Saturday {
				continue
			}
			if weekday == time.Sunday && looksLikeEquitySymbol(instrument.Code) {
				continue
			}
		}
		days = append(days, current)
	}

	return downloadParallel(c, ctx, len(days), func(ctx context.Context, idx int) ([]Bar, int64, error) {
		current := days[idx]
		return c.fetchBarChunk(ctx, instrument, side, current, from, to, "minute",
			func() ([]string, error) {
				return c.datafeedBarSegments(instrument, side, current, "_candles_min_1.bi5")
			},
			func() []string {
				return []string{
					"v1", "candles", "minute", instrument.Code, string(side),
					strconv.Itoa(current.Year()),
					strconv.Itoa(int(current.Month())),
					strconv.Itoa(current.Day()),
				}
			})
	}, "minute", func(idx int) string {
		return days[idx].Format("2006-01-02")
	})
}

func (c *Client) downloadHourlyBars(ctx context.Context, instrument Instrument, side PriceSide, from time.Time, to time.Time) ([]Bar, error) {
	months := make([]time.Time, 0)
	for current := monthStartUTC(from); current.Before(to); current = current.AddDate(0, 1, 0) {
		months = append(months, current)
	}

	return downloadParallel(c, ctx, len(months), func(ctx context.Context, idx int) ([]Bar, int64, error) {
		current := months[idx]
		var payload candlePayload
		n, err := c.getJSONWithBytes(ctx, []string{
			"v1", "candles", "hour", instrument.Code, string(side),
			strconv.Itoa(current.Year()),
			strconv.Itoa(int(current.Month())),
		}, &payload)
		if err != nil {
			return nil, 0, err
		}
		return filterBars(decodeBars(payload), from, to), n, nil
	}, "hour", func(idx int) string {
		return months[idx].Format("2006-01")
	})
}

func (c *Client) downloadDailyBars(ctx context.Context, instrument Instrument, side PriceSide, from time.Time, to time.Time) ([]Bar, error) {
	years := make([]int, 0)
	for current := from.UTC().Year(); current <= to.UTC().Year(); current++ {
		years = append(years, current)
	}

	return downloadParallel(c, ctx, len(years), func(ctx context.Context, idx int) ([]Bar, int64, error) {
		year := years[idx]
		var payload candlePayload
		n, err := c.getJSONWithBytes(ctx, []string{
			"v1", "candles", "day", instrument.Code, string(side),
			strconv.Itoa(year),
		}, &payload)
		if err != nil {
			return nil, 0, err
		}
		return filterBars(decodeBars(payload), from, to), n, nil
	}, "day", func(idx int) string {
		return strconv.Itoa(years[idx])
	})
}

// fetchBarChunk downloads a single bar chunk for a given day using either the datafeed or jetta engine.
func (c *Client) fetchBarChunk(
	ctx context.Context,
	instrument Instrument,
	side PriceSide,
	current time.Time,
	from time.Time,
	to time.Time,
	scope string,
	datafeedSegments func() ([]string, error),
	jettaSegments func() []string,
) ([]Bar, int64, error) {
	if c.engine == EngineDatafeed {
		segments, err := datafeedSegments()
		if err != nil {
			return nil, 0, err
		}
		bytesData, err := c.getRawBytes(ctx, segments)
		if err != nil || len(bytesData) == 0 {
			return nil, 0, err
		}
		decoded, err := DecodeBarsBi5(bytes.NewReader(bytesData), current, instrument.PriceScale)
		if err != nil {
			return nil, 0, err
		}
		return filterBars(decoded, from, to), int64(len(bytesData)), nil
	}

	var payload candlePayload
	n, err := c.getJSONWithBytes(ctx, jettaSegments(), &payload)
	if err != nil {
		return nil, 0, err
	}
	return filterBars(decodeBars(payload), from, to), n, nil
}

// datafeedBarSegments builds URL segments for the datafeed engine bar endpoint.
func (c *Client) datafeedBarSegments(instrument Instrument, side PriceSide, current time.Time, suffix string) ([]string, error) {
	symbolClean := formatDatafeedSymbol(instrument.Code)
	monthStr := fmt.Sprintf("%02d", int(current.Month())-1)
	dayStr := fmt.Sprintf("%02d", current.Day())
	return []string{
		"datafeed", symbolClean,
		strconv.Itoa(current.Year()),
		monthStr,
		dayStr,
		string(side) + suffix,
	}, nil
}
