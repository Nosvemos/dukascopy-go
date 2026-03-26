package dukascopy

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"sort"
	"strings"
	"time"
)

type Granularity string

const (
	GranularityTick Granularity = "tick"
	GranularityM1   Granularity = "m1"
	GranularityM3   Granularity = "m3"
	GranularityM5   Granularity = "m5"
	GranularityM15  Granularity = "m15"
	GranularityM30  Granularity = "m30"
	GranularityH1   Granularity = "h1"
	GranularityH4   Granularity = "h4"
	GranularityD1   Granularity = "d1"
	GranularityW1   Granularity = "w1"
	GranularityMN1  Granularity = "mn1"
)

type PriceSide string

const (
	PriceSideBid PriceSide = "BID"
	PriceSideAsk PriceSide = "ASK"
)

type ResultKind string

const (
	ResultKindBar  ResultKind = "bar"
	ResultKindTick ResultKind = "tick"
)

type Instrument struct {
	ID          int    `json:"id"`
	Name        string `json:"name"`
	Code        string `json:"code"`
	Description string `json:"description"`
	PriceScale  int    `json:"priceScale"`
}

type instrumentsResponse struct {
	Instruments []Instrument `json:"instruments"`
}

type DownloadRequest struct {
	Symbol      string
	Granularity Granularity
	Side        PriceSide
	From        time.Time
	To          time.Time
}

type Bar struct {
	Time   time.Time
	Open   float64
	High   float64
	Low    float64
	Close  float64
	Volume float64
}

type Tick struct {
	Time      time.Time
	Ask       float64
	Bid       float64
	AskVolume float64
	BidVolume float64
}

type DownloadResult struct {
	Kind       ResultKind
	Instrument Instrument
	Bars       []Bar
	Ticks      []Tick
	BidBars    []Bar
	AskBars    []Bar
}

type Client struct {
	baseURL    *url.URL
	httpClient *http.Client
}

func NewClient(rawBaseURL string, timeout time.Duration) *Client {
	parsed, err := url.Parse(strings.TrimRight(strings.TrimSpace(rawBaseURL), "/"))
	if err != nil {
		panic(err)
	}

	return &Client{
		baseURL: parsed,
		httpClient: &http.Client{
			Timeout: timeout,
		},
	}
}

func (c *Client) ListInstruments(ctx context.Context) ([]Instrument, error) {
	var payload instrumentsResponse
	if err := c.getJSON(ctx, []string{"v1", "instruments"}, &payload); err != nil {
		return nil, err
	}

	sort.Slice(payload.Instruments, func(i, j int) bool {
		return payload.Instruments[i].Name < payload.Instruments[j].Name
	})

	return payload.Instruments, nil
}

func (c *Client) Download(ctx context.Context, request DownloadRequest) (DownloadResult, error) {
	instruments, err := c.ListInstruments(ctx)
	if err != nil {
		return DownloadResult{}, err
	}

	instrument, err := ResolveInstrument(instruments, request.Symbol)
	if err != nil {
		return DownloadResult{}, err
	}

	side, err := normalizeSide(request.Side)
	if err != nil {
		return DownloadResult{}, err
	}

	switch normalizeGranularity(request.Granularity) {
	case GranularityTick:
		ticks, err := c.downloadTicks(ctx, instrument, request.From, request.To)
		if err != nil {
			return DownloadResult{}, err
		}
		return DownloadResult{Kind: ResultKindTick, Instrument: instrument, Ticks: ticks}, nil
	default:
		bars, err := c.downloadBars(ctx, instrument, side, request.Granularity, request.From, request.To)
		if err != nil {
			return DownloadResult{}, err
		}
		return DownloadResult{Kind: ResultKindBar, Instrument: instrument, Bars: bars}, nil
	}
}

func (c *Client) DownloadBarsForSide(ctx context.Context, request DownloadRequest, side PriceSide) (Instrument, []Bar, error) {
	instruments, err := c.ListInstruments(ctx)
	if err != nil {
		return Instrument{}, nil, err
	}

	instrument, err := ResolveInstrument(instruments, request.Symbol)
	if err != nil {
		return Instrument{}, nil, err
	}

	normalizedSide, err := normalizeSide(side)
	if err != nil {
		return Instrument{}, nil, err
	}

	bars, err := c.downloadBars(ctx, instrument, normalizedSide, request.Granularity, request.From, request.To)
	return instrument, bars, err
}

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
	var all []Bar
	for current := midnightUTC(from); current.Before(to); current = current.AddDate(0, 0, 1) {
		var payload candlePayload
		if err := c.getJSON(ctx, []string{
			"v1", "candles", "minute", instrument.Code, string(side),
			fmt.Sprintf("%d", current.Year()),
			fmt.Sprintf("%d", int(current.Month())),
			fmt.Sprintf("%d", current.Day()),
		}, &payload); err != nil {
			return nil, err
		}
		all = append(all, filterBars(decodeBars(payload), from, to)...)
	}
	return all, nil
}

func (c *Client) downloadHourlyBars(ctx context.Context, instrument Instrument, side PriceSide, from time.Time, to time.Time) ([]Bar, error) {
	var all []Bar
	for current := monthStartUTC(from); current.Before(to); current = current.AddDate(0, 1, 0) {
		var payload candlePayload
		if err := c.getJSON(ctx, []string{
			"v1", "candles", "hour", instrument.Code, string(side),
			fmt.Sprintf("%d", current.Year()),
			fmt.Sprintf("%d", int(current.Month())),
		}, &payload); err != nil {
			return nil, err
		}
		all = append(all, filterBars(decodeBars(payload), from, to)...)
	}
	return all, nil
}

func (c *Client) downloadDailyBars(ctx context.Context, instrument Instrument, side PriceSide, from time.Time, to time.Time) ([]Bar, error) {
	var all []Bar
	for year := from.Year(); year <= to.Add(-time.Nanosecond).Year(); year++ {
		var payload candlePayload
		if err := c.getJSON(ctx, []string{
			"v1", "candles", "day", instrument.Code, string(side),
			fmt.Sprintf("%d", year),
		}, &payload); err != nil {
			return nil, err
		}
		all = append(all, filterBars(decodeBars(payload), from, to)...)
	}
	return all, nil
}

func (c *Client) downloadTicks(ctx context.Context, instrument Instrument, from time.Time, to time.Time) ([]Tick, error) {
	var all []Tick
	for current := hourStartUTC(from); current.Before(to); current = current.Add(time.Hour) {
		var payload tickPayload
		if err := c.getJSON(ctx, []string{
			"v1", "ticks", instrument.Code,
			fmt.Sprintf("%d", current.Year()),
			fmt.Sprintf("%d", int(current.Month())),
			fmt.Sprintf("%d", current.Day()),
			fmt.Sprintf("%d", current.Hour()),
		}, &payload); err != nil {
			return nil, err
		}
		all = append(all, filterTicks(decodeTicks(payload), from, to)...)
	}
	return all, nil
}

func (c *Client) getJSON(ctx context.Context, segments []string, target any) error {
	requestURL := *c.baseURL
	requestURL.Path = path.Join(append([]string{c.baseURL.Path}, segments...)...)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, requestURL.String(), nil)
	if err != nil {
		return err
	}

	res, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	if res.StatusCode < 200 || res.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(res.Body, 512))
		return fmt.Errorf("dukascopy api %s returned %s: %s", requestURL.String(), res.Status, strings.TrimSpace(string(body)))
	}

	if err := json.NewDecoder(res.Body).Decode(target); err != nil {
		return fmt.Errorf("decode %s: %w", requestURL.String(), err)
	}
	return nil
}
