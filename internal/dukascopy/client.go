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
	"sync"
	"time"
	"bufio"
	"os"
	"path/filepath"
	"flag"
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

type ProgressEvent struct {
	Kind       string
	Scope      string
	Current    int
	Total      int
	Detail     string
	Attempt    int
	MaxAttempt int
}

type ProgressFunc func(ProgressEvent)

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
	baseURL     *url.URL
	httpClient  *http.Client
	maxRetries  int
	backoff     time.Duration
	rateLimit   time.Duration
	progress    ProgressFunc
	rateMu      sync.Mutex
	nextSlot    time.Time
	cacheMu     sync.RWMutex
	instruments []Instrument
	proxyPool   *ProxyPool
}

func NewClient(rawBaseURL string, timeout time.Duration) *Client {
	parsed, err := url.Parse(strings.TrimRight(strings.TrimSpace(rawBaseURL), "/"))
	if err != nil {
		panic(err)
	}

	pool := &ProxyPool{}
	transport := &http.Transport{
		Proxy: pool.GetNextProxy,
	}

	return &Client{
		baseURL: parsed,
		httpClient: &http.Client{
			Timeout:   timeout,
			Transport: transport,
		},
		proxyPool:  pool,
		maxRetries: 3,
		backoff:    500 * time.Millisecond,
	}
}

func (c *Client) WithRetries(maxRetries int) *Client {
	if maxRetries < 0 {
		maxRetries = 0
	}
	c.maxRetries = maxRetries
	return c
}

func (c *Client) WithBackoff(backoff time.Duration) *Client {
	if backoff <= 0 {
		backoff = 500 * time.Millisecond
	}
	c.backoff = backoff
	return c
}

func (c *Client) WithRateLimit(rateLimit time.Duration) *Client {
	if rateLimit < 0 {
		rateLimit = 0
	}
	c.rateLimit = rateLimit
	return c
}

func (c *Client) WithProgress(progress ProgressFunc) *Client {
	c.progress = progress
	return c
}

func (c *Client) ListInstruments(ctx context.Context) ([]Instrument, error) {
	c.cacheMu.RLock()
	if len(c.instruments) > 0 {
		cached := cloneInstruments(c.instruments)
		c.cacheMu.RUnlock()
		return cached, nil
	}
	c.cacheMu.RUnlock()

	c.cacheMu.Lock()
	defer c.cacheMu.Unlock()
	if len(c.instruments) > 0 {
		return cloneInstruments(c.instruments), nil
	}

	if cached, ok := loadLocalCache(); ok && len(cached) > 0 {
		c.instruments = cloneInstruments(cached)
		return cloneInstruments(c.instruments), nil
	}

	var payload instrumentsResponse
	if err := c.getJSON(ctx, []string{"v1", "instruments"}, &payload); err != nil {
		return nil, err
	}

	sort.Slice(payload.Instruments, func(i, j int) bool {
		return payload.Instruments[i].Name < payload.Instruments[j].Name
	})

	c.instruments = cloneInstruments(payload.Instruments)
	go saveLocalCache(c.instruments)

	return cloneInstruments(c.instruments), nil
}

func (c *Client) LoadProxies(path string) error {
	return c.proxyPool.LoadFromFile(path)
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
	days := make([]time.Time, 0)
	for current := midnightUTC(from); current.Before(to); current = current.AddDate(0, 0, 1) {
		if !IsCryptoSymbol(instrument.Code) && current.UTC().Weekday() == time.Saturday {
			continue
		}
		days = append(days, current)
	}

	for index, current := range days {
		c.emitProgress(ProgressEvent{
			Kind:    "chunk",
			Scope:   "minute",
			Current: index + 1,
			Total:   len(days),
			Detail:  current.Format("2006-01-02"),
		})
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
	months := make([]time.Time, 0)
	for current := monthStartUTC(from); current.Before(to); current = current.AddDate(0, 1, 0) {
		months = append(months, current)
	}

	for index, current := range months {
		c.emitProgress(ProgressEvent{
			Kind:    "chunk",
			Scope:   "hour",
			Current: index + 1,
			Total:   len(months),
			Detail:  current.Format("2006-01"),
		})
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
	years := make([]int, 0)
	for year := from.Year(); year <= to.Add(-time.Nanosecond).Year(); year++ {
		years = append(years, year)
	}

	for index, year := range years {
		c.emitProgress(ProgressEvent{
			Kind:    "chunk",
			Scope:   "day",
			Current: index + 1,
			Total:   len(years),
			Detail:  fmt.Sprintf("%d", year),
		})
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
	hours := make([]time.Time, 0)
	for current := hourStartUTC(from); current.Before(to); current = current.Add(time.Hour) {
		if IsMarketClosed(instrument.Code, current) {
			continue
		}
		hours = append(hours, current)
	}

	for index, current := range hours {
		c.emitProgress(ProgressEvent{
			Kind:    "chunk",
			Scope:   "tick",
			Current: index + 1,
			Total:   len(hours),
			Detail:  current.Format(time.RFC3339),
		})
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

	var lastErr error
	for attempt := 0; attempt <= c.maxRetries; attempt++ {
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, requestURL.String(), nil)
		if err := c.waitForRateLimit(ctx); err != nil {
			return err
		}

		res, err := c.httpClient.Do(req)
		if err == nil {
			func() {
				defer res.Body.Close()
				if res.StatusCode >= 200 && res.StatusCode < 300 {
					lastErr = json.NewDecoder(res.Body).Decode(target)
					if lastErr != nil {
						lastErr = fmt.Errorf("decode %s: %w", requestURL.String(), lastErr)
					}
					return
				}

				body, _ := io.ReadAll(io.LimitReader(res.Body, 512))
				lastErr = fmt.Errorf("dukascopy api %s returned %s: %s", requestURL.String(), res.Status, strings.TrimSpace(string(body)))
				if !shouldRetryResponse(res.StatusCode, body) {
					attempt = c.maxRetries
				}
			}()
		} else {
			lastErr = err
		}

		if lastErr == nil {
			return nil
		}

		if attempt == c.maxRetries {
			break
		}

		c.emitProgress(ProgressEvent{
			Kind:       "retry",
			Scope:      "http",
			Detail:     requestURL.String(),
			Attempt:    attempt + 1,
			MaxAttempt: c.maxRetries + 1,
		})

		wait := c.backoff * time.Duration(attempt+1)
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}

	return lastErr
}

func shouldRetryStatus(statusCode int) bool {
	return statusCode == http.StatusTooManyRequests || statusCode >= 500
}

func shouldRetryResponse(statusCode int, body []byte) bool {
	if shouldRetryStatus(statusCode) {
		return true
	}

	payload := strings.ToLower(strings.TrimSpace(string(body)))
	if payload == "" {
		return false
	}

	return strings.Contains(payload, `"statuscode":500`) ||
		strings.Contains(payload, `"error":"internal server error"`) ||
		strings.Contains(payload, `"message":"failed to load historical`)
}

func (c *Client) waitForRateLimit(ctx context.Context) error {
	if c.rateLimit <= 0 {
		return nil
	}

	c.rateMu.Lock()
	slot := time.Now()
	if c.nextSlot.After(slot) {
		slot = c.nextSlot
	}
	c.nextSlot = slot.Add(c.rateLimit)
	c.rateMu.Unlock()

	wait := time.Until(slot)
	if wait <= 0 {
		return nil
	}

	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func (c *Client) emitProgress(event ProgressEvent) {
	if c.progress == nil {
		return
	}
	c.progress(event)
}

func cloneInstruments(instruments []Instrument) []Instrument {
	if len(instruments) == 0 {
		return nil
	}

	cloned := make([]Instrument, len(instruments))
	copy(cloned, instruments)
	return cloned
}

func IsCryptoSymbol(symbol string) bool {
	symbol = strings.ToUpper(strings.TrimSpace(symbol))
	replacer := strings.NewReplacer("/", "", "-", "", "_", "", " ", "", ".", "")
	symbol = replacer.Replace(symbol)
	if symbol == "" {
		return false
	}
	cryptoPrefixes := []string{
		"BTC", "ETH", "LTC", "XRP", "BCH", "ADA", "DOT", "SOL", "DOGE", "XLM", "LINK", "AVAX", "USDT",
	}
	for _, prefix := range cryptoPrefixes {
		if strings.HasPrefix(symbol, prefix) {
			return true
		}
	}
	return false
}

func IsMarketClosed(symbol string, t time.Time) bool {
	if IsCryptoSymbol(symbol) {
		return false
	}
	t = t.UTC()
	weekday := t.Weekday()
	hour := t.Hour()

	if weekday == time.Friday {
		return hour >= 22
	}
	if weekday == time.Saturday {
		return true
	}
	if weekday == time.Sunday {
		return hour < 22
	}
	return false
}

type ProxyPool struct {
	mu      sync.Mutex
	proxies []*url.URL
	current int
}

func (p *ProxyPool) LoadFromFile(path string) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()

	p.mu.Lock()
	defer p.mu.Unlock()

	p.proxies = nil
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		if !strings.Contains(line, "://") {
			line = "http://" + line
		}
		u, err := url.Parse(line)
		if err == nil {
			p.proxies = append(p.proxies, u)
		}
	}
	return scanner.Err()
}

func (p *ProxyPool) GetNextProxy(req *http.Request) (*url.URL, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if len(p.proxies) == 0 {
		return nil, nil
	}
	u := p.proxies[p.current]
	p.current = (p.current + 1) % len(p.proxies)
	return u, nil
}

var localCacheFilePath string = ""

type localCachePayload struct {
	Timestamp   time.Time    `json:"timestamp"`
	Instruments []Instrument `json:"instruments"`
}

func getLocalCachePath() string {
	if localCacheFilePath != "" {
		return localCacheFilePath
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return ""
	}
	dir := filepath.Join(home, ".dukascopy")
	_ = os.MkdirAll(dir, 0o755)
	return filepath.Join(dir, "instruments_cache.json")
}

func loadLocalCache() ([]Instrument, bool) {
	if localCacheFilePath == "" && (flag.Lookup("test.v") != nil || os.Getenv("DUKASCOPY_TEST_ENV") == "true") {
		return nil, false
	}
	path := getLocalCachePath()
	if path == "" {
		return nil, false
	}
	info, err := os.Stat(path)
	if err != nil {
		return nil, false
	}
	if time.Since(info.ModTime()) > 24*time.Hour {
		return nil, false
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, false
	}
	defer file.Close()

	var payload localCachePayload
	if err := json.NewDecoder(file).Decode(&payload); err != nil {
		return nil, false
	}
	if time.Since(payload.Timestamp) > 24*time.Hour {
		return nil, false
	}
	return payload.Instruments, true
}

func saveLocalCache(instruments []Instrument) {
	if localCacheFilePath == "" && (flag.Lookup("test.v") != nil || os.Getenv("DUKASCOPY_TEST_ENV") == "true") {
		return
	}
	path := getLocalCachePath()
	if path == "" {
		return
	}
	file, err := os.Create(path)
	if err != nil {
		return
	}
	defer file.Close()

	payload := localCachePayload{
		Timestamp:   time.Now().UTC(),
		Instruments: instruments,
	}
	_ = json.NewEncoder(file).Encode(payload)
}


