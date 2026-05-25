package dukascopy

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path"
	"strings"
	"sync"
	"time"
)

type Engine string

const (
	EngineJetta    Engine = "jetta"
	EngineDatafeed Engine = "datafeed"
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
	Rows       int
	Bytes      int64
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
	engine      Engine
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
		engine:     EngineJetta,
	}
}

func (c *Client) WithEngine(engine Engine) *Client {
	if engine == "" {
		engine = EngineJetta
	}
	c.engine = engine
	return c
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

func (c *Client) LoadProxies(path string) error {
	return c.proxyPool.LoadFromFile(path)
}

type countingReader struct {
	io.Reader
	count *int64
}

func (cr *countingReader) Read(p []byte) (int, error) {
	n, err := cr.Reader.Read(p)
	*cr.count += int64(n)
	return n, err
}

func isNoDataError(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "returned 404") ||
		strings.Contains(msg, "returned 400") ||
		strings.Contains(msg, "failed to load historical") ||
		strings.Contains(msg, "not found") ||
		strings.Contains(msg, "bad request")
}

func (c *Client) getJSON(ctx context.Context, segments []string, target any) error {
	_, err := c.getJSONWithBytes(ctx, segments, target)
	return err
}

func (c *Client) getJSONWithBytes(ctx context.Context, segments []string, target any) (int64, error) {
	requestURL := *c.baseURL
	requestURL.Path = path.Join(append([]string{c.baseURL.Path}, segments...)...)

	var lastErr error
	for attempt := 0; attempt <= c.maxRetries; attempt++ {
		var attemptBytes int64
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, requestURL.String(), nil)
		if err := c.waitForRateLimit(ctx); err != nil {
			return 0, err
		}

		res, err := c.httpClient.Do(req)
		if err == nil {
			func() {
				defer res.Body.Close()
				if res.StatusCode >= 200 && res.StatusCode < 300 {
					reader := &countingReader{Reader: res.Body, count: &attemptBytes}
					lastErr = json.NewDecoder(reader).Decode(target)
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
			return attemptBytes, nil
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
			return 0, ctx.Err()
		case <-timer.C:
		}
	}

	return 0, lastErr
}

func (c *Client) getRawBytes(ctx context.Context, segments []string) ([]byte, error) {
	requestURL := *c.baseURL
	if c.engine == EngineDatafeed && (requestURL.Host == "jetta.dukascopy.com" || requestURL.Host == "") {
		requestURL.Host = "datafeed.dukascopy.com"
	}
	requestURL.Path = path.Join(append([]string{c.baseURL.Path}, segments...)...)

	var lastErr error
	for attempt := 0; attempt <= c.maxRetries; attempt++ {
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, requestURL.String(), nil)
		if err := c.waitForRateLimit(ctx); err != nil {
			return nil, err
		}

		res, err := c.httpClient.Do(req)
		if err == nil {
			if res.StatusCode == http.StatusOK {
				body, err := io.ReadAll(res.Body)
				res.Body.Close()
				if err != nil {
					lastErr = err
				} else {
					return body, nil
				}
			} else if res.StatusCode == http.StatusNotFound {
				res.Body.Close()
				return nil, nil
			} else {
				body, _ := io.ReadAll(io.LimitReader(res.Body, 512))
				res.Body.Close()
				lastErr = fmt.Errorf("datafeed api %s returned %s: %s", requestURL.String(), res.Status, strings.TrimSpace(string(body)))
				if !shouldRetryResponse(res.StatusCode, body) {
					attempt = c.maxRetries
				}
			}
		} else {
			lastErr = err
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
			return nil, ctx.Err()
		case <-timer.C:
		}
	}

	return nil, lastErr
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

func cloneInstruments(instruments []Instrument) []Instrument {
	if len(instruments) == 0 {
		return nil
	}

	cloned := make([]Instrument, len(instruments))
	copy(cloned, instruments)
	return cloned
}

func formatDatafeedSymbol(code string) string {
	return strings.ToUpper(strings.NewReplacer("/", "", "-", "", "_", "", " ", "", ".", "").Replace(code))
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
