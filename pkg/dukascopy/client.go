package dukascopy

import (
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

// Engine selects which Dukascopy backend the client downloads from.
type Engine string

const (
	// EngineJetta uses the JSON API (the default).
	EngineJetta Engine = "jetta"
	// EngineDatafeed uses the LZMA-compressed .bi5 datafeed files.
	EngineDatafeed Engine = "datafeed"
)

// Granularity is the timeframe of requested data: ticks or a bar timeframe.
type Granularity string

// Supported granularities. Use NormalizeGranularity to accept aliases such as
// "minute", "hour" or "month".
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

// PriceSide selects bid or ask prices. Functions in this package accept any
// casing and surrounding whitespace ("ask", " Ask ") and normalize it.
type PriceSide string

const (
	// PriceSideBid selects bid prices.
	PriceSideBid PriceSide = "BID"
	// PriceSideAsk selects ask prices.
	PriceSideAsk PriceSide = "ASK"
)

// ResultKind tells whether a DownloadResult holds bars or ticks.
type ResultKind string

const (
	// ResultKindBar marks a result carrying Bars.
	ResultKindBar ResultKind = "bar"
	// ResultKindTick marks a result carrying Ticks.
	ResultKindTick ResultKind = "tick"
)

// Instrument describes a tradable Dukascopy instrument.
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

// DownloadRequest describes a download over the half-open range [From, To).
type DownloadRequest struct {
	Symbol      string
	Granularity Granularity
	Side        PriceSide
	From        time.Time
	To          time.Time
}

// ProgressEvent reports download progress. Kind is "chunk" for a finished
// chunk or "retry" for an HTTP retry.
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

// ProgressFunc receives progress events.
//
// The function may be called concurrently from several download workers
// (retry events are emitted from inside workers), so implementations must be
// safe for concurrent use.
type ProgressFunc func(ProgressEvent)

// Bar is one OHLCV candle. Time is the UTC start of the bar interval.
type Bar struct {
	Time      time.Time
	Open      float64
	High      float64
	Low       float64
	Close     float64
	Volume    float64
	TickCount int64
}

// Tick is one quote update.
type Tick struct {
	Time      time.Time
	Ask       float64
	Bid       float64
	AskVolume float64
	BidVolume float64
}

// DownloadResult is the result of Client.Download. Bars is set when Kind is
// ResultKindBar and Ticks when Kind is ResultKindTick.
type DownloadResult struct {
	Kind       ResultKind
	Instrument Instrument
	Bars       []Bar
	Ticks      []Tick
	BidBars    []Bar
	AskBars    []Bar
}

// Client downloads historical market data from Dukascopy.
//
// Configure a Client with the With* methods before its first use; the
// methods are not safe to call concurrently with downloads. Download methods
// themselves are safe for concurrent use.
type Client struct {
	baseURL         *url.URL
	httpClient      *http.Client
	maxRetries      int
	backoff         time.Duration
	rateLimit       time.Duration
	adaptiveRate    time.Duration
	forceUpdate     bool
	progress        ProgressFunc
	rateMu          sync.Mutex
	nextSlot        time.Time
	cacheMu         sync.RWMutex
	instruments     []Instrument
	resolvedSymbols map[string]Instrument
	proxyPool       *ProxyPool
	engine          Engine
}

// Default transport timeouts used by NewClient. The overall per-request
// deadline is the timeout passed to NewClient.
const (
	defaultDialTimeout           = 30 * time.Second
	defaultTLSHandshakeTimeout   = 10 * time.Second
	defaultResponseHeaderTimeout = 60 * time.Second
)

// NewClient creates a client for the API at rawBaseURL (for example
// "https://jetta.dukascopy.com"). The URL must use the http or https scheme
// and include a host. timeout bounds each HTTP request; zero means no
// overall limit, although the default transport still applies dial, TLS
// handshake and response-header timeouts.
func NewClient(rawBaseURL string, timeout time.Duration) (*Client, error) {
	parsed, err := url.Parse(strings.TrimRight(strings.TrimSpace(rawBaseURL), "/"))
	if err != nil {
		return nil, fmt.Errorf("dukascopy: parse base url %q: %w", rawBaseURL, err)
	}
	if err := validateBaseURL(parsed); err != nil {
		return nil, fmt.Errorf("dukascopy: invalid base url %q: %w", rawBaseURL, err)
	}

	pool := &ProxyPool{}
	dialer := &net.Dialer{
		Timeout:   defaultDialTimeout,
		KeepAlive: 30 * time.Second,
	}
	transport := &http.Transport{
		Proxy:                 pool.GetNextProxy,
		DialContext:           dialer.DialContext,
		MaxIdleConnsPerHost:   16,
		MaxConnsPerHost:       32,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   defaultTLSHandshakeTimeout,
		ResponseHeaderTimeout: defaultResponseHeaderTimeout,
		ExpectContinueTimeout: 1 * time.Second,
		DisableCompression:    false,
		ForceAttemptHTTP2:     true,
	}

	return &Client{
		baseURL: parsed,
		httpClient: &http.Client{
			Timeout:   timeout,
			Transport: transport,
		},
		proxyPool:       pool,
		maxRetries:      3,
		backoff:         500 * time.Millisecond,
		engine:          EngineJetta,
		resolvedSymbols: make(map[string]Instrument),
	}, nil
}

func validateBaseURL(u *url.URL) error {
	if u == nil {
		return fmt.Errorf("base url is not set")
	}
	switch strings.ToLower(u.Scheme) {
	case "http", "https":
	case "":
		return fmt.Errorf("missing scheme (expected http:// or https://)")
	default:
		return fmt.Errorf("unsupported scheme %q (expected http or https)", u.Scheme)
	}
	if u.Host == "" {
		return fmt.Errorf("missing host")
	}
	return nil
}

// WithEngine selects the download backend. An empty engine means EngineJetta.
func (c *Client) WithEngine(engine Engine) *Client {
	if engine == "" {
		engine = EngineJetta
	}
	c.engine = engine
	return c
}

// WithRetries sets how many times a failed request is retried (negative
// values mean 0). Only network errors, 429 and 5xx responses are retried.
func (c *Client) WithRetries(maxRetries int) *Client {
	if maxRetries < 0 {
		maxRetries = 0
	}
	c.maxRetries = maxRetries
	return c
}

// WithBackoff sets the base retry delay (non-positive values mean 500ms).
// Retries use exponential backoff with full jitter, capped at 30s, unless
// the server sends a Retry-After header on a 429 or 503 response.
func (c *Client) WithBackoff(backoff time.Duration) *Client {
	if backoff <= 0 {
		backoff = 500 * time.Millisecond
	}
	c.backoff = backoff
	return c
}

// WithRateLimit sets the minimum interval between requests (negative values
// mean 0, no limit). The effective interval adapts upward on 429, 5xx and
// network errors and decays back toward this value on success.
func (c *Client) WithRateLimit(rateLimit time.Duration) *Client {
	if rateLimit < 0 {
		rateLimit = 0
	}
	c.rateLimit = rateLimit
	c.adaptiveRate = rateLimit
	return c
}

// WithProgress registers a progress callback. See ProgressFunc for the
// concurrency contract.
func (c *Client) WithProgress(progress ProgressFunc) *Client {
	c.progress = progress
	return c
}

// WithForceUpdate makes ListInstruments skip the on-disk instruments cache
// and fetch the list from the API.
func (c *Client) WithForceUpdate(force bool) *Client {
	c.forceUpdate = force
	return c
}

// LoadProxies loads a proxy list (one proxy per line) and rotates requests
// through it. It returns an error, and leaves the current proxy list
// unchanged, if any line is invalid or the file contains no proxies. See
// ProxyPool.LoadFromFile for the accepted format.
func (c *Client) LoadProxies(path string) error {
	return c.proxyPool.LoadFromFile(path)
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
