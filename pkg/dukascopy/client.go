package dukascopy

import (
	"net/http"
	"net/url"
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
	baseURL      *url.URL
	httpClient   *http.Client
	maxRetries   int
	backoff      time.Duration
	rateLimit    time.Duration
	adaptiveRate time.Duration
	forceUpdate  bool
	progress     ProgressFunc
	rateMu       sync.Mutex
	nextSlot     time.Time
	cacheMu      sync.RWMutex
	instruments  []Instrument
	proxyPool    *ProxyPool
	engine       Engine
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
	c.adaptiveRate = rateLimit
	return c
}

func (c *Client) WithProgress(progress ProgressFunc) *Client {
	c.progress = progress
	return c
}

func (c *Client) WithForceUpdate(force bool) *Client {
	c.forceUpdate = force
	return c
}

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
