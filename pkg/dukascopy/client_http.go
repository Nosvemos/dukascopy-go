package dukascopy

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"path"
	"strconv"
	"strings"
	"time"
)

// ErrNoData reports that Dukascopy has no data for a requested chunk. The
// API signals this with HTTP 404. Test for it with errors.Is; an
// *HTTPStatusError with StatusCode 404 matches it.
var ErrNoData = errors.New("dukascopy: no data available")

// HTTPStatusError is returned when the API answers with a non-2xx status
// after all retries. Body holds at most the first 512 bytes of the response.
type HTTPStatusError struct {
	StatusCode int
	URL        string
	Body       string

	retryAfter time.Duration
}

func (e *HTTPStatusError) Error() string {
	msg := fmt.Sprintf("dukascopy api %s returned %d %s", e.URL, e.StatusCode, http.StatusText(e.StatusCode))
	if e.Body != "" {
		msg += ": " + e.Body
	}
	return msg
}

// Is makes errors.Is(err, ErrNoData) true for a 404 response.
func (e *HTTPStatusError) Is(target error) bool {
	return target == ErrNoData && e.StatusCode == http.StatusNotFound
}

const (
	userAgent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36"

	errorBodyLimit   = 512
	drainBodyLimit   = 64 << 10
	maxRetryDelay    = 30 * time.Second
	maxRetryAfter    = 2 * time.Minute
	maxAdaptiveRate  = 5 * time.Second
	maxSoftErrorRate = 2 * time.Second
)

// maxRawBodyBytes bounds a single datafeed download. It is a variable so
// tests can lower it.
var maxRawBodyBytes int64 = 64 << 20

type rateSignal int

const (
	// rateSuccess: 2xx or 404 (a normal "no data" answer).
	rateSuccess rateSignal = iota
	// rateThrottled: 429 Too Many Requests.
	rateThrottled
	// rateServerError: 5xx, network or decode error.
	rateServerError
	// rateNeutral: other client errors, which say nothing about load.
	rateNeutral
)

func rateSignalForStatus(statusCode int) rateSignal {
	switch {
	case statusCode >= 200 && statusCode < 300, statusCode == http.StatusNotFound:
		return rateSuccess
	case statusCode == http.StatusTooManyRequests:
		return rateThrottled
	case statusCode >= 500:
		return rateServerError
	default:
		return rateNeutral
	}
}

func (c *Client) adjustRateLimit(signal rateSignal) {
	c.rateMu.Lock()
	defer c.rateMu.Unlock()

	if c.adaptiveRate == 0 {
		c.adaptiveRate = c.rateLimit
	}

	switch signal {
	case rateThrottled:
		if c.adaptiveRate == 0 {
			c.adaptiveRate = 200 * time.Millisecond
		} else {
			c.adaptiveRate *= 2
		}
		if c.adaptiveRate > maxAdaptiveRate {
			c.adaptiveRate = maxAdaptiveRate
		}
	case rateServerError:
		next := c.adaptiveRate + 50*time.Millisecond
		if next > maxSoftErrorRate {
			// The soft-error cap never lowers an interval raised by 429s.
			next = max(c.adaptiveRate, maxSoftErrorRate)
		}
		c.adaptiveRate = next
	case rateSuccess:
		if c.adaptiveRate > c.rateLimit {
			c.adaptiveRate -= 10 * time.Millisecond
			if c.adaptiveRate < c.rateLimit {
				c.adaptiveRate = c.rateLimit
			}
		}
	}
}

// isNoDataError reports whether err means "no data for this chunk", which
// download workers turn into an empty chunk. Only HTTP 404 qualifies.
func isNoDataError(err error) bool {
	return err != nil && errors.Is(err, ErrNoData)
}

func (c *Client) getJSON(ctx context.Context, segments []string, target any) error {
	_, err := c.getJSONWithBytes(ctx, segments, target)
	return err
}

func (c *Client) getJSONWithBytes(ctx context.Context, segments []string, target any) (int64, error) {
	if c.baseURL == nil {
		return 0, errors.New("dukascopy: client has no base url")
	}
	requestURL := *c.baseURL
	requestURL.Path = path.Join(append([]string{c.baseURL.Path}, segments...)...)

	var total int64
	err := c.doWithRetry(ctx, requestURL.String(), "application/json, text/plain, */*", func(body io.Reader) error {
		var n int64
		reader := &countingReader{Reader: body, count: &n}
		err := json.NewDecoder(reader).Decode(target)
		total = n
		if err != nil {
			return fmt.Errorf("decode %s: %w", requestURL.String(), err)
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	return total, nil
}

// getRawBytes downloads a datafeed file. A 404 yields (nil, nil).
func (c *Client) getRawBytes(ctx context.Context, segments []string) ([]byte, error) {
	if c.baseURL == nil {
		return nil, errors.New("dukascopy: client has no base url")
	}
	requestURL := *c.baseURL
	if c.engine == EngineDatafeed && (requestURL.Host == "jetta.dukascopy.com" || requestURL.Host == "") {
		requestURL.Host = "datafeed.dukascopy.com"
	}
	requestURL.Path = path.Join(append([]string{c.baseURL.Path}, segments...)...)

	var data []byte
	err := c.doWithRetry(ctx, requestURL.String(), "*/*", func(body io.Reader) error {
		limit := maxRawBodyBytes
		b, err := io.ReadAll(io.LimitReader(body, limit+1))
		if err != nil {
			return err
		}
		if int64(len(b)) > limit {
			return &bodyTooLargeError{url: requestURL.String(), limit: limit}
		}
		data = b
		return nil
	})
	if err != nil {
		if isNoDataError(err) {
			return nil, nil
		}
		return nil, err
	}
	return data, nil
}

type bodyTooLargeError struct {
	url   string
	limit int64
}

func (e *bodyTooLargeError) Error() string {
	return fmt.Sprintf("dukascopy: response from %s exceeds %d bytes", e.url, e.limit)
}

// doWithRetry performs a GET with rate limiting and retries. onOK consumes
// the body of a 2xx response; an error from it is retried unless it is a
// *bodyTooLargeError or the context is done.
func (c *Client) doWithRetry(ctx context.Context, requestURL string, accept string, onOK func(io.Reader) error) error {
	for attempt := 0; ; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, requestURL, nil)
		if err != nil {
			return fmt.Errorf("dukascopy: build request for %s: %w", requestURL, err)
		}
		req.Header.Set("User-Agent", userAgent)
		req.Header.Set("Accept", accept)
		req.Header.Set("Accept-Language", "en-US,en;q=0.9")
		if err := c.waitForRateLimit(ctx); err != nil {
			return err
		}

		var (
			lastErr    error
			retryable  bool
			retryAfter time.Duration
		)
		res, err := c.httpClient.Do(req)
		if err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return ctxErr
			}
			lastErr = err
			retryable = true
			c.adjustRateLimit(rateServerError)
		} else {
			retryable, retryAfter, lastErr = c.handleResponse(res, requestURL, onOK)
		}

		if lastErr == nil {
			return nil
		}
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		if !retryable || attempt >= c.maxRetries {
			return lastErr
		}

		c.emitProgress(ProgressEvent{
			Kind:       "retry",
			Scope:      "http",
			Detail:     requestURL,
			Attempt:    attempt + 1,
			MaxAttempt: c.maxRetries + 1,
		})

		if err := sleepContext(ctx, c.retryDelay(attempt, retryAfter)); err != nil {
			return err
		}
	}
}

// handleResponse consumes and closes res. It returns whether a failed
// attempt may be retried, any server-provided Retry-After delay, and the
// attempt error.
func (c *Client) handleResponse(res *http.Response, requestURL string, onOK func(io.Reader) error) (bool, time.Duration, error) {
	defer closeBody(res.Body)

	if res.StatusCode >= 200 && res.StatusCode < 300 {
		if err := onOK(res.Body); err != nil {
			var tooLarge *bodyTooLargeError
			if errors.As(err, &tooLarge) {
				return false, 0, err
			}
			c.adjustRateLimit(rateServerError)
			return true, 0, err
		}
		c.adjustRateLimit(rateSuccess)
		return false, 0, nil
	}

	body, _ := io.ReadAll(io.LimitReader(res.Body, errorBodyLimit))
	statusErr := &HTTPStatusError{
		StatusCode: res.StatusCode,
		URL:        requestURL,
		Body:       strings.TrimSpace(string(body)),
	}
	if res.StatusCode == http.StatusTooManyRequests || res.StatusCode == http.StatusServiceUnavailable {
		statusErr.retryAfter = parseRetryAfter(res.Header.Get("Retry-After"), time.Now())
	}
	retryable := shouldRetryResponse(res.StatusCode, body)
	signal := rateSignalForStatus(res.StatusCode)
	if signal == rateNeutral && retryable {
		// A 4xx wrapping an internal server error is server load, not a
		// client mistake.
		signal = rateServerError
	}
	c.adjustRateLimit(signal)
	return retryable, statusErr.retryAfter, statusErr
}

// closeBody drains a bounded amount of the body so the connection can be
// reused, then closes it.
func closeBody(body io.ReadCloser) {
	_, _ = io.CopyN(io.Discard, body, drainBodyLimit)
	_ = body.Close()
}

// retryDelay returns the wait before retry number attempt+1: the server's
// Retry-After when present (capped at maxRetryAfter), otherwise exponential
// backoff with full jitter capped at maxRetryDelay.
func (c *Client) retryDelay(attempt int, retryAfter time.Duration) time.Duration {
	if retryAfter > 0 {
		return min(retryAfter, maxRetryAfter)
	}
	ceiling := backoffCeiling(c.backoff, attempt)
	if ceiling <= 0 {
		return 0
	}
	return rand.N(ceiling + 1)
}

func backoffCeiling(base time.Duration, attempt int) time.Duration {
	if base <= 0 {
		return 0
	}
	ceiling := base
	for i := 0; i < attempt; i++ {
		if ceiling >= maxRetryDelay/2 {
			return maxRetryDelay
		}
		ceiling *= 2
	}
	return min(ceiling, maxRetryDelay)
}

// parseRetryAfter parses a Retry-After header in delay-seconds or HTTP-date
// form. It returns 0 when the header is absent, invalid or in the past.
func parseRetryAfter(value string, now time.Time) time.Duration {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0
	}
	if seconds, err := strconv.Atoi(value); err == nil {
		if seconds <= 0 {
			return 0
		}
		return time.Duration(seconds) * time.Second
	}
	if when, err := http.ParseTime(value); err == nil {
		if d := when.Sub(now); d > 0 {
			return d
		}
	}
	return 0
}

func sleepContext(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func (c *Client) waitForRateLimit(ctx context.Context) error {
	c.rateMu.Lock()
	limit := c.rateLimit
	if c.adaptiveRate > 0 {
		limit = c.adaptiveRate
	}
	c.rateMu.Unlock()

	if limit <= 0 {
		return nil
	}

	c.rateMu.Lock()
	slot := time.Now()
	if c.nextSlot.After(slot) {
		slot = c.nextSlot
	}
	c.nextSlot = slot.Add(limit)
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
