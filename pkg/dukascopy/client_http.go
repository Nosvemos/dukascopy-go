package dukascopy

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"path"
	"strings"
	"time"
)

func (c *Client) adjustRateLimit(isError bool, statusCode int) {
	c.rateMu.Lock()
	defer c.rateMu.Unlock()

	if c.adaptiveRate == 0 {
		c.adaptiveRate = c.rateLimit
	}

	if isError {
		if statusCode == 429 {
			if c.adaptiveRate == 0 {
				c.adaptiveRate = 200 * time.Millisecond
			} else {
				c.adaptiveRate *= 2
			}
			if c.adaptiveRate > 5*time.Second {
				c.adaptiveRate = 5 * time.Second
			}
		} else {
			c.adaptiveRate += 50 * time.Millisecond
			if c.adaptiveRate > 2*time.Second {
				c.adaptiveRate = 2 * time.Second
			}
		}
	} else {
		if c.adaptiveRate > c.rateLimit {
			c.adaptiveRate -= 10 * time.Millisecond
			if c.adaptiveRate < c.rateLimit {
				c.adaptiveRate = c.rateLimit
			}
		}
	}
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
						c.adjustRateLimit(true, res.StatusCode)
					} else {
						c.adjustRateLimit(false, 0)
					}
					return
				}

				body, _ := io.ReadAll(io.LimitReader(res.Body, 512))
				lastErr = fmt.Errorf("dukascopy api %s returned %s: %s", requestURL.String(), res.Status, strings.TrimSpace(string(body)))
				c.adjustRateLimit(true, res.StatusCode)
				if !shouldRetryResponse(res.StatusCode, body) {
					attempt = c.maxRetries
				}
			}()
		} else {
			lastErr = err
			c.adjustRateLimit(true, 0)
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
					c.adjustRateLimit(true, 0)
				} else {
					c.adjustRateLimit(false, 0)
					return body, nil
				}
			} else if res.StatusCode == http.StatusNotFound {
				res.Body.Close()
				c.adjustRateLimit(false, 0)
				return nil, nil
			} else {
				body, _ := io.ReadAll(io.LimitReader(res.Body, 512))
				res.Body.Close()
				lastErr = fmt.Errorf("datafeed api %s returned %s: %s", requestURL.String(), res.Status, strings.TrimSpace(string(body)))
				c.adjustRateLimit(true, res.StatusCode)
				if !shouldRetryResponse(res.StatusCode, body) {
					attempt = c.maxRetries
				}
			}
		} else {
			lastErr = err
			c.adjustRateLimit(true, 0)
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
