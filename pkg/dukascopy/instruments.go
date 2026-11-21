package dukascopy

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

var (
	localCacheFilePath string = ""
	cachePathMu        sync.RWMutex
)

const localCacheTTL = 24 * time.Hour

func setLocalCacheFilePath(path string) {
	cachePathMu.Lock()
	defer cachePathMu.Unlock()
	localCacheFilePath = path
}

func getLocalCacheFilePath() string {
	cachePathMu.RLock()
	defer cachePathMu.RUnlock()
	return localCacheFilePath
}

func isTestEnvironment() bool {
	return flag.Lookup("test.v") != nil || os.Getenv("DUKASCOPY_TEST_ENV") == "true"
}

type localCachePayload struct {
	Timestamp   time.Time    `json:"timestamp"`
	Instruments []Instrument `json:"instruments"`
}

// ListInstruments returns the instruments known to Dukascopy, sorted by
// name. Results are cached in memory and in an on-disk cache
// (~/.dukascopy/instruments_cache.json, valid for 24h) unless
// WithForceUpdate(true) is set.
//
// Outside tests, if the API call fails, DefaultInstruments is returned as a
// fallback. The fallback is kept in memory for this client only and is never
// written to the on-disk cache.
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

	if !c.forceUpdate {
		if cached, ok := loadLocalCache(); ok && len(cached) > 0 {
			c.instruments = cloneInstruments(cached)
			return cloneInstruments(c.instruments), nil
		}
	}

	var payload instrumentsResponse
	fetchErr := c.getJSON(ctx, []string{"v1", "instruments"}, &payload)
	testEnv := isTestEnvironment()
	instruments, persist, err := buildInstrumentList(payload.Instruments, fetchErr, !testEnv, !testEnv)
	if err != nil {
		return nil, err
	}

	c.instruments = cloneInstruments(instruments)
	c.resolvedSymbols = make(map[string]Instrument)
	if persist {
		// Synchronous so the write cannot be cut off at process exit. A
		// failed cache write is not fatal: the list is still usable.
		_ = saveLocalCache(c.instruments)
	}

	return cloneInstruments(c.instruments), nil
}

// buildInstrumentList turns the result of the instruments API call into the
// list to use, and reports whether that list may be persisted to the on-disk
// cache. Only a successful fetch is persisted; the DefaultInstruments
// fallback never is.
func buildInstrumentList(fetched []Instrument, fetchErr error, allowFallback bool, mergeDefaults bool) ([]Instrument, bool, error) {
	var instruments []Instrument
	persist := false
	if fetchErr != nil {
		if !allowFallback {
			return nil, false, fetchErr
		}
		instruments = cloneInstruments(DefaultInstruments)
	} else {
		instruments = cloneInstruments(fetched)
		persist = true
		if mergeDefaults {
			existingCodes := make(map[string]bool, len(instruments))
			for _, inst := range instruments {
				existingCodes[compactSymbol(inst.Code)] = true
			}
			for _, inst := range DefaultInstruments {
				if !existingCodes[compactSymbol(inst.Code)] {
					instruments = append(instruments, inst)
				}
			}
		}
	}

	sort.Slice(instruments, func(i, j int) bool {
		return instruments[i].Name < instruments[j].Name
	})
	return instruments, persist, nil
}

var cryptoPrefixes = []string{
	"BTC", "ETH", "LTC", "XRP", "BCH", "ADA", "DOT", "SOL", "DOGE", "XLM", "LINK", "AVAX", "USDT",
}

// IsCryptoSymbol reports whether symbol looks like a cryptocurrency pair
// (for example "BTC/USD" or "ETHUSD"). Crypto markets trade around the
// clock, so no weekend hours are skipped for them. Six-letter FX codes such
// as USDTRY or USDTHB are not mistaken for USDT (Tether) pairs.
func IsCryptoSymbol(symbol string) bool {
	symbol = compactSymbol(symbol)
	if symbol == "" {
		return false
	}
	for _, prefix := range cryptoPrefixes {
		if !strings.HasPrefix(symbol, prefix) {
			continue
		}
		if prefix == "USDT" && len(symbol) == 6 {
			// USD + a three-letter currency starting with T (TRY, THB, TWD...).
			continue
		}
		return true
	}
	return false
}

// IsMarketClosed reports whether the hour starting at t is certainly
// outside FX trading hours, so downloads can skip it. Crypto symbols are
// never closed. For everything else the closed window is Friday 22:00 UTC
// to Sunday 21:00 UTC, which is closed both during US daylight saving time
// (market opens Sunday 21:00 UTC) and outside it (Sunday 22:00 UTC).
//
// It does not model exchange sessions for indices or stocks: hours without
// data are simply answered with 404 by Dukascopy and yield no rows.
func IsMarketClosed(symbol string, t time.Time) bool {
	if IsCryptoSymbol(symbol) {
		return false
	}
	t = t.UTC()
	switch t.Weekday() {
	case time.Friday:
		return t.Hour() >= 22
	case time.Saturday:
		return true
	case time.Sunday:
		return t.Hour() < 21
	default:
		return false
	}
}

// getLocalCachePath returns the cache file path. It has no side effects; the
// directory is created only when the cache is written.
func getLocalCachePath() string {
	path := getLocalCacheFilePath()
	if path != "" {
		return path
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return ""
	}
	return filepath.Join(home, ".dukascopy", "instruments_cache.json")
}

func loadLocalCache() ([]Instrument, bool) {
	if getLocalCacheFilePath() == "" && isTestEnvironment() {
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
	if time.Since(info.ModTime()) > localCacheTTL {
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
	if time.Since(payload.Timestamp) > localCacheTTL {
		return nil, false
	}
	return payload.Instruments, true
}

// saveLocalCache writes the cache atomically: it writes a temporary file in
// the same directory and renames it over the cache file.
func saveLocalCache(instruments []Instrument) error {
	if getLocalCacheFilePath() == "" && isTestEnvironment() {
		return nil
	}
	path := getLocalCachePath()
	if path == "" {
		return errors.New("dukascopy: no instruments cache path")
	}
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}

	tmp, err := os.CreateTemp(dir, filepath.Base(path)+".*.tmp")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	cleanup := func() { _ = os.Remove(tmpName) }

	payload := localCachePayload{
		Timestamp:   time.Now().UTC(),
		Instruments: instruments,
	}
	if err := json.NewEncoder(tmp).Encode(payload); err != nil {
		_ = tmp.Close()
		cleanup()
		return err
	}
	if err := tmp.Close(); err != nil {
		cleanup()
		return err
	}
	if err := os.Rename(tmpName, path); err != nil {
		cleanup()
		return err
	}
	return nil
}
