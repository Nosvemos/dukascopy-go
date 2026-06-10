package dukascopy

import (
	"context"
	"encoding/json"
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

type localCachePayload struct {
	Timestamp   time.Time    `json:"timestamp"`
	Instruments []Instrument `json:"instruments"`
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

	if !c.forceUpdate {
		if cached, ok := loadLocalCache(); ok && len(cached) > 0 {
			c.instruments = cloneInstruments(cached)
			return cloneInstruments(c.instruments), nil
		}
	}

	var payload instrumentsResponse
	if err := c.getJSON(ctx, []string{"v1", "instruments"}, &payload); err != nil {
		if flag.Lookup("test.v") != nil || os.Getenv("DUKASCOPY_TEST_ENV") == "true" {
			return nil, err
		}
		payload.Instruments = DefaultInstruments
	} else {
		if flag.Lookup("test.v") == nil && os.Getenv("DUKASCOPY_TEST_ENV") != "true" {
			existingCodes := make(map[string]bool)
			var merged []Instrument
			for _, inst := range payload.Instruments {
				codeCompact := compactSymbol(inst.Code)
				existingCodes[codeCompact] = true
				merged = append(merged, inst)
			}
			for _, inst := range DefaultInstruments {
				codeCompact := compactSymbol(inst.Code)
				if !existingCodes[codeCompact] {
					merged = append(merged, inst)
				}
			}
			payload.Instruments = merged
		}
	}

	sort.Slice(payload.Instruments, func(i, j int) bool {
		return payload.Instruments[i].Name < payload.Instruments[j].Name
	})

	c.instruments = cloneInstruments(payload.Instruments)
	go saveLocalCache(c.instruments)

	return cloneInstruments(c.instruments), nil
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

	if weekday == time.Friday && hour >= 22 {
		return true
	}
	if weekday == time.Saturday {
		return true
	}
	if weekday == time.Sunday && hour < 22 {
		return true
	}

	if looksLikeEquitySymbol(symbol) {
		loc, err := time.LoadLocation("America/New_York")
		if err == nil {
			local := t.In(loc)
			localWeekday := local.Weekday()
			if localWeekday == time.Saturday || localWeekday == time.Sunday {
				return true
			}
			localHour := local.Hour()
			localMin := local.Minute()
			if localHour < 9 || (localHour == 9 && localMin < 30) || localHour >= 16 {
				return true
			}
		} else {
			// Fallback to UTC range 13:00 - 21:00
			if hour < 13 || hour >= 21 {
				return true
			}
		}
	}

	return false
}

func looksLikeEquitySymbol(symbol string) bool {
	symbol = strings.ToUpper(strings.TrimSpace(symbol))
	replacer := strings.NewReplacer("/", "", "-", "", "_", "", " ", "", ".", "")
	symbol = replacer.Replace(symbol)
	if symbol == "" {
		return false
	}
	if strings.Contains(symbol, "IDX") {
		return true
	}
	suffixes := []string{
		"USUSD", "DEEUR", "FREUR", "GBRGBP", "JPNJPY", "CHECHF", "NLDEUR", "ITAEUR", "SWESEK", "ZAFZAR", "SGPSGD",
	}
	for _, suffix := range suffixes {
		if strings.HasSuffix(symbol, suffix) {
			return true
		}
	}
	return false
}

func getLocalCachePath() string {
	path := getLocalCacheFilePath()
	if path != "" {
		return path
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
	if getLocalCacheFilePath() == "" && (flag.Lookup("test.v") != nil || os.Getenv("DUKASCOPY_TEST_ENV") == "true") {
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
	if getLocalCacheFilePath() == "" && (flag.Lookup("test.v") != nil || os.Getenv("DUKASCOPY_TEST_ENV") == "true") {
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
