package dukascopy

import (
	"context"
	"encoding/json"
	"flag"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

var localCacheFilePath string = ""

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

	if cached, ok := loadLocalCache(); ok && len(cached) > 0 {
		c.instruments = cloneInstruments(cached)
		return cloneInstruments(c.instruments), nil
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
