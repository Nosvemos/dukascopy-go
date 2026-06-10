package csvout

import (
	"path/filepath"
	"strings"
	"time"
)

func IsExpectedMarketClosureGap(previous time.Time, current time.Time, expectedInterval time.Duration) bool {
	return IsExpectedGapForProfile(previous, current, expectedInterval, "", MarketProfileOTC24x5)
}

func IsExpectedGapForProfile(previous time.Time, current time.Time, expectedInterval time.Duration, symbol string, profile string) bool {
	if expectedInterval <= 0 || !current.After(previous) || current.Sub(previous) <= expectedInterval {
		return false
	}

	switch ResolveGapMarketProfile(symbol, profile) {
	case MarketProfileAlways, MarketProfileCrypto24x7:
		return false
	case MarketProfileFX24x5:
		return isExpectedFXGap(previous, current, expectedInterval)
	case MarketProfileOTC24x5:
		return isExpectedOTCGap(previous, current, expectedInterval)
	case MarketProfileEquity:
		return isExpectedEquityGap(previous, current, expectedInterval)
	default:
		return isExpectedOTCGap(previous, current, expectedInterval)
	}
}

func isExpectedOTCGap(previous time.Time, current time.Time, expectedInterval time.Duration) bool {
	probe := previous.Add(expectedInterval).UTC()
	for probe.Before(current) {
		if !isLikelyOTCMarketClosed(probe) {
			return false
		}
		next := nextLikelyOTCClosureBoundary(probe)
		if !next.After(probe) {
			return false
		}
		probe = next
	}
	return true
}

func isExpectedFXGap(previous time.Time, current time.Time, expectedInterval time.Duration) bool {
	probe := previous.Add(expectedInterval).UTC()
	for probe.Before(current) {
		if !isLikelyFXMarketClosed(probe) {
			return false
		}
		next := nextLikelyFXClosureBoundary(probe)
		if !next.After(probe) {
			return false
		}
		probe = next
	}
	return true
}

func isLikelyOTCMarketClosed(timestamp time.Time) bool {
	if isLikelyHolidayMarketClosed(timestamp, MarketProfileOTC24x5) {
		return true
	}
	local := timestamp.In(gapMarketLocation())
	switch local.Weekday() {
	case time.Saturday:
		return true
	case time.Sunday:
		return local.Hour() < 18
	case time.Friday:
		return local.Hour() > 16 || (local.Hour() == 16 && local.Minute() >= 59)
	case time.Monday, time.Tuesday, time.Wednesday, time.Thursday:
		return local.Hour() == 17 || (local.Hour() == 16 && local.Minute() >= 59)
	default:
		return false
	}
}

func nextLikelyOTCClosureBoundary(timestamp time.Time) time.Time {
	if next, ok := nextHolidayClosureBoundary(timestamp, MarketProfileOTC24x5); ok {
		return next
	}
	local := timestamp.In(gapMarketLocation())
	switch local.Weekday() {
	case time.Friday:
		if local.Hour() > 16 || (local.Hour() == 16 && local.Minute() >= 59) {
			return time.Date(local.Year(), local.Month(), local.Day()+1, 0, 0, 0, 0, local.Location()).UTC()
		}
	case time.Saturday:
		return time.Date(local.Year(), local.Month(), local.Day()+1, 0, 0, 0, 0, local.Location()).UTC()
	case time.Sunday:
		if local.Hour() < 18 {
			return time.Date(local.Year(), local.Month(), local.Day(), 18, 0, 0, 0, local.Location()).UTC()
		}
	case time.Monday, time.Tuesday, time.Wednesday, time.Thursday:
		if local.Hour() == 16 && local.Minute() >= 59 {
			return time.Date(local.Year(), local.Month(), local.Day(), 17, 0, 0, 0, local.Location()).UTC()
		}
		if local.Hour() == 17 {
			return time.Date(local.Year(), local.Month(), local.Day(), 18, 0, 0, 0, local.Location()).UTC()
		}
	}
	return timestamp
}

func ResolveGapMarketProfile(symbol string, explicitProfile string) string {
	profile := strings.ToLower(strings.TrimSpace(explicitProfile))
	switch profile {
	case "", MarketProfileAuto:
	case MarketProfileFX24x5, MarketProfileOTC24x5, MarketProfileCrypto24x7, MarketProfileAlways, MarketProfileEquity:
		return profile
	default:
		return MarketProfileOTC24x5
	}

	if looksLikeCryptoSymbol(symbol) {
		return MarketProfileCrypto24x7
	}
	if looksLikeMetalSymbol(symbol) {
		return MarketProfileOTC24x5
	}
	if looksLikeForexSymbol(symbol) {
		return MarketProfileFX24x5
	}
	if looksLikeEquitySymbol(symbol) {
		return MarketProfileEquity
	}
	return MarketProfileOTC24x5
}

func defaultGapSymbol(path string, explicit string) string {
	if strings.TrimSpace(explicit) != "" {
		return normalizeGapSymbol(explicit)
	}
	return inferSymbolHintFromPath(path)
}

func inferSymbolHintFromPath(path string) string {
	base := strings.ToLower(filepath.Base(strings.TrimSpace(path)))
	base = strings.TrimSuffix(base, ".gz")
	base = strings.TrimSuffix(base, ".csv")
	base = strings.TrimSuffix(base, ".parquet")
	parts := strings.FieldsFunc(base, func(r rune) bool {
		return r == '-' || r == '_' || r == '.' || r == ' '
	})
	for _, part := range parts {
		normalized := normalizeGapSymbol(part)
		if len(normalized) >= 6 && len(normalized) <= 12 {
			return normalized
		}
	}
	return ""
}

func normalizeGapSymbol(value string) string {
	value = strings.ToUpper(strings.TrimSpace(value))
	replacer := strings.NewReplacer("/", "", "-", "", "_", "", " ", "", ".", "")
	return replacer.Replace(value)
}

func looksLikeCryptoSymbol(symbol string) bool {
	symbol = normalizeGapSymbol(symbol)
	if symbol == "" {
		return false
	}
	cryptoPrefixes := []string{
		"BTC", "ETH", "LTC", "XRP", "BCH", "ADA", "DOT", "SOL", "DOGE", "XLM", "LINK", "AVAX",
	}
	for _, prefix := range cryptoPrefixes {
		if strings.HasPrefix(symbol, prefix) {
			return true
		}
	}
	return false
}

func looksLikeMetalSymbol(symbol string) bool {
	symbol = normalizeGapSymbol(symbol)
	if len(symbol) < 3 {
		return false
	}
	metals := []string{"XAU", "XAG", "XPT", "XPD"}
	for _, metal := range metals {
		if strings.HasPrefix(symbol, metal) {
			return true
		}
	}
	return false
}

func looksLikeForexSymbol(symbol string) bool {
	symbol = normalizeGapSymbol(symbol)
	if len(symbol) != 6 {
		return false
	}
	codes := map[string]struct{}{
		"USD": {}, "EUR": {}, "GBP": {}, "JPY": {}, "CHF": {}, "AUD": {}, "NZD": {}, "CAD": {},
		"SEK": {}, "NOK": {}, "DKK": {}, "SGD": {}, "HKD": {}, "TRY": {}, "PLN": {}, "CZK": {},
		"HUF": {}, "MXN": {}, "ZAR": {}, "CNH": {},
	}
	_, leftOK := codes[symbol[:3]]
	_, rightOK := codes[symbol[3:]]
	return leftOK && rightOK
}

func isLikelyFXMarketClosed(timestamp time.Time) bool {
	if isLikelyHolidayMarketClosed(timestamp, MarketProfileFX24x5) {
		return true
	}
	local := timestamp.In(gapMarketLocation())
	switch local.Weekday() {
	case time.Saturday:
		return true
	case time.Sunday:
		return local.Hour() < 17
	case time.Friday:
		return local.Hour() > 16 || (local.Hour() == 16 && local.Minute() >= 59)
	default:
		return false
	}
}

func nextLikelyFXClosureBoundary(timestamp time.Time) time.Time {
	if next, ok := nextHolidayClosureBoundary(timestamp, MarketProfileFX24x5); ok {
		return next
	}
	local := timestamp.In(gapMarketLocation())
	switch local.Weekday() {
	case time.Friday:
		if local.Hour() > 16 || (local.Hour() == 16 && local.Minute() >= 59) {
			return time.Date(local.Year(), local.Month(), local.Day()+1, 0, 0, 0, 0, local.Location()).UTC()
		}
	case time.Saturday:
		return time.Date(local.Year(), local.Month(), local.Day()+1, 0, 0, 0, 0, local.Location()).UTC()
	case time.Sunday:
		if local.Hour() < 17 {
			return time.Date(local.Year(), local.Month(), local.Day(), 17, 0, 0, 0, local.Location()).UTC()
		}
	}
	return timestamp
}

func gapMarketLocation() *time.Location {
	if location, err := time.LoadLocation("America/New_York"); err == nil {
		return location
	}
	return time.UTC
}

func isExpectedEquityGap(previous time.Time, current time.Time, expectedInterval time.Duration) bool {
	probe := previous.Add(expectedInterval).UTC()
	for probe.Before(current) {
		if !isLikelyEquityMarketClosed(probe) {
			return false
		}
		next := nextLikelyEquityClosureBoundary(probe)
		if !next.After(probe) {
			return false
		}
		probe = next
	}
	return true
}

func isLikelyEquityMarketClosed(timestamp time.Time) bool {
	if isLikelyHolidayMarketClosed(timestamp, MarketProfileEquity) {
		return true
	}
	local := timestamp.In(gapMarketLocation())
	if local.Weekday() == time.Saturday || local.Weekday() == time.Sunday {
		return true
	}

	hour := local.Hour()
	min := local.Minute()

	if hour < 9 || (hour == 9 && min < 30) {
		return true
	}
	if hour >= 16 {
		return true
	}
	return false
}

func nextLikelyEquityClosureBoundary(timestamp time.Time) time.Time {
	if next, ok := nextHolidayClosureBoundary(timestamp, MarketProfileEquity); ok {
		return next
	}
	local := timestamp.In(gapMarketLocation())
	weekday := local.Weekday()

	if weekday == time.Friday && (local.Hour() >= 16) {
		return time.Date(local.Year(), local.Month(), local.Day()+3, 9, 30, 0, 0, local.Location()).UTC()
	}
	if weekday == time.Saturday {
		return time.Date(local.Year(), local.Month(), local.Day()+2, 9, 30, 0, 0, local.Location()).UTC()
	}
	if weekday == time.Sunday {
		return time.Date(local.Year(), local.Month(), local.Day()+1, 9, 30, 0, 0, local.Location()).UTC()
	}

	hour := local.Hour()
	min := local.Minute()
	if hour >= 16 {
		return time.Date(local.Year(), local.Month(), local.Day()+1, 9, 30, 0, 0, local.Location()).UTC()
	}
	if hour < 9 || (hour == 9 && min < 30) {
		return time.Date(local.Year(), local.Month(), local.Day(), 9, 30, 0, 0, local.Location()).UTC()
	}

	return timestamp
}

func looksLikeEquitySymbol(symbol string) bool {
	symbol = normalizeGapSymbol(symbol)
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
