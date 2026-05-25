package csvout

import (
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"
)

type CSVStats struct {
	Path                       string
	Format                     string
	Compressed                 bool
	GapProfile                 string
	GapSymbol                  string
	Columns                    []string
	Rows                       int
	FirstTimestamp             time.Time
	LastTimestamp              time.Time
	HasTimestamp               bool
	DuplicateRows              int
	DuplicateStamps            int
	OutOfOrderRows             int
	GapCount                   int
	MissingIntervals           int
	ExpectedInterval           string
	LargestGap                 string
	ExpectedGapCount           int
	ExpectedMissingIntervals   int
	ExpectedLargestGap         string
	SuspiciousGapCount         int
	SuspiciousMissingIntervals int
	SuspiciousLargestGap       string
	SuspiciousGaps             []GapDetail
	InferredTimeframe          string
}

type InspectOptions struct {
	Symbol                  string
	MarketProfile           string
	IncludeSuspiciousGaps   bool
	MaxSuspiciousGapDetails int
}

type gapObservation struct {
	Previous time.Time
	Current  time.Time
	Interval time.Duration
}

type GapDetail struct {
	PreviousTimestamp time.Time
	CurrentTimestamp  time.Time
	MissingFrom       time.Time
	MissingTo         time.Time
	MissingIntervals  int
	Interval          string
}

const (
	MarketProfileAuto       = "auto"
	MarketProfileFX24x5     = "fx-24x5"
	MarketProfileOTC24x5    = "otc-24x5"
	MarketProfileCrypto24x7 = "crypto-24x7"
	MarketProfileAlways     = "always"
)

func AuditCSV(path string) (FileAudit, error) {
	if isParquetPath(path) {
		return auditParquet(path)
	}
	file, err := os.Open(path)
	if err != nil {
		return FileAudit{}, err
	}
	defer file.Close()

	info, err := file.Stat()
	if err != nil {
		return FileAudit{}, err
	}

	hasher := sha256.New()
	rawReader := io.TeeReader(file, hasher)
	readCloser := io.NopCloser(rawReader)
	if isGzipPath(path) {
		gzipReader, err := gzip.NewReader(rawReader)
		if err != nil {
			return FileAudit{}, err
		}
		readCloser = gzipReader
	}
	defer readCloser.Close()

	reader := csvReaderFactory(readCloser)
	if _, err := reader.Read(); err != nil {
		if errors.Is(err, io.EOF) {
			return FileAudit{Bytes: info.Size(), SHA256: hex.EncodeToString(hasher.Sum(nil))}, nil
		}
		return FileAudit{}, err
	}

	rows := 0
	for {
		record, readErr := reader.Read()
		if errors.Is(readErr, io.EOF) {
			break
		}
		if readErr != nil {
			return FileAudit{}, readErr
		}
		if len(record) == 0 {
			continue
		}
		rows++
	}

	return FileAudit{
		Rows:   rows,
		Bytes:  info.Size(),
		SHA256: hex.EncodeToString(hasher.Sum(nil)),
	}, nil
}

func InspectCSV(path string) (CSVStats, error) {
	return InspectCSVWithOptions(path, InspectOptions{})
}

func InspectCSVWithOptions(path string, options InspectOptions) (CSVStats, error) {
	if isParquetPath(path) {
		return inspectParquetWithOptions(path, options)
	}
	_, reader, closeReader, err := openCSVReader(path)
	if err != nil {
		return CSVStats{}, err
	}
	defer closeReader()

	header, err := reader.Read()
	if err != nil {
		return CSVStats{}, err
	}

	stats := CSVStats{
		Path:       path,
		Format:     "csv",
		Compressed: isGzipPath(path),
		GapSymbol:  defaultGapSymbol(path, options.Symbol),
		Columns:    cloneColumns(header),
	}
	stats.GapProfile = ResolveGapMarketProfile(stats.GapSymbol, options.MarketProfile)
	timestampIndex := indexOfColumn(header, "timestamp")
	stats.HasTimestamp = timestampIndex >= 0

	seenRows := make(map[string]int)
	seenTimestamps := make(map[string]int)
	var intervals []time.Duration
	var gapObservations []gapObservation
	var previousTimestamp time.Time

	for {
		record, readErr := reader.Read()
		if errors.Is(readErr, io.EOF) {
			break
		}
		if readErr != nil {
			return CSVStats{}, readErr
		}
		if len(record) == 0 {
			continue
		}

		stats.Rows++
		rowKey := strings.Join(record, "\x1f")
		if seenRows[rowKey] > 0 {
			stats.DuplicateRows++
		}
		seenRows[rowKey]++

		if !stats.HasTimestamp || timestampIndex >= len(record) {
			continue
		}

		timestamp, err := time.Parse(timestampLayout, record[timestampIndex])
		if err != nil {
			return CSVStats{}, fmt.Errorf("parse CSV timestamp %q: %w", record[timestampIndex], err)
		}
		timestamp = timestamp.UTC()
		if stats.FirstTimestamp.IsZero() || timestamp.Before(stats.FirstTimestamp) {
			stats.FirstTimestamp = timestamp
		}
		if stats.LastTimestamp.IsZero() || timestamp.After(stats.LastTimestamp) {
			stats.LastTimestamp = timestamp
		}

		stampKey := timestamp.Format(timestampLayout)
		if seenTimestamps[stampKey] > 0 {
			stats.DuplicateStamps++
		}
		seenTimestamps[stampKey]++

		if !previousTimestamp.IsZero() {
			delta := timestamp.Sub(previousTimestamp)
			if delta > 0 {
				intervals = append(intervals, delta)
				gapObservations = append(gapObservations, gapObservation{
					Previous: previousTimestamp,
					Current:  timestamp,
					Interval: delta,
				})
			} else if delta < 0 {
				stats.OutOfOrderRows++
			}
		}
		previousTimestamp = timestamp
	}

	expectedInterval := inferExpectedInterval(intervals)
	if expectedInterval > 0 {
		stats.ExpectedInterval = expectedInterval.String()
	}
	stats.InferredTimeframe = inferTimeframe(intervals)

	applyGapStats(&stats, gapObservations, expectedInterval, stats.GapSymbol, stats.GapProfile, options)

	return stats, nil
}

func inferTimeframe(intervals []time.Duration) string {
	best := inferExpectedInterval(intervals)
	if best <= 0 {
		return "unknown"
	}

	switch best {
	case time.Millisecond:
		return "1ms"
	case time.Second:
		return "1s"
	case time.Minute:
		return "m1"
	case 3 * time.Minute:
		return "m3"
	case 5 * time.Minute:
		return "m5"
	case 15 * time.Minute:
		return "m15"
	case 30 * time.Minute:
		return "m30"
	case time.Hour:
		return "h1"
	case 4 * time.Hour:
		return "h4"
	case 24 * time.Hour:
		return "d1"
	case 7 * 24 * time.Hour:
		return "w1"
	default:
		return best.String()
	}
}

func inferExpectedInterval(intervals []time.Duration) time.Duration {
	if len(intervals) == 0 {
		return 0
	}

	counts := make(map[time.Duration]int)
	best := time.Duration(0)
	bestCount := 0
	for _, interval := range intervals {
		counts[interval]++
		if counts[interval] > bestCount {
			best = interval
			bestCount = counts[interval]
		}
	}
	return best
}

func estimateMissingIntervals(interval time.Duration, expected time.Duration) int {
	if expected <= 0 || interval <= expected {
		return 0
	}
	missing := int(interval/expected) - 1
	if missing < 1 {
		return 1
	}
	return missing
}

func applyGapStats(stats *CSVStats, observations []gapObservation, expectedInterval time.Duration, symbol string, profile string, options InspectOptions) {
	if stats == nil || expectedInterval <= 0 {
		return
	}

	profile = ResolveGapMarketProfile(symbol, profile)
	recurringPatterns := recurringExpectedGapPatterns(observations, expectedInterval, symbol, profile)

	var (
		largestGap           time.Duration
		largestExpectedGap   time.Duration
		largestSuspiciousGap time.Duration
	)

	for _, observation := range observations {
		if observation.Interval <= expectedInterval {
			continue
		}

		missing := estimateMissingIntervals(observation.Interval, expectedInterval)
		stats.GapCount++
		stats.MissingIntervals += missing
		if observation.Interval > largestGap {
			largestGap = observation.Interval
		}

		if IsExpectedGapForProfile(observation.Previous, observation.Current, expectedInterval, symbol, profile) ||
			recurringPatterns[gapPatternKey(observation)] {
			stats.ExpectedGapCount++
			stats.ExpectedMissingIntervals += missing
			if observation.Interval > largestExpectedGap {
				largestExpectedGap = observation.Interval
			}
			continue
		}

		stats.SuspiciousGapCount++
		stats.SuspiciousMissingIntervals += missing
		if observation.Interval > largestSuspiciousGap {
			largestSuspiciousGap = observation.Interval
		}
		if options.IncludeSuspiciousGaps && shouldAppendGapDetail(stats.SuspiciousGaps, options.MaxSuspiciousGapDetails) {
			stats.SuspiciousGaps = append(stats.SuspiciousGaps, newGapDetail(observation, expectedInterval, missing))
		}
	}

	if largestGap > 0 {
		stats.LargestGap = largestGap.String()
	}
	if largestExpectedGap > 0 {
		stats.ExpectedLargestGap = largestExpectedGap.String()
	}
	if largestSuspiciousGap > 0 {
		stats.SuspiciousLargestGap = largestSuspiciousGap.String()
	}
}

func recurringExpectedGapPatterns(observations []gapObservation, expectedInterval time.Duration, symbol string, profile string) map[string]bool {
	profile = ResolveGapMarketProfile(symbol, profile)
	if profile == MarketProfileCrypto24x7 || profile == MarketProfileAlways {
		return nil
	}

	counts := make(map[string]int)
	for _, observation := range observations {
		if observation.Interval <= expectedInterval {
			continue
		}
		if IsExpectedGapForProfile(observation.Previous, observation.Current, expectedInterval, symbol, profile) {
			continue
		}
		counts[gapPatternKey(observation)]++
	}

	if len(counts) == 0 {
		return nil
	}

	threshold := 3
	patterns := make(map[string]bool)
	for key, count := range counts {
		if count >= threshold {
			patterns[key] = true
		}
	}
	return patterns
}

func gapPatternKey(observation gapObservation) string {
	location := gapMarketLocation()
	previous := observation.Previous.In(location)
	current := observation.Current.In(location)
	return fmt.Sprintf(
		"%d-%02d:%02d-%d-%02d:%02d",
		previous.Weekday(),
		previous.Hour(),
		previous.Minute(),
		current.Weekday(),
		current.Hour(),
		current.Minute(),
	)
}

func shouldAppendGapDetail(existing []GapDetail, max int) bool {
	if max == 0 {
		return true
	}
	if max < 0 {
		return false
	}
	return len(existing) < max
}

func newGapDetail(observation gapObservation, expectedInterval time.Duration, missing int) GapDetail {
	missingFrom := observation.Previous.Add(expectedInterval).UTC()
	missingTo := observation.Current.Add(-expectedInterval).UTC()
	if missing < 1 {
		missingFrom = time.Time{}
		missingTo = time.Time{}
	}
	return GapDetail{
		PreviousTimestamp: observation.Previous.UTC(),
		CurrentTimestamp:  observation.Current.UTC(),
		MissingFrom:       missingFrom,
		MissingTo:         missingTo,
		MissingIntervals:  missing,
		Interval:          observation.Interval.String(),
	}
}

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
	case MarketProfileFX24x5, MarketProfileOTC24x5, MarketProfileCrypto24x7, MarketProfileAlways:
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

type marketHolidayKind int

const (
	marketHolidayNone marketHolidayKind = iota
	marketHolidayEarlyClose
	marketHolidayFullClose
)

func isLikelyHolidayMarketClosed(timestamp time.Time, profile string) bool {
	start, end, ok := holidayClosureWindow(timestamp, profile)
	if !ok {
		return false
	}
	return !timestamp.Before(start) && timestamp.Before(end)
}

func nextHolidayClosureBoundary(timestamp time.Time, profile string) (time.Time, bool) {
	_, end, ok := holidayClosureWindow(timestamp, profile)
	if !ok || !end.After(timestamp) {
		return time.Time{}, false
	}
	return end.UTC(), true
}

func holidayClosureWindow(timestamp time.Time, profile string) (time.Time, time.Time, bool) {
	local := timestamp.In(gapMarketLocation())
	dayStart := time.Date(local.Year(), local.Month(), local.Day(), 0, 0, 0, 0, local.Location())
	kind := usMarketHolidayKind(dayStart)
	if kind == marketHolidayNone {
		return time.Time{}, time.Time{}, false
	}

	switch kind {
	case marketHolidayFullClose:
		switch profile {
		case MarketProfileOTC24x5:
			start := dayStart
			end := dayStart.Add(18 * time.Hour)
			return start.UTC(), end.UTC(), true
		case MarketProfileFX24x5:
			start := dayStart
			end := dayStart.Add(17 * time.Hour)
			return start.UTC(), end.UTC(), true
		}
	case marketHolidayEarlyClose:
		switch profile {
		case MarketProfileOTC24x5:
			start := time.Date(local.Year(), local.Month(), local.Day(), 13, 0, 0, 0, local.Location())
			end := time.Date(local.Year(), local.Month(), local.Day(), 18, 0, 0, 0, local.Location())
			return start.UTC(), end.UTC(), true
		case MarketProfileFX24x5:
			start := time.Date(local.Year(), local.Month(), local.Day(), 13, 0, 0, 0, local.Location())
			end := time.Date(local.Year(), local.Month(), local.Day(), 17, 0, 0, 0, local.Location())
			return start.UTC(), end.UTC(), true
		}
	}

	return time.Time{}, time.Time{}, false
}

func usMarketHolidayKind(localDay time.Time) marketHolidayKind {
	year, month, day := localDay.Date()
	date := time.Date(year, month, day, 0, 0, 0, 0, localDay.Location())

	if sameLocalDay(date, nthWeekdayOfMonth(year, time.January, time.Monday, 3, localDay.Location())) {
		return marketHolidayEarlyClose
	}
	if sameLocalDay(date, nthWeekdayOfMonth(year, time.February, time.Monday, 3, localDay.Location())) {
		return marketHolidayEarlyClose
	}
	if sameLocalDay(date, goodFriday(year, localDay.Location())) {
		return marketHolidayFullClose
	}
	if sameLocalDay(date, lastWeekdayOfMonth(year, time.May, time.Monday, localDay.Location())) {
		return marketHolidayEarlyClose
	}
	if year >= 2022 && sameLocalDay(date, observedFixedHoliday(year, time.June, 19, localDay.Location())) {
		return marketHolidayEarlyClose
	}
	if sameLocalDay(date, observedFixedHoliday(year, time.July, 4, localDay.Location())) {
		return marketHolidayEarlyClose
	}
	if sameLocalDay(date, nthWeekdayOfMonth(year, time.September, time.Monday, 1, localDay.Location())) {
		return marketHolidayEarlyClose
	}
	if sameLocalDay(date, nthWeekdayOfMonth(year, time.November, time.Thursday, 4, localDay.Location())) {
		return marketHolidayEarlyClose
	}
	if sameLocalDay(date, observedFixedHoliday(year, time.December, 25, localDay.Location())) {
		return marketHolidayFullClose
	}
	if sameLocalDay(date, observedFixedHoliday(year, time.January, 1, localDay.Location())) {
		return marketHolidayFullClose
	}
	return marketHolidayNone
}

func sameLocalDay(left time.Time, right time.Time) bool {
	ly, lm, ld := left.Date()
	ry, rm, rd := right.Date()
	return ly == ry && lm == rm && ld == rd
}

func observedFixedHoliday(year int, month time.Month, day int, location *time.Location) time.Time {
	date := time.Date(year, month, day, 0, 0, 0, 0, location)
	switch date.Weekday() {
	case time.Saturday:
		return date.AddDate(0, 0, -1)
	case time.Sunday:
		return date.AddDate(0, 0, 1)
	default:
		return date
	}
}

func nthWeekdayOfMonth(year int, month time.Month, weekday time.Weekday, n int, location *time.Location) time.Time {
	date := time.Date(year, month, 1, 0, 0, 0, 0, location)
	for date.Weekday() != weekday {
		date = date.AddDate(0, 0, 1)
	}
	return date.AddDate(0, 0, (n-1)*7)
}

func lastWeekdayOfMonth(year int, month time.Month, weekday time.Weekday, location *time.Location) time.Time {
	date := time.Date(year, month+1, 0, 0, 0, 0, 0, location)
	for date.Weekday() != weekday {
		date = date.AddDate(0, 0, -1)
	}
	return date
}

func goodFriday(year int, location *time.Location) time.Time {
	return easterSunday(year, location).AddDate(0, 0, -2)
}

func easterSunday(year int, location *time.Location) time.Time {
	a := year % 19
	b := year / 100
	c := year % 100
	d := b / 4
	e := b % 4
	f := (b + 8) / 25
	g := (b - f + 1) / 3
	h := (19*a + b - d - g + 15) % 30
	i := c / 4
	k := c % 4
	l := (32 + 2*e + 2*i - h - k) % 7
	m := (a + 11*h + 22*l) / 451
	month := (h + l - 7*m + 114) / 31
	day := ((h + l - 7*m + 114) % 31) + 1
	return time.Date(year, time.Month(month), day, 0, 0, 0, 0, location)
}
