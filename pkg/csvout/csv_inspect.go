package csvout

import (
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"hash/maphash"
	"io"
	"os"
	"time"
)

// CSVStats holds the statistics reported by InspectCSV.
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

// InspectOptions tunes InspectCSVWithOptions.
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

// GapDetail describes one suspicious gap.
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
	MarketProfileEquity     = "equity-24x5"
)

// AuditCSV counts data rows and hashes a CSV, CSV.GZ or Parquet file.
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

// InspectCSV is InspectCSVWithOptions with default options.
func InspectCSV(path string) (CSVStats, error) {
	return InspectCSVWithOptions(path, InspectOptions{})
}

// InspectCSVWithOptions computes statistics for a CSV/CSV.GZ/Parquet file
// using DefaultConfig.
func InspectCSVWithOptions(path string, options InspectOptions) (CSVStats, error) {
	return DefaultConfig().InspectCSVWithOptions(path, options)
}

// InspectCSVWithOptions computes row, duplicate, ordering and gap statistics
// for a CSV/CSV.GZ/Parquet file, reading it with c's delimiter and parsing
// timestamps with c.ParseTimestamp.
func (c *Config) InspectCSVWithOptions(path string, options InspectOptions) (CSVStats, error) {
	if isParquetPath(path) {
		return c.inspectParquetWithOptions(path, options)
	}
	_, readerFactory, closeReader, err := c.openCSVReader(path)
	if err != nil {
		return CSVStats{}, err
	}
	defer closeReader()
	reader := readerFactory()

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

	seenRows := make(map[uint64]struct{})
	seenTimestamps := make(map[int64]struct{})
	hasher := newRowHasher()
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
		rowKey := hasher.sum(record)
		if _, seen := seenRows[rowKey]; seen {
			stats.DuplicateRows++
		} else {
			seenRows[rowKey] = struct{}{}
		}

		if !stats.HasTimestamp || timestampIndex >= len(record) {
			continue
		}

		timestamp, err := c.ParseTimestamp(record[timestampIndex])
		if err != nil {
			return CSVStats{}, fmt.Errorf("parse CSV timestamp %q: %w", record[timestampIndex], err)
		}
		if stats.FirstTimestamp.IsZero() || timestamp.Before(stats.FirstTimestamp) {
			stats.FirstTimestamp = timestamp
		}
		if stats.LastTimestamp.IsZero() || timestamp.After(stats.LastTimestamp) {
			stats.LastTimestamp = timestamp
		}

		stampKey := timestamp.UnixNano()
		if _, seen := seenTimestamps[stampKey]; seen {
			stats.DuplicateStamps++
		} else {
			seenTimestamps[stampKey] = struct{}{}
		}

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

// rowHashSeed is fixed for the process so hashes are comparable across calls.
var rowHashSeed = maphash.MakeSeed()

// rowHasher hashes CSV records to 64-bit keys for duplicate detection without
// retaining the row text. Fields are joined with the 0x1f unit separator, as
// the former string key was, so equivalence is unchanged up to 64-bit hash
// collisions.
type rowHasher struct {
	h maphash.Hash
}

func newRowHasher() *rowHasher {
	r := &rowHasher{}
	r.h.SetSeed(rowHashSeed)
	return r
}

func (r *rowHasher) sum(record []string) uint64 {
	r.h.Reset()
	for i, field := range record {
		if i > 0 {
			r.h.WriteByte(0x1f)
		}
		r.h.WriteString(field)
	}
	return r.h.Sum64()
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
