package csvout

import (
	"compress/gzip"
	"crypto/sha256"
	"encoding/csv"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/Nosvemos/dukascopy-data-downloader/internal/dukascopy"
)

const timestampLayout = time.RFC3339Nano

type Profile string

const (
	ProfileSimple Profile = "simple"
	ProfileFull   Profile = "full"
)

var simpleBarColumns = []string{"timestamp", "open", "high", "low", "close", "volume"}
var fullBarColumns = []string{"timestamp", "mid_open", "mid_high", "mid_low", "mid_close", "spread", "volume", "bid_open", "bid_high", "bid_low", "bid_close", "ask_open", "ask_high", "ask_low", "ask_close"}
var simpleTickColumns = []string{"timestamp", "bid", "ask"}
var fullTickColumns = []string{"timestamp", "bid", "ask", "bid_volume", "ask_volume"}

type ResumeState struct {
	Exists     bool
	Columns    []string
	HasRows    bool
	LastRecord []string
	LastTime   time.Time
}

type FileAudit struct {
	Rows   int
	Bytes  int64
	SHA256 string
}

type CSVStats struct {
	Path              string
	Format            string
	Compressed        bool
	Columns           []string
	Rows              int
	FirstTimestamp    time.Time
	LastTimestamp     time.Time
	HasTimestamp      bool
	DuplicateRows     int
	DuplicateStamps   int
	OutOfOrderRows    int
	GapCount          int
	MissingIntervals  int
	ExpectedInterval  string
	LargestGap        string
	InferredTimeframe string
}

func BarColumnsForProfile(profile Profile) []string {
	switch profile {
	case ProfileSimple:
		return cloneColumns(simpleBarColumns)
	case ProfileFull:
		return cloneColumns(fullBarColumns)
	default:
		return nil
	}
}

func TickColumnsForProfile(profile Profile) []string {
	switch profile {
	case ProfileSimple:
		return cloneColumns(simpleTickColumns)
	case ProfileFull:
		return cloneColumns(fullTickColumns)
	default:
		return nil
	}
}

func ParseBarColumns(value string) ([]string, error) {
	return parseColumns(value, map[string]struct{}{
		"timestamp": {},
		"open":      {},
		"high":      {},
		"low":       {},
		"close":     {},
		"mid_open":  {},
		"mid_high":  {},
		"mid_low":   {},
		"mid_close": {},
		"spread":    {},
		"volume":    {},
		"bid_open":  {},
		"bid_high":  {},
		"bid_low":   {},
		"bid_close": {},
		"ask_open":  {},
		"ask_high":  {},
		"ask_low":   {},
		"ask_close": {},
	})
}

func ParseTickColumns(value string) ([]string, error) {
	return parseColumns(value, map[string]struct{}{
		"timestamp":  {},
		"bid":        {},
		"ask":        {},
		"bid_volume": {},
		"ask_volume": {},
	})
}

func BarColumnsNeedBidAsk(columns []string) bool {
	for _, column := range columns {
		if strings.HasPrefix(column, "bid_") || strings.HasPrefix(column, "ask_") || strings.HasPrefix(column, "mid_") || column == "spread" {
			return true
		}
	}
	return false
}

func WriteBars(outputPath string, instrument dukascopy.Instrument, columns []string, primaryBars []dukascopy.Bar, bidBars []dukascopy.Bar, askBars []dukascopy.Bar) error {
	if isParquetPath(outputPath) {
		return writeBarsParquet(outputPath, instrument, columns, primaryBars, bidBars, askBars)
	}
	if err := ensureParentDir(outputPath); err != nil {
		return err
	}

	_, csvWriter, closeWriter, err := createCSVWriter(outputPath)
	if err != nil {
		return err
	}
	defer closeWriter()

	if err := csvWriter.Write(columns); err != nil {
		return err
	}

	if BarColumnsNeedBidAsk(columns) {
		rows, err := combineBarRows(bidBars, askBars)
		if err != nil {
			return err
		}

		for _, row := range rows {
			record := make([]string, 0, len(columns))
			for _, column := range columns {
				value, valueErr := formatBarColumn(column, instrument.PriceScale, row.Bid, row.Ask)
				if valueErr != nil {
					return valueErr
				}
				record = append(record, value)
			}
			if err := csvWriter.Write(record); err != nil {
				return err
			}
		}

		return csvWriter.Error()
	}

	for _, bar := range primaryBars {
		record := make([]string, 0, len(columns))
		for _, column := range columns {
			value, err := formatPrimaryBarColumn(column, instrument.PriceScale, bar)
			if err != nil {
				return err
			}
			record = append(record, value)
		}
		if err := csvWriter.Write(record); err != nil {
			return err
		}
	}

	return csvWriter.Error()
}

func WriteBarsToWriter(w io.Writer, instrument dukascopy.Instrument, columns []string, primaryBars []dukascopy.Bar, bidBars []dukascopy.Bar, askBars []dukascopy.Bar) error {
	csvWriter := csv.NewWriter(w)
	defer csvWriter.Flush()

	if err := csvWriter.Write(columns); err != nil {
		return err
	}

	if BarColumnsNeedBidAsk(columns) {
		rows, err := combineBarRows(bidBars, askBars)
		if err != nil {
			return err
		}

		for _, row := range rows {
			record := make([]string, 0, len(columns))
			for _, column := range columns {
				value, valueErr := formatBarColumn(column, instrument.PriceScale, row.Bid, row.Ask)
				if valueErr != nil {
					return valueErr
				}
				record = append(record, value)
			}
			if err := csvWriter.Write(record); err != nil {
				return err
			}
		}

		return csvWriter.Error()
	}

	for _, bar := range primaryBars {
		record := make([]string, 0, len(columns))
		for _, column := range columns {
			value, err := formatPrimaryBarColumn(column, instrument.PriceScale, bar)
			if err != nil {
				return err
			}
			record = append(record, value)
		}
		if err := csvWriter.Write(record); err != nil {
			return err
		}
	}

	return csvWriter.Error()
}

func WriteTicks(outputPath string, instrument dukascopy.Instrument, columns []string, ticks []dukascopy.Tick) error {
	if isParquetPath(outputPath) {
		return writeTicksParquet(outputPath, instrument, columns, ticks)
	}
	if err := ensureParentDir(outputPath); err != nil {
		return err
	}

	_, csvWriter, closeWriter, err := createCSVWriter(outputPath)
	if err != nil {
		return err
	}
	defer closeWriter()

	if err := csvWriter.Write(columns); err != nil {
		return err
	}

	for _, tick := range ticks {
		record := make([]string, 0, len(columns))
		for _, column := range columns {
			value, valueErr := formatTickColumn(column, instrument.PriceScale, tick)
			if valueErr != nil {
				return valueErr
			}
			record = append(record, value)
		}
		if err := csvWriter.Write(record); err != nil {
			return err
		}
	}

	return csvWriter.Error()
}

func WriteTicksToWriter(w io.Writer, instrument dukascopy.Instrument, columns []string, ticks []dukascopy.Tick) error {
	csvWriter := csv.NewWriter(w)
	defer csvWriter.Flush()

	if err := csvWriter.Write(columns); err != nil {
		return err
	}

	for _, tick := range ticks {
		record := make([]string, 0, len(columns))
		for _, column := range columns {
			value, valueErr := formatTickColumn(column, instrument.PriceScale, tick)
			if valueErr != nil {
				return valueErr
			}
			record = append(record, value)
		}
		if err := csvWriter.Write(record); err != nil {
			return err
		}
	}

	return csvWriter.Error()
}

func WriteBarsAtomic(outputPath string, instrument dukascopy.Instrument, columns []string, primaryBars []dukascopy.Bar, bidBars []dukascopy.Bar, askBars []dukascopy.Bar) error {
	tempPath, err := createAtomicTempPath(outputPath)
	if err != nil {
		return err
	}
	defer os.Remove(tempPath)

	if err := WriteBars(tempPath, instrument, columns, primaryBars, bidBars, askBars); err != nil {
		return err
	}
	return replaceFile(tempPath, outputPath)
}

func WriteTicksAtomic(outputPath string, instrument dukascopy.Instrument, columns []string, ticks []dukascopy.Tick) error {
	tempPath, err := createAtomicTempPath(outputPath)
	if err != nil {
		return err
	}
	defer os.Remove(tempPath)

	if err := WriteTicks(tempPath, instrument, columns, ticks); err != nil {
		return err
	}
	return replaceFile(tempPath, outputPath)
}

func AssembleCSVFromParts(outputPath string, partPaths []string, from time.Time, to time.Time) error {
	if isParquetPath(outputPath) {
		return assembleParquetFromCSVParts(outputPath, partPaths, from, to)
	}
	if len(partPaths) == 0 {
		return fmt.Errorf("no partition files were provided for assembly")
	}

	tempPath, err := createAtomicTempPath(outputPath)
	if err != nil {
		return err
	}
	defer os.Remove(tempPath)

	if err := ensureParentDir(tempPath); err != nil {
		return err
	}

	target, err := os.Create(tempPath)
	if err != nil {
		return err
	}
	csvWriter := csv.NewWriter(target)
	headerWritten := false
	var header []string
	timestampIndex := -1
	lastTimestamp := ""
	var lastRecord []string

	for _, partPath := range partPaths {
		file, err := os.Open(partPath)
		if err != nil {
			target.Close()
			return err
		}

		reader := csv.NewReader(file)
		partHeader, err := reader.Read()
		if err != nil {
			file.Close()
			target.Close()
			if errors.Is(err, io.EOF) {
				continue
			}
			return err
		}

		if !headerWritten {
			header = cloneColumns(partHeader)
			timestampIndex = indexOfColumn(header, "timestamp")
			if timestampIndex < 0 {
				file.Close()
				target.Close()
				return fmt.Errorf("partition file %s does not contain a timestamp column", partPath)
			}
			if err := csvWriter.Write(header); err != nil {
				file.Close()
				target.Close()
				return err
			}
			headerWritten = true
		} else if !HeadersMatch(header, partHeader) {
			file.Close()
			target.Close()
			return fmt.Errorf("partition file %s header does not match the assembled CSV header", partPath)
		}

		for {
			record, readErr := reader.Read()
			if errors.Is(readErr, io.EOF) {
				break
			}
			if readErr != nil {
				file.Close()
				target.Close()
				return readErr
			}
			if len(record) == 0 {
				continue
			}
			if timestampIndex >= len(record) {
				file.Close()
				target.Close()
				return fmt.Errorf("partition file %s contains a malformed row", partPath)
			}

			timestamp, err := time.Parse(timestampLayout, record[timestampIndex])
			if err != nil {
				file.Close()
				target.Close()
				return fmt.Errorf("parse partition timestamp %q: %w", record[timestampIndex], err)
			}
			timestamp = timestamp.UTC()
			if timestamp.Before(from) || !timestamp.Before(to) {
				continue
			}

			currentTimestamp := timestamp.Format(timestampLayout)
			if currentTimestamp == lastTimestamp {
				if !recordsEqual(record, lastRecord) {
					file.Close()
					target.Close()
					return fmt.Errorf("conflicting duplicate timestamp %s while assembling %s", currentTimestamp, outputPath)
				}
				continue
			}

			if err := csvWriter.Write(record); err != nil {
				file.Close()
				target.Close()
				return err
			}
			lastTimestamp = currentTimestamp
			lastRecord = cloneColumns(record)
		}

		file.Close()
	}

	csvWriter.Flush()
	if err := csvWriter.Error(); err != nil {
		target.Close()
		return err
	}
	if err := target.Close(); err != nil {
		return err
	}

	return replaceFile(tempPath, outputPath)
}

func ExtractCSVRange(sourcePath string, outputPath string, from time.Time, to time.Time) error {
	if isParquetPath(sourcePath) {
		return extractRangeFromParquet(sourcePath, outputPath, from, to)
	}
	if isParquetPath(outputPath) {
		return extractRangeCSVToParquet(sourcePath, outputPath, from, to)
	}
	_, csvReader, closeReader, err := openCSVReader(sourcePath)
	if err != nil {
		return err
	}
	defer closeReader()

	header, err := csvReader.Read()
	if err != nil {
		return err
	}

	timestampIndex := indexOfColumn(header, "timestamp")
	if timestampIndex < 0 {
		return fmt.Errorf("source CSV %s does not contain a timestamp column", sourcePath)
	}

	tempPath, err := createAtomicTempPath(outputPath)
	if err != nil {
		return err
	}
	defer os.Remove(tempPath)

	_, csvWriter, closeWriter, err := createCSVWriter(tempPath)
	if err != nil {
		return err
	}

	if err := csvWriter.Write(header); err != nil {
		closeWriter()
		return err
	}

	for {
		record, readErr := csvReader.Read()
		if errors.Is(readErr, io.EOF) {
			break
		}
		if readErr != nil {
			closeWriter()
			return readErr
		}
		if len(record) == 0 {
			continue
		}
		if timestampIndex >= len(record) {
			closeWriter()
			return fmt.Errorf("source CSV %s contains a malformed row", sourcePath)
		}

		timestamp, err := time.Parse(timestampLayout, record[timestampIndex])
		if err != nil {
			closeWriter()
			return fmt.Errorf("parse source CSV timestamp %q: %w", record[timestampIndex], err)
		}
		timestamp = timestamp.UTC()
		if timestamp.Before(from) || !timestamp.Before(to) {
			continue
		}

		if err := csvWriter.Write(record); err != nil {
			closeWriter()
			return err
		}
	}

	csvWriter.Flush()
	if err := csvWriter.Error(); err != nil {
		closeWriter()
		return err
	}
	if err := closeWriter(); err != nil {
		return err
	}

	return replaceFile(tempPath, outputPath)
}

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

	reader := csv.NewReader(readCloser)
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
	if isParquetPath(path) {
		return inspectParquet(path)
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
		Columns:    cloneColumns(header),
	}
	timestampIndex := indexOfColumn(header, "timestamp")
	stats.HasTimestamp = timestampIndex >= 0

	seenRows := make(map[string]int)
	seenTimestamps := make(map[string]int)
	var intervals []time.Duration
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
			} else if delta < 0 {
				stats.OutOfOrderRows++
			}
		}
		previousTimestamp = timestamp
	}

	expectedInterval := inferExpectedInterval(intervals)
	stats.InferredTimeframe = inferTimeframe(intervals)
	if expectedInterval > 0 {
		stats.ExpectedInterval = expectedInterval.String()
		var largestGap time.Duration
		for _, interval := range intervals {
			if interval <= expectedInterval {
				continue
			}
			stats.GapCount++
			stats.MissingIntervals += estimateMissingIntervals(interval, expectedInterval)
			if interval > largestGap {
				largestGap = interval
			}
		}
		if largestGap > 0 {
			stats.LargestGap = largestGap.String()
		}
	}
	return stats, nil
}

func ColumnsContainTimestamp(columns []string) bool {
	for _, column := range columns {
		if strings.EqualFold(strings.TrimSpace(column), "timestamp") {
			return true
		}
	}
	return false
}

func InspectExistingCSV(outputPath string) (ResumeState, error) {
	file, err := os.Open(outputPath)
	if err != nil {
		return ResumeState{}, err
	}
	defer file.Close()

	reader := csv.NewReader(file)
	header, err := reader.Read()
	if errors.Is(err, io.EOF) {
		return ResumeState{Exists: true}, nil
	}
	if err != nil {
		return ResumeState{}, err
	}

	state := ResumeState{
		Exists:  true,
		Columns: cloneColumns(header),
	}

	lastRecord := []string(nil)
	for {
		record, readErr := reader.Read()
		if errors.Is(readErr, io.EOF) {
			break
		}
		if readErr != nil {
			return ResumeState{}, readErr
		}
		if len(record) == 0 {
			continue
		}
		lastRecord = cloneColumns(record)
	}

	if len(lastRecord) == 0 {
		return state, nil
	}

	timestampIndex := indexOfColumn(header, "timestamp")
	if timestampIndex < 0 {
		return ResumeState{}, fmt.Errorf("existing CSV %s does not include a timestamp column, so --resume cannot be used", outputPath)
	}
	if timestampIndex >= len(lastRecord) {
		return ResumeState{}, fmt.Errorf("existing CSV %s has a malformed last row", outputPath)
	}

	lastTime, err := time.Parse(timestampLayout, lastRecord[timestampIndex])
	if err != nil {
		return ResumeState{}, fmt.Errorf("parse existing CSV timestamp %q: %w", lastRecord[timestampIndex], err)
	}

	state.HasRows = true
	state.LastRecord = lastRecord
	state.LastTime = lastTime.UTC()
	return state, nil
}

func HeadersMatch(expected []string, actual []string) bool {
	if len(expected) != len(actual) {
		return false
	}
	for index := range expected {
		if expected[index] != actual[index] {
			return false
		}
	}
	return true
}

func MergeResumeCSV(existingPath string, tempPath string, duplicateTail []string) (int, error) {
	file, err := os.Open(tempPath)
	if err != nil {
		return 0, err
	}
	defer file.Close()

	reader := csv.NewReader(file)
	if _, err := reader.Read(); err != nil {
		if errors.Is(err, io.EOF) {
			return 0, nil
		}
		return 0, err
	}

	target, err := os.OpenFile(existingPath, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		return 0, err
	}
	defer target.Close()

	writer := csv.NewWriter(target)
	defer writer.Flush()

	foundDuplicateTail := duplicateTail == nil
	foundAnyRows := false
	appended := 0

	for {
		record, readErr := reader.Read()
		if errors.Is(readErr, io.EOF) {
			break
		}
		if readErr != nil {
			return 0, readErr
		}
		if len(record) == 0 {
			continue
		}

		foundAnyRows = true
		if !foundDuplicateTail {
			if recordsEqual(record, duplicateTail) {
				foundDuplicateTail = true
			}
			continue
		}

		if err := writer.Write(record); err != nil {
			return 0, err
		}
		appended++
	}

	if !foundAnyRows {
		return 0, nil
	}

	if !foundDuplicateTail {
		return 0, fmt.Errorf("existing CSV tail was not found in resumed data; aborting to avoid corrupting %s", existingPath)
	}

	return appended, writer.Error()
}

type combinedBarRow struct {
	Time time.Time
	Bid  dukascopy.Bar
	Ask  dukascopy.Bar
}

func combineBarRows(bidBars []dukascopy.Bar, askBars []dukascopy.Bar) ([]combinedBarRow, error) {
	if len(bidBars) != len(askBars) {
		return nil, fmt.Errorf("bid/ask bar length mismatch: %d vs %d", len(bidBars), len(askBars))
	}

	rows := make([]combinedBarRow, 0, len(bidBars))
	for index := range bidBars {
		if !bidBars[index].Time.Equal(askBars[index].Time) {
			return nil, fmt.Errorf("bid/ask timestamp mismatch at row %d: %s vs %s", index, bidBars[index].Time.UTC().Format(timestampLayout), askBars[index].Time.UTC().Format(timestampLayout))
		}
		rows = append(rows, combinedBarRow{
			Time: bidBars[index].Time,
			Bid:  bidBars[index],
			Ask:  askBars[index],
		})
	}

	return rows, nil
}

func formatPrimaryBarColumn(column string, scale int, bar dukascopy.Bar) (string, error) {
	switch column {
	case "timestamp":
		return bar.Time.UTC().Format(timestampLayout), nil
	case "open":
		return formatPrice(bar.Open, scale), nil
	case "high":
		return formatPrice(bar.High, scale), nil
	case "low":
		return formatPrice(bar.Low, scale), nil
	case "close":
		return formatPrice(bar.Close, scale), nil
	case "mid_open":
		return formatPrice(bar.Open, scale), nil
	case "mid_high":
		return formatPrice(bar.High, scale), nil
	case "mid_low":
		return formatPrice(bar.Low, scale), nil
	case "mid_close":
		return formatPrice(bar.Close, scale), nil
	case "volume":
		return formatVolume(bar.Volume), nil
	default:
		return "", fmt.Errorf("column %q requires bid/ask data or is unsupported for simple bars", column)
	}
}

func formatBarColumn(column string, scale int, bid dukascopy.Bar, ask dukascopy.Bar) (string, error) {
	roundedBidOpen := roundToScale(bid.Open, scale)
	roundedBidHigh := roundToScale(bid.High, scale)
	roundedBidLow := roundToScale(bid.Low, scale)
	roundedBidClose := roundToScale(bid.Close, scale)
	roundedAskOpen := roundToScale(ask.Open, scale)
	roundedAskHigh := roundToScale(ask.High, scale)
	roundedAskLow := roundToScale(ask.Low, scale)
	roundedAskClose := roundToScale(ask.Close, scale)

	switch column {
	case "timestamp":
		return bid.Time.UTC().Format(timestampLayout), nil
	case "open":
		return formatMidPrice(midpoint(roundedBidOpen, roundedAskOpen), scale), nil
	case "high":
		return formatMidPrice(midpoint(roundedBidHigh, roundedAskHigh), scale), nil
	case "low":
		return formatMidPrice(midpoint(roundedBidLow, roundedAskLow), scale), nil
	case "close":
		return formatMidPrice(midpoint(roundedBidClose, roundedAskClose), scale), nil
	case "mid_open":
		return formatMidPrice(midpoint(roundedBidOpen, roundedAskOpen), scale), nil
	case "mid_high":
		return formatMidPrice(midpoint(roundedBidHigh, roundedAskHigh), scale), nil
	case "mid_low":
		return formatMidPrice(midpoint(roundedBidLow, roundedAskLow), scale), nil
	case "mid_close":
		return formatMidPrice(midpoint(roundedBidClose, roundedAskClose), scale), nil
	case "spread":
		return formatPrice(roundedAskClose-roundedBidClose, scale), nil
	case "volume":
		return formatVolume(bid.Volume), nil
	case "bid_open":
		return formatPrice(roundedBidOpen, scale), nil
	case "bid_high":
		return formatPrice(roundedBidHigh, scale), nil
	case "bid_low":
		return formatPrice(roundedBidLow, scale), nil
	case "bid_close":
		return formatPrice(roundedBidClose, scale), nil
	case "ask_open":
		return formatPrice(roundedAskOpen, scale), nil
	case "ask_high":
		return formatPrice(roundedAskHigh, scale), nil
	case "ask_low":
		return formatPrice(roundedAskLow, scale), nil
	case "ask_close":
		return formatPrice(roundedAskClose, scale), nil
	default:
		return "", fmt.Errorf("unsupported bar column %q", column)
	}
}

func formatTickColumn(column string, scale int, tick dukascopy.Tick) (string, error) {
	switch column {
	case "timestamp":
		return tick.Time.UTC().Format(timestampLayout), nil
	case "bid":
		return formatPrice(tick.Bid, scale), nil
	case "ask":
		return formatPrice(tick.Ask, scale), nil
	case "bid_volume":
		return formatVolume(tick.BidVolume), nil
	case "ask_volume":
		return formatVolume(tick.AskVolume), nil
	default:
		return "", fmt.Errorf("unsupported tick column %q", column)
	}
}

func parseColumns(value string, allowed map[string]struct{}) ([]string, error) {
	parts := strings.Split(value, ",")
	columns := make([]string, 0, len(parts))
	for _, part := range parts {
		column := strings.TrimSpace(strings.ToLower(part))
		if column == "" {
			continue
		}
		if _, ok := allowed[column]; !ok {
			return nil, fmt.Errorf("unsupported column %q", column)
		}
		columns = append(columns, column)
	}
	if len(columns) == 0 {
		return nil, fmt.Errorf("at least one column must be provided")
	}
	return columns, nil
}

func cloneColumns(columns []string) []string {
	cloned := make([]string, len(columns))
	copy(cloned, columns)
	return cloned
}

func recordsEqual(left []string, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

func indexOfColumn(columns []string, needle string) int {
	for index, column := range columns {
		if strings.EqualFold(strings.TrimSpace(column), needle) {
			return index
		}
	}
	return -1
}

func ensureParentDir(outputPath string) error {
	dir := filepath.Dir(outputPath)
	if dir == "." || dir == "" {
		return nil
	}
	return os.MkdirAll(dir, 0o755)
}

func createAtomicTempPath(outputPath string) (string, error) {
	if err := ensureParentDir(outputPath); err != nil {
		return "", err
	}

	pattern := filepath.Base(outputPath) + ".tmp-*"
	if isParquetPath(outputPath) {
		base := filepath.Base(strings.TrimSuffix(outputPath, ".parquet"))
		pattern = base + ".tmp-*.parquet"
	} else if isGzipPath(outputPath) {
		base := filepath.Base(strings.TrimSuffix(outputPath, ".gz"))
		pattern = base + ".tmp-*.gz"
	}

	file, err := os.CreateTemp(filepath.Dir(outputPath), pattern)
	if err != nil {
		return "", err
	}
	path := file.Name()
	if err := file.Close(); err != nil {
		return "", err
	}
	return path, nil
}

func replaceFile(sourcePath string, targetPath string) error {
	if err := ensureParentDir(targetPath); err != nil {
		return err
	}
	if _, err := os.Stat(targetPath); err == nil {
		if err := os.Remove(targetPath); err != nil {
			return err
		}
	} else if err != nil && !os.IsNotExist(err) {
		return err
	}
	return os.Rename(sourcePath, targetPath)
}

func createCSVWriter(outputPath string) (*os.File, *csv.Writer, func() error, error) {
	file, err := os.Create(outputPath)
	if err != nil {
		return nil, nil, nil, err
	}

	if isGzipPath(outputPath) {
		gzipWriter := gzip.NewWriter(file)
		writer := csv.NewWriter(gzipWriter)
		closeWriter := func() error {
			writer.Flush()
			if err := writer.Error(); err != nil {
				gzipWriter.Close()
				file.Close()
				return err
			}
			if err := gzipWriter.Close(); err != nil {
				file.Close()
				return err
			}
			return file.Close()
		}
		return file, writer, closeWriter, nil
	}

	writer := csv.NewWriter(file)
	closeWriter := func() error {
		writer.Flush()
		if err := writer.Error(); err != nil {
			file.Close()
			return err
		}
		return file.Close()
	}
	return file, writer, closeWriter, nil
}

func openCSVReader(path string) (*os.File, *csv.Reader, func() error, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, nil, nil, err
	}

	if isGzipPath(path) {
		gzipReader, err := gzip.NewReader(file)
		if err != nil {
			file.Close()
			return nil, nil, nil, err
		}
		closeReader := func() error {
			if err := gzipReader.Close(); err != nil {
				file.Close()
				return err
			}
			return file.Close()
		}
		return file, csv.NewReader(gzipReader), closeReader, nil
	}

	closeReader := func() error {
		return file.Close()
	}
	return file, csv.NewReader(file), closeReader, nil
}

func isGzipPath(path string) bool {
	return strings.HasSuffix(strings.ToLower(strings.TrimSpace(path)), ".gz")
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

func formatPrice(value float64, scale int) string {
	if scale <= 0 {
		return strconv.FormatFloat(value, 'f', -1, 64)
	}
	return strconv.FormatFloat(value, 'f', scale, 64)
}

func formatMidPrice(value float64, scale int) string {
	precision := scale + 1
	if precision < 0 {
		precision = -1
	}
	factor := math.Pow10(precision)
	rounded := math.Round(value*factor) / factor
	return strconv.FormatFloat(rounded, 'f', -1, 64)
}

func formatVolume(value float64) string {
	return strconv.FormatFloat(value, 'f', -1, 64)
}

func midpoint(a float64, b float64) float64 {
	return (a + b) / 2
}

func roundToScale(value float64, scale int) float64 {
	if scale < 0 {
		return value
	}
	factor := math.Pow10(scale)
	return math.Round(value*factor) / factor
}
