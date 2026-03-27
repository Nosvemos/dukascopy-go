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

	"github.com/Nosvemos/dukascopy-go/internal/dukascopy"
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

type csvRecordWriter interface {
	Write(record []string) error
	Flush()
	Error() error
}

type csvRecordReader interface {
	Read() ([]string, error)
}

var csvWriterFactory = func(w io.Writer) csvRecordWriter { return csv.NewWriter(w) }
var csvReaderFactory = func(r io.Reader) csvRecordReader { return csv.NewReader(r) }

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

	return writeBarsCSV(csvWriter, instrument, columns, primaryBars, bidBars, askBars)
}

func WriteBarsToWriter(w io.Writer, instrument dukascopy.Instrument, columns []string, primaryBars []dukascopy.Bar, bidBars []dukascopy.Bar, askBars []dukascopy.Bar) error {
	csvWriter := csvWriterFactory(w)
	defer csvWriter.Flush()

	return writeBarsCSV(csvWriter, instrument, columns, primaryBars, bidBars, askBars)
}

func WriteBarsRowsToWriter(w io.Writer, instrument dukascopy.Instrument, columns []string, primaryBars []dukascopy.Bar, bidBars []dukascopy.Bar, askBars []dukascopy.Bar) error {
	csvWriter := csvWriterFactory(w)
	defer csvWriter.Flush()

	return writeBarsCSVRows(csvWriter, instrument, columns, primaryBars, bidBars, askBars, false)
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

	return writeTicksCSV(csvWriter, instrument, columns, ticks)
}

func WriteTicksToWriter(w io.Writer, instrument dukascopy.Instrument, columns []string, ticks []dukascopy.Tick) error {
	csvWriter := csvWriterFactory(w)
	defer csvWriter.Flush()

	return writeTicksCSV(csvWriter, instrument, columns, ticks)
}

func WriteTicksRowsToWriter(w io.Writer, instrument dukascopy.Instrument, columns []string, ticks []dukascopy.Tick) error {
	csvWriter := csvWriterFactory(w)
	defer csvWriter.Flush()

	return writeTicksCSVRows(csvWriter, instrument, columns, ticks, false)
}

func writeBarsCSV(csvWriter csvRecordWriter, instrument dukascopy.Instrument, columns []string, primaryBars []dukascopy.Bar, bidBars []dukascopy.Bar, askBars []dukascopy.Bar) error {
	return writeBarsCSVRows(csvWriter, instrument, columns, primaryBars, bidBars, askBars, true)
}

func writeBarsCSVRows(csvWriter csvRecordWriter, instrument dukascopy.Instrument, columns []string, primaryBars []dukascopy.Bar, bidBars []dukascopy.Bar, askBars []dukascopy.Bar, includeHeader bool) error {
	if includeHeader {
		if err := csvWriter.Write(columns); err != nil {
			return err
		}
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

func writeTicksCSV(csvWriter csvRecordWriter, instrument dukascopy.Instrument, columns []string, ticks []dukascopy.Tick) error {
	return writeTicksCSVRows(csvWriter, instrument, columns, ticks, true)
}

func writeTicksCSVRows(csvWriter csvRecordWriter, instrument dukascopy.Instrument, columns []string, ticks []dukascopy.Tick, includeHeader bool) error {
	if includeHeader {
		if err := csvWriter.Write(columns); err != nil {
			return err
		}
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

	_, csvWriter, closeWriter, err := createCSVWriter(tempPath)
	if err != nil {
		return err
	}
	headerWritten := false
	var header []string
	timestampIndex := -1
	lastTimestamp := ""
	var lastRecord []string

	for _, partPath := range partPaths {
		file, err := os.Open(partPath)
		if err != nil {
			closeWriter()
			return err
		}

		reader := csvReaderFactory(file)
		partHeader, err := reader.Read()
		if err != nil {
			file.Close()
			if errors.Is(err, io.EOF) {
				continue
			}
			closeWriter()
			return err
		}

		if !headerWritten {
			header = cloneColumns(partHeader)
			timestampIndex = indexOfColumn(header, "timestamp")
			if timestampIndex < 0 {
				file.Close()
				closeWriter()
				return fmt.Errorf("partition file %s does not contain a timestamp column", partPath)
			}
			if err := csvWriter.Write(header); err != nil {
				file.Close()
				closeWriter()
				return err
			}
			headerWritten = true
		} else if !HeadersMatch(header, partHeader) {
			file.Close()
			closeWriter()
			return fmt.Errorf("partition file %s header does not match the assembled CSV header", partPath)
		}

		for {
			record, readErr := reader.Read()
			if errors.Is(readErr, io.EOF) {
				break
			}
			if readErr != nil {
				file.Close()
				closeWriter()
				return readErr
			}
			if len(record) == 0 {
				continue
			}
			if timestampIndex >= len(record) {
				file.Close()
				closeWriter()
				return fmt.Errorf("partition file %s contains a malformed row", partPath)
			}

			timestamp, err := time.Parse(timestampLayout, record[timestampIndex])
			if err != nil {
				file.Close()
				closeWriter()
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
					closeWriter()
					return fmt.Errorf("conflicting duplicate timestamp %s while assembling %s", currentTimestamp, outputPath)
				}
				continue
			}

			if err := csvWriter.Write(record); err != nil {
				file.Close()
				closeWriter()
				return err
			}
			lastTimestamp = currentTimestamp
			lastRecord = cloneColumns(record)
		}

		file.Close()
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

func StreamCSVRowsAfter(path string, w io.Writer, after time.Time, includeHeader bool) (int, time.Time, error) {
	if isParquetPath(path) {
		return 0, time.Time{}, fmt.Errorf("streaming parquet rows to writer is not supported")
	}

	_, csvReader, closeReader, err := openCSVReader(path)
	if err != nil {
		return 0, time.Time{}, err
	}
	defer closeReader()

	header, err := csvReader.Read()
	if err != nil {
		return 0, time.Time{}, err
	}

	timestampIndex := indexOfColumn(header, "timestamp")
	if timestampIndex < 0 {
		return 0, time.Time{}, fmt.Errorf("source CSV %s does not contain a timestamp column", path)
	}

	csvWriter := csvWriterFactory(w)
	defer csvWriter.Flush()

	if includeHeader {
		if err := csvWriter.Write(header); err != nil {
			return 0, time.Time{}, err
		}
	}

	rows := 0
	lastTimestamp := after
	for {
		record, readErr := csvReader.Read()
		if errors.Is(readErr, io.EOF) {
			break
		}
		if readErr != nil {
			return 0, time.Time{}, readErr
		}
		if len(record) == 0 {
			continue
		}
		if timestampIndex >= len(record) {
			return 0, time.Time{}, fmt.Errorf("source CSV %s contains a malformed row", path)
		}

		timestamp, err := time.Parse(timestampLayout, record[timestampIndex])
		if err != nil {
			return 0, time.Time{}, fmt.Errorf("parse source CSV timestamp %q: %w", record[timestampIndex], err)
		}
		timestamp = timestamp.UTC()
		if !after.IsZero() && (!timestamp.After(after)) {
			continue
		}

		if err := csvWriter.Write(record); err != nil {
			return 0, time.Time{}, err
		}
		rows++
		lastTimestamp = timestamp
	}

	if err := csvWriter.Error(); err != nil {
		return 0, time.Time{}, err
	}
	return rows, lastTimestamp, nil
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
	stats.InferredTimeframe = inferTimeframe(intervals)
	if expectedInterval > 0 {
		stats.ExpectedInterval = expectedInterval.String()
		applyGapStats(&stats, gapObservations, expectedInterval, stats.GapSymbol, stats.GapProfile, options)
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
	_, reader, closeReader, err := openCSVReader(outputPath)
	if err != nil {
		return ResumeState{}, err
	}
	defer closeReader()

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
	_, reader, closeReader, err := openCSVReader(tempPath)
	if err != nil {
		return 0, err
	}
	defer closeReader()

	if _, err := reader.Read(); err != nil {
		if errors.Is(err, io.EOF) {
			return 0, nil
		}
		return 0, err
	}

	_, writer, closeWriter, err := openAppendCSVWriter(existingPath)
	if err != nil {
		return 0, err
	}
	defer closeWriter()

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

func openAppendCSVWriter(path string) (*os.File, csvRecordWriter, func() error, error) {
	file, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		return nil, nil, nil, err
	}

	if isGzipPath(path) {
		gzipWriter := gzip.NewWriter(file)
		writer := csvWriterFactory(gzipWriter)
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

	writer := csvWriterFactory(file)
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

func createCSVWriter(outputPath string) (*os.File, csvRecordWriter, func() error, error) {
	file, err := os.Create(outputPath)
	if err != nil {
		return nil, nil, nil, err
	}

	if isGzipPath(outputPath) {
		gzipWriter := gzip.NewWriter(file)
		writer := csvWriterFactory(gzipWriter)
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

	writer := csvWriterFactory(file)
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

func openCSVReader(path string) (*os.File, csvRecordReader, func() error, error) {
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
		return file, csvReaderFactory(gzipReader), closeReader, nil
	}

	closeReader := func() error {
		return file.Close()
	}
	return file, csvReaderFactory(file), closeReader, nil
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
