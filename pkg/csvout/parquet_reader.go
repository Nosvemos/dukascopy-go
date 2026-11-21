package csvout

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"
)

func auditParquet(path string) (FileAudit, error) {
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
	if _, err := io.Copy(hasher, file); err != nil {
		return FileAudit{}, err
	}

	rowCount, err := parquetRowCount(path)
	if err != nil {
		return FileAudit{}, err
	}

	return FileAudit{
		Rows:   rowCount,
		Bytes:  info.Size(),
		SHA256: hex.EncodeToString(hasher.Sum(nil)),
	}, nil
}

func inspectParquet(path string) (CSVStats, error) {
	return DefaultConfig().inspectParquetWithOptions(path, InspectOptions{})
}

func (c *Config) inspectParquetWithOptions(path string, options InspectOptions) (CSVStats, error) {
	stats := CSVStats{
		Path:       path,
		Compressed: false,
		Format:     "parquet",
		GapSymbol:  defaultGapSymbol(path, options.Symbol),
	}
	stats.GapProfile = ResolveGapMarketProfile(stats.GapSymbol, options.MarketProfile)

	file, parquetFile, closeFile, err := openParquetFile(path)
	if err != nil {
		return CSVStats{}, err
	}
	defer closeFile()

	stats.Columns = parquetColumns(parquetFile)
	timestampIndex := indexOfColumn(stats.Columns, "timestamp")
	stats.HasTimestamp = timestampIndex >= 0

	reader := c.parquetReaderFactory(file, parquetFile.Schema())
	defer reader.Close()

	seenRows := make(map[uint64]struct{})
	seenTimestamps := make(map[int64]struct{})
	hasher := newRowHasher()
	var intervals []time.Duration
	var gapObservations []gapObservation
	var previousTimestamp time.Time

	for {
		rows := make([]map[string]any, 256)
		for index := range rows {
			rows[index] = make(map[string]any, len(stats.Columns))
		}

		count, err := reader.Read(rows)
		if err != nil && !errors.Is(err, io.EOF) {
			return CSVStats{}, err
		}

		for _, row := range rows[:count] {
			record := parquetRecordStrings(stats.Columns, row)
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

			timestamp, err := c.rowTimestamp(row)
			if err != nil {
				return CSVStats{}, fmt.Errorf("parse parquet timestamp %q: %w", record[timestampIndex], err)
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

		if errors.Is(err, io.EOF) {
			break
		}
	}

	expectedInterval := inferExpectedInterval(intervals)
	stats.InferredTimeframe = inferTimeframe(intervals)
	if expectedInterval > 0 {
		stats.ExpectedInterval = expectedInterval.String()
		applyGapStats(&stats, gapObservations, expectedInterval, stats.GapSymbol, stats.GapProfile, options)
	}

	return stats, nil
}

func (c *Config) extractRangeFromParquet(sourcePath string, outputPath string, from time.Time, to time.Time) error {
	file, parquetFile, closeFile, err := openParquetFile(sourcePath)
	if err != nil {
		return err
	}
	defer closeFile()

	columns := parquetColumns(parquetFile)
	if indexOfColumn(columns, "timestamp") < 0 {
		return fmt.Errorf("source parquet %s does not contain a timestamp column", sourcePath)
	}

	reader := c.parquetReaderFactory(file, parquetFile.Schema())
	defer reader.Close()

	if isParquetPath(outputPath) {
		tempPath, err := createAtomicTempPath(outputPath)
		if err != nil {
			return err
		}
		defer os.Remove(tempPath)

		outFile, err := os.Create(tempPath)
		if err != nil {
			return err
		}
		writer := c.parquetWriterFactory(outFile, parquetSchemaForColumns(columns))
		writer.SetKeyValueMetadata(parquetColumnsMetadataKey, strings.Join(columns, ","))

		rowNumber := 0
		for {
			rows := make([]map[string]any, 256)
			for index := range rows {
				rows[index] = make(map[string]any, len(columns))
			}

			count, err := reader.Read(rows)
			if err != nil && !errors.Is(err, io.EOF) {
				writer.Close()
				outFile.Close()
				return err
			}

			filtered := make([]map[string]any, 0, count)
			for _, row := range rows[:count] {
				rowNumber++
				timestamp, parseErr := c.rowTimestamp(row)
				if parseErr != nil {
					writer.Close()
					outFile.Close()
					return fmt.Errorf("source parquet %s row %d: %w", sourcePath, rowNumber, parseErr)
				}
				if timestamp.Before(from) || !timestamp.Before(to) {
					continue
				}
				filtered = append(filtered, row)
			}
			if len(filtered) > 0 {
				if _, err := writer.Write(filtered); err != nil {
					writer.Close()
					outFile.Close()
					return err
				}
			}

			if errors.Is(err, io.EOF) {
				break
			}
		}

		if err := errors.Join(writer.Close(), outFile.Close()); err != nil {
			return err
		}
		return commitAtomic(tempPath, outputPath)
	}

	tempPath, err := createAtomicTempPath(outputPath)
	if err != nil {
		return err
	}
	defer os.Remove(tempPath)

	_, csvWriter, closeWriter, err := c.createCSVWriter(tempPath)
	if err != nil {
		return err
	}
	if err := csvWriter.Write(columns); err != nil {
		closeWriter()
		return err
	}

	rowNumber := 0
	for {
		rows := make([]map[string]any, 256)
		for index := range rows {
			rows[index] = make(map[string]any, len(columns))
		}

		count, err := reader.Read(rows)
		if err != nil && !errors.Is(err, io.EOF) {
			closeWriter()
			return err
		}

		for _, row := range rows[:count] {
			rowNumber++
			timestamp, parseErr := c.rowTimestamp(row)
			if parseErr != nil {
				closeWriter()
				return fmt.Errorf("source parquet %s row %d: %w", sourcePath, rowNumber, parseErr)
			}
			if timestamp.Before(from) || !timestamp.Before(to) {
				continue
			}
			record := parquetRecordStrings(columns, row)
			if err := csvWriter.Write(record); err != nil {
				closeWriter()
				return err
			}
		}

		if errors.Is(err, io.EOF) {
			break
		}
	}

	if err := closeWriter(); err != nil {
		return err
	}
	return commitAtomic(tempPath, outputPath)
}

func (c *Config) extractRangeCSVToParquet(sourcePath string, outputPath string, from time.Time, to time.Time) error {
	_, readerFactory, closeReader, err := c.openCSVReader(sourcePath)
	if err != nil {
		return err
	}
	defer closeReader()

	reader := readerFactory()
	header, err := reader.Read()
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

	file, err := os.Create(tempPath)
	if err != nil {
		return err
	}
	writer := c.parquetWriterFactory(file, parquetSchemaForColumns(header))
	writer.SetKeyValueMetadata(parquetColumnsMetadataKey, strings.Join(header, ","))
	batcher := newParquetBatcher(writer)

	for {
		record, readErr := reader.Read()
		if errors.Is(readErr, io.EOF) {
			break
		}
		if readErr != nil {
			writer.Close()
			file.Close()
			return readErr
		}
		if len(record) == 0 {
			continue
		}
		if timestampIndex >= len(record) {
			writer.Close()
			file.Close()
			return fmt.Errorf("source CSV %s contains a malformed row", sourcePath)
		}

		timestamp, err := c.ParseTimestamp(record[timestampIndex])
		if err != nil {
			writer.Close()
			file.Close()
			return fmt.Errorf("parse source CSV timestamp %q: %w", record[timestampIndex], err)
		}
		if timestamp.Before(from) || !timestamp.Before(to) {
			continue
		}

		row, err := parquetRecordFromCSVRecord(header, record)
		if err != nil {
			writer.Close()
			file.Close()
			return err
		}
		if err := batcher.add(row); err != nil {
			writer.Close()
			file.Close()
			return err
		}
	}

	flushErr := batcher.flush()
	if err := errors.Join(flushErr, writer.Close(), file.Close()); err != nil {
		return err
	}
	return commitAtomic(tempPath, outputPath)
}

func extractRangeCSVToParquet(sourcePath string, outputPath string, from time.Time, to time.Time) error {
	return DefaultConfig().extractRangeCSVToParquet(sourcePath, outputPath, from, to)
}

func extractRangeFromParquet(sourcePath string, outputPath string, from time.Time, to time.Time) error {
	return DefaultConfig().extractRangeFromParquet(sourcePath, outputPath, from, to)
}
