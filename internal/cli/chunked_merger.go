package cli

import (
	"compress/gzip"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/Nosvemos/dukascopy-go/pkg/csvout"
	"github.com/Nosvemos/dukascopy-go/pkg/dukascopy"
)

// mergeChunks streams all records from chunk files sequentially to the output path(s).
func mergeChunks(
	stdout io.Writer,
	outputPath string,
	partPaths []string,
	from time.Time,
	to time.Time,
	partitionMode string,
	resultKind dukascopy.ResultKind,
	barColumns []string,
	tickColumns []string,
	hive bool,
) (totalRowsWritten int, retErr error) {
	columns := barColumns
	if resultKind == dukascopy.ResultKindTick {
		columns = tickColumns
	}

	isParquet := strings.HasSuffix(strings.ToLower(outputPath), ".parquet")
	isArrow := strings.HasSuffix(strings.ToLower(outputPath), ".arrow") ||
		strings.HasSuffix(strings.ToLower(outputPath), ".ipc") ||
		strings.HasSuffix(strings.ToLower(outputPath), ".feather")
	isGzip := strings.HasSuffix(strings.ToLower(outputPath), ".gz")

	// Determine if we need to route to partitioned output files
	isPartitionedOutput := partitionMode != "" && partitionMode != partitionNone

	var currentPartitionKey string

	var mainCsvFileWriter *os.File
	var mainGzipWriter *gzip.Writer
	var mainCsvWriter *csv.Writer
	var mainParquetWriter *csvout.ParquetStreamWriter
	var mainArrowWriter *csvout.ArrowStreamWriter

	var partCsvFileWriter *os.File
	var partGzipWriter *gzip.Writer
	var partCsvWriter *csv.Writer
	var partParquetWriter *csvout.ParquetStreamWriter
	var partArrowWriter *csvout.ArrowStreamWriter

	closeMainWriter := func() error {
		var errs []string
		if mainParquetWriter != nil {
			if err := mainParquetWriter.Close(); err != nil {
				errs = append(errs, err.Error())
			}
			mainParquetWriter = nil
		}
		if mainArrowWriter != nil {
			if err := mainArrowWriter.Close(); err != nil {
				errs = append(errs, err.Error())
			}
			mainArrowWriter = nil
		}
		if mainCsvWriter != nil {
			mainCsvWriter.Flush()
			if err := mainCsvWriter.Error(); err != nil {
				errs = append(errs, err.Error())
			}
			mainCsvWriter = nil
		}
		if mainGzipWriter != nil {
			if err := mainGzipWriter.Close(); err != nil {
				errs = append(errs, err.Error())
			}
			mainGzipWriter = nil
		}
		if mainCsvFileWriter != nil {
			if err := mainCsvFileWriter.Close(); err != nil {
				errs = append(errs, err.Error())
			}
			mainCsvFileWriter = nil
		}
		if len(errs) > 0 {
			return errors.New(strings.Join(errs, "; "))
		}
		return nil
	}
	defer func() {
		if err := closeMainWriter(); err != nil && retErr == nil {
			retErr = err
		}
	}()

	closePartWriter := func() error {
		var errs []string
		if partParquetWriter != nil {
			if err := partParquetWriter.Close(); err != nil {
				errs = append(errs, err.Error())
			}
			partParquetWriter = nil
		}
		if partArrowWriter != nil {
			if err := partArrowWriter.Close(); err != nil {
				errs = append(errs, err.Error())
			}
			partArrowWriter = nil
		}
		if partCsvWriter != nil {
			partCsvWriter.Flush()
			if err := partCsvWriter.Error(); err != nil {
				errs = append(errs, err.Error())
			}
			partCsvWriter = nil
		}
		if partGzipWriter != nil {
			if err := partGzipWriter.Close(); err != nil {
				errs = append(errs, err.Error())
			}
			partGzipWriter = nil
		}
		if partCsvFileWriter != nil {
			if err := partCsvFileWriter.Close(); err != nil {
				errs = append(errs, err.Error())
			}
			partCsvFileWriter = nil
		}
		if len(errs) > 0 {
			return errors.New(strings.Join(errs, "; "))
		}
		return nil
	}
	defer func() {
		if err := closePartWriter(); err != nil && retErr == nil {
			retErr = err
		}
	}()

	initMainWriter := func() error {
		targetPath := outputPath
		if isArrow {
			var err error
			mainArrowWriter, err = csvout.CreateArrowStreamWriter(targetPath, columns)
			if err != nil {
				return err
			}
		} else if isParquet {
			var err error
			mainParquetWriter, err = csvout.CreateParquetStreamWriter(targetPath, columns)
			if err != nil {
				return err
			}
		} else {
			var w io.Writer
			if targetPath == "-" {
				w = stdout
			} else {
				if err := os.MkdirAll(filepath.Dir(targetPath), 0o755); err != nil {
					return err
				}
				var err error
				mainCsvFileWriter, err = os.Create(targetPath)
				if err != nil {
					return err
				}
				w = mainCsvFileWriter
			}
			if isGzip {
				mainGzipWriter = gzip.NewWriter(w)
				w = mainGzipWriter
			}
			mainCsvWriter = csv.NewWriter(w)
			mainCsvWriter.Comma = csvout.CSVDelimiter

			if !csvout.HideCSVHeader {
				if err := mainCsvWriter.Write(columns); err != nil {
					return err
				}
			}
		}
		return nil
	}

	initPartWriter := func(key string, t time.Time) error {
		if err := closePartWriter(); err != nil {
			return err
		}

		var targetPath string
		if hive {
			targetPath = getHivePartitionPath(outputPath, t, partitionMode)
		} else {
			targetPath = getPartitionOutputPath(outputPath, key)
		}

		if isArrow {
			var err error
			partArrowWriter, err = csvout.CreateArrowStreamWriter(targetPath, columns)
			if err != nil {
				return err
			}
		} else if isParquet {
			var err error
			partParquetWriter, err = csvout.CreateParquetStreamWriter(targetPath, columns)
			if err != nil {
				return err
			}
		} else {
			if err := os.MkdirAll(filepath.Dir(targetPath), 0o755); err != nil {
				return err
			}
			var err error
			partCsvFileWriter, err = os.Create(targetPath)
			if err != nil {
				return err
			}
			var w io.Writer = partCsvFileWriter
			if isGzip {
				partGzipWriter = gzip.NewWriter(w)
				w = partGzipWriter
			}
			partCsvWriter = csv.NewWriter(w)
			partCsvWriter.Comma = csvout.CSVDelimiter

			if !csvout.HideCSVHeader {
				if err := partCsvWriter.Write(columns); err != nil {
					return err
				}
			}
		}
		return nil
	}

	// Always initialize the main writer
	if err := initMainWriter(); err != nil {
		return 0, err
	}

	timestampIndex := indexOfColumn(columns, "timestamp")
	if timestampIndex < 0 {
		return 0, fmt.Errorf("missing timestamp column")
	}

	cfg := csvout.DefaultConfig()
	cfg.HideHeader = true

	parquetBatchSize := 50000
	mainParquetBatch := make([]map[string]any, 0, parquetBatchSize)
	partParquetBatch := make([]map[string]any, 0, parquetBatchSize)
	mainArrowBatch := make([]map[string]any, 0, parquetBatchSize)
	partArrowBatch := make([]map[string]any, 0, parquetBatchSize)
	totalRowsWritten = 0

	for _, partPath := range partPaths {
		file, err := os.Open(partPath)
		if err != nil {
			if os.IsNotExist(err) {
				continue // skip missing chunks (should not happen if download succeeded)
			}
			return 0, err
		}

		reader := csv.NewReader(file)
		reader.Comma = csvout.CSVDelimiter
		reader.FieldsPerRecord = -1 // flexible

		isHeader := true
		for {
			record, err := reader.Read()
			if err == io.EOF {
				break
			}
			if err != nil {
				file.Close()
				return 0, err
			}
			if len(record) == 0 {
				continue
			}
			if isHeader {
				isHeader = false
				continue // skip header row in chunk file
			}

			// Parse timestamp to verify ranges and route partitions
			tsVal := record[timestampIndex]
			timestamp, err := time.Parse(time.RFC3339Nano, tsVal)
			if err != nil {
				// Fallback to other layouts
				timestamp, err = time.Parse(time.RFC3339, tsVal)
			}
			if err != nil {
				file.Close()
				return 0, fmt.Errorf("failed to parse timestamp %q: %w", tsVal, err)
			}

			timestamp = timestamp.UTC()
			if timestamp.Before(from) || !timestamp.Before(to) {
				continue
			}

			// Partition routing
			if isPartitionedOutput {
				key := getPartitionKey(timestamp, partitionMode)
				if key != currentPartitionKey {
					// Flush current parquet/arrow batch before switching partitions
					if isParquet && len(partParquetBatch) > 0 {
						if err := partParquetWriter.WriteBatch(partParquetBatch); err != nil {
							file.Close()
							return 0, err
						}
						partParquetBatch = partParquetBatch[:0]
					}
					if isArrow && len(partArrowBatch) > 0 {
						if err := partArrowWriter.WriteBatch(partArrowBatch); err != nil {
							file.Close()
							return 0, err
						}
						partArrowBatch = partArrowBatch[:0]
					}

					currentPartitionKey = key
					if err := initPartWriter(key, timestamp); err != nil {
						file.Close()
						return 0, err
					}
				}
			}

			// Write record
			if isParquet || isArrow {
				row := make(map[string]any, len(columns))
				for index, colName := range columns {
					if index >= len(record) {
						file.Close()
						return 0, fmt.Errorf("malformed CSV row in chunk %s", partPath)
					}
					val, err := parquetValueForColumn(colName, record[index])
					if err != nil {
						file.Close()
						return 0, err
					}
					row[colName] = val
				}
				if isParquet {
					mainParquetBatch = append(mainParquetBatch, row)
					totalRowsWritten++
					if len(mainParquetBatch) >= parquetBatchSize {
						if err := mainParquetWriter.WriteBatch(mainParquetBatch); err != nil {
							file.Close()
							return 0, err
						}
						mainParquetBatch = mainParquetBatch[:0]
					}

					if isPartitionedOutput {
						partParquetBatch = append(partParquetBatch, row)
						if len(partParquetBatch) >= parquetBatchSize {
							if err := partParquetWriter.WriteBatch(partParquetBatch); err != nil {
								file.Close()
								return 0, err
							}
							partParquetBatch = partParquetBatch[:0]
						}
					}
				} else if isArrow {
					mainArrowBatch = append(mainArrowBatch, row)
					totalRowsWritten++
					if len(mainArrowBatch) >= parquetBatchSize {
						if err := mainArrowWriter.WriteBatch(mainArrowBatch); err != nil {
							file.Close()
							return 0, err
						}
						mainArrowBatch = mainArrowBatch[:0]
					}

					if isPartitionedOutput {
						partArrowBatch = append(partArrowBatch, row)
						if len(partArrowBatch) >= parquetBatchSize {
							if err := partArrowWriter.WriteBatch(partArrowBatch); err != nil {
								file.Close()
								return 0, err
							}
							partArrowBatch = partArrowBatch[:0]
						}
					}
				}
			} else {
				if err := mainCsvWriter.Write(record); err != nil {
					file.Close()
					return 0, err
				}
				totalRowsWritten++

				if isPartitionedOutput {
					if err := partCsvWriter.Write(record); err != nil {
						file.Close()
						return 0, err
					}
				}
			}
		}

		file.Close()
	}

	// Flush remaining parquet/arrow records
	if isParquet {
		if len(mainParquetBatch) > 0 {
			if err := mainParquetWriter.WriteBatch(mainParquetBatch); err != nil {
				return 0, err
			}
		}
		if isPartitionedOutput && len(partParquetBatch) > 0 {
			if err := partParquetWriter.WriteBatch(partParquetBatch); err != nil {
				return 0, err
			}
		}
	} else if isArrow {
		if len(mainArrowBatch) > 0 {
			if err := mainArrowWriter.WriteBatch(mainArrowBatch); err != nil {
				return 0, err
			}
		}
		if isPartitionedOutput && len(partArrowBatch) > 0 {
			if err := partArrowWriter.WriteBatch(partArrowBatch); err != nil {
				return 0, err
			}
		}
	}

	return totalRowsWritten, nil
}
