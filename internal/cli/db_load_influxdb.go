package cli

import (
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

// ingestInfluxDB reads the CSV file and streams InfluxDB Line Protocol to
// the /api/v2/write endpoint in gzip-compressed batches.
func ingestInfluxDB(
	ctx context.Context,
	stdout io.Writer,
	stderr io.Writer,
	inputPath string,
	rawURL string,
	measurement string,
	org string,
	bucket string,
	token string,
	symbolTag string,
	batchSize int,
	timeout time.Duration,
) error {
	lower := strings.ToLower(inputPath)
	if strings.HasSuffix(lower, ".parquet") {
		return errors.New("InfluxDB direct ingestion requires CSV input; convert to CSV first using dukascopy-go download")
	}
	if !strings.HasSuffix(lower, ".csv") && !strings.HasSuffix(lower, ".csv.gz") {
		return fmt.Errorf("unsupported file type for InfluxDB ingestion: %q (supported: .csv, .csv.gz)", inputPath)
	}

	f, err := os.Open(inputPath)
	if err != nil {
		return fmt.Errorf("cannot open input file: %w", err)
	}
	defer f.Close()

	var reader io.Reader = f
	if strings.HasSuffix(lower, ".csv.gz") {
		gz, gzErr := gzip.NewReader(f)
		if gzErr != nil {
			return fmt.Errorf("failed to open gzip reader: %w", gzErr)
		}
		defer gz.Close()
		reader = gz
	}

	// The delimiter is detected from the header (files written with
	// --csv-delimiter or the ninjatrader preset use ';').
	csvReader := newDetectedCSVReader(reader)
	csvReader.FieldsPerRecord = -1 // flexible

	// Read header
	header, err := csvReader.Read()
	if err != nil {
		return fmt.Errorf("failed to read CSV header: %w", err)
	}

	if batchSize <= 0 {
		batchSize = defaultInfluxDBBatchRows
	}
	if timeout <= 0 {
		timeout = defaultDBLoadTimeout
	}

	colIndex := buildColumnIndex(header)
	endpoint := buildInfluxDBWriteURL(rawURL, org, bucket)
	httpClient := &http.Client{Timeout: timeout}

	var (
		totalRows int
		batchRows int
		batchBuf  bytes.Buffer
		gzWriter  *gzip.Writer
	)

	gzWriter = gzip.NewWriter(&batchBuf)

	fmt.Fprintf(stderr, "%sdb-load%s streaming CSV rows to InfluxDB bucket %q measurement %q...\n",
		colorize(colorCyan), colorize(colorReset), bucket, measurement)

	flushBatch := func() error {
		if batchRows == 0 {
			return nil
		}
		if err := gzWriter.Close(); err != nil {
			return err
		}
		data := make([]byte, batchBuf.Len())
		copy(data, batchBuf.Bytes())

		req, reqErr := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(data))
		if reqErr != nil {
			return fmt.Errorf("failed to build InfluxDB request: %w", reqErr)
		}
		req.Header.Set("Authorization", "Token "+token)
		req.Header.Set("Content-Encoding", "gzip")
		req.Header.Set("Content-Type", "text/plain; charset=utf-8")

		resp, doErr := httpClient.Do(req)
		if doErr != nil {
			return fmt.Errorf("InfluxDB HTTP request failed: %w", doErr)
		}
		defer resp.Body.Close()
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))

		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return fmt.Errorf("InfluxDB returned %s: %s", resp.Status, strings.TrimSpace(string(body)))
		}

		totalRows += batchRows
		batchRows = 0
		batchBuf.Reset()
		gzWriter.Reset(&batchBuf)
		return nil
	}

	var skippedRows int
	var warnCount int
	var readRows int

	// fail wraps an error so that rows committed by earlier batches are
	// reported instead of being silently dropped from the result.
	fail := func(err error) error {
		if totalRows > 0 {
			return &partialLoadError{Target: "InfluxDB", CommittedRows: totalRows, Err: err}
		}
		return err
	}

	for {
		if err := ctx.Err(); err != nil {
			return fail(err)
		}
		row, readErr := csvReader.Read()
		if readErr == io.EOF {
			break
		}
		readRows++
		if readErr != nil {
			return fail(fmt.Errorf("CSV read error at row %d: %w", readRows, readErr))
		}

		line, lineErr := buildLineProtocol(row, colIndex, measurement, symbolTag)
		if lineErr != nil && isNonFiniteFieldError(lineErr) {
			return fail(fmt.Errorf("row %d: %w", readRows, lineErr))
		}
		if lineErr != nil || line == "" {
			skippedRows++
			if warnCount < 10 {
				warnCount++
				reason := "empty protocol line"
				if lineErr != nil {
					reason = lineErr.Error()
				}
				fmt.Fprintf(stderr, "%sdb-load%s warning: skipping malformed row %d: %s\n",
					colorize(colorYellow), colorize(colorReset), readRows, reason)
			}
			continue // skip malformed rows
		}

		_, _ = fmt.Fprintln(gzWriter, line)
		batchRows++

		if batchRows >= batchSize {
			if err := flushBatch(); err != nil {
				return fail(err)
			}
			fmt.Fprintf(stderr, "  ... %d rows written\n", totalRows)
		}
	}

	// Flush remaining rows
	if err := flushBatch(); err != nil {
		return fail(err)
	}

	if skippedRows > 0 {
		fmt.Fprintf(stderr, "%sdb-load%s warning: skipped %d malformed rows during InfluxDB ingestion\n",
			colorize(colorYellow), colorize(colorReset), skippedRows)
	}

	fmt.Fprintf(stdout, "%sdb-load%s InfluxDB: successfully wrote %d rows from %q into bucket %q measurement %q\n",
		colorize(colorGreen), colorize(colorReset), totalRows, inputPath, bucket, measurement)
	return nil
}

func buildInfluxDBWriteURL(rawURL string, org string, bucket string) string {
	base := strings.TrimRight(strings.TrimSpace(rawURL), "/")
	u, err := url.Parse(base)
	if err != nil {
		return base + "/api/v2/write?org=" + url.QueryEscape(org) + "&bucket=" + url.QueryEscape(bucket) + "&precision=ms"
	}
	u.Path = strings.TrimRight(u.Path, "/") + "/api/v2/write"
	q := u.Query()
	q.Set("org", org)
	q.Set("bucket", bucket)
	q.Set("precision", "ms")
	u.RawQuery = q.Encode()
	return u.String()
}

// buildLineProtocol converts a CSV row to InfluxDB Line Protocol string.
// It supports both tick schema (timestamp,bid,ask) and bar schema (timestamp,open,high,low,close,volume).
func buildLineProtocol(row []string, colIndex map[string]int, measurement string, symbolTag string) (string, error) {
	getField := func(name string) (string, bool) {
		i, ok := colIndex[name]
		if !ok || i >= len(row) {
			return "", false
		}
		return strings.TrimSpace(row[i]), true
	}

	// Parse timestamp (RFC3339, any preset/--timestamp-format layout, or Unix millis)
	var tsMS int64
	if tsStr, ok := getField("timestamp"); ok && tsStr != "" {
		t, err := parseLoadTimestamp(tsStr)
		if err != nil {
			return "", err
		}
		tsMS = t.UnixMilli()
	} else {
		return "", errors.New("missing timestamp column")
	}

	var fields []string

	// Try bar schema first
	barCols := []string{"open", "high", "low", "close", "volume",
		"mid_open", "mid_high", "mid_low", "mid_close",
		"bid_open", "ask_open"}
	isBar := false
	for _, col := range barCols {
		if _, ok := colIndex[col]; ok {
			isBar = true
			break
		}
	}

	if isBar {
		for _, col := range []string{"open", "high", "low", "close", "volume",
			"mid_open", "mid_high", "mid_low", "mid_close", "spread",
			"bid_open", "bid_high", "bid_low", "bid_close",
			"ask_open", "ask_high", "ask_low", "ask_close"} {
			if v, ok := getField(col); ok && v != "" {
				if f, err := strconv.ParseFloat(v, 64); err == nil {
					if math.IsNaN(f) || math.IsInf(f, 0) {
						return "", &nonFiniteFieldError{Column: col, Value: v}
					}
					fields = append(fields, col+"="+v)
				}
			}
		}
	} else {
		// Tick schema
		for _, col := range []string{"bid", "ask", "bid_volume", "ask_volume"} {
			if v, ok := getField(col); ok && v != "" {
				if f, err := strconv.ParseFloat(v, 64); err == nil {
					if math.IsNaN(f) || math.IsInf(f, 0) {
						return "", &nonFiniteFieldError{Column: col, Value: v}
					}
					fields = append(fields, col+"="+v)
				}
			}
		}
	}

	if len(fields) == 0 {
		return "", errors.New("no numeric fields found")
	}

	tags := ""
	if strings.TrimSpace(symbolTag) != "" {
		escapedSymbol := strings.ReplaceAll(strings.ToLower(strings.TrimSpace(symbolTag)), ",", "\\,")
		escapedSymbol = strings.ReplaceAll(escapedSymbol, "=", "\\=")
		escapedSymbol = strings.ReplaceAll(escapedSymbol, " ", "\\ ")
		tags = ",symbol=" + escapedSymbol
	}

	return fmt.Sprintf("%s%s %s %d", measurement, tags, strings.Join(fields, ","), tsMS), nil
}
