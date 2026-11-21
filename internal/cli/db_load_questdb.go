package cli

import (
	"bufio"
	"compress/gzip"
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

// ingestQuestDB streams the CSV file to QuestDB via native InfluxDB Line Protocol.
// It supports two write paths:
//   - TCP ILP (recommended): plain TCP socket on port 9009
//   - HTTP ILP: HTTP POST to /write on port 9000
func ingestQuestDB(
	ctx context.Context,
	stdout io.Writer,
	stderr io.Writer,
	inputPath string,
	dbURL string,
	table string,
	ilpPort int,
	symbolTag string,
	batchSize int,
	timeout time.Duration,
) error {
	lower := strings.ToLower(inputPath)
	if strings.HasSuffix(lower, ".parquet") {
		return fmt.Errorf("questdb ingestion requires CSV input; convert to CSV first using dukascopy-go download")
	}
	if !strings.HasSuffix(lower, ".csv") && !strings.HasSuffix(lower, ".csv.gz") {
		return fmt.Errorf("unsupported file type for QuestDB ingestion: %q (supported: .csv, .csv.gz)", inputPath)
	}

	f, err := os.Open(inputPath)
	if err != nil {
		return fmt.Errorf("cannot open input file: %w", err)
	}
	defer f.Close()

	info, _ := f.Stat()
	sizeMB := float64(info.Size()) / (1024 * 1024)

	var reader io.Reader = f
	var gzReader *gzip.Reader
	if strings.HasSuffix(lower, ".csv.gz") {
		gzReader, err = gzip.NewReader(f)
		if err != nil {
			return fmt.Errorf("failed to open gzip reader: %w", err)
		}
		defer gzReader.Close()
		reader = gzReader
	}

	// The delimiter is detected from the header (files written with
	// --csv-delimiter or the ninjatrader preset use ';').
	csvReader := newDetectedCSVReader(reader)
	csvReader.FieldsPerRecord = -1

	header, err := csvReader.Read()
	if err != nil {
		return fmt.Errorf("failed to read CSV header: %w", err)
	}

	colIndex := buildColumnIndex(header)

	// Determine write path: TCP or HTTP
	mode, host, port, httpURL, err := parseQuestDBURL(dbURL, ilpPort)
	if err != nil {
		return err
	}

	fmt.Fprintf(stderr, "%sdb-load%s streaming %.1f MB to QuestDB table %q [%s %s:%d]...\n",
		colorize(colorCyan), colorize(colorReset), sizeMB, table, mode, host, port)

	var totalRows int

	switch mode {
	case "tcp":
		totalRows, err = questDBTCPWrite(ctx, csvReader, colIndex, host, port, table, symbolTag, batchSize, timeout, stderr)
	case "http":
		totalRows, err = questDBHTTPWrite(ctx, csvReader, colIndex, httpURL, table, symbolTag, batchSize, timeout, stderr)
	default:
		return fmt.Errorf("unknown questdb write mode %q", mode)
	}

	if err != nil {
		if mode == "http" && totalRows > 0 {
			return &partialLoadError{Target: "QuestDB", CommittedRows: totalRows, Err: err}
		}
		return err
	}

	fmt.Fprintf(stdout, "%sdb-load%s QuestDB: successfully wrote %d rows from %q into table %q\n",
		colorize(colorGreen), colorize(colorReset), totalRows, inputPath, table)
	return nil
}

// parseQuestDBURL determines whether to use TCP or HTTP ILP based on the URL scheme.
func parseQuestDBURL(rawURL string, ilpPort int) (mode string, host string, port int, httpURL string, err error) {
	rawURL = strings.TrimSpace(rawURL)

	// Check if it's an HTTP URL
	if strings.HasPrefix(rawURL, "http://") || strings.HasPrefix(rawURL, "https://") {
		parsed, parseErr := url.Parse(rawURL)
		if parseErr != nil {
			return "", "", 0, "", fmt.Errorf("invalid questdb http url %q: %w", rawURL, parseErr)
		}
		httpPort := parsed.Port()
		if httpPort == "" {
			httpPort = "9000"
		}
		port, _ = strconv.Atoi(httpPort)
		return "http", parsed.Hostname(), port, rawURL, nil
	}

	// TCP URL: tcp://host:port or plain host:port
	rawURL = strings.TrimPrefix(rawURL, "tcp://")

	host, portStr, splitErr := net.SplitHostPort(rawURL)
	if splitErr != nil {
		// No port in URL, use default or override
		host = rawURL
		if ilpPort > 0 {
			port = ilpPort
		} else {
			port = 9009
		}
	} else {
		port, _ = strconv.Atoi(portStr)
		if ilpPort > 0 {
			port = ilpPort
		}
	}

	if host == "" {
		host = "localhost"
	}

	return "tcp", host, port, "", nil
}

// questDBDialRetries / questDBDialBackoff control how often the initial TCP
// connection is retried. Only the dial is retried: ILP over TCP has no
// acknowledgements, so once rows have been sent a reconnect could neither
// tell which rows arrived nor safely resume.
var (
	questDBDialRetries = 3
	questDBDialBackoff = 500 * time.Millisecond
)

// questDBTCPBufferSize is the size of the buffered writer wrapping the socket.
const questDBTCPBufferSize = 64 * 1024

// questDBTCPWrite sends ILP lines to QuestDB over a plain TCP socket.
func questDBTCPWrite(
	ctx context.Context,
	csvReader *csv.Reader,
	colIndex map[string]int,
	host string,
	port int,
	table string,
	symbolTag string,
	batchSize int,
	timeout time.Duration,
	stderr io.Writer,
) (int, error) {
	if batchSize <= 0 {
		batchSize = defaultQuestDBBatchRows
	}
	if timeout <= 0 {
		timeout = defaultDBLoadTimeout
	}
	addr := net.JoinHostPort(host, strconv.Itoa(port))
	dialer := &net.Dialer{Timeout: timeout}

	var conn net.Conn
	for attempt := 0; ; attempt++ {
		var dialErr error
		conn, dialErr = dialer.DialContext(ctx, "tcp", addr)
		if dialErr == nil {
			break
		}
		if ctx.Err() != nil {
			return 0, ctx.Err()
		}
		if attempt >= questDBDialRetries {
			return 0, fmt.Errorf("failed to connect to QuestDB at %s: %w", addr, dialErr)
		}
		if err := sleepWithContext(ctx, time.Duration(attempt+1)*questDBDialBackoff); err != nil {
			return 0, err
		}
	}
	defer conn.Close()
	// Closing the socket on cancellation unblocks a pending write.
	stopAfter := context.AfterFunc(ctx, func() { _ = conn.Close() })
	defer stopAfter()

	writer := bufio.NewWriterSize(conn, questDBTCPBufferSize)

	var (
		confirmedRows int // rows whose bytes were fully flushed to the socket
		bufferedRows  int // rows handed to the writer since the last flush
		readRows      int // data rows read from the CSV (for row numbers)
		skippedRows   int
		warnCount     int
	)

	// sendFailure reports a write failure. Nothing is retried once data may
	// have reached the server.
	sendFailure := func(writeErr error) (int, error) {
		if ctx.Err() != nil {
			writeErr = ctx.Err()
		}
		if confirmedRows == 0 && bufferedRows == 0 {
			return 0, fmt.Errorf("questdb tcp write to %s failed before any rows were sent: %w", addr, writeErr)
		}
		return confirmedRows, fmt.Errorf("questdb tcp write to %s failed: %d rows were sent before the failure and up to %d more may have reached the server; not resuming (rerunning would duplicate the sent rows): %w",
			addr, confirmedRows, bufferedRows, writeErr)
	}

	flush := func() error {
		if err := conn.SetWriteDeadline(time.Now().Add(timeout)); err != nil {
			return err
		}
		if err := writer.Flush(); err != nil {
			return err
		}
		confirmedRows += bufferedRows
		bufferedRows = 0
		return nil
	}

	for {
		if err := ctx.Err(); err != nil {
			if confirmedRows+bufferedRows == 0 {
				return 0, err
			}
			return sendFailure(err)
		}

		row, readErr := csvReader.Read()
		if readErr == io.EOF {
			break
		}
		readRows++
		if readErr != nil {
			err := fmt.Errorf("CSV read error at row %d: %w", readRows, readErr)
			if confirmedRows+bufferedRows == 0 {
				return 0, err
			}
			// Flush what was read before the bad row so the reported count
			// matches what the server received.
			if flushErr := flush(); flushErr != nil {
				return sendFailure(flushErr)
			}
			return confirmedRows, fmt.Errorf("%w (%d rows were already sent to QuestDB)", err, confirmedRows)
		}

		line, lineErr := buildQuestDBLineProtocol(row, colIndex, table, symbolTag)
		if lineErr != nil && isNonFiniteFieldError(lineErr) {
			err := fmt.Errorf("row %d: %w", readRows, lineErr)
			if confirmedRows+bufferedRows == 0 {
				return 0, err
			}
			if flushErr := flush(); flushErr != nil {
				return sendFailure(flushErr)
			}
			return confirmedRows, fmt.Errorf("%w (%d rows were already sent to QuestDB)", err, confirmedRows)
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
			continue
		}

		// The buffered writer flushes to the socket on its own when full;
		// make sure that implicit write is bounded by a deadline too.
		if writer.Available() < len(line)+1 {
			if err := conn.SetWriteDeadline(time.Now().Add(timeout)); err != nil {
				return sendFailure(err)
			}
		}
		if _, err := writer.WriteString(line); err != nil {
			return sendFailure(err)
		}
		if err := writer.WriteByte('\n'); err != nil {
			return sendFailure(err)
		}
		bufferedRows++

		if bufferedRows >= batchSize {
			if err := flush(); err != nil {
				return sendFailure(err)
			}
			fmt.Fprintf(stderr, "  ... %d rows written\n", confirmedRows)
		}
	}

	if err := flush(); err != nil {
		return sendFailure(err)
	}
	if skippedRows > 0 {
		fmt.Fprintf(stderr, "%sdb-load%s warning: skipped %d malformed rows during QuestDB TCP ingestion\n",
			colorize(colorYellow), colorize(colorReset), skippedRows)
	}
	return confirmedRows, nil
}

// questDBHTTPWrite sends ILP lines to QuestDB via HTTP POST /write. Each batch
// is committed independently; on failure it returns the number of rows that
// earlier batches already committed together with the error.
func questDBHTTPWrite(
	ctx context.Context,
	csvReader *csv.Reader,
	colIndex map[string]int,
	baseURL string,
	table string,
	symbolTag string,
	batchSize int,
	timeout time.Duration,
	stderr io.Writer,
) (int, error) {
	if batchSize <= 0 {
		batchSize = defaultQuestDBBatchRows
	}
	if timeout <= 0 {
		timeout = defaultDBLoadTimeout
	}
	baseURL = strings.TrimRight(baseURL, "/")
	endpoint := baseURL + "/write?precision=n"

	httpClient := &http.Client{Timeout: timeout}

	var (
		totalRows int
		batchRows int
		readRows  int
		batchBuf  strings.Builder
	)

	flushHTTPBatch := func() error {
		if batchRows == 0 {
			return nil
		}

		req, reqErr := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, strings.NewReader(batchBuf.String()))
		if reqErr != nil {
			return fmt.Errorf("failed to build QuestDB request: %w", reqErr)
		}
		req.Header.Set("Content-Type", "text/plain; charset=utf-8")

		resp, doErr := httpClient.Do(req)
		if doErr != nil {
			return fmt.Errorf("questdb HTTP request failed: %w", doErr)
		}
		defer resp.Body.Close()

		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return fmt.Errorf("questdb returned %s: %s", resp.Status, strings.TrimSpace(string(body)))
		}

		totalRows += batchRows
		batchRows = 0
		batchBuf.Reset()
		return nil
	}

	var skippedRows int
	var warnCount int

	for {
		if err := ctx.Err(); err != nil {
			return totalRows, err
		}

		row, readErr := csvReader.Read()
		if readErr == io.EOF {
			break
		}
		readRows++
		if readErr != nil {
			return totalRows, fmt.Errorf("CSV read error at row %d: %w", readRows, readErr)
		}

		line, lineErr := buildQuestDBLineProtocol(row, colIndex, table, symbolTag)
		if lineErr != nil && isNonFiniteFieldError(lineErr) {
			return totalRows, fmt.Errorf("row %d: %w", readRows, lineErr)
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
			continue
		}

		batchBuf.WriteString(line)
		batchBuf.WriteString("\n")
		batchRows++

		if batchRows >= batchSize {
			if err := flushHTTPBatch(); err != nil {
				return totalRows, err
			}
			fmt.Fprintf(stderr, "  ... %d rows written\n", totalRows)
		}
	}

	if err := flushHTTPBatch(); err != nil {
		return totalRows, err
	}

	if skippedRows > 0 {
		fmt.Fprintf(stderr, "%sdb-load%s warning: skipped %d malformed rows during QuestDB HTTP ingestion\n",
			colorize(colorYellow), colorize(colorReset), skippedRows)
	}

	return totalRows, nil
}

// buildQuestDBLineProtocol converts a CSV row to QuestDB InfluxDB Line Protocol string with nanosecond precision.
func buildQuestDBLineProtocol(row []string, colIndex map[string]int, table string, symbolTag string) (string, error) {
	getField := func(name string) (string, bool) {
		i, ok := colIndex[name]
		if !ok || i >= len(row) {
			return "", false
		}
		return strings.TrimSpace(row[i]), true
	}

	var tsNS int64
	if tsStr, ok := getField("timestamp"); ok && tsStr != "" {
		t, err := parseLoadTimestamp(tsStr)
		if err != nil {
			return "", err
		}
		tsNS = t.UnixNano()
	} else {
		return "", fmt.Errorf("missing timestamp column")
	}

	var fields []string
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
		return "", fmt.Errorf("no numeric fields found")
	}

	tags := ""
	if strings.TrimSpace(symbolTag) != "" {
		escapedSymbol := strings.ReplaceAll(strings.ToLower(strings.TrimSpace(symbolTag)), ",", "\\,")
		escapedSymbol = strings.ReplaceAll(escapedSymbol, "=", "\\=")
		escapedSymbol = strings.ReplaceAll(escapedSymbol, " ", "\\ ")
		tags = ",symbol=" + escapedSymbol
	}

	return fmt.Sprintf("%s%s %s %d", table, tags, strings.Join(fields, ","), tsNS), nil
}
