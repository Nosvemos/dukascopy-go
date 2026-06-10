package cli

import (
	"compress/gzip"
	"context"
	"encoding/csv"
	"fmt"
	"io"
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

	csvReader := csv.NewReader(reader)
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
	if strings.HasPrefix(rawURL, "tcp://") {
		rawURL = strings.TrimPrefix(rawURL, "tcp://")
	}

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
	addr := net.JoinHostPort(host, strconv.Itoa(port))
	dialer := &net.Dialer{Timeout: timeout}

	var totalRows int

	for retry := 0; retry <= 3; retry++ {
		conn, dialErr := dialer.DialContext(ctx, "tcp", addr)
		if dialErr != nil {
			if retry == 3 {
				return 0, fmt.Errorf("failed to connect to QuestDB at %s: %w", addr, dialErr)
			}
			select {
			case <-ctx.Done():
				return 0, ctx.Err()
			case <-time.After(time.Duration(retry+1) * 500 * time.Millisecond):
			}
			continue
		}

		totalRows = 0
		batchCount := 0
		writeErr := func() error {
			defer conn.Close()

			if tcpConn, ok := conn.(*net.TCPConn); ok {
				_ = tcpConn.SetNoDelay(true)
			}

			for {
				if ctx.Err() != nil {
					return ctx.Err()
				}

				row, readErr := csvReader.Read()
				if readErr == io.EOF {
					break
				}
				if readErr != nil {
					return fmt.Errorf("CSV read error at row %d: %w", totalRows+2, readErr)
				}

				line, lineErr := buildLineProtocol(row, colIndex, table, symbolTag)
				if lineErr != nil || line == "" {
					continue
				}

				if _, writeError := fmt.Fprintf(conn, "%s\n", line); writeError != nil {
					return fmt.Errorf("questdb tcp write error at row %d: %w", totalRows+2, writeError)
				}
				totalRows++
				batchCount++

				if batchCount%batchSize == 0 {
					fmt.Fprintf(stderr, "  ... %d rows written\n", totalRows)
				}
			}
			return nil
		}()

		if writeErr != nil {
			if retry == 3 {
				return 0, writeErr
			}
			continue
		}

		break
	}

	return totalRows, nil
}

// questDBHTTPWrite sends ILP lines to QuestDB via HTTP POST /write.
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
	baseURL = strings.TrimRight(baseURL, "/")
	endpoint := baseURL + "/write"

	httpClient := &http.Client{Timeout: timeout}

	var (
		totalRows int
		batchRows int
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

	for {
		if ctx.Err() != nil {
			return 0, ctx.Err()
		}

		row, readErr := csvReader.Read()
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			return 0, fmt.Errorf("CSV read error at row %d: %w", totalRows+batchRows+2, readErr)
		}

		line, lineErr := buildLineProtocol(row, colIndex, table, symbolTag)
		if lineErr != nil || line == "" {
			continue
		}

		batchBuf.WriteString(line)
		batchBuf.WriteString("\n")
		batchRows++

		if batchRows >= batchSize {
			if err := flushHTTPBatch(); err != nil {
				return 0, err
			}
			fmt.Fprintf(stderr, "  ... %d rows written\n", totalRows)
		}
	}

	if err := flushHTTPBatch(); err != nil {
		return 0, err
	}

	return totalRows, nil
}
