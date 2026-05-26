package cli

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/csv"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

// ---------------------------------------------------------------------------
// db-load: Direct database ingestion — ClickHouse and InfluxDB
// ---------------------------------------------------------------------------

const (
	dbClickHouse = "clickhouse"
	dbInfluxDB   = "influxdb"
	dbPostgres   = "postgres"

	defaultClickHouseBatchRows = 10000
	defaultInfluxDBBatchRows   = 5000
)

// runDBLoad is the CLI entry-point for the `db-load` command.
func runDBLoad(args []string, stdout io.Writer, stderr io.Writer) error {
	fs := flag.NewFlagSet("db-load", flag.ContinueOnError)
	fs.SetOutput(stdout)
	fs.Usage = func() {
		fmt.Fprintf(stdout, "%sdb-load:%s Ingest a CSV or Parquet file directly into ClickHouse or InfluxDB\n\n", colorize(colorCyan), colorize(colorReset))
		fmt.Fprint(stdout, "Usage:\n  dukascopy-go db-load [options]\n\nOptions:\n")
		fs.PrintDefaults()
		fmt.Fprint(stdout, "\nExamples:\n  dukascopy-go db-load --input ./eurusd_m1.csv --db clickhouse --url http://localhost:8123 --table eurusd_m1\n  dukascopy-go db-load --input ./eurusd_tick.csv --db influxdb --url http://localhost:8086 --org myorg --bucket mybucket --token mytoken --table eurusd_tick --symbol eurusd\n")
	}

	input    := fs.String("input", "", "path to the local CSV or Parquet file to ingest (required)")
	dbType   := fs.String("db", "", "target database: clickhouse, influxdb, or postgres (required)")
	dbURL    := fs.String("url", "", "database URL, e.g. http://localhost:8123 or postgres://user:pass@localhost:5432/dbname (required)")
	table    := fs.String("table", "", "target table or InfluxDB measurement name (required)")
	user     := fs.String("user", "default", "ClickHouse user (optional)")
	password := fs.String("password", "", "ClickHouse password or InfluxDB token (optional)")
	token    := fs.String("token", "", "InfluxDB API token (takes precedence over --password)")
	org      := fs.String("org", "", "InfluxDB organization (required for InfluxDB)")
	bucket   := fs.String("bucket", "", "InfluxDB bucket (required for InfluxDB)")
	symbol   := fs.String("symbol", "", "instrument symbol hint for tagging InfluxDB rows")
	batch    := fs.Int("batch", 0, "rows per HTTP batch (0 = use default for target db)")
	timeout  := fs.Duration("timeout", 120*time.Second, "HTTP request timeout for each batch")

	if err := fs.Parse(args); err != nil {
		return err
	}

	// Validation
	if strings.TrimSpace(*input) == "" {
		return errors.New("--input is required")
	}
	if strings.TrimSpace(*dbType) == "" {
		return errors.New("--db is required (clickhouse or influxdb)")
	}
	if strings.TrimSpace(*dbURL) == "" {
		return errors.New("--url is required")
	}
	if strings.TrimSpace(*table) == "" {
		return errors.New("--table is required")
	}

	dbTypeLower := strings.ToLower(strings.TrimSpace(*dbType))
	if dbTypeLower != dbClickHouse && dbTypeLower != dbInfluxDB && dbTypeLower != dbPostgres {
		return fmt.Errorf("unknown --db %q (supported: clickhouse, influxdb, postgres)", *dbType)
	}

	inputPath := strings.TrimSpace(*input)
	info, err := os.Stat(inputPath)
	if err != nil {
		return fmt.Errorf("cannot access --input file %q: %w", inputPath, err)
	}
	if info.IsDir() {
		return fmt.Errorf("--input must be a file, not a directory: %q", inputPath)
	}

	// Choose batch size
	batchSize := *batch
	if batchSize <= 0 {
		if dbTypeLower == dbClickHouse {
			batchSize = defaultClickHouseBatchRows
		} else {
			batchSize = defaultInfluxDBBatchRows
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	switch dbTypeLower {
	case dbClickHouse:
		return ingestClickHouse(ctx, stdout, stderr, inputPath, *dbURL, *table, *user, *password, *timeout)
	case dbPostgres:
		return ingestPostgres(ctx, stdout, stderr, inputPath, *dbURL, *table)
	case dbInfluxDB:
		authToken := strings.TrimSpace(*token)
		if authToken == "" {
			authToken = strings.TrimSpace(*password)
		}
		if authToken == "" {
			return errors.New("--token (or --password) is required for InfluxDB")
		}
		if strings.TrimSpace(*org) == "" {
			return errors.New("--org is required for InfluxDB")
		}
		if strings.TrimSpace(*bucket) == "" {
			return errors.New("--bucket is required for InfluxDB")
		}
		return ingestInfluxDB(ctx, stdout, stderr, inputPath, *dbURL, *table, *org, *bucket, authToken, *symbol, batchSize, *timeout)
	}
	return nil
}

// ---------------------------------------------------------------------------
// ClickHouse ingestion (zero driver — pure HTTP POST file stream)
// ---------------------------------------------------------------------------

// ingestClickHouse streams the local file directly to ClickHouse via HTTP.
// ClickHouse accepts CSV and Parquet natively at millions of rows/second.
func ingestClickHouse(
	ctx context.Context,
	stdout io.Writer,
	stderr io.Writer,
	inputPath string,
	rawURL string,
	table string,
	user string,
	password string,
	timeout time.Duration,
) error {
	format, err := detectClickHouseFormat(inputPath)
	if err != nil {
		return err
	}

	f, err := os.Open(inputPath)
	if err != nil {
		return fmt.Errorf("cannot open input file: %w", err)
	}
	defer f.Close()

	info, _ := f.Stat()
	sizeMB := float64(info.Size()) / (1024 * 1024)

	query := fmt.Sprintf("INSERT INTO %s FORMAT %s", table, format)
	endpoint := buildClickHouseURL(rawURL, query, user, password)

	fmt.Fprintf(stderr, "%sdb-load%s streaming %.1f MB to ClickHouse table %q [%s]...\n",
		colorize(colorCyan), colorize(colorReset), sizeMB, table, format)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, f)
	if err != nil {
		return fmt.Errorf("failed to build ClickHouse request: %w", err)
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	if format == "CSV" || format == "CSVWithNames" {
		req.Header.Set("Content-Type", "text/csv")
	} else {
		req.Header.Set("Content-Type", "application/octet-stream")
	}

	httpClient := &http.Client{Timeout: timeout}
	resp, err := httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("ClickHouse HTTP request failed: %w", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("ClickHouse returned %s: %s", resp.Status, strings.TrimSpace(string(body)))
	}

	fmt.Fprintf(stdout, "%sdb-load%s ClickHouse: successfully ingested %q into table %q\n",
		colorize(colorGreen), colorize(colorReset), inputPath, table)
	return nil
}

func buildClickHouseURL(rawURL string, query string, user string, password string) string {
	base := strings.TrimRight(strings.TrimSpace(rawURL), "/")
	u, err := url.Parse(base)
	if err != nil {
		return base + "/?query=" + url.QueryEscape(query)
	}
	q := u.Query()
	q.Set("query", query)
	if user != "" && user != "default" {
		q.Set("user", user)
	}
	if password != "" {
		q.Set("password", password)
	}
	u.RawQuery = q.Encode()
	return u.String()
}

func detectClickHouseFormat(path string) (string, error) {
	lower := strings.ToLower(path)
	switch {
	case strings.HasSuffix(lower, ".parquet"):
		return "Parquet", nil
	case strings.HasSuffix(lower, ".csv.gz"):
		return "CSVWithNames", nil
	case strings.HasSuffix(lower, ".csv"):
		return "CSVWithNames", nil
	default:
		return "", fmt.Errorf("unsupported file type for ClickHouse ingestion: %q (supported: .csv, .csv.gz, .parquet)", path)
	}
}

// ---------------------------------------------------------------------------
// InfluxDB ingestion (HTTP Line Protocol batch writer)
// ---------------------------------------------------------------------------

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

	csvReader := csv.NewReader(reader)
	csvReader.FieldsPerRecord = -1 // flexible

	// Read header
	header, err := csvReader.Read()
	if err != nil {
		return fmt.Errorf("failed to read CSV header: %w", err)
	}

	colIndex := buildColumnIndex(header)
	endpoint := buildInfluxDBWriteURL(rawURL, org, bucket)
	httpClient := &http.Client{Timeout: timeout}

	var (
		totalRows    int
		batchRows    int
		batchBuf     bytes.Buffer
		gzWriter     *gzip.Writer
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

	for {
		row, readErr := csvReader.Read()
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			return fmt.Errorf("CSV read error at row %d: %w", totalRows+batchRows+2, readErr)
		}

		line, lineErr := buildLineProtocol(row, colIndex, measurement, symbolTag)
		if lineErr != nil || line == "" {
			continue // skip malformed rows
		}

		_, _ = fmt.Fprintln(gzWriter, line)
		batchRows++

		if batchRows >= batchSize {
			if err := flushBatch(); err != nil {
				return err
			}
			fmt.Fprintf(stderr, "  ... %d rows written\n", totalRows)
		}
	}

	// Flush remaining rows
	if err := flushBatch(); err != nil {
		return err
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

// buildColumnIndex returns a map from column name → position for fast lookup.
func buildColumnIndex(header []string) map[string]int {
	idx := make(map[string]int, len(header))
	for i, col := range header {
		idx[strings.TrimSpace(strings.ToLower(col))] = i
	}
	return idx
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

	// Parse timestamp (RFC3339 or Unix millis)
	var tsMS int64
	if tsStr, ok := getField("timestamp"); ok && tsStr != "" {
		if t, err := time.Parse(time.RFC3339Nano, tsStr); err == nil {
			tsMS = t.UnixMilli()
		} else if t, err := time.Parse(time.RFC3339, tsStr); err == nil {
			tsMS = t.UnixMilli()
		} else if ms, err := strconv.ParseInt(tsStr, 10, 64); err == nil {
			tsMS = ms
		} else {
			return "", fmt.Errorf("unrecognized timestamp format: %q", tsStr)
		}
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
				if _, err := strconv.ParseFloat(v, 64); err == nil {
					fields = append(fields, col+"="+v)
				}
			}
		}
	} else {
		// Tick schema
		for _, col := range []string{"bid", "ask", "bid_volume", "ask_volume"} {
			if v, ok := getField(col); ok && v != "" {
				if _, err := strconv.ParseFloat(v, 64); err == nil {
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
		tags = ",symbol=" + strings.ToLower(strings.TrimSpace(symbolTag))
	}

	return fmt.Sprintf("%s%s %s %d", measurement, tags, strings.Join(fields, ","), tsMS), nil
}

type DBLoadOptions struct {
	DBType    string
	DBURL     string
	Table     string
	InputPath string
	User      string
	Password  string
	Token     string
	Org       string
	Bucket    string
	SymbolTag string
	BatchSize int
	Timeout   time.Duration
}

func DBLoad(ctx context.Context, stdout io.Writer, stderr io.Writer, opt DBLoadOptions) error {
	dbTypeLower := strings.ToLower(strings.TrimSpace(opt.DBType))
	switch dbTypeLower {
	case "clickhouse":
		return ingestClickHouse(ctx, stdout, stderr, opt.InputPath, opt.DBURL, opt.Table, opt.User, opt.Password, opt.Timeout)
	case "postgres":
		return ingestPostgres(ctx, stdout, stderr, opt.InputPath, opt.DBURL, opt.Table)
	case "influxdb":
		authToken := strings.TrimSpace(opt.Token)
		if authToken == "" {
			authToken = strings.TrimSpace(opt.Password)
		}
		return ingestInfluxDB(ctx, stdout, stderr, opt.InputPath, opt.DBURL, opt.Table, opt.Org, opt.Bucket, authToken, opt.SymbolTag, opt.BatchSize, opt.Timeout)
	default:
		return fmt.Errorf("unknown --db %q (supported: clickhouse, influxdb, postgres)", opt.DBType)
	}
}
