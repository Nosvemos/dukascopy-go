package cli

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// buildClickHouseURL tests
// ---------------------------------------------------------------------------

func TestBuildClickHouseURL_Basic(t *testing.T) {
	got := buildClickHouseURL("http://localhost:8123", "INSERT INTO t FORMAT CSVWithNames", "", "")
	if !strings.Contains(got, "query=") {
		t.Errorf("expected query param in URL, got: %s", got)
	}
	if !strings.Contains(got, "INSERT+INTO+t") && !strings.Contains(got, "INSERT%20INTO%20t") {
		t.Errorf("expected query value in URL, got: %s", got)
	}
}

func TestBuildClickHouseURL_WithCredentials(t *testing.T) {
	got := buildClickHouseURL("http://localhost:8123", "INSERT INTO t FORMAT CSV", "admin", "secret")
	if !strings.Contains(got, "user=admin") {
		t.Errorf("expected user param, got: %s", got)
	}
	if !strings.Contains(got, "password=secret") {
		t.Errorf("expected password param, got: %s", got)
	}
}

func TestBuildClickHouseURL_DefaultUser(t *testing.T) {
	// "default" user should NOT be included (it's the CH default)
	got := buildClickHouseURL("http://localhost:8123", "q", "default", "")
	if strings.Contains(got, "user=") {
		t.Errorf("should not include user=default, got: %s", got)
	}
}

// ---------------------------------------------------------------------------
// detectClickHouseFormat tests
// ---------------------------------------------------------------------------

func TestDetectClickHouseFormat(t *testing.T) {
	tests := []struct {
		path   string
		format string
		hasErr bool
	}{
		{"data.parquet", "Parquet", false},
		{"data.csv", "CSVWithNames", false},
		{"data.csv.gz", "CSVWithNames", false},
		{"data.json", "", true},
		{"data.txt", "", true},
	}
	for _, tc := range tests {
		got, err := detectClickHouseFormat(tc.path)
		if tc.hasErr {
			if err == nil {
				t.Errorf("%q: expected error, got format=%q", tc.path, got)
			}
		} else {
			if err != nil {
				t.Errorf("%q: unexpected error: %v", tc.path, err)
			}
			if got != tc.format {
				t.Errorf("%q: format = %q, want %q", tc.path, got, tc.format)
			}
		}
	}
}

// ---------------------------------------------------------------------------
// buildInfluxDBWriteURL tests
// ---------------------------------------------------------------------------

func TestBuildInfluxDBWriteURL(t *testing.T) {
	got := buildInfluxDBWriteURL("http://localhost:8086", "my-org", "my-bucket")
	if !strings.Contains(got, "/api/v2/write") {
		t.Errorf("expected /api/v2/write path, got: %s", got)
	}
	if !strings.Contains(got, "org=my-org") {
		t.Errorf("expected org param, got: %s", got)
	}
	if !strings.Contains(got, "bucket=my-bucket") {
		t.Errorf("expected bucket param, got: %s", got)
	}
	if !strings.Contains(got, "precision=ms") {
		t.Errorf("expected precision=ms, got: %s", got)
	}
}

// ---------------------------------------------------------------------------
// buildColumnIndex tests
// ---------------------------------------------------------------------------

func TestBuildColumnIndex(t *testing.T) {
	header := []string{"timestamp", "bid", "ask", "bid_volume", "ask_volume"}
	idx := buildColumnIndex(header)
	if idx["timestamp"] != 0 {
		t.Errorf("timestamp index = %d, want 0", idx["timestamp"])
	}
	if idx["bid"] != 1 {
		t.Errorf("bid index = %d, want 1", idx["bid"])
	}
	if idx["ask_volume"] != 4 {
		t.Errorf("ask_volume index = %d, want 4", idx["ask_volume"])
	}
}

// ---------------------------------------------------------------------------
// buildLineProtocol tests
// ---------------------------------------------------------------------------

func TestBuildLineProtocol_Tick(t *testing.T) {
	header := []string{"timestamp", "bid", "ask", "bid_volume", "ask_volume"}
	colIdx := buildColumnIndex(header)
	row := []string{"2024-01-02T00:00:00Z", "1.08650", "1.08652", "1.5", "2.0"}

	line, err := buildLineProtocol(row, colIdx, "eurusd", "eurusd")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.HasPrefix(line, "eurusd,symbol=eurusd") {
		t.Errorf("expected measurement prefix, got: %q", line)
	}
	if !strings.Contains(line, "bid=1.08650") {
		t.Errorf("expected bid field, got: %q", line)
	}
	if !strings.Contains(line, "ask=1.08652") {
		t.Errorf("expected ask field, got: %q", line)
	}
}

func TestBuildLineProtocol_Bar(t *testing.T) {
	header := []string{"timestamp", "open", "high", "low", "close", "volume"}
	colIdx := buildColumnIndex(header)
	row := []string{"2024-01-02T00:00:00Z", "1.0860", "1.0875", "1.0850", "1.0870", "12345.6"}

	line, err := buildLineProtocol(row, colIdx, "eurusd_m1", "eurusd")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(line, "open=1.0860") {
		t.Errorf("expected open field, got: %q", line)
	}
	if !strings.Contains(line, "volume=12345.6") {
		t.Errorf("expected volume field, got: %q", line)
	}
}

func TestBuildLineProtocol_MissingTimestamp(t *testing.T) {
	header := []string{"bid", "ask"}
	colIdx := buildColumnIndex(header)
	row := []string{"1.0865", "1.0867"}

	_, err := buildLineProtocol(row, colIdx, "eurusd", "")
	if err == nil {
		t.Fatal("expected error for missing timestamp")
	}
}

func TestBuildLineProtocol_NoSymbolTag(t *testing.T) {
	header := []string{"timestamp", "bid", "ask"}
	colIdx := buildColumnIndex(header)
	row := []string{"2024-01-02T00:00:00Z", "1.0865", "1.0867"}

	line, err := buildLineProtocol(row, colIdx, "eurusd", "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if strings.Contains(line, "symbol=") {
		t.Errorf("unexpected symbol tag in line: %q", line)
	}
}

func TestBuildLineProtocol_UnixMsTimestamp(t *testing.T) {
	header := []string{"timestamp", "bid", "ask"}
	colIdx := buildColumnIndex(header)
	tsStr := fmt.Sprintf("%d", time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC).UnixMilli())
	row := []string{tsStr, "1.0865", "1.0867"}

	line, err := buildLineProtocol(row, colIdx, "eurusd", "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(line, tsStr) {
		t.Errorf("expected unix ms timestamp in line, got: %q", line)
	}
}

func TestBuildLineProtocol_InvalidTimestamp(t *testing.T) {
	header := []string{"timestamp", "bid", "ask"}
	colIdx := buildColumnIndex(header)
	row := []string{"not-a-time", "1.0865", "1.0867"}

	_, err := buildLineProtocol(row, colIdx, "eurusd", "")
	if err == nil {
		t.Fatal("expected error for invalid timestamp")
	}
}

// ---------------------------------------------------------------------------
// runDBLoad validation tests
// ---------------------------------------------------------------------------

func TestRunDBLoad_MissingInput(t *testing.T) {
	var stdout, stderr bytes.Buffer
	err := runDBLoad([]string{"--db", "clickhouse", "--url", "http://localhost:8123", "--table", "t"}, &stdout, &stderr)
	if err == nil {
		t.Fatal("expected error for missing --input")
	}
	if !strings.Contains(err.Error(), "--input") {
		t.Errorf("expected --input in error, got: %v", err)
	}
}

func TestRunDBLoad_MissingDB(t *testing.T) {
	var stdout, stderr bytes.Buffer
	err := runDBLoad([]string{"--input", "f.csv", "--url", "http://localhost:8123", "--table", "t"}, &stdout, &stderr)
	if err == nil {
		t.Fatal("expected error for missing --db")
	}
}

func TestRunDBLoad_MissingURL(t *testing.T) {
	var stdout, stderr bytes.Buffer
	err := runDBLoad([]string{"--input", "f.csv", "--db", "clickhouse", "--table", "t"}, &stdout, &stderr)
	if err == nil {
		t.Fatal("expected error for missing --url")
	}
}

func TestRunDBLoad_MissingTable(t *testing.T) {
	var stdout, stderr bytes.Buffer
	err := runDBLoad([]string{"--input", "f.csv", "--db", "clickhouse", "--url", "http://localhost:8123"}, &stdout, &stderr)
	if err == nil {
		t.Fatal("expected error for missing --table")
	}
}

func TestRunDBLoad_UnknownDB(t *testing.T) {
	var stdout, stderr bytes.Buffer
	err := runDBLoad([]string{"--input", "f.csv", "--db", "mongodb", "--url", "http://localhost:27017", "--table", "t"}, &stdout, &stderr)
	if err == nil {
		t.Fatal("expected error for unknown --db")
	}
}

func TestRunDBLoad_InputNotFound(t *testing.T) {
	var stdout, stderr bytes.Buffer
	err := runDBLoad([]string{
		"--input", "/nonexistent/file.csv",
		"--db", "clickhouse",
		"--url", "http://localhost:8123",
		"--table", "t",
	}, &stdout, &stderr)
	if err == nil {
		t.Fatal("expected error for non-existent input file")
	}
}

func TestRunDBLoad_InputIsDirectory(t *testing.T) {
	dir := t.TempDir()
	var stdout, stderr bytes.Buffer
	err := runDBLoad([]string{
		"--input", dir,
		"--db", "clickhouse",
		"--url", "http://localhost:8123",
		"--table", "t",
	}, &stdout, &stderr)
	if err == nil {
		t.Fatal("expected error when input is a directory")
	}
}

func TestRunDBLoad_InfluxDB_MissingToken(t *testing.T) {
	f, _ := os.CreateTemp("", "*.csv")
	_, _ = f.WriteString("timestamp,bid,ask\n")
	f.Close()
	defer os.Remove(f.Name())

	var stdout, stderr bytes.Buffer
	err := runDBLoad([]string{
		"--input", f.Name(),
		"--db", "influxdb",
		"--url", "http://localhost:8086",
		"--table", "eurusd",
		"--org", "my-org",
		"--bucket", "market-data",
	}, &stdout, &stderr)
	if err == nil {
		t.Fatal("expected error for missing InfluxDB token")
	}
}

func TestRunDBLoad_InfluxDB_MissingOrg(t *testing.T) {
	f, _ := os.CreateTemp("", "*.csv")
	_, _ = f.WriteString("timestamp,bid,ask\n")
	f.Close()
	defer os.Remove(f.Name())

	var stdout, stderr bytes.Buffer
	err := runDBLoad([]string{
		"--input", f.Name(),
		"--db", "influxdb",
		"--url", "http://localhost:8086",
		"--table", "eurusd",
		"--token", "MY_TOKEN",
		"--bucket", "market-data",
	}, &stdout, &stderr)
	if err == nil {
		t.Fatal("expected error for missing --org")
	}
}

func TestRunDBLoad_InfluxDB_MissingBucket(t *testing.T) {
	f, _ := os.CreateTemp("", "*.csv")
	_, _ = f.WriteString("timestamp,bid,ask\n")
	f.Close()
	defer os.Remove(f.Name())

	var stdout, stderr bytes.Buffer
	err := runDBLoad([]string{
		"--input", f.Name(),
		"--db", "influxdb",
		"--url", "http://localhost:8086",
		"--table", "eurusd",
		"--token", "MY_TOKEN",
		"--org", "my-org",
	}, &stdout, &stderr)
	if err == nil {
		t.Fatal("expected error for missing --bucket")
	}
}

func TestRunDBLoad_InfluxDB_ParquetUnsupported(t *testing.T) {
	f, _ := os.CreateTemp("", "*.parquet")
	f.Close()
	defer os.Remove(f.Name())

	var stdout, stderr bytes.Buffer
	err := runDBLoad([]string{
		"--input", f.Name(),
		"--db", "influxdb",
		"--url", "http://localhost:8086",
		"--table", "eurusd",
		"--token", "MY_TOKEN",
		"--org", "my-org",
		"--bucket", "market-data",
	}, &stdout, &stderr)
	if err == nil {
		t.Fatal("expected error for parquet with influxdb")
	}
	if !strings.Contains(err.Error(), "CSV") {
		t.Errorf("expected CSV mention in error, got: %v", err)
	}
}

// ---------------------------------------------------------------------------
// ingestClickHouse with mock HTTP server
// ---------------------------------------------------------------------------

func TestIngestClickHouse_MockServer(t *testing.T) {
	var receivedQuery string
	var receivedBody []byte

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedQuery = r.URL.Query().Get("query")
		receivedBody, _ = io.ReadAll(r.Body)
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	f, _ := os.CreateTemp("", "*.csv")
	_, _ = f.WriteString("timestamp,bid,ask\n2024-01-02T00:00:00Z,1.0865,1.0867\n")
	f.Close()
	defer os.Remove(f.Name())

	var stdout, stderr bytes.Buffer
	err := ingestClickHouse(context.Background(), &stdout, &stderr, f.Name(), ts.URL, "eurusd", "", "", 30*time.Second)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !strings.Contains(receivedQuery, "eurusd") {
		t.Errorf("expected table name in query, got: %q", receivedQuery)
	}
	if !strings.Contains(receivedQuery, "CSVWithNames") {
		t.Errorf("expected CSVWithNames format in query, got: %q", receivedQuery)
	}
	if !strings.Contains(string(receivedBody), "timestamp,bid,ask") {
		t.Errorf("expected CSV header in body, got: %q", string(receivedBody))
	}
}

func TestIngestClickHouse_ServerError(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte("DB Error: table not found"))
	}))
	defer ts.Close()

	f, _ := os.CreateTemp("", "*.csv")
	_, _ = f.WriteString("timestamp,bid,ask\n")
	f.Close()
	defer os.Remove(f.Name())

	var stdout, stderr bytes.Buffer
	err := ingestClickHouse(context.Background(), &stdout, &stderr, f.Name(), ts.URL, "t", "", "", 30*time.Second)
	if err == nil {
		t.Fatal("expected error for server 500")
	}
	if !strings.Contains(err.Error(), "500") && !strings.Contains(err.Error(), "DB Error") {
		t.Errorf("expected HTTP status in error, got: %v", err)
	}
}

func TestIngestClickHouse_ParquetFile(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		query := r.URL.Query().Get("query")
		if !strings.Contains(query, "Parquet") {
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte("expected Parquet format"))
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	f, _ := os.CreateTemp("", "*.parquet")
	_, _ = f.Write([]byte("PAR1"))
	f.Close()
	defer os.Remove(f.Name())

	var stdout, stderr bytes.Buffer
	err := ingestClickHouse(context.Background(), &stdout, &stderr, f.Name(), ts.URL, "t", "", "", 30*time.Second)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

// ---------------------------------------------------------------------------
// ingestInfluxDB with mock HTTP server
// ---------------------------------------------------------------------------

func TestIngestInfluxDB_MockServer(t *testing.T) {
	var requestCount int
	var lastBody []byte

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestCount++
		body, _ := io.ReadAll(r.Body)
		gr, err := gzip.NewReader(bytes.NewReader(body))
		if err == nil {
			lastBody, _ = io.ReadAll(gr)
			gr.Close()
		} else {
			lastBody = body
		}
		if r.Header.Get("Authorization") == "" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer ts.Close()

	f, _ := os.CreateTemp("", "*.csv")
	_, _ = f.WriteString("timestamp,bid,ask,bid_volume,ask_volume\n")
	_, _ = f.WriteString("2024-01-02T00:00:00Z,1.08650,1.08652,1.5,2.0\n")
	_, _ = f.WriteString("2024-01-02T00:00:01Z,1.08651,1.08653,1.2,1.8\n")
	f.Close()
	defer os.Remove(f.Name())

	var stdout, stderr bytes.Buffer
	err := ingestInfluxDB(context.Background(), &stdout, &stderr, f.Name(), ts.URL, "eurusd", "my-org", "market-data",
		"MY_TOKEN", "eurusd", 100, 30*time.Second)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if requestCount == 0 {
		t.Fatal("expected at least one HTTP request")
	}
	if !strings.Contains(string(lastBody), "eurusd") {
		t.Errorf("expected measurement name in line protocol body, got: %q", string(lastBody))
	}
}

func TestIngestInfluxDB_BatchFlush(t *testing.T) {
	var batchCount int
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		batchCount++
		w.WriteHeader(http.StatusNoContent)
	}))
	defer ts.Close()

	f, _ := os.CreateTemp("", "*.csv")
	_, _ = f.WriteString("timestamp,bid,ask\n")
	for i := 0; i < 25; i++ {
		tsStr := time.Date(2024, 1, 2, 0, i/60, i%60, 0, time.UTC).Format(time.RFC3339)
		_, _ = fmt.Fprintf(f, "%s,1.0865%d,1.0867%d\n", tsStr, i, i)
	}
	f.Close()
	defer os.Remove(f.Name())

	var stdout, stderr bytes.Buffer
	// batchSize=10, 25 rows => 3 batches
	err := ingestInfluxDB(context.Background(), &stdout, &stderr, f.Name(), ts.URL, "eurusd", "org", "bucket",
		"token", "", 10, 30*time.Second)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if batchCount < 2 {
		t.Errorf("expected at least 2 batches with batchSize=10 and 25 rows, got %d", batchCount)
	}
}

func TestIngestInfluxDB_GZIPInput(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	defer ts.Close()

	f, _ := os.CreateTemp("", "*.csv.gz")
	gz := gzip.NewWriter(f)
	_, _ = gz.Write([]byte("timestamp,bid,ask\n2024-01-02T00:00:00Z,1.0865,1.0867\n"))
	gz.Close()
	f.Close()
	defer os.Remove(f.Name())

	var stdout, stderr bytes.Buffer
	err := ingestInfluxDB(context.Background(), &stdout, &stderr, f.Name(), ts.URL, "eurusd", "org", "bucket",
		"token", "", 100, 30*time.Second)
	if err != nil {
		t.Fatalf("unexpected error with gzip input: %v", err)
	}
}

func TestIngestInfluxDB_ServerError(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write([]byte("unauthorized"))
	}))
	defer ts.Close()

	f, _ := os.CreateTemp("", "*.csv")
	_, _ = f.WriteString("timestamp,bid,ask\n2024-01-02T00:00:00Z,1.0865,1.0867\n")
	f.Close()
	defer os.Remove(f.Name())

	var stdout, stderr bytes.Buffer
	err := ingestInfluxDB(context.Background(), &stdout, &stderr, f.Name(), ts.URL, "t", "org", "bucket",
		"token", "", 100, 30*time.Second)
	if err == nil {
		t.Fatal("expected error for server 403")
	}
}

// ---------------------------------------------------------------------------
// Full CLI e2e db-load path tests
// ---------------------------------------------------------------------------

func TestRunDBLoad_ClickHouseE2E(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	dir := t.TempDir()
	csvPath := filepath.Join(dir, "data.csv")
	_ = os.WriteFile(csvPath, []byte("timestamp,bid,ask\n2024-01-02T00:00:00Z,1.0865,1.0867\n"), 0o644)

	var stdout, stderr bytes.Buffer
	err := runDBLoad([]string{
		"--input", csvPath,
		"--db", "clickhouse",
		"--url", ts.URL,
		"--table", "eurusd_m1",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(stdout.String(), "eurusd_m1") {
		t.Errorf("expected table name in output, got: %q", stdout.String())
	}
}

func TestRunDBLoad_InfluxDBE2E(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	defer ts.Close()

	dir := t.TempDir()
	csvPath := filepath.Join(dir, "data.csv")
	_ = os.WriteFile(csvPath, []byte("timestamp,bid,ask\n2024-01-02T00:00:00Z,1.0865,1.0867\n"), 0o644)

	var stdout, stderr bytes.Buffer
	err := runDBLoad([]string{
		"--input", csvPath,
		"--db", "influxdb",
		"--url", ts.URL,
		"--table", "eurusd",
		"--token", "MY_TOKEN",
		"--org", "my-org",
		"--bucket", "market-data",
		"--symbol", "eurusd",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	out := stdout.String()
	if !strings.Contains(out, "market-data") || !strings.Contains(out, "eurusd") {
		t.Errorf("expected bucket and measurement in output, got: %q", out)
	}
}
