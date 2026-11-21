package cli

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func writeTickCSV(t *testing.T, rows int) string {
	t.Helper()
	var b strings.Builder
	b.WriteString("timestamp,bid,ask\n")
	base := time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC)
	for i := 0; i < rows; i++ {
		fmt.Fprintf(&b, "%s,1.%05d,1.%05d\n", base.Add(time.Duration(i)*time.Second).Format(time.RFC3339), i%100000, (i+2)%100000)
	}
	path := filepath.Join(t.TempDir(), "ticks.csv")
	if err := os.WriteFile(path, []byte(b.String()), 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

// ---------------------------------------------------------------------------
// ClickHouse
// ---------------------------------------------------------------------------

func TestIngestClickHouse_CredentialsGoInHeadersNotURL(t *testing.T) {
	var (
		gotURL, gotUser, gotKey, gotQuery string
	)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotURL = r.URL.String()
		gotQuery = r.URL.Query().Get("query")
		gotUser = r.Header.Get("X-ClickHouse-User")
		gotKey = r.Header.Get("X-ClickHouse-Key")
		_, _ = io.Copy(io.Discard, r.Body)
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	path := writeTickCSV(t, 2)
	var stdout, stderr bytes.Buffer
	if err := ingestClickHouse(context.Background(), &stdout, &stderr, path, ts.URL, "market.eurusd", "admin", "s3cr3t-pass", 5*time.Second); err != nil {
		t.Fatalf("ingestClickHouse: %v", err)
	}
	if strings.Contains(gotURL, "s3cr3t") || strings.Contains(gotURL, "password") {
		t.Fatalf("password leaked into request URL: %s", gotURL)
	}
	if gotUser != "admin" || gotKey != "s3cr3t-pass" {
		t.Fatalf("expected credentials in X-ClickHouse headers, got user=%q key=%q", gotUser, gotKey)
	}
	if !strings.Contains(gotQuery, "INSERT INTO `market`.`eurusd` FORMAT CSVWithNames") {
		t.Fatalf("expected each identifier part quoted, got query %q", gotQuery)
	}
}

func TestIngestClickHouse_URLUserinfoMovedToHeaders(t *testing.T) {
	var gotURL, gotUser, gotKey string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotURL = r.URL.String()
		gotUser = r.Header.Get("X-ClickHouse-User")
		gotKey = r.Header.Get("X-ClickHouse-Key")
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	withCreds := strings.Replace(ts.URL, "http://", "http://bob:urlpass@", 1)
	path := writeTickCSV(t, 1)
	if err := ingestClickHouse(context.Background(), io.Discard, io.Discard, path, withCreds, "t", "default", "", 5*time.Second); err != nil {
		t.Fatalf("ingestClickHouse: %v", err)
	}
	if strings.Contains(gotURL, "urlpass") {
		t.Fatalf("password leaked into request URL: %s", gotURL)
	}
	if gotUser != "bob" || gotKey != "urlpass" {
		t.Fatalf("expected URL userinfo to become headers, got user=%q key=%q", gotUser, gotKey)
	}
}

func TestIngestClickHouse_TransportErrorDoesNotLeakCredentials(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	ln.Close() // nothing listens there any more

	path := writeTickCSV(t, 1)
	err = ingestClickHouse(context.Background(), io.Discard, io.Discard, path, "http://bob:urlsecret@"+addr, "t", "admin", "flagsecret", 2*time.Second)
	if err == nil {
		t.Fatal("expected a transport error")
	}
	for _, secret := range []string{"flagsecret", "urlsecret"} {
		if strings.Contains(err.Error(), secret) {
			t.Fatalf("error leaks credential %q: %v", secret, err)
		}
	}
}

func TestBuildClickHouseURLHasNoCredentials(t *testing.T) {
	got := buildClickHouseURL("http://u:p@localhost:8123", "INSERT INTO t FORMAT CSV")
	if strings.Contains(got, "u:p@") || strings.Contains(got, "password") {
		t.Fatalf("credentials must not be part of the URL: %s", got)
	}
}

func TestQuoteClickHouseTable(t *testing.T) {
	cases := map[string]string{
		"eurusd":    "`eurusd`",
		"db.eurusd": "`db`.`eurusd`",
	}
	for in, want := range cases {
		got, err := quoteClickHouseTable(in)
		if err != nil || got != want {
			t.Errorf("quoteClickHouseTable(%q) = %q, %v; want %q", in, got, err, want)
		}
	}
	for _, bad := range []string{"db.", ".t", "a.b.c"} {
		if _, err := quoteClickHouseTable(bad); err == nil {
			t.Errorf("quoteClickHouseTable(%q) expected error", bad)
		}
	}
}

func TestIngestClickHouse_LargeUploadIsNotCutByOverallTimeout(t *testing.T) {
	// The server consumes the upload slowly so the whole request takes much
	// longer than the configured timeout. Only connect/response-header
	// timeouts may apply, so the upload must still succeed.
	timeout := 300 * time.Millisecond
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		buf := make([]byte, 256*1024)
		for {
			_, err := r.Body.Read(buf)
			if err != nil {
				break
			}
			time.Sleep(15 * time.Millisecond)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	path := filepath.Join(t.TempDir(), "big.csv")
	data := bytes.Repeat([]byte("2024-01-02T00:00:00Z,1.00000,1.00002\n"), 24*1024*1024/38)
	if err := os.WriteFile(path, append([]byte("timestamp,bid,ask\n"), data...), 0o644); err != nil {
		t.Fatal(err)
	}

	start := time.Now()
	if err := ingestClickHouse(context.Background(), io.Discard, io.Discard, path, ts.URL, "t", "", "", timeout); err != nil {
		t.Fatalf("large upload failed: %v (after %s)", err, time.Since(start))
	}
	if elapsed := time.Since(start); elapsed < 2*timeout {
		t.Logf("upload finished in %s; slow-server simulation may not have exceeded the timeout", elapsed)
	}
}

// ---------------------------------------------------------------------------
// DBLoad defaults
// ---------------------------------------------------------------------------

func TestDBLoadAppliesDefaultBatchSize(t *testing.T) {
	var requests int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&requests, 1)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer ts.Close()

	path := writeTickCSV(t, 25)
	err := DBLoad(context.Background(), io.Discard, io.Discard, DBLoadOptions{
		DBType:    "influxdb",
		DBURL:     ts.URL,
		Table:     "eurusd",
		InputPath: path,
		Token:     "tok",
		Org:       "org",
		Bucket:    "bucket",
		// BatchSize and Timeout intentionally zero
	})
	if err != nil {
		t.Fatalf("DBLoad: %v", err)
	}
	if got := atomic.LoadInt32(&requests); got != 1 {
		t.Fatalf("expected the default batch size to send 25 rows in 1 request, got %d requests", got)
	}
}

func TestNormalizeDBLoadOptions(t *testing.T) {
	path := writeTickCSV(t, 1)
	opt, err := normalizeDBLoadOptions(DBLoadOptions{DBType: " QuestDB ", DBURL: "tcp://x:1", Table: " t ", InputPath: path})
	if err != nil {
		t.Fatal(err)
	}
	if opt.DBType != dbQuestDB || opt.Table != "t" || opt.BatchSize != defaultQuestDBBatchRows || opt.Timeout != defaultDBLoadTimeout {
		t.Fatalf("unexpected normalized options: %+v", opt)
	}
	if _, err := normalizeDBLoadOptions(DBLoadOptions{DBType: "influxdb", DBURL: "http://x", Table: "t", InputPath: path, Org: "o", Bucket: "b"}); err == nil {
		t.Fatal("expected missing InfluxDB token to be rejected by DBLoad too")
	}
}

// ---------------------------------------------------------------------------
// QuestDB TCP
// ---------------------------------------------------------------------------

type questDBTestServer struct {
	addr    string
	accepts int32
	mu      sync.Mutex
	lines   []string
}

// startQuestDBTestServer starts a TCP listener on loopback. When closeAfter
// is >= 0 every connection is closed after reading that many bytes.
func startQuestDBTestServer(t *testing.T, closeAfter int) *questDBTestServer {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := &questDBTestServer{addr: ln.Addr().String()}
	t.Cleanup(func() { ln.Close() })
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			atomic.AddInt32(&srv.accepts, 1)
			go func(c net.Conn) {
				defer c.Close()
				if closeAfter >= 0 {
					_, _ = io.CopyN(io.Discard, c, int64(closeAfter))
					return
				}
				scanner := bufio.NewScanner(c)
				for scanner.Scan() {
					srv.mu.Lock()
					srv.lines = append(srv.lines, scanner.Text())
					srv.mu.Unlock()
				}
			}(conn)
		}
	}()
	return srv
}

// settledAccepts waits briefly so in-flight accepts are counted, then returns
// the number of connections the server has accepted.
func (s *questDBTestServer) settledAccepts() int32 {
	deadline := time.Now().Add(500 * time.Millisecond)
	for atomic.LoadInt32(&s.accepts) == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	time.Sleep(100 * time.Millisecond) // give a would-be reconnect time to land
	return atomic.LoadInt32(&s.accepts)
}

func (s *questDBTestServer) lineCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.lines)
}

func TestDBLoadQuestDBTCPWithZeroBatchSize(t *testing.T) {
	srv := startQuestDBTestServer(t, -1)
	path := writeTickCSV(t, 3)
	err := DBLoad(context.Background(), io.Discard, io.Discard, DBLoadOptions{
		DBType:    "questdb",
		DBURL:     "tcp://" + srv.addr,
		Table:     "eurusd",
		InputPath: path,
	})
	if err != nil {
		t.Fatalf("DBLoad questdb: %v", err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for srv.lineCount() < 3 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if got := srv.lineCount(); got != 3 {
		t.Fatalf("expected 3 ILP lines, got %d", got)
	}
}

func TestQuestDBTCPDoesNotReconnectAfterSendFailure(t *testing.T) {
	srv := startQuestDBTestServer(t, 4096)
	path := writeTickCSV(t, 300000)

	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	reader := csv.NewReader(f)
	header, _ := reader.Read()
	host, portStr, _ := net.SplitHostPort(srv.addr)
	var port int
	fmt.Sscanf(portStr, "%d", &port)

	sent, err := questDBTCPWrite(context.Background(), reader, buildColumnIndex(header), host, port, "eurusd", "", 100, 2*time.Second, io.Discard)
	if err == nil {
		t.Fatal("expected a write failure once the server closed the connection")
	}
	if !strings.Contains(err.Error(), "rows were sent") || !strings.Contains(err.Error(), "not resuming") {
		t.Fatalf("expected the error to state how many rows were sent, got: %v", err)
	}
	if sent <= 0 || sent >= 300000 {
		t.Fatalf("unexpected sent row count %d", sent)
	}
	if got := srv.settledAccepts(); got != 1 {
		t.Fatalf("expected exactly one connection (no reconnect after data was sent), got %d", got)
	}
}

func TestQuestDBTCPDoesNotRetryCSVErrors(t *testing.T) {
	srv := startQuestDBTestServer(t, -1)
	csvData := "timestamp,bid,ask\n2024-01-02T00:00:00Z,1.1,1.2\n2024-01-02T00:00:01Z,\"broken,1.2\n"
	reader := csv.NewReader(strings.NewReader(csvData))
	reader.FieldsPerRecord = -1
	header, _ := reader.Read()
	host, portStr, _ := net.SplitHostPort(srv.addr)
	var port int
	fmt.Sscanf(portStr, "%d", &port)

	_, err := questDBTCPWrite(context.Background(), reader, buildColumnIndex(header), host, port, "eurusd", "", 100, 2*time.Second, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "CSV read error") {
		t.Fatalf("expected CSV read error, got %v", err)
	}
	if got := srv.settledAccepts(); got != 1 {
		t.Fatalf("CSV errors must not trigger reconnects, got %d connections", got)
	}
}

func TestQuestDBTCPRetriesOnlyInitialDial(t *testing.T) {
	previousRetries, previousBackoff := questDBDialRetries, questDBDialBackoff
	questDBDialRetries, questDBDialBackoff = 1, 10*time.Millisecond
	defer func() { questDBDialRetries, questDBDialBackoff = previousRetries, previousBackoff }()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().(*net.TCPAddr)
	ln.Close()

	reader := csv.NewReader(strings.NewReader("timestamp,bid\n2024-01-02T00:00:00Z,1\n"))
	header, _ := reader.Read()
	_, err = questDBTCPWrite(context.Background(), reader, buildColumnIndex(header), "127.0.0.1", addr.Port, "t", "", 10, time.Second, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "failed to connect") {
		t.Fatalf("expected dial failure, got %v", err)
	}
}

// ---------------------------------------------------------------------------
// Partial batch failures
// ---------------------------------------------------------------------------

func failingAfterFirstBatchServer(t *testing.T) *httptest.Server {
	var calls int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		if atomic.AddInt32(&calls, 1) == 1 {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte("boom"))
	}))
	t.Cleanup(ts.Close)
	return ts
}

func TestQuestDBHTTPReportsCommittedRowsOnFailure(t *testing.T) {
	ts := failingAfterFirstBatchServer(t)
	reader := csv.NewReader(strings.NewReader("timestamp,bid,ask\n" +
		"2024-01-02T00:00:00Z,1.1,1.2\n2024-01-02T00:00:01Z,1.1,1.2\n" +
		"2024-01-02T00:00:02Z,1.1,1.2\n2024-01-02T00:00:03Z,1.1,1.2\n"))
	header, _ := reader.Read()
	committed, err := questDBHTTPWrite(context.Background(), reader, buildColumnIndex(header), ts.URL, "t", "", 2, time.Second, io.Discard)
	if err == nil {
		t.Fatal("expected failure on the second batch")
	}
	if committed != 2 {
		t.Fatalf("expected 2 committed rows to be reported, got %d", committed)
	}

	path := writeTickCSV(t, 4)
	ts2 := failingAfterFirstBatchServer(t)
	err = ingestQuestDB(context.Background(), io.Discard, io.Discard, path, ts2.URL, "t", 0, "", 2, time.Second)
	var partial *partialLoadError
	if !errors.As(err, &partial) || partial.CommittedRows != 2 || !strings.Contains(err.Error(), "2 rows were already committed") {
		t.Fatalf("expected partial load error with 2 committed rows, got %v", err)
	}
}

func TestInfluxDBReportsCommittedRowsOnFailure(t *testing.T) {
	ts := failingAfterFirstBatchServer(t)
	path := writeTickCSV(t, 5)
	err := ingestInfluxDB(context.Background(), io.Discard, io.Discard, path, ts.URL, "t", "org", "bucket", "tok", "", 2, time.Second)
	var partial *partialLoadError
	if !errors.As(err, &partial) || partial.CommittedRows != 2 {
		t.Fatalf("expected partial load error with 2 committed rows, got %v", err)
	}
	if !strings.Contains(err.Error(), "2 rows were already committed") {
		t.Fatalf("error should state the committed row count: %v", err)
	}
}

// ---------------------------------------------------------------------------
// Line protocol NaN / Inf
// ---------------------------------------------------------------------------

func TestLineProtocolEncodersRejectNonFiniteValues(t *testing.T) {
	colIdx := buildColumnIndex([]string{"timestamp", "bid", "ask"})
	for _, value := range []string{"NaN", "Inf", "-Inf", "+Inf", "infinity"} {
		row := []string{"2024-01-02T00:00:00Z", value, "1.2"}
		if _, err := buildLineProtocol(row, colIdx, "t", ""); !isNonFiniteFieldError(err) {
			t.Errorf("buildLineProtocol(%q): expected non-finite error, got %v", value, err)
		}
		if _, err := buildQuestDBLineProtocol(row, colIdx, "t", ""); !isNonFiniteFieldError(err) {
			t.Errorf("buildQuestDBLineProtocol(%q): expected non-finite error, got %v", value, err)
		}
	}
}

func TestInfluxDBNonFiniteValueFailsWithRowNumber(t *testing.T) {
	var bodies []string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gz, err := gzip.NewReader(r.Body)
		if err == nil {
			data, _ := io.ReadAll(gz)
			bodies = append(bodies, string(data))
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer ts.Close()

	path := filepath.Join(t.TempDir(), "nan.csv")
	if err := os.WriteFile(path, []byte("timestamp,bid,ask\n2024-01-02T00:00:00Z,1.1,1.2\n2024-01-02T00:00:01Z,NaN,1.2\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	err := ingestInfluxDB(context.Background(), io.Discard, io.Discard, path, ts.URL, "t", "org", "bucket", "tok", "", 100, time.Second)
	if err == nil || !strings.Contains(err.Error(), "row 2") {
		t.Fatalf("expected a row-numbered error, got %v", err)
	}
	for _, body := range bodies {
		if strings.Contains(body, "NaN") {
			t.Fatalf("NaN must never be emitted as line protocol: %q", body)
		}
	}
}

// ---------------------------------------------------------------------------
// Postgres
// ---------------------------------------------------------------------------

func TestInferChunkIntervalUsesFileNameOnly(t *testing.T) {
	path := filepath.Join("data", "tick-archive", "eurusd_h1.csv")
	if got := inferChunkIntervalFromPath(path); got != "7 days" {
		t.Fatalf("expected directory names to be ignored, got %q", got)
	}
}

func TestWithPostgresConnectTimeout(t *testing.T) {
	got := withPostgresConnectTimeout("postgres://u:p@localhost:5432/db?sslmode=disable", 15*time.Second)
	if !strings.Contains(got, "connect_timeout=15") || !strings.Contains(got, "sslmode=disable") {
		t.Fatalf("unexpected URL DSN: %s", got)
	}
	if got := withPostgresConnectTimeout("host=localhost dbname=db", 2*time.Second); got != "host=localhost dbname=db connect_timeout=2" {
		t.Fatalf("unexpected key/value DSN: %s", got)
	}
	keep := "postgres://localhost/db?connect_timeout=3"
	if got := withPostgresConnectTimeout(keep, time.Second); got != keep {
		t.Fatalf("explicit connect_timeout must be kept, got %s", got)
	}
}

func TestIngestPostgresPingIsBounded(t *testing.T) {
	// A server that accepts TCP connections but never answers the startup
	// handshake used to hang db-load forever.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	var held []net.Conn
	var mu sync.Mutex
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			mu.Lock()
			held = append(held, c)
			mu.Unlock()
		}
	}()
	defer func() {
		mu.Lock()
		for _, c := range held {
			c.Close()
		}
		mu.Unlock()
	}()

	previous := postgresSetupTimeout
	postgresSetupTimeout = time.Second
	defer func() { postgresSetupTimeout = previous }()

	path := writeTickCSV(t, 1)
	dsn := "postgres://u:p@" + ln.Addr().String() + "/db?sslmode=disable"
	done := make(chan error, 1)
	go func() {
		done <- ingestPostgres(context.Background(), io.Discard, io.Discard, path, dsn, "t", false, "")
	}()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("expected a connection error")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("ingestPostgres did not give up on an unresponsive server")
	}
}
