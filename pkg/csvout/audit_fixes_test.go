package csvout

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"encoding/csv"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/Nosvemos/dukascopy-go/pkg/dukascopy"
	"github.com/apache/arrow/go/v18/arrow/ipc"
	parquet "github.com/parquet-go/parquet-go"
)

func athens(t *testing.T) *time.Location {
	t.Helper()
	loc, err := time.LoadLocation("Europe/Athens")
	if err != nil {
		t.Fatalf("load Europe/Athens: %v", err)
	}
	return loc
}

func testBars(start time.Time, n int, step time.Duration) []dukascopy.Bar {
	bars := make([]dukascopy.Bar, n)
	for i := range bars {
		price := 1.1 + float64(i)*0.0001
		bars[i] = dukascopy.Bar{Time: start.Add(time.Duration(i) * step), Open: price, High: price + 0.0002, Low: price - 0.0002, Close: price + 0.0001, Volume: float64(10 + i)}
	}
	return bars
}

var testInstrument = dukascopy.Instrument{Code: "EURUSD", Name: "EUR/USD", PriceScale: 5}

// Item 1
func TestConfigParseTimestampRoundTripNonUTCZonelessLayout(t *testing.T) {
	cfg := &Config{Location: athens(t), TimestampFormat: "2006.01.02 15:04", CSVDelimiter: ','}
	for _, instant := range []time.Time{
		time.Date(2024, 1, 2, 0, 1, 0, 0, time.UTC),
		time.Date(2024, 7, 15, 21, 30, 0, 0, time.UTC), // summer time (+03:00)
	} {
		formatted := cfg.formatTime(instant)
		parsed, err := cfg.ParseTimestamp(formatted)
		if err != nil {
			t.Fatalf("ParseTimestamp(%q): %v", formatted, err)
		}
		if !parsed.Equal(instant) {
			t.Fatalf("round trip of %v via %q gave %v", instant, formatted, parsed)
		}
	}
}

func TestConfigParseTimestampFallbackLayoutsUseLocation(t *testing.T) {
	cfg := &Config{Location: athens(t), TimestampFormat: time.RFC3339Nano}
	got, err := cfg.ParseTimestamp("2024-01-02 02:00:00")
	if err != nil {
		t.Fatal(err)
	}
	if want := time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC); !got.Equal(want) {
		t.Fatalf("got %v want %v", got, want)
	}
	// Zone-qualified values keep their own offset.
	got, err = cfg.ParseTimestamp("2024-01-02T00:00:00Z")
	if err != nil || !got.Equal(time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC)) {
		t.Fatalf("RFC3339 value: got %v, %v", got, err)
	}
}

func TestConfigParseTimestampUnixMilliNeedsThirteenDigits(t *testing.T) {
	cfg := &Config{}
	if _, err := cfg.ParseTimestamp("123456789012"); err == nil {
		t.Fatal("12-digit integer must not be read as Unix milliseconds")
	}
	if _, err := cfg.ParseTimestamp("42"); err == nil {
		t.Fatal("short integer must not be read as Unix milliseconds")
	}
	got, err := cfg.ParseTimestamp("1704153660000")
	if err != nil || !got.Equal(time.Date(2024, 1, 2, 0, 1, 0, 0, time.UTC)) {
		t.Fatalf("13-digit millis: got %v, %v", got, err)
	}
	got, err = ParseFlexibleTimestamp("1704153660000")
	if err != nil || !got.Equal(time.Date(2024, 1, 2, 0, 1, 0, 0, time.UTC)) {
		t.Fatalf("ParseFlexibleTimestamp: got %v, %v", got, err)
	}
}

func TestInspectExistingCSVUsesConfigLocation(t *testing.T) {
	cfg := &Config{Location: athens(t), TimestampFormat: "2006.01.02 15:04", CSVDelimiter: ','}
	path := filepath.Join(t.TempDir(), "mt4.csv")
	bars := testBars(time.Date(2024, 3, 4, 10, 0, 0, 0, time.UTC), 3, time.Minute)
	if err := cfg.WriteBars(path, testInstrument, []string{"timestamp", "open", "high", "low", "close", "volume"}, bars, nil, nil); err != nil {
		t.Fatal(err)
	}
	state, err := cfg.InspectExistingCSV(path)
	if err != nil {
		t.Fatal(err)
	}
	if !state.LastTime.Equal(bars[2].Time) {
		t.Fatalf("LastTime = %v, want %v", state.LastTime, bars[2].Time)
	}
}

// Item 2
func TestParquetNonDefaultTimestampFormatCleanAndExtract(t *testing.T) {
	cfg := &Config{Location: athens(t), TimestampFormat: "2006.01.02 15:04", CSVDelimiter: ','}
	dir := t.TempDir()
	path := filepath.Join(dir, "bars.parquet")
	start := time.Date(2024, 3, 4, 10, 0, 0, 0, time.UTC)
	bars := testBars(start, 5, time.Minute)
	cols := []string{"timestamp", "open", "high", "low", "close", "volume"}
	if err := cfg.WriteBars(path, testInstrument, cols, bars, nil, nil); err != nil {
		t.Fatal(err)
	}

	removed, err := cfg.CleanDuplicates(path)
	if err != nil {
		t.Fatalf("CleanDuplicates: %v", err)
	}
	if removed != 0 {
		t.Fatalf("removed = %d, want 0", removed)
	}
	if rows, err := parquetRowCount(path); err != nil || rows != 5 {
		t.Fatalf("rows after clean = %d, %v; want 5", rows, err)
	}

	out := filepath.Join(dir, "range.csv")
	if err := cfg.ExtractCSVRange(path, out, start.Add(time.Minute), start.Add(3*time.Minute)); err != nil {
		t.Fatalf("ExtractCSVRange: %v", err)
	}
	data, _ := os.ReadFile(out)
	if lines := strings.Count(strings.TrimSpace(string(data)), "\n"); lines != 2 {
		t.Fatalf("extracted %d data rows, want 2:\n%s", lines, data)
	}

	outParquet := filepath.Join(dir, "range.parquet")
	if err := cfg.ExtractCSVRange(path, outParquet, start.Add(time.Minute), start.Add(3*time.Minute)); err != nil {
		t.Fatalf("ExtractCSVRange parquet: %v", err)
	}
	if rows, err := parquetRowCount(outParquet); err != nil || rows != 2 {
		t.Fatalf("parquet range rows = %d, %v; want 2", rows, err)
	}

	csvToParquet := filepath.Join(dir, "fromcsv.parquet")
	if err := cfg.ExtractCSVRange(out, csvToParquet, start, start.Add(time.Hour)); err != nil {
		t.Fatalf("CSV -> parquet extract: %v", err)
	}
	if rows, err := parquetRowCount(csvToParquet); err != nil || rows != 2 {
		t.Fatalf("csv->parquet rows = %d, %v; want 2", rows, err)
	}
}

func TestCleanParquetDuplicatesRejectsUnparseableTimestamp(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bad.parquet")
	cols := []string{"timestamp", "close"}
	if err := writeParquetRecords(path, cols, []map[string]any{{"timestamp": "garbage", "close": 1.0}}); err != nil {
		t.Fatal(err)
	}
	if _, err := CleanDuplicates(path); err == nil {
		t.Fatal("expected error for unparseable parquet timestamp")
	}
	if rows, err := parquetRowCount(path); err != nil || rows != 1 {
		t.Fatalf("file must be left untouched, rows = %d, %v", rows, err)
	}
}

// Item 3
func TestReplaceFileKeepsTargetWhenRenameFails(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, "target.csv")
	if err := os.WriteFile(target, []byte("original"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := replaceFile(filepath.Join(dir, "missing.csv"), target); err == nil {
		t.Fatal("expected rename error")
	}
	data, err := os.ReadFile(target)
	if err != nil || string(data) != "original" {
		t.Fatalf("target lost after failed replace: %q, %v", data, err)
	}
}

// Item 4
func TestCreateAtomicTempPathKeepsExtensionChain(t *testing.T) {
	dir := t.TempDir()
	cases := map[string]string{
		"a.csv": ".csv", "a.csv.gz": ".csv.gz", "a.parquet": ".parquet", "a.arrow": ".arrow",
		"a.ipc": ".ipc", "a.feather": ".feather", "a.json": ".json", "a.jsonl": ".jsonl",
		"a.jsonl.gz": ".jsonl.gz", "EURUSD.2024.CSV": ".CSV", "noext": "",
	}
	for name, chain := range cases {
		tempPath, err := createAtomicTempPath(filepath.Join(dir, name))
		if err != nil {
			t.Fatal(err)
		}
		base := filepath.Base(tempPath)
		if !strings.HasSuffix(base, chain) || !strings.Contains(base, ".tmp-") {
			t.Errorf("%s: temp name %q does not end with %q", name, base, chain)
		}
		if runtime.GOOS != "windows" {
			if info, err := os.Stat(tempPath); err == nil && info.Mode().Perm() != 0o644 {
				t.Errorf("%s: temp mode %v, want 0644", name, info.Mode().Perm())
			}
		}
		os.Remove(tempPath)
	}
}

func TestAtomicWritersProduceRequestedFormat(t *testing.T) {
	dir := t.TempDir()
	start := time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC)
	bars := testBars(start, 3, time.Minute)
	ticks := []dukascopy.Tick{{Time: start, Bid: 1.1, Ask: 1.2, BidVolume: 1, AskVolume: 2}}
	barCols := []string{"timestamp", "open", "high", "low", "close", "volume"}
	tickCols := []string{"timestamp", "bid", "ask"}

	checkFormat := func(t *testing.T, path string) {
		t.Helper()
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		lower := strings.ToLower(path)
		switch {
		case isArrowPath(lower):
			f, _ := os.Open(path)
			defer f.Close()
			r, err := ipc.NewFileReader(f)
			if err != nil {
				t.Fatalf("%s is not an Arrow IPC file: %v", path, err)
			}
			r.Close()
		case strings.HasSuffix(lower, ".parquet"):
			if _, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data))); err != nil {
				t.Fatalf("%s is not parquet: %v", path, err)
			}
		case strings.HasSuffix(lower, ".gz"):
			zr, err := gzip.NewReader(bytes.NewReader(data))
			if err != nil {
				t.Fatalf("%s is not gzip: %v", path, err)
			}
			inner, _ := io.ReadAll(zr)
			if strings.Contains(lower, ".json") {
				assertJSONLines(t, path, inner)
			} else if !strings.HasPrefix(string(inner), "timestamp,") {
				t.Fatalf("%s: unexpected gzip CSV content %q", path, inner)
			}
		case strings.HasSuffix(lower, ".json"), strings.HasSuffix(lower, ".jsonl"):
			assertJSONLines(t, path, data)
		default:
			if !strings.HasPrefix(string(data), "timestamp,") {
				t.Fatalf("%s: unexpected CSV content %q", path, data)
			}
		}
	}

	for _, ext := range []string{".csv", ".csv.gz", ".parquet", ".arrow", ".ipc", ".feather", ".json", ".jsonl", ".jsonl.gz"} {
		t.Run(ext, func(t *testing.T) {
			barPath := filepath.Join(dir, "bars"+ext)
			if err := WriteBarsAtomic(barPath, testInstrument, barCols, bars, nil, nil); err != nil {
				t.Fatal(err)
			}
			checkFormat(t, barPath)
			tickPath := filepath.Join(dir, "ticks"+ext)
			if err := WriteTicksAtomic(tickPath, testInstrument, tickCols, ticks); err != nil {
				t.Fatal(err)
			}
			checkFormat(t, tickPath)
			featPath := filepath.Join(dir, "feat"+ext)
			if err := WriteBarsWithFeaturesAtomic(featPath, testInstrument, barCols, bars, []string{"f"}, [][]float64{{1}, {2}, {3}}); err != nil {
				t.Fatal(err)
			}
			checkFormat(t, featPath)
		})
	}
}

func assertJSONLines(t *testing.T, path string, data []byte) {
	t.Helper()
	scanner := bufio.NewScanner(bytes.NewReader(data))
	lines := 0
	for scanner.Scan() {
		var obj map[string]any
		if err := json.Unmarshal(scanner.Bytes(), &obj); err != nil {
			t.Fatalf("%s line %d is not JSON: %v (%q)", path, lines+1, err, scanner.Text())
		}
		lines++
	}
	if lines == 0 {
		t.Fatalf("%s has no JSON lines", path)
	}
}

// Items 5 and 6
type flushFailWriter struct {
	w       io.Writer
	flushed bool
}

func (f *flushFailWriter) Write(record []string) error {
	_, err := io.WriteString(f.w, strings.Join(record, ",")+"\n")
	return err
}
func (f *flushFailWriter) Flush() { f.flushed = true }
func (f *flushFailWriter) Error() error {
	if f.flushed {
		return errors.New("disk full")
	}
	return nil
}

func TestMergeResumeCSVReportsFinalFlushErrorAndRestoresFile(t *testing.T) {
	for _, ext := range []string{".csv", ".csv.gz"} {
		t.Run(ext, func(t *testing.T) {
			dir := t.TempDir()
			existing := filepath.Join(dir, "existing"+ext)
			temp := filepath.Join(dir, "resume"+ext)
			cols := []string{"timestamp", "open", "high", "low", "close", "volume"}
			start := time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC)
			if err := WriteBars(existing, testInstrument, cols, testBars(start, 2, time.Minute), nil, nil); err != nil {
				t.Fatal(err)
			}
			if err := WriteBars(temp, testInstrument, cols, testBars(start.Add(2*time.Minute), 2, time.Minute), nil, nil); err != nil {
				t.Fatal(err)
			}
			before, _ := os.ReadFile(existing)

			cfg := DefaultConfig()
			cfg.WriterFactory = func(w io.Writer) csvRecordWriter { return &flushFailWriter{w: w} }
			appended, err := cfg.MergeResumeCSV(existing, temp, nil)
			if err == nil {
				t.Fatal("expected the final flush error to be returned")
			}
			if appended != 0 {
				t.Fatalf("appended = %d on error, want 0", appended)
			}
			after, _ := os.ReadFile(existing)
			if !bytes.Equal(before, after) {
				t.Fatalf("existing file not restored: before %d bytes, after %d bytes", len(before), len(after))
			}
		})
	}
}

func TestMergeResumeCSVAppendsRows(t *testing.T) {
	dir := t.TempDir()
	existing := filepath.Join(dir, "existing.csv")
	temp := filepath.Join(dir, "resume.csv")
	cols := []string{"timestamp", "close"}
	start := time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC)
	all := testBars(start, 4, time.Minute)
	if err := WriteBars(existing, testInstrument, cols, all[:2], nil, nil); err != nil {
		t.Fatal(err)
	}
	if err := WriteBars(temp, testInstrument, cols, all[1:], nil, nil); err != nil {
		t.Fatal(err)
	}
	state, err := InspectExistingCSV(existing)
	if err != nil {
		t.Fatal(err)
	}
	appended, err := MergeResumeCSV(existing, temp, state.LastRecord)
	if err != nil || appended != 2 {
		t.Fatalf("appended = %d, %v; want 2", appended, err)
	}
	audit, err := AuditCSV(existing)
	if err != nil || audit.Rows != 4 {
		t.Fatalf("rows = %d, %v; want 4", audit.Rows, err)
	}
}

// Item 7
type limitedWriter struct {
	limit int
	n     int
}

func (l *limitedWriter) Write(p []byte) (int, error) {
	if l.n+len(p) > l.limit {
		return 0, errors.New("short write")
	}
	l.n += len(p)
	return len(p), nil
}

func TestWriteArrowRecordsReportsFooterWriteError(t *testing.T) {
	cols := []string{"timestamp", "close"}
	records := []map[string]any{{"timestamp": "2024-01-02T00:00:00Z", "close": 1.1}}
	var full bytes.Buffer
	if err := writeArrowRecordsTo(&full, cols, records); err != nil {
		t.Fatal(err)
	}
	// Allow everything except the very last write (part of the footer).
	if err := writeArrowRecordsTo(&limitedWriter{limit: full.Len() - 1}, cols, records); err == nil {
		t.Fatal("expected footer write error to be returned")
	}
}

// Item 8
func TestJSONLUsesConfigColumnOrderAndGzip(t *testing.T) {
	cfg := &Config{Location: athens(t), TimestampFormat: "2006.01.02 15:04", CSVDelimiter: ','}
	dir := t.TempDir()
	start := time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC)
	cols := []string{"volume", "timestamp", "close", "tick_count"}
	bars := testBars(start, 1, time.Minute)
	bars[0].TickCount = 7

	path := filepath.Join(dir, "bars.jsonl")
	if err := cfg.WriteBars(path, testInstrument, cols, bars, nil, nil); err != nil {
		t.Fatal(err)
	}
	data, _ := os.ReadFile(path)
	want := `{"volume":10,"timestamp":"2024.01.02 02:00","close":1.1001,"tick_count":7}` + "\n"
	if string(data) != want {
		t.Fatalf("got %q\nwant %q", data, want)
	}

	gzPath := filepath.Join(dir, "bars.jsonl.gz")
	if err := cfg.WriteBars(gzPath, testInstrument, cols, bars, nil, nil); err != nil {
		t.Fatal(err)
	}
	raw, _ := os.ReadFile(gzPath)
	zr, err := gzip.NewReader(bytes.NewReader(raw))
	if err != nil {
		t.Fatalf("jsonl.gz is not gzip: %v", err)
	}
	inner, _ := io.ReadAll(zr)
	if string(inner) != want {
		t.Fatalf("gz content %q, want %q", inner, want)
	}

	var buf bytes.Buffer
	ticks := []dukascopy.Tick{{Time: start, Bid: 1.1, Ask: 1.2}}
	if err := cfg.WriteTicksJSONLToWriter(&buf, testInstrument, []string{"ask", "timestamp", "bid"}, ticks); err != nil {
		t.Fatal(err)
	}
	if got := buf.String(); got != `{"ask":1.2,"timestamp":"2024.01.02 02:00","bid":1.1}`+"\n" {
		t.Fatalf("tick jsonl = %q", got)
	}
}

func TestJSONLFeaturesNonFiniteAndPadding(t *testing.T) {
	path := filepath.Join(t.TempDir(), "f.jsonl")
	bars := testBars(time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC), 2, time.Minute)
	nan := []float64{0}
	nan[0] = nan[0] / nan[0] // NaN without importing math
	if err := WriteBarsWithFeatures(path, testInstrument, []string{"timestamp"}, bars, []string{"a", "b"}, [][]float64{nan}); err != nil {
		t.Fatal(err)
	}
	data, _ := os.ReadFile(path)
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	if len(lines) != 2 || !strings.HasSuffix(lines[0], `"a":null,"b":null}`) || !strings.HasSuffix(lines[1], `"a":null,"b":null}`) {
		t.Fatalf("unexpected JSONL:\n%s", data)
	}
}

// Item 9
func TestCleanDuplicatesMalformedRowReturnsError(t *testing.T) {
	path := filepath.Join(t.TempDir(), "short.csv")
	if err := os.WriteFile(path, []byte("open,timestamp\n1.1,2024-01-02T00:00:00Z\n1.2\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	cfg := DefaultConfig()
	cfg.ReaderFactory = func(r io.Reader) csvRecordReader {
		reader := csv.NewReader(r)
		reader.FieldsPerRecord = -1
		return reader
	}
	_, err := cfg.CleanDuplicates(path)
	if err == nil || !strings.Contains(err.Error(), "malformed row 2") {
		t.Fatalf("expected malformed row error, got %v", err)
	}
}

// Item 10
type failingIOWriter struct{}

func (failingIOWriter) Write([]byte) (int, error) { return 0, errors.New("pipe closed") }

func TestStreamCSVRowsAfterReportsFinalFlushError(t *testing.T) {
	path := filepath.Join(t.TempDir(), "rows.csv")
	if err := WriteBars(path, testInstrument, []string{"timestamp", "close"}, testBars(time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC), 2, time.Minute), nil, nil); err != nil {
		t.Fatal(err)
	}
	if _, _, err := StreamCSVRowsAfter(path, failingIOWriter{}, time.Time{}, true); err == nil {
		t.Fatal("expected write error from final flush")
	}
}

// Item 11
func TestReadBarsFromCSVUsesConfigDelimiterAndGzip(t *testing.T) {
	cfg := &Config{Location: athens(t), TimestampFormat: "2006.01.02 15:04", CSVDelimiter: ';'}
	path := filepath.Join(t.TempDir(), "bars.csv.gz")
	bars := testBars(time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC), 3, time.Minute)
	cols := []string{"timestamp", "open", "high", "low", "close", "volume", "tick_count"}
	if err := cfg.WriteBars(path, testInstrument, cols, bars, nil, nil); err != nil {
		t.Fatal(err)
	}
	got, header, err := cfg.ReadBarsFromCSV(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 || len(header) != len(cols) {
		t.Fatalf("got %d bars, header %v", len(got), header)
	}
	if !got[1].Time.Equal(bars[1].Time) || got[1].Close != 1.1002 {
		t.Fatalf("bar 1 = %+v, want time %v close 1.1002", got[1], bars[1].Time)
	}
}

func TestReadBarsFromCSVReportsBadCells(t *testing.T) {
	dir := t.TempDir()
	cases := map[string]string{
		"badfloat.csv": "timestamp,close\n2024-01-02T00:00:00Z,1.1\n2024-01-02T00:01:00Z,abc\n",
		"badtime.csv":  "timestamp,close\n2024-01-02T00:00:00Z,1.1\nnot-a-time,1.2\n",
	}
	for name, content := range cases {
		path := filepath.Join(dir, name)
		if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
		_, _, err := ReadBarsFromCSV(path)
		if err == nil || !strings.Contains(err.Error(), "data row 2") {
			t.Fatalf("%s: expected error naming data row 2, got %v", name, err)
		}
	}
}

func TestWriteBarsWithFeaturesPadsMissingValues(t *testing.T) {
	path := filepath.Join(t.TempDir(), "feat.csv")
	bars := testBars(time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC), 3, time.Minute)
	featRows := [][]float64{{1, 2}, {3}} // second row short, third missing
	if err := WriteBarsWithFeatures(path, testInstrument, []string{"timestamp", "close"}, bars, []string{"a", "b"}, featRows); err != nil {
		t.Fatal(err)
	}
	f, _ := os.Open(path)
	defer f.Close()
	records, err := csv.NewReader(f).ReadAll() // default reader rejects ragged rows
	if err != nil {
		t.Fatalf("ragged CSV: %v", err)
	}
	if len(records) != 4 || records[2][3] != "" || records[3][2] != "" {
		t.Fatalf("unexpected records %v", records)
	}
}

// Item 12
func TestNYSEHolidayCalendar(t *testing.T) {
	loc := gapMarketLocation()
	day := func(y int, m time.Month, d int) time.Time { return time.Date(y, m, d, 0, 0, 0, 0, loc) }
	cases := []struct {
		date time.Time
		want marketHolidayKind
	}{
		{day(2021, 12, 31), marketHolidayNone},      // Jan 1 2022 is Saturday: not observed
		{day(2022, 1, 3), marketHolidayNone},        // and not moved to Monday
		{day(2023, 1, 2), marketHolidayFullClose},   // Jan 1 2023 Sunday -> Monday
		{day(2024, 1, 15), marketHolidayFullClose},  // MLK
		{day(2024, 2, 19), marketHolidayFullClose},  // Washington's Birthday
		{day(2024, 3, 29), marketHolidayFullClose},  // Good Friday
		{day(2024, 5, 27), marketHolidayFullClose},  // Memorial Day
		{day(2022, 6, 20), marketHolidayFullClose},  // Juneteenth observed
		{day(2021, 6, 18), marketHolidayNone},       // before Juneteenth existed
		{day(2024, 7, 3), marketHolidayEarlyClose},  // July 3, Wednesday
		{day(2024, 7, 4), marketHolidayFullClose},   // Independence Day
		{day(2026, 7, 3), marketHolidayFullClose},   // July 4 2026 Saturday -> Friday
		{day(2022, 7, 1), marketHolidayNone},        // July 3 2022 Sunday: no early close
		{day(2024, 9, 2), marketHolidayFullClose},   // Labor Day
		{day(2024, 11, 28), marketHolidayFullClose}, // Thanksgiving
		{day(2023, 11, 24), marketHolidayEarlyClose},
		{day(2024, 12, 24), marketHolidayEarlyClose},
		{day(2024, 12, 25), marketHolidayFullClose},
		{day(2021, 12, 24), marketHolidayFullClose}, // Dec 25 2021 Saturday -> Friday
		{day(2022, 12, 26), marketHolidayFullClose}, // Dec 25 2022 Sunday -> Monday
		{day(2024, 5, 8), marketHolidayNone},
	}
	for _, tc := range cases {
		if got := nyseHolidayKind(tc.date); got != tc.want {
			t.Errorf("%s: got %v want %v", tc.date.Format("2006-01-02 Mon"), got, tc.want)
		}
	}
}

func TestEquityHolidayGapsExpected(t *testing.T) {
	loc := gapMarketLocation()
	ny := func(y int, m time.Month, d, h, min int) time.Time { return time.Date(y, m, d, h, min, 0, 0, loc).UTC() }
	// Full day missing on MLK day is expected for equities.
	if !IsExpectedGapForProfile(ny(2024, 1, 12, 15, 59), ny(2024, 1, 16, 9, 30), time.Minute, "", MarketProfileEquity) {
		t.Error("MLK day closure should be expected for equity profile")
	}
	// Missing session on 2021-12-31 is suspicious (normal trading day).
	if IsExpectedGapForProfile(ny(2021, 12, 30, 15, 59), ny(2022, 1, 3, 9, 30), time.Minute, "", MarketProfileEquity) {
		t.Error("2021-12-31 was a normal NYSE session")
	}
	// Day after Thanksgiving closes at 13:00.
	if !IsExpectedGapForProfile(ny(2023, 11, 24, 12, 59), ny(2023, 11, 27, 9, 30), time.Minute, "", MarketProfileEquity) {
		t.Error("early close after 13:00 on 2023-11-24 should be expected")
	}
	if IsExpectedGapForProfile(ny(2023, 11, 24, 11, 59), ny(2023, 11, 27, 9, 30), time.Minute, "", MarketProfileEquity) {
		t.Error("missing 12:00-13:00 on an early-close day is suspicious")
	}
}

func TestOTCHolidayUsesCalendarDates(t *testing.T) {
	loc := gapMarketLocation()
	if got := usMarketHolidayKind(time.Date(2021, 12, 31, 0, 0, 0, 0, loc)); got != marketHolidayNone {
		t.Errorf("2021-12-31: got %v, want none", got)
	}
	if got := usMarketHolidayKind(time.Date(2022, 1, 1, 0, 0, 0, 0, loc)); got != marketHolidayFullClose {
		t.Errorf("2022-01-01: got %v, want full close", got)
	}
	if got := usMarketHolidayKind(time.Date(2022, 12, 25, 0, 0, 0, 0, loc)); got != marketHolidayFullClose {
		t.Errorf("2022-12-25: got %v, want full close", got)
	}
	if got := usMarketHolidayKind(time.Date(2022, 12, 26, 0, 0, 0, 0, loc)); got != marketHolidayNone {
		t.Errorf("2022-12-26: got %v, want none", got)
	}
}

// Item 13
func TestIndexCFDsUseOTCProfile(t *testing.T) {
	for _, symbol := range []string{"USA500IDXUSD", "USATECHIDXUSD", "DEUIDXEUR", "JPNIDXJPY", "usa500.idx/usd"} {
		if got := ResolveGapMarketProfile(symbol, ""); got != MarketProfileOTC24x5 {
			t.Errorf("%s: got %s want %s", symbol, got, MarketProfileOTC24x5)
		}
	}
	if got := ResolveGapMarketProfile("AAPLUSUSD", ""); got != MarketProfileEquity {
		t.Errorf("AAPLUSUSD: got %s", got)
	}
	if got := ResolveGapMarketProfile("USA500IDXUSD", MarketProfileEquity); got != MarketProfileEquity {
		t.Errorf("explicit equity selection must win, got %s", got)
	}
}

// Item 14
func TestGapMarketLocationIsNewYork(t *testing.T) {
	loc := gapMarketLocation()
	if loc.String() != "America/New_York" {
		t.Fatalf("location = %s", loc)
	}
	if gapMarketLocation() != loc {
		t.Fatal("location must be cached")
	}
}

// Item 15
func TestInspectDuplicateCountsUnchanged(t *testing.T) {
	path := filepath.Join(t.TempDir(), "dups.csv")
	content := "timestamp,close\n" +
		"2024-01-02T00:00:00Z,1\n" +
		"2024-01-02T00:00:00Z,1\n" + // duplicate row + stamp
		"2024-01-02T01:00:00+01:00,2\n" + // same instant, different text: duplicate stamp only
		"2024-01-02T00:01:00Z,1\n"
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	stats, err := InspectCSV(path)
	if err != nil {
		t.Fatal(err)
	}
	if stats.Rows != 4 || stats.DuplicateRows != 1 || stats.DuplicateStamps != 2 {
		t.Fatalf("stats = rows %d dupRows %d dupStamps %d", stats.Rows, stats.DuplicateRows, stats.DuplicateStamps)
	}
}

// Item 16
func TestAssembleParquetBatchesManyRows(t *testing.T) {
	dir := t.TempDir()
	start := time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC)
	n := parquetWriteBatchSize*2 + 17
	cols := []string{"timestamp", "close"}
	part1 := filepath.Join(dir, "p1.csv")
	part2 := filepath.Join(dir, "p2.csv")
	all := testBars(start, n, time.Second)
	if err := WriteBars(part1, testInstrument, cols, all[:n/2], nil, nil); err != nil {
		t.Fatal(err)
	}
	if err := WriteBars(part2, testInstrument, cols, all[n/2-1:], nil, nil); err != nil { // one overlapping row
		t.Fatal(err)
	}
	out := filepath.Join(dir, "out.parquet")
	if err := AssembleCSVFromParts(out, []string{part1, part2}, start, start.Add(time.Hour*24)); err != nil {
		t.Fatal(err)
	}
	if rows, err := parquetRowCount(out); err != nil || rows != n {
		t.Fatalf("rows = %d, %v; want %d", rows, err, n)
	}
	stats, err := InspectCSV(out)
	if err != nil || stats.DuplicateStamps != 0 || stats.OutOfOrderRows != 0 {
		t.Fatalf("stats %+v, %v", stats, err)
	}
}
