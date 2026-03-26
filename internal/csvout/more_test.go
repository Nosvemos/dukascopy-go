package csvout

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Nosvemos/dukascopy-go/internal/dukascopy"
)

func TestAtomicWritersAndReaderHelpers(t *testing.T) {
	dir := t.TempDir()
	instrument := dukascopy.Instrument{PriceScale: 3}
	barPath := filepath.Join(dir, "bars.parquet")
	tickPath := filepath.Join(dir, "ticks.csv")

	bidBars := []dukascopy.Bar{{Time: time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC), Open: 100, High: 101, Low: 99, Close: 100.3, Volume: 1}}
	askBars := []dukascopy.Bar{{Time: bidBars[0].Time, Open: 100.2, High: 101.2, Low: 99.2, Close: 100.7, Volume: 1}}
	ticks := []dukascopy.Tick{{Time: bidBars[0].Time, Bid: 100.1, Ask: 100.3, BidVolume: 10, AskVolume: 11}}

	if err := WriteBarsAtomic(barPath, instrument, []string{"timestamp", "mid_close", "spread"}, nil, bidBars, askBars); err != nil {
		t.Fatalf("WriteBarsAtomic returned error: %v", err)
	}
	if err := WriteTicksAtomic(tickPath, instrument, []string{"timestamp", "bid", "ask"}, ticks); err != nil {
		t.Fatalf("WriteTicksAtomic returned error: %v", err)
	}

	if audit, err := auditParquet(barPath); err != nil || audit.Rows != 1 {
		t.Fatalf("unexpected parquet audit: %+v %v", audit, err)
	}
	if rows, err := parquetRowCount(barPath); err != nil || rows != 1 {
		t.Fatalf("unexpected parquet row count: %d %v", rows, err)
	}

	file, reader, closeReader, err := openCSVReader(tickPath)
	if err != nil {
		t.Fatalf("openCSVReader returned error: %v", err)
	}
	_ = file
	defer closeReader()
	header, err := reader.Read()
	if err != nil || len(header) != 3 {
		t.Fatalf("unexpected CSV header: %v %v", header, err)
	}
}

func TestParquetAssemblyAndExtractionHelpers(t *testing.T) {
	dir := t.TempDir()
	part1 := filepath.Join(dir, "part1.csv")
	part2 := filepath.Join(dir, "part2.csv")
	content1 := "timestamp,mid_close\n2024-01-02T00:00:00Z,100.5\n2024-01-02T00:01:00Z,101.5\n"
	content2 := "timestamp,mid_close\n2024-01-02T00:02:00Z,102.5\n2024-01-02T00:03:00Z,103.5\n"
	if err := os.WriteFile(part1, []byte(content1), 0o644); err != nil {
		t.Fatalf("WriteFile returned error: %v", err)
	}
	if err := os.WriteFile(part2, []byte(content2), 0o644); err != nil {
		t.Fatalf("WriteFile returned error: %v", err)
	}

	outputPath := filepath.Join(dir, "assembled.parquet")
	from := time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC)
	to := time.Date(2024, 1, 2, 0, 4, 0, 0, time.UTC)
	if err := assembleParquetFromCSVParts(outputPath, []string{part1, part2}, from, to); err != nil {
		t.Fatalf("assembleParquetFromCSVParts returned error: %v", err)
	}

	extractCSV := filepath.Join(dir, "extract.csv")
	if err := extractRangeFromParquet(outputPath, extractCSV, from.Add(time.Minute), to.Add(-time.Minute)); err != nil {
		t.Fatalf("extractRangeFromParquet to csv returned error: %v", err)
	}
	data, err := os.ReadFile(extractCSV)
	if err != nil {
		t.Fatalf("ReadFile returned error: %v", err)
	}
	if !strings.Contains(string(data), "2024-01-02T00:01:00Z") {
		t.Fatalf("unexpected extracted CSV content: %s", string(data))
	}

	extractParquet := filepath.Join(dir, "extract.parquet")
	if err := extractRangeCSVToParquet(part1, extractParquet, from, to); err != nil {
		t.Fatalf("extractRangeCSVToParquet returned error: %v", err)
	}
	if _, err := inspectParquet(extractParquet); err != nil {
		t.Fatalf("inspectParquet returned error: %v", err)
	}
}

func TestParquetRecordHelpers(t *testing.T) {
	columns := []string{"timestamp", "mid_close"}
	record, err := parquetRecordFromCSVRecord(columns, []string{"2024-01-02T00:00:00Z", "100.5"})
	if err != nil {
		t.Fatalf("parquetRecordFromCSVRecord returned error: %v", err)
	}
	if parquetStringValue(record["mid_close"]) != "100.5" {
		t.Fatalf("unexpected parquetStringValue: %s", parquetStringValue(record["mid_close"]))
	}
	if _, ok := parquetTimestampFromRow(record); !ok {
		t.Fatal("expected parquetTimestampFromRow to parse timestamp")
	}
	if parquetStringValue(nil) != "" {
		t.Fatal("expected nil parquet string value to be empty")
	}
}
