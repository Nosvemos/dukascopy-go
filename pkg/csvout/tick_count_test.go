package csvout

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Nosvemos/dukascopy-go/pkg/dukascopy"
	"github.com/apache/arrow/go/v18/arrow"
	"github.com/apache/arrow/go/v18/arrow/array"
	"github.com/apache/arrow/go/v18/arrow/ipc"
)

func TestTickCountRoundTripsAcrossOutputFormats(t *testing.T) {
	const wantTickCount int64 = 9_007_199_254_740_993
	instrument := dukascopy.Instrument{PriceScale: 5}
	columns := []string{"timestamp", "volume", "tick_count"}
	bar := dukascopy.Bar{
		Time:      time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC),
		Volume:    123.4567,
		TickCount: wantTickCount,
	}
	dir := t.TempDir()

	csvPath := filepath.Join(dir, "ticks.csv")
	if err := WriteBars(csvPath, instrument, columns, []dukascopy.Bar{bar}, nil, nil); err != nil {
		t.Fatalf("WriteBars CSV: %v", err)
	}
	csvFile, err := os.Open(csvPath)
	if err != nil {
		t.Fatalf("open CSV: %v", err)
	}
	csvRows, err := csv.NewReader(csvFile).ReadAll()
	if closeErr := csvFile.Close(); closeErr != nil {
		t.Fatalf("close CSV: %v", closeErr)
	}
	if err != nil {
		t.Fatalf("read CSV: %v", err)
	}
	if got := csvRows[1][1]; got != "123.4567" {
		t.Fatalf("CSV volume = %q, want unchanged liquidity volume %q", got, "123.4567")
	}
	if got := csvRows[1][2]; got != "9007199254740993" {
		t.Fatalf("CSV tick_count = %q, want exact integer", got)
	}

	readBars, headers, err := ReadBarsFromCSV(csvPath)
	if err != nil {
		t.Fatalf("ReadBarsFromCSV: %v", err)
	}
	if len(headers) != len(columns) || len(readBars) != 1 || readBars[0].TickCount != wantTickCount {
		t.Fatalf("feature CSV read lost tick_count: headers=%v bars=%+v", headers, readBars)
	}

	jsonlPath := filepath.Join(dir, "ticks.jsonl")
	if err := WriteBars(jsonlPath, instrument, columns, []dukascopy.Bar{bar}, nil, nil); err != nil {
		t.Fatalf("WriteBars JSONL: %v", err)
	}
	jsonlFile, err := os.Open(jsonlPath)
	if err != nil {
		t.Fatalf("open JSONL: %v", err)
	}
	decoder := json.NewDecoder(jsonlFile)
	decoder.UseNumber()
	var jsonRow map[string]any
	if err := decoder.Decode(&jsonRow); err != nil {
		_ = jsonlFile.Close()
		t.Fatalf("decode JSONL: %v", err)
	}
	if err := jsonlFile.Close(); err != nil {
		t.Fatalf("close JSONL: %v", err)
	}
	if got := jsonRow["tick_count"].(json.Number).String(); got != "9007199254740993" {
		t.Fatalf("JSONL tick_count = %q, want exact integer", got)
	}

	parquetPath := filepath.Join(dir, "ticks.parquet")
	if err := WriteBars(parquetPath, instrument, columns, []dukascopy.Bar{bar}, nil, nil); err != nil {
		t.Fatalf("WriteBars Parquet: %v", err)
	}
	parquetFile, parquetData, closeParquet, err := openParquetFile(parquetPath)
	if err != nil {
		t.Fatalf("open Parquet: %v", err)
	}
	defer closeParquet()
	parquetReader := DefaultConfig().parquetReaderFactory(parquetFile, parquetData.Schema())
	defer parquetReader.Close()
	parquetRows := []map[string]any{{}}
	rowsRead, err := parquetReader.Read(parquetRows)
	if err != nil && err != io.EOF {
		t.Fatalf("read Parquet: %v", err)
	}
	parquetTickCount, ok := parquetRows[0]["tick_count"].(int64)
	if rowsRead != 1 || !ok || parquetTickCount != wantTickCount {
		t.Fatalf("Parquet tick_count = %#v (rows=%d), want int64 %d", parquetRows[0]["tick_count"], rowsRead, wantTickCount)
	}

	arrowPath := filepath.Join(dir, "ticks.arrow")
	if err := WriteBars(arrowPath, instrument, columns, []dukascopy.Bar{bar}, nil, nil); err != nil {
		t.Fatalf("WriteBars Arrow: %v", err)
	}
	arrowFile, err := os.Open(arrowPath)
	if err != nil {
		t.Fatalf("open Arrow: %v", err)
	}
	defer arrowFile.Close()
	arrowReader, err := ipc.NewFileReader(arrowFile)
	if err != nil {
		t.Fatalf("read Arrow file: %v", err)
	}
	defer arrowReader.Close()
	if got := arrowReader.Schema().Field(2).Type.ID(); got != arrow.INT64 {
		t.Fatalf("Arrow tick_count type = %s, want INT64", got)
	}
	arrowRecord, err := arrowReader.Record(0)
	if err != nil {
		t.Fatalf("read Arrow record: %v", err)
	}
	defer arrowRecord.Release()
	arrowTickCount, ok := arrowRecord.Column(2).(*array.Int64)
	if !ok || arrowTickCount.Value(0) != wantTickCount {
		t.Fatalf("Arrow tick_count = %#v, want int64 %d", arrowRecord.Column(2), wantTickCount)
	}
}

func TestTickCountFormatsBidAskBars(t *testing.T) {
	const wantTickCount int64 = 42
	timeAt := time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC)
	var output bytes.Buffer
	columns := []string{"timestamp", "bid_close", "volume", "tick_count"}
	if err := WriteBarsToWriter(&output, dukascopy.Instrument{}, columns, nil,
		[]dukascopy.Bar{{Time: timeAt, Volume: 2, TickCount: wantTickCount}},
		[]dukascopy.Bar{{Time: timeAt, Volume: 3, TickCount: wantTickCount}}); err != nil {
		t.Fatalf("WriteBarsToWriter: %v", err)
	}
	rows, err := csv.NewReader(&output).ReadAll()
	if err != nil {
		t.Fatalf("read paired CSV: %v", err)
	}
	if got := rows[1][2]; got != "5" {
		t.Fatalf("paired volume = %q, want the existing bid + ask liquidity volume", got)
	}
	if got := rows[1][3]; got != "42" {
		t.Fatalf("paired tick_count = %q, want %d", got, wantTickCount)
	}
}
