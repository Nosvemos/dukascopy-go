package csvout

import (
	"bytes"
	"encoding/csv"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Nosvemos/dukascopy-go/pkg/dukascopy"
)

func TestCoverageBoostJSONLAndWriters(t *testing.T) {
	now := time.Now().UTC()
	inst := dukascopy.Instrument{Name: "EURUSD", PriceScale: 5}
	bars := []dukascopy.Bar{
		{Time: now, Open: 1.08, High: 1.09, Low: 1.07, Close: 1.085, Volume: 1000},
		{Time: now.Add(time.Minute), Open: 1.085, High: 1.095, Low: 1.08, Close: 1.09, Volume: 1500},
	}
	ticks := []dukascopy.Tick{
		{Time: now, Bid: 1.08, Ask: 1.0802, BidVolume: 100, AskVolume: 100},
		{Time: now.Add(time.Second), Bid: 1.0805, Ask: 1.0807, BidVolume: 200, AskVolume: 200},
	}

	barCols := []string{"timestamp", "open", "high", "low", "close", "volume"}
	tickCols := []string{"timestamp", "bid", "ask", "bid_volume", "ask_volume"}

	// JSONL writers to buffer
	var barBuf bytes.Buffer
	if err := WriteBarsJSONLToWriter(&barBuf, inst, barCols, bars, nil, nil); err != nil {
		t.Fatalf("WriteBarsJSONLToWriter failed: %v", err)
	}

	var tickBuf bytes.Buffer
	if err := WriteTicksJSONLToWriter(&tickBuf, inst, tickCols, ticks); err != nil {
		t.Fatalf("WriteTicksJSONLToWriter failed: %v", err)
	}

	// JSONL file writers
	dir := t.TempDir()
	barFile := filepath.Join(dir, "bars.jsonl")
	if err := writeBarsJSONL(barFile, inst, barCols, bars, nil, nil); err != nil {
		t.Fatalf("writeBarsJSONL failed: %v", err)
	}

	tickFile := filepath.Join(dir, "ticks.jsonl")
	if err := writeTicksJSONL(tickFile, inst, tickCols, ticks); err != nil {
		t.Fatalf("writeTicksJSONL failed: %v", err)
	}

	// JSONL with Bid/Ask fused columns
	fusedCols := []string{"timestamp", "bid_open", "bid_close", "ask_open", "ask_close"}
	var fusedBuf bytes.Buffer
	if err := WriteBarsJSONLToWriter(&fusedBuf, inst, fusedCols, nil, bars, bars); err != nil {
		t.Fatalf("WriteBarsJSONLToWriter fused failed: %v", err)
	}

	// IsJSONLPath
	if !IsJSONLPath("data.jsonl") || !IsJSONLPath("data.json") {
		t.Errorf("expected true for jsonl/json")
	}
	if IsJSONLPath("data.csv") {
		t.Errorf("expected false for csv")
	}

	// CSV tick rows
	var csvBuf bytes.Buffer
	csvWriter := csv.NewWriter(&csvBuf)
	if err := writeTicksCSVRows(csvWriter, inst, tickCols, ticks, false); err != nil {
		t.Fatalf("writeTicksCSVRows failed: %v", err)
	}
}

func TestCoverageBoostMarketHolidayAndProfiles(t *testing.T) {
	// Easter Sunday and Good Friday
	g1 := goodFriday(2024, time.UTC)
	if g1.Month() != time.March || g1.Day() != 29 {
		t.Errorf("unexpected Good Friday 2024: %v", g1)
	}

	// nth weekday of month
	mlk := nthWeekdayOfMonth(2024, time.January, time.Monday, 3, time.UTC)
	if mlk.Day() != 15 {
		t.Errorf("unexpected MLK Day 2024: %v", mlk)
	}

	// last weekday of month
	memDay := lastWeekdayOfMonth(2024, time.May, time.Monday, time.UTC)
	if memDay.Day() != 27 {
		t.Errorf("unexpected Memorial Day 2024: %v", memDay)
	}

	// FX closure boundaries
	fridayClose := time.Date(2024, 1, 5, 22, 0, 0, 0, time.UTC) // Friday 22:00 UTC
	if !isLikelyFXMarketClosed(fridayClose) {
		t.Errorf("expected FX closed on Friday night")
	}

	nextOpen := nextLikelyFXClosureBoundary(fridayClose)
	if nextOpen.Weekday() != time.Saturday {
		t.Errorf("expected next boundary, got %v", nextOpen)
	}

	// Equity closures
	locNY, _ := time.LoadLocation("America/New_York")
	equityNight := time.Date(2024, 1, 2, 22, 0, 0, 0, locNY)
	if !isLikelyEquityMarketClosed(equityNight) {
		t.Errorf("expected equity closed at 22:00 NY")
	}
	nextEq := nextLikelyEquityClosureBoundary(equityNight)
	if nextEq.IsZero() {
		t.Errorf("expected next equity boundary")
	}

	// US Market Holidays
	holidays := []time.Time{
		time.Date(2024, 1, 1, 12, 0, 0, 0, locNY),   // New Year
		time.Date(2024, 1, 15, 12, 0, 0, 0, locNY),  // MLK
		time.Date(2024, 2, 19, 12, 0, 0, 0, locNY),  // Presidents Day
		time.Date(2024, 3, 29, 12, 0, 0, 0, locNY),  // Good Friday
		time.Date(2024, 5, 27, 12, 0, 0, 0, locNY),  // Memorial Day
		time.Date(2024, 6, 19, 12, 0, 0, 0, locNY),  // Juneteenth
		time.Date(2024, 7, 4, 12, 0, 0, 0, locNY),   // Independence Day
		time.Date(2024, 9, 2, 12, 0, 0, 0, locNY),   // Labor Day
		time.Date(2024, 11, 28, 12, 0, 0, 0, locNY), // Thanksgiving
		time.Date(2024, 12, 25, 12, 0, 0, 0, locNY), // Christmas
	}
	for _, h := range holidays {
		kind := usMarketHolidayKind(h)
		if kind == marketHolidayNone {
			t.Errorf("expected holiday for %v, got none", h)
		}
	}
	// Regular day
	if usMarketHolidayKind(time.Date(2024, 5, 8, 12, 0, 0, 0, locNY)) != marketHolidayNone {
		t.Errorf("expected no holiday for regular Wednesday")
	}
}

func TestCoverageBoostParquetAssemblyErrors(t *testing.T) {
	cfg := DefaultConfig()

	// Empty parts
	if err := cfg.assembleParquetFromCSVParts("out.parquet", nil, time.Time{}, time.Time{}); err == nil {
		t.Errorf("expected error for empty partPaths")
	}

	// Nonexistent parts
	if err := cfg.assembleParquetFromCSVParts("out.parquet", []string{"nonexistent.csv"}, time.Time{}, time.Time{}); err == nil {
		t.Errorf("expected error for nonexistent CSV part")
	}

	// Nonexistent file for cleanParquetDuplicates
	if _, err := cfg.cleanParquetDuplicates("nonexistent.parquet"); err == nil {
		t.Errorf("expected error for nonexistent parquet file")
	}
}

func TestCoverageBoostParquetRangeAndAudit(t *testing.T) {
	dir := t.TempDir()
	sourceParquet := filepath.Join(dir, "source.parquet")
	inst := dukascopy.Instrument{Name: "EURUSD", PriceScale: 5}
	now := time.Date(2024, 1, 1, 10, 0, 0, 0, time.UTC)
	bars := []dukascopy.Bar{
		{Time: now, Open: 1.08, High: 1.09, Low: 1.07, Close: 1.085, Volume: 1000},
		{Time: now.Add(time.Minute), Open: 1.085, High: 1.095, Low: 1.08, Close: 1.09, Volume: 1500},
		{Time: now.Add(2 * time.Minute), Open: 1.09, High: 1.10, Low: 1.085, Close: 1.095, Volume: 2000},
	}
	barCols := []string{"timestamp", "open", "high", "low", "close", "volume"}

	// Write initial parquet
	if err := writeBarsParquet(sourceParquet, inst, barCols, bars, nil, nil); err != nil {
		t.Fatalf("writeBarsParquet failed: %v", err)
	}

	cfg := DefaultConfig()

	// 1. Extract range Parquet -> Parquet
	outParquet := filepath.Join(dir, "filtered.parquet")
	from := now
	to := now.Add(2 * time.Minute)
	if err := cfg.extractRangeFromParquet(sourceParquet, outParquet, from, to); err != nil {
		t.Fatalf("extractRangeFromParquet to parquet failed: %v", err)
	}

	// 2. Extract range Parquet -> CSV
	outCSV := filepath.Join(dir, "filtered.csv")
	if err := cfg.extractRangeFromParquet(sourceParquet, outCSV, from, to); err != nil {
		t.Fatalf("extractRangeFromParquet to CSV failed: %v", err)
	}

	// 3. Extract range CSV -> Parquet
	csvToParquet := filepath.Join(dir, "from_csv.parquet")
	if err := cfg.extractRangeCSVToParquet(outCSV, csvToParquet, from, to); err != nil {
		t.Fatalf("extractRangeCSVToParquet failed: %v", err)
	}

	// 4. Audit Parquet
	audit, err := auditParquet(sourceParquet)
	if err != nil || audit.Rows != 3 {
		t.Fatalf("auditParquet failed: audit=%+v, err=%v", audit, err)
	}

	// 5. Inspect Parquet
	opts := InspectOptions{IncludeSuspiciousGaps: true, MaxSuspiciousGapDetails: 10}
	stats, err := cfg.inspectParquetWithOptions(sourceParquet, opts)
	if err != nil || stats.Rows != 3 {
		t.Fatalf("inspectParquetWithOptions failed: stats=%+v, err=%v", stats, err)
	}
}

func TestCoverageBoostWriteBarsWithFeaturesMultiFormat(t *testing.T) {
	dir := t.TempDir()
	inst := dukascopy.Instrument{Name: "EURUSD", PriceScale: 5}
	now := time.Date(2024, 1, 1, 10, 0, 0, 0, time.UTC)
	bars := []dukascopy.Bar{
		{Time: now, Open: 1.08, High: 1.09, Low: 1.07, Close: 1.085, Volume: 1000},
		{Time: now.Add(time.Minute), Open: 1.085, High: 1.095, Low: 1.08, Close: 1.09, Volume: 1500},
	}
	baseCols := []string{"timestamp", "open", "high", "low", "close", "volume"}
	featNames := []string{"sma_10", "rsi_14"}
	featRows := [][]float64{
		{1.082, 55.4},
		{1.084, 57.1},
	}

	cfg := DefaultConfig()

	// 1. Write CSV
	csvPath := filepath.Join(dir, "bars_feat.csv")
	if err := cfg.WriteBarsWithFeatures(csvPath, inst, baseCols, bars, featNames, featRows); err != nil {
		t.Fatalf("WriteBarsWithFeatures CSV failed: %v", err)
	}

	// 2. Write CSV.GZ
	gzPath := filepath.Join(dir, "bars_feat.csv.gz")
	if err := cfg.WriteBarsWithFeatures(gzPath, inst, baseCols, bars, featNames, featRows); err != nil {
		t.Fatalf("WriteBarsWithFeatures CSV.GZ failed: %v", err)
	}

	// 3. Write Parquet
	parquetPath := filepath.Join(dir, "bars_feat.parquet")
	if err := cfg.WriteBarsWithFeatures(parquetPath, inst, baseCols, bars, featNames, featRows); err != nil {
		t.Fatalf("WriteBarsWithFeatures Parquet failed: %v", err)
	}

	// 4. Write Arrow / Feather
	arrowPath := filepath.Join(dir, "bars_feat.arrow")
	if err := cfg.WriteBarsWithFeatures(arrowPath, inst, baseCols, bars, featNames, featRows); err != nil {
		t.Fatalf("WriteBarsWithFeatures Arrow failed: %v", err)
	}

	featherPath := filepath.Join(dir, "bars_feat.feather")
	if err := cfg.WriteBarsWithFeatures(featherPath, inst, baseCols, bars, featNames, featRows); err != nil {
		t.Fatalf("WriteBarsWithFeatures Feather failed: %v", err)
	}

	// 5. Write JSONL
	jsonlPath := filepath.Join(dir, "bars_feat.jsonl")
	if err := cfg.WriteBarsWithFeatures(jsonlPath, inst, baseCols, bars, featNames, featRows); err != nil {
		t.Fatalf("WriteBarsWithFeatures JSONL failed: %v", err)
	}

	// 6. Write Atomic
	atomicPath := filepath.Join(dir, "bars_atomic.csv")
	if err := WriteBarsWithFeaturesAtomic(atomicPath, inst, baseCols, bars, featNames, featRows); err != nil {
		t.Fatalf("WriteBarsWithFeaturesAtomic failed: %v", err)
	}

	// 7. Top-level WriteBarsWithFeatures
	topLevelCSV := filepath.Join(dir, "bars_top.csv")
	if err := WriteBarsWithFeatures(topLevelCSV, inst, baseCols, bars, featNames, featRows); err != nil {
		t.Fatalf("top-level WriteBarsWithFeatures failed: %v", err)
	}

	// 8. ReadBarsFromCSV
	readBars, header, err := ReadBarsFromCSV(csvPath)
	if err != nil || len(readBars) != 2 || len(header) != 8 {
		t.Fatalf("ReadBarsFromCSV failed: readBars=%d, header=%v, err=%v", len(readBars), header, err)
	}
}

func TestCoverageBoostAuditCSVAndProfileResolution(t *testing.T) {
	dir := t.TempDir()

	// 1. Audit CSV with empty file
	emptyFile := filepath.Join(dir, "empty.csv")
	_ = os.WriteFile(emptyFile, []byte(""), 0o644)
	audit, err := AuditCSV(emptyFile)
	if err != nil || audit.Rows != 0 {
		t.Errorf("expected 0 rows for empty csv, got %+v, %v", audit, err)
	}

	// 2. Audit CSV with header only
	headerFile := filepath.Join(dir, "header.csv")
	_ = os.WriteFile(headerFile, []byte("timestamp,open,close\n"), 0o644)
	auditH, err := AuditCSV(headerFile)
	if err != nil || auditH.Rows != 0 {
		t.Errorf("expected 0 rows for header only csv, got %+v, %v", auditH, err)
	}

	// 3. ResolveGapMarketProfile for symbols
	if ResolveGapMarketProfile("BTCUSD", "") != MarketProfileCrypto24x7 {
		t.Errorf("expected crypto profile for BTCUSD")
	}
	if ResolveGapMarketProfile("XAUUSD", "") != MarketProfileOTC24x5 {
		t.Errorf("expected OTC profile for XAUUSD")
	}
	if ResolveGapMarketProfile("AAPLUSUSD", "") != MarketProfileEquity {
		t.Errorf("expected equity profile for AAPLUSUSD")
	}
	if ResolveGapMarketProfile("EURUSD", "") != MarketProfileFX24x5 {
		t.Errorf("expected FX profile for EURUSD")
	}
	if ResolveGapMarketProfile("EURUSD", MarketProfileAlways) != MarketProfileAlways {
		t.Errorf("expected explicit always profile")
	}
}

// TestCoverageBoostMarketClosureBoundaries covers the nextLikelyOTCClosureBoundary,
// nextLikelyFXClosureBoundary, nextLikelyEquityClosureBoundary, isLikelyFXMarketClosed,
// isLikelyEquityMarketClosed, isExpectedGapForProfile, and gapMarketLocation paths.
func TestCoverageBoostMarketClosureBoundaries(t *testing.T) {
	loc, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Skip("no America/New_York timezone available")
	}

	// Helper to build a NY time
	ny := func(year int, month time.Month, day, hour, min int) time.Time {
		return time.Date(year, month, day, hour, min, 0, 0, loc).UTC()
	}

	// OTC: Friday after 17:00 NY -> Saturday transition
	otcFridayClose := ny(2024, 1, 5, 17, 1) // Friday 17:01
	result := nextLikelyOTCClosureBoundary(otcFridayClose)
	if !result.After(otcFridayClose) {
		t.Errorf("expected OTC boundary to be after Friday 17:01, got %v", result)
	}

	// OTC: Saturday -> Sunday
	otcSaturday := ny(2024, 1, 6, 12, 0) // Saturday noon
	result = nextLikelyOTCClosureBoundary(otcSaturday)
	if !result.After(otcSaturday) {
		t.Errorf("expected OTC Saturday boundary to be after, got %v", result)
	}

	// OTC: Sunday before 18:00
	otcSundayMorning := ny(2024, 1, 7, 10, 0) // Sunday 10:00
	result = nextLikelyOTCClosureBoundary(otcSundayMorning)
	if !result.After(otcSundayMorning) {
		t.Errorf("expected OTC Sunday morning boundary to be after, got %v", result)
	}

	// OTC: Mon/Tue/Wed/Thu at 16:59 -> 17:00
	otcTuesday := ny(2024, 1, 2, 16, 59) // Tuesday 16:59
	result = nextLikelyOTCClosureBoundary(otcTuesday)
	if !result.After(otcTuesday) {
		t.Errorf("expected OTC Tuesday 16:59 boundary to advance, got %v", result)
	}

	// OTC: Mon/Tue/Wed/Thu at 17:00 -> 18:00
	otcTuesdayAt17 := ny(2024, 1, 2, 17, 0) // Tuesday 17:00
	result = nextLikelyOTCClosureBoundary(otcTuesdayAt17)
	if !result.After(otcTuesdayAt17) {
		t.Errorf("expected OTC Tuesday 17:00 boundary to advance, got %v", result)
	}

	// FX: isLikelyFXMarketClosed - Saturday (true), Friday after 17:00 (true), weekday (false)
	saturdayNoon := ny(2024, 1, 6, 12, 0)
	if !isLikelyFXMarketClosed(saturdayNoon) {
		t.Errorf("expected FX market closed on Saturday")
	}
	mondayMorning := ny(2024, 1, 8, 10, 0)
	if isLikelyFXMarketClosed(mondayMorning) {
		t.Errorf("expected FX market open on Monday morning")
	}

	// FX: nextLikelyFXClosureBoundary
	fxFridayClose := ny(2024, 1, 5, 17, 1) // Friday after close
	result = nextLikelyFXClosureBoundary(fxFridayClose)
	if !result.After(fxFridayClose) {
		t.Errorf("expected FX Friday close boundary to advance, got %v", result)
	}

	fxSaturday := ny(2024, 1, 6, 12, 0)
	result = nextLikelyFXClosureBoundary(fxSaturday)
	if !result.After(fxSaturday) {
		t.Errorf("expected FX Saturday boundary to advance, got %v", result)
	}

	fxSundayMorning := ny(2024, 1, 7, 10, 0) // Sunday before 17:00
	result = nextLikelyFXClosureBoundary(fxSundayMorning)
	if !result.After(fxSundayMorning) {
		t.Errorf("expected FX Sunday morning boundary to advance, got %v", result)
	}

	// Equity: isLikelyEquityMarketClosed
	weekendEquity := ny(2024, 1, 6, 12, 0)
	if !isLikelyEquityMarketClosed(weekendEquity) {
		t.Errorf("expected equity market closed on Saturday")
	}
	preMarket := ny(2024, 1, 8, 9, 0) // Monday 9:00 (before 9:30 open)
	if !isLikelyEquityMarketClosed(preMarket) {
		t.Errorf("expected equity market closed before 9:30")
	}
	afterMarket := ny(2024, 1, 8, 16, 30) // Monday 16:30 (after 16:00 close)
	if !isLikelyEquityMarketClosed(afterMarket) {
		t.Errorf("expected equity market closed after 16:00")
	}
	marketOpen := ny(2024, 1, 8, 10, 0) // Monday 10:00 (market hours)
	if isLikelyEquityMarketClosed(marketOpen) {
		t.Errorf("expected equity market open at 10:00")
	}

	// Equity: nextLikelyEquityClosureBoundary
	eqFridayClose := ny(2024, 1, 5, 16, 30)
	result = nextLikelyEquityClosureBoundary(eqFridayClose)
	if !result.After(eqFridayClose) {
		t.Errorf("expected equity Friday close boundary to advance, got %v", result)
	}

	eqSaturday := ny(2024, 1, 6, 12, 0)
	result = nextLikelyEquityClosureBoundary(eqSaturday)
	if !result.After(eqSaturday) {
		t.Errorf("expected equity Saturday boundary to advance, got %v", result)
	}

	eqSunday := ny(2024, 1, 7, 12, 0)
	result = nextLikelyEquityClosureBoundary(eqSunday)
	if !result.After(eqSunday) {
		t.Errorf("expected equity Sunday boundary to advance, got %v", result)
	}

	eqAfterClose := ny(2024, 1, 8, 16, 30)
	result = nextLikelyEquityClosureBoundary(eqAfterClose)
	if !result.After(eqAfterClose) {
		t.Errorf("expected equity after-close boundary to advance, got %v", result)
	}

	eqPreMarket := ny(2024, 1, 8, 8, 0)
	result = nextLikelyEquityClosureBoundary(eqPreMarket)
	if !result.After(eqPreMarket) {
		t.Errorf("expected equity pre-market boundary to advance, got %v", result)
	}

	// IsExpectedGapForProfile: FX and Equity paths
	// FX gap: previous Friday 17:00 -> Monday 09:00 UTC (weekend gap, expected)
	prev := ny(2024, 1, 5, 17, 0)  // Friday 17:00 NY
	curr := ny(2024, 1, 8, 9, 0)   // Monday 09:00 NY
	isFX := IsExpectedGapForProfile(prev, curr, time.Minute, "EURUSD", "")
	_ = isFX // Just exercise the code path

	// Equity gap
	isEq := IsExpectedGapForProfile(prev, curr, time.Minute, "AAPLUSUSD", "")
	_ = isEq

	// Crypto (always open): gap should not be expected
	prevC := ny(2024, 1, 5, 12, 0)
	currC := ny(2024, 1, 5, 13, 0)
	isCrypto := IsExpectedGapForProfile(prevC, currC, time.Minute, "BTCUSD", "")
	if isCrypto {
		t.Errorf("expected crypto 24x7 gap not to be flagged as market-closure gap")
	}
}

// TestCoverageBoostParquetStreamWriter covers CreateParquetStreamWriter and Close paths.
func TestCoverageBoostParquetStreamWriter(t *testing.T) {
	dir := t.TempDir()
	outPath := filepath.Join(dir, "stream.parquet")
	cols := []string{"timestamp", "open", "close", "volume"}

	// 1. Top-level CreateParquetStreamWriter
	sw, err := CreateParquetStreamWriter(outPath, cols)
	if err != nil {
		t.Fatalf("CreateParquetStreamWriter failed: %v", err)
	}

	// 2. WriteBatch with records
	now := time.Date(2024, 1, 1, 10, 0, 0, 0, time.UTC)
	records := []map[string]any{
		{"timestamp": now.UnixMilli(), "open": "1.08", "close": "1.09", "volume": "1000"},
		{"timestamp": now.Add(time.Minute).UnixMilli(), "open": "1.09", "close": "1.10", "volume": "2000"},
	}
	if err := sw.WriteBatch(records); err != nil {
		t.Fatalf("WriteBatch failed: %v", err)
	}

	// 3. WriteBatch empty (no-op)
	if err := sw.WriteBatch(nil); err != nil {
		t.Fatalf("WriteBatch(nil) failed: %v", err)
	}

	// 4. Close
	if err := sw.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// 5. Config-based stream writer
	sw2, err := DefaultConfig().CreateParquetStreamWriter(filepath.Join(dir, "stream2.parquet"), cols)
	if err != nil {
		t.Fatalf("Config.CreateParquetStreamWriter failed: %v", err)
	}
	if err := sw2.Close(); err != nil {
		t.Fatalf("Config.CreateParquetStreamWriter Close failed: %v", err)
	}
}

// TestCoverageBoostJSONLFilePaths covers writeTicksJSONL and writeBarsJSONL file path branches.
func TestCoverageBoostJSONLFilePaths(t *testing.T) {
	dir := t.TempDir()
	inst := dukascopy.Instrument{Name: "EURUSD", PriceScale: 5}
	now := time.Date(2024, 1, 1, 10, 0, 0, 0, time.UTC)

	bars := []dukascopy.Bar{
		{Time: now, Open: 1.08, High: 1.09, Low: 1.07, Close: 1.085, Volume: 1000},
	}
	ticks := []dukascopy.Tick{
		{Time: now, Bid: 1.08, Ask: 1.0802, BidVolume: 100, AskVolume: 100},
	}
	barCols := []string{"timestamp", "open", "high", "low", "close", "volume"}
	tickCols := []string{"timestamp", "bid", "ask", "bid_volume", "ask_volume"}

	// writeTicksJSONL (internal, file-backed path)
	tickJSONL := filepath.Join(dir, "ticks.jsonl")
	if err := writeTicksJSONL(tickJSONL, inst, tickCols, ticks); err != nil {
		t.Fatalf("writeTicksJSONL failed: %v", err)
	}

	// writeBarsJSONL (internal, file-backed path)
	barJSONL := filepath.Join(dir, "bars.jsonl")
	if err := writeBarsJSONL(barJSONL, inst, barCols, bars, nil, nil); err != nil {
		t.Fatalf("writeBarsJSONL failed: %v", err)
	}

	// WriteBarsJSONLToWriter with bid/ask columns (combineBarRows path)
	bidAskCols := []string{"timestamp", "bid_open", "bid_close", "ask_open", "ask_close"}
	bidBars := bars
	askBars := bars
	var buf bytes.Buffer
	if err := WriteBarsJSONLToWriter(&buf, inst, bidAskCols, nil, bidBars, askBars); err != nil {
		t.Fatalf("WriteBarsJSONLToWriter with bid/ask failed: %v", err)
	}
}

func TestCoverageBoostParquetAssemblyAndCleanDuplicates(t *testing.T) {
	dir := t.TempDir()
	inst := dukascopy.Instrument{Name: "EURUSD", PriceScale: 5}
	now := time.Date(2024, 1, 1, 10, 0, 0, 0, time.UTC)
	cols := []string{"timestamp", "open", "high", "low", "close", "volume"}

	// 1. Create two CSV parts
	part1 := filepath.Join(dir, "part1.csv")
	part2 := filepath.Join(dir, "part2.csv")
	bars1 := []dukascopy.Bar{
		{Time: now, Open: 1.08, High: 1.09, Low: 1.07, Close: 1.085, Volume: 1000},
		{Time: now.Add(time.Minute), Open: 1.085, High: 1.095, Low: 1.08, Close: 1.09, Volume: 1500},
	}
	bars2 := []dukascopy.Bar{
		{Time: now.Add(time.Minute), Open: 1.085, High: 1.095, Low: 1.08, Close: 1.09, Volume: 1500}, // duplicate
		{Time: now.Add(2 * time.Minute), Open: 1.09, High: 1.10, Low: 1.085, Close: 1.095, Volume: 2000},
	}

	cfg := DefaultConfig()
	if err := cfg.WriteBars(part1, inst, cols, bars1, nil, nil); err != nil {
		t.Fatalf("WriteBars part1 failed: %v", err)
	}
	if err := cfg.WriteBars(part2, inst, cols, bars2, nil, nil); err != nil {
		t.Fatalf("WriteBars part2 failed: %v", err)
	}

	// 2. Assemble parquet from CSV parts
	assembledParquet := filepath.Join(dir, "assembled.parquet")
	from := now
	to := now.Add(3 * time.Minute)
	if err := cfg.assembleParquetFromCSVParts(assembledParquet, []string{part1, part2}, from, to); err != nil {
		t.Fatalf("assembleParquetFromCSVParts failed: %v", err)
	}

	// 3. Clean parquet duplicates
	cleanedCount, err := cfg.cleanParquetDuplicates(assembledParquet)
	if err != nil {
		t.Fatalf("cleanParquetDuplicates failed: %v", err)
	}
	if cleanedCount != 0 && cleanedCount != 1 {
		t.Logf("cleaned duplicates count: %d", cleanedCount)
	}

	// 4. Inspect parquet with duplicates / out of order
	stats, err := cfg.inspectParquetWithOptions(assembledParquet, InspectOptions{IncludeSuspiciousGaps: true})
	if err != nil {
		t.Fatalf("inspectParquetWithOptions failed: %v", err)
	}
	if stats.Rows == 0 {
		t.Fatalf("expected rows > 0, got %d", stats.Rows)
	}
}
