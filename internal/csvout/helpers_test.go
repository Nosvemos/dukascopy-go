package csvout

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Nosvemos/dukascopy-go/internal/dukascopy"
)

func TestProfilesParseAndColumnHelpers(t *testing.T) {
	if got := BarColumnsForProfile(ProfileSimple); len(got) == 0 || got[0] != "timestamp" {
		t.Fatalf("unexpected simple bar columns: %v", got)
	}
	if got := BarColumnsForProfile(Profile("wat")); got != nil {
		t.Fatalf("expected unknown bar profile to return nil, got %v", got)
	}
	if got := TickColumnsForProfile(ProfileFull); len(got) == 0 || got[len(got)-1] != "ask_volume" {
		t.Fatalf("unexpected full tick columns: %v", got)
	}
	if got := TickColumnsForProfile(Profile("wat")); got != nil {
		t.Fatalf("expected unknown tick profile to return nil, got %v", got)
	}
	if _, err := ParseBarColumns("timestamp,mid_close,spread"); err != nil {
		t.Fatalf("ParseBarColumns returned error: %v", err)
	}
	if _, err := ParseTickColumns("timestamp,bid,ask"); err != nil {
		t.Fatalf("ParseTickColumns returned error: %v", err)
	}
	if _, err := ParseBarColumns("timestamp,nope"); err == nil {
		t.Fatal("expected invalid bar column error")
	}
	if !BarColumnsNeedBidAsk([]string{"timestamp", "spread"}) {
		t.Fatal("expected spread column to require bid/ask data")
	}
	if !ColumnsContainTimestamp([]string{"mid_close", " Timestamp "}) {
		t.Fatal("expected timestamp column detection to be case-insensitive")
	}
	if !HeadersMatch([]string{"a", "b"}, []string{"a", "b"}) || HeadersMatch([]string{"a"}, []string{"b"}) {
		t.Fatal("unexpected header matching behavior")
	}
}

func TestWriteBarsAndTicksAndInspectCSV(t *testing.T) {
	dir := t.TempDir()
	instrument := dukascopy.Instrument{PriceScale: 3}
	barPath := filepath.Join(dir, "bars.csv")
	gzipPath := filepath.Join(dir, "bars.csv.gz")
	tickPath := filepath.Join(dir, "ticks.parquet")

	primaryBars := []dukascopy.Bar{
		{Time: time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC), Open: 100, High: 101, Low: 99, Close: 100.5, Volume: 1500},
		{Time: time.Date(2024, 1, 2, 0, 1, 0, 0, time.UTC), Open: 101, High: 102, Low: 100, Close: 101.5, Volume: 2500},
	}
	bidBars := []dukascopy.Bar{
		{Time: primaryBars[0].Time, Open: 100.0, High: 101.0, Low: 99.0, Close: 100.3, Volume: 1500},
		{Time: primaryBars[1].Time, Open: 101.0, High: 102.0, Low: 100.0, Close: 101.3, Volume: 2500},
	}
	askBars := []dukascopy.Bar{
		{Time: primaryBars[0].Time, Open: 100.2, High: 101.2, Low: 99.2, Close: 100.7, Volume: 1500},
		{Time: primaryBars[1].Time, Open: 101.2, High: 102.2, Low: 100.2, Close: 101.7, Volume: 2500},
	}
	ticks := []dukascopy.Tick{
		{Time: primaryBars[0].Time, Bid: 100.1, Ask: 100.3, BidVolume: 10, AskVolume: 11},
		{Time: primaryBars[1].Time, Bid: 101.1, Ask: 101.3, BidVolume: 12, AskVolume: 13},
	}

	if err := WriteBars(barPath, instrument, []string{"timestamp", "open", "high", "low", "close", "volume"}, primaryBars, nil, nil); err != nil {
		t.Fatalf("WriteBars returned error: %v", err)
	}
	if err := WriteBars(gzipPath, instrument, []string{"timestamp", "mid_close", "spread", "bid_close", "ask_close"}, nil, bidBars, askBars); err != nil {
		t.Fatalf("WriteBars gzip returned error: %v", err)
	}
	if err := WriteTicks(tickPath, instrument, []string{"timestamp", "bid", "ask", "bid_volume"}, ticks); err != nil {
		t.Fatalf("WriteTicks parquet returned error: %v", err)
	}

	stats, err := InspectCSV(barPath)
	if err != nil {
		t.Fatalf("InspectCSV returned error: %v", err)
	}
	if stats.Rows != 2 || stats.InferredTimeframe != "m1" {
		t.Fatalf("unexpected CSV stats: %+v", stats)
	}

	gzipStats, err := InspectCSV(gzipPath)
	if err != nil {
		t.Fatalf("InspectCSV gzip returned error: %v", err)
	}
	if !gzipStats.Compressed || gzipStats.Rows != 2 {
		t.Fatalf("unexpected gzip stats: %+v", gzipStats)
	}

	parquetStats, err := InspectCSV(tickPath)
	if err != nil {
		t.Fatalf("InspectCSV parquet returned error: %v", err)
	}
	if parquetStats.Format != "parquet" || parquetStats.Rows != 2 {
		t.Fatalf("unexpected parquet stats: %+v", parquetStats)
	}
}

func TestCSVErrorBranches(t *testing.T) {
	dir := t.TempDir()
	badCSV := filepath.Join(dir, "bad.csv")
	if err := os.WriteFile(badCSV, []byte("timestamp\nbad-time\n"), 0o644); err != nil {
		t.Fatalf("WriteFile returned error: %v", err)
	}
	if _, err := InspectCSV(badCSV); err == nil {
		t.Fatal("expected InspectCSV timestamp parse error")
	}

	noTimestamp := filepath.Join(dir, "notimestamp.csv")
	if err := os.WriteFile(noTimestamp, []byte("open\n1\n"), 0o644); err != nil {
		t.Fatalf("WriteFile returned error: %v", err)
	}
	if err := ExtractCSVRange(noTimestamp, filepath.Join(dir, "out.csv"), time.Now(), time.Now().Add(time.Hour)); err == nil {
		t.Fatal("expected ExtractCSVRange missing timestamp error")
	}

	if _, err := MergeResumeCSV(noTimestamp, noTimestamp, []string{"missing"}); err == nil {
		t.Fatal("expected MergeResumeCSV duplicate tail error")
	}
}

func TestWriterHelpersAndAssembly(t *testing.T) {
	dir := t.TempDir()
	part1 := filepath.Join(dir, "part1.csv")
	part2 := filepath.Join(dir, "part2.csv")
	assembled := filepath.Join(dir, "assembled.csv")

	content1 := "timestamp,open\n2024-01-02T00:00:00Z,100\n2024-01-02T00:01:00Z,101\n"
	content2 := "timestamp,open\n2024-01-02T00:02:00Z,102\n2024-01-02T00:03:00Z,103\n"
	if err := os.WriteFile(part1, []byte(content1), 0o644); err != nil {
		t.Fatalf("WriteFile returned error: %v", err)
	}
	if err := os.WriteFile(part2, []byte(content2), 0o644); err != nil {
		t.Fatalf("WriteFile returned error: %v", err)
	}

	if err := AssembleCSVFromParts(assembled, []string{part1, part2}, time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC), time.Date(2024, 1, 2, 0, 4, 0, 0, time.UTC)); err != nil {
		t.Fatalf("AssembleCSVFromParts returned error: %v", err)
	}
	extracted := filepath.Join(dir, "range.csv")
	if err := ExtractCSVRange(assembled, extracted, time.Date(2024, 1, 2, 0, 1, 0, 0, time.UTC), time.Date(2024, 1, 2, 0, 3, 0, 0, time.UTC)); err != nil {
		t.Fatalf("ExtractCSVRange returned error: %v", err)
	}
	data, err := os.ReadFile(extracted)
	if err != nil {
		t.Fatalf("ReadFile returned error: %v", err)
	}
	if !strings.Contains(string(data), "2024-01-02T00:02:00Z") || strings.Contains(string(data), "2024-01-02T00:03:00Z") {
		t.Fatalf("unexpected extracted content: %s", string(data))
	}

	tempPath, err := createAtomicTempPath(filepath.Join(dir, "nested", "file.csv.gz"))
	if err != nil {
		t.Fatalf("createAtomicTempPath returned error: %v", err)
	}
	if !strings.HasSuffix(tempPath, ".gz") {
		t.Fatalf("expected gzip temp path, got %q", tempPath)
	}

	replacement := filepath.Join(dir, "replacement.csv")
	if err := os.WriteFile(replacement, []byte("new"), 0o644); err != nil {
		t.Fatalf("WriteFile returned error: %v", err)
	}
	target := filepath.Join(dir, "target.csv")
	if err := os.WriteFile(target, []byte("old"), 0o644); err != nil {
		t.Fatalf("WriteFile returned error: %v", err)
	}
	if err := replaceFile(replacement, target); err != nil {
		t.Fatalf("replaceFile returned error: %v", err)
	}
	finalData, err := os.ReadFile(target)
	if err != nil {
		t.Fatalf("ReadFile returned error: %v", err)
	}
	if string(finalData) != "new" {
		t.Fatalf("unexpected replacement data: %q", string(finalData))
	}
}

func TestFormattingAndInferenceHelpers(t *testing.T) {
	if got := inferTimeframe([]time.Duration{time.Minute, time.Minute, 5 * time.Minute}); got != "m1" {
		t.Fatalf("unexpected inferred timeframe: %q", got)
	}
	if got := inferTimeframe(nil); got != "unknown" {
		t.Fatalf("expected unknown timeframe, got %q", got)
	}
	if got := inferExpectedInterval([]time.Duration{time.Hour, time.Hour, 2 * time.Hour}); got != time.Hour {
		t.Fatalf("unexpected expected interval: %s", got)
	}
	if got := estimateMissingIntervals(5*time.Minute, time.Minute); got != 4 {
		t.Fatalf("unexpected missing interval count: %d", got)
	}
	if !IsExpectedMarketClosureGap(
		time.Date(2024, 1, 5, 21, 59, 0, 0, time.UTC),
		time.Date(2024, 1, 7, 23, 0, 0, 0, time.UTC),
		time.Minute,
	) {
		t.Fatal("expected weekend closure gap to be classified as expected")
	}
	if got := ResolveGapMarketProfile("btcusd", MarketProfileAuto); got != MarketProfileCrypto24x7 {
		t.Fatalf("expected crypto profile, got %q", got)
	}
	if got := ResolveGapMarketProfile("eurusd", MarketProfileAuto); got != MarketProfileFX24x5 {
		t.Fatalf("expected fx profile, got %q", got)
	}
	if IsExpectedGapForProfile(
		time.Date(2024, 1, 5, 21, 59, 0, 0, time.UTC),
		time.Date(2024, 1, 7, 23, 0, 0, 0, time.UTC),
		time.Minute,
		"btcusd",
		MarketProfileAuto,
	) {
		t.Fatal("expected crypto profile to keep weekend gaps suspicious")
	}
	if IsExpectedMarketClosureGap(
		time.Date(2024, 1, 2, 10, 0, 0, 0, time.UTC),
		time.Date(2024, 1, 2, 10, 5, 0, 0, time.UTC),
		time.Minute,
	) {
		t.Fatal("expected mid-session gap to stay suspicious")
	}
	if !IsExpectedGapForProfile(
		time.Date(2018, 1, 15, 18, 0, 0, 0, time.UTC),
		time.Date(2018, 1, 15, 23, 0, 0, 0, time.UTC),
		time.Minute,
		"xauusd",
		MarketProfileAuto,
	) {
		t.Fatal("expected MLK holiday afternoon closure to be classified as expected for otc symbols")
	}
	if !IsExpectedGapForProfile(
		time.Date(2024, 3, 29, 4, 0, 0, 0, time.UTC),
		time.Date(2024, 3, 29, 21, 0, 0, 0, time.UTC),
		time.Minute,
		"eurusd",
		MarketProfileAuto,
	) {
		t.Fatal("expected Good Friday daytime closure to be classified as expected for fx symbols")
	}
	if got := formatPrice(1.23456, 3); got != "1.235" {
		t.Fatalf("unexpected formatted price: %q", got)
	}
	if got := formatPrice(1.23456, 0); got != "1.23456" {
		t.Fatalf("unexpected scale-zero formatted price: %q", got)
	}
	if got := formatVolume(1234); got != "1234" {
		t.Fatalf("unexpected formatted volume: %q", got)
	}
	if got := midpoint(1, 3); got != 2 {
		t.Fatalf("unexpected midpoint: %f", got)
	}
	if got := roundToScale(1.23456, 2); got != 1.23 {
		t.Fatalf("unexpected rounded value: %f", got)
	}
	if got := roundToScale(1.23456, -1); got != 1.23456 {
		t.Fatalf("unexpected negative-scale round result: %f", got)
	}
}

func TestInspectCSVClassifiesExpectedAndSuspiciousGaps(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "gaps.csv")
	content := strings.Join([]string{
		"timestamp,open",
		"2024-01-05T21:57:00Z,1",
		"2024-01-05T21:58:00Z,2",
		"2024-01-05T21:59:00Z,3",
		"2024-01-07T23:00:00Z,4",
		"2024-01-07T23:01:00Z,5",
		"2024-01-07T23:02:00Z,6",
		"2024-01-07T23:07:00Z,7",
		"",
	}, "\n")
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("WriteFile returned error: %v", err)
	}

	stats, err := InspectCSV(path)
	if err != nil {
		t.Fatalf("InspectCSV returned error: %v", err)
	}
	if stats.ExpectedGapCount != 1 || stats.SuspiciousGapCount != 1 {
		t.Fatalf("expected one expected and one suspicious gap, got %+v", stats)
	}
	if stats.ExpectedMissingIntervals == 0 || stats.SuspiciousMissingIntervals == 0 {
		t.Fatalf("expected missing interval buckets to be populated, got %+v", stats)
	}
}

func TestInspectCSVClassifiesHolidayGapAsExpected(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "xauusd-holiday.csv")
	content := strings.Join([]string{
		"timestamp,open",
		"2018-01-15T17:59:00Z,1",
		"2018-01-15T18:00:00Z,2",
		"2018-01-15T23:00:00Z,3",
		"2018-01-15T23:01:00Z,4",
		"",
	}, "\n")
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("WriteFile returned error: %v", err)
	}

	stats, err := InspectCSVWithOptions(path, InspectOptions{Symbol: "xauusd"})
	if err != nil {
		t.Fatalf("InspectCSVWithOptions returned error: %v", err)
	}
	if stats.ExpectedGapCount != 1 {
		t.Fatalf("expected holiday gap to be expected, got %+v", stats)
	}
	if stats.SuspiciousGapCount != 0 {
		t.Fatalf("expected no suspicious holiday gaps, got %+v", stats)
	}
}

func TestFormatColumnsAndWriterOutputHelpers(t *testing.T) {
	bar := dukascopy.Bar{Time: time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC), Open: 100, High: 101, Low: 99, Close: 100.5, Volume: 2000}
	bid := dukascopy.Bar{Time: bar.Time, Open: 100.0, High: 101.0, Low: 99.0, Close: 100.3, Volume: 2000}
	ask := dukascopy.Bar{Time: bar.Time, Open: 100.2, High: 101.2, Low: 99.2, Close: 100.7, Volume: 2000}
	tick := dukascopy.Tick{Time: bar.Time, Bid: 100.1, Ask: 100.3, BidVolume: 12, AskVolume: 13}

	if got, err := formatPrimaryBarColumn("open", 3, bar); err != nil || got != "100.000" {
		t.Fatalf("unexpected primary bar column: %q %v", got, err)
	}
	if _, err := formatPrimaryBarColumn("spread", 3, bar); err == nil {
		t.Fatal("expected unsupported simple spread column error")
	}
	if got, err := formatBarColumn("mid_close", 3, bid, ask); err != nil || got != "100.5" {
		t.Fatalf("unexpected bar column: %q %v", got, err)
	}
	if _, err := formatBarColumn("wat", 3, bid, ask); err == nil {
		t.Fatal("expected unsupported bar column error")
	}
	if got, err := formatTickColumn("bid", 3, tick); err != nil || got != "100.100" {
		t.Fatalf("unexpected tick column: %q %v", got, err)
	}
	if _, err := formatTickColumn("wat", 3, tick); err == nil {
		t.Fatal("expected unsupported tick column error")
	}

	var buffer bytes.Buffer
	if err := WriteBarsToWriter(&buffer, dukascopy.Instrument{PriceScale: 3}, []string{"timestamp", "mid_close", "spread"}, nil, []dukascopy.Bar{bid}, []dukascopy.Bar{ask}); err != nil {
		t.Fatalf("WriteBarsToWriter returned error: %v", err)
	}
	if !strings.Contains(buffer.String(), "mid_close") || !strings.Contains(buffer.String(), "100.5") {
		t.Fatalf("unexpected bar writer output: %s", buffer.String())
	}

	buffer.Reset()
	if err := WriteTicksToWriter(&buffer, dukascopy.Instrument{PriceScale: 3}, []string{"timestamp", "bid"}, []dukascopy.Tick{tick}); err != nil {
		t.Fatalf("WriteTicksToWriter returned error: %v", err)
	}
	if !strings.Contains(buffer.String(), "100.100") {
		t.Fatalf("unexpected tick writer output: %s", buffer.String())
	}

	if got, err := parquetValueForColumn("timestamp", "2024-01-02T00:00:00Z"); err != nil || got.(string) == "" {
		t.Fatalf("unexpected parquet timestamp conversion: %v %v", got, err)
	}
	if _, err := parquetValueForColumn("mid_close", "bad"); err == nil {
		t.Fatal("expected invalid parquet numeric conversion error")
	}
	if !recordsEqual([]string{"a"}, []string{"a"}) || recordsEqual([]string{"a"}, []string{"b"}) {
		t.Fatal("unexpected recordsEqual behavior")
	}
	if indexOfColumn([]string{"a", "timestamp"}, "timestamp") != 1 || indexOfColumn([]string{"a"}, "timestamp") != -1 {
		t.Fatal("unexpected indexOfColumn behavior")
	}
	if parquetStringValue(7) != "7" || parquetStringValue(time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC)) == "" {
		t.Fatal("unexpected parquetStringValue conversions")
	}
	if _, ok := parquetTimestampFromRow(map[string]any{"timestamp": "bad"}); ok {
		t.Fatal("expected invalid parquet timestamp parse to fail")
	}
}

func TestOpenCSVReaderAndWritersWithGzip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "sample.csv.gz")
	_, writer, closeWriter, err := createCSVWriter(path)
	if err != nil {
		t.Fatalf("createCSVWriter returned error: %v", err)
	}
	if err := writer.Write([]string{"timestamp", "open"}); err != nil {
		t.Fatalf("writer.Write returned error: %v", err)
	}
	if err := writer.Write([]string{"2024-01-02T00:00:00Z", "100"}); err != nil {
		t.Fatalf("writer.Write returned error: %v", err)
	}
	if err := closeWriter(); err != nil {
		t.Fatalf("closeWriter returned error: %v", err)
	}

	_, reader, closeReader, err := openCSVReader(path)
	if err != nil {
		t.Fatalf("openCSVReader returned error: %v", err)
	}
	defer closeReader()
	header, err := reader.Read()
	if err != nil || len(header) != 2 {
		t.Fatalf("unexpected gzip reader header: %v %v", header, err)
	}
}
