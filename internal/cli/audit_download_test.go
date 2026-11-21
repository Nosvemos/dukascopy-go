package cli

import (
	"context"
	"errors"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/Nosvemos/dukascopy-go/internal/checkpoint"
	"github.com/Nosvemos/dukascopy-go/pkg/csvout"
	"github.com/Nosvemos/dukascopy-go/pkg/dukascopy"
)

// Three m1 bars per day on Jan 2-4 2024 = 9 rows across three day chunks.
var threeDayArgs = []string{"--symbol", "xauusd", "--timeframe", "m1", "--from", "2024-01-02T00:00:00Z", "--to", "2024-01-04T00:02:00Z"}

func TestChunkedDownloadOutputFormats(t *testing.T) {
	cases := []struct {
		name      string
		args      []string
		delimiter rune
		header    bool
		first     string
		last      string
	}{
		{name: "mt4", args: []string{"--preset", "mt4"}, delimiter: ',', header: false, first: "2024.01.02 00:00", last: "2024.01.04 00:02"},
		{name: "mt5", args: []string{"--preset", "mt5"}, delimiter: ',', header: false, first: "2024.01.02 00:00:00", last: "2024.01.04 00:02:00"},
		{name: "backtrader", args: []string{"--preset", "backtrader"}, delimiter: ',', header: true, first: "2024-01-02 00:00:00", last: "2024-01-04 00:02:00"},
		{name: "vectorbt", args: []string{"--preset", "vectorbt"}, delimiter: ',', header: true, first: "2024-01-02 00:00:00", last: "2024-01-04 00:02:00"},
		{name: "freqtrade", args: []string{"--preset", "freqtrade"}, delimiter: ',', header: true, first: "2024-01-02 00:00:00", last: "2024-01-04 00:02:00"},
		{name: "lean", args: []string{"--preset", "lean"}, delimiter: ',', header: false, first: "20240102 00:00", last: "20240104 00:02"},
		{name: "ninjatrader", args: []string{"--preset", "ninjatrader"}, delimiter: ';', header: false, first: "20240102 000000", last: "20240104 000200"},
		{name: "nautilus", args: []string{"--preset", "nautilus"}, delimiter: ',', header: true, first: "2024-01-02T00:00:00Z", last: "2024-01-04T00:02:00Z"},
		{name: "no-header", args: []string{"--no-header"}, delimiter: ',', header: false, first: "2024-01-02T00:00:00Z", last: "2024-01-04T00:02:00Z"},
		{name: "custom format and timezone", args: []string{"--timestamp-format", "2006/01/02 15:04", "--timezone", "Europe/Athens"}, delimiter: ',', header: true, first: "2024/01/02 02:00", last: "2024/01/04 02:02"},
		{name: "custom delimiter", args: []string{"--csv-delimiter", "|"}, delimiter: '|', header: true, first: "2024-01-02T00:00:00Z", last: "2024-01-04T00:02:00Z"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fake := newFakeDukascopy(t)
			dir := t.TempDir()
			output := filepath.Join(dir, "out.csv")
			args := append(append([]string{}, threeDayArgs...), "--output", output, "--cache-dir", filepath.Join(dir, "cache"))
			args = append(args, tc.args...)
			if logs, err := runTestDownload(t, fake, args...); err != nil {
				t.Fatalf("download: %v\n%s", err, logs)
			}

			rows := readDelimited(t, output, tc.delimiter)
			if tc.header {
				if rows[0][0] != "timestamp" {
					t.Fatalf("expected a header row, got %q", rows[0])
				}
				rows = rows[1:]
			} else if rows[0][0] == "timestamp" {
				t.Fatalf("expected no header row, got %q", rows[0])
			}
			if len(rows) != 9 {
				t.Fatalf("expected 9 data rows, got %d: %q", len(rows), rows)
			}
			if rows[0][0] != tc.first || rows[len(rows)-1][0] != tc.last {
				t.Fatalf("first/last timestamps = %q/%q, want %q/%q", rows[0][0], rows[len(rows)-1][0], tc.first, tc.last)
			}
			for _, row := range rows {
				if len(row) != 6 {
					t.Fatalf("expected 6 simple columns, got %q", row)
				}
			}
		})
	}
}

func TestChunkedDownloadJSONLOutputIsJSON(t *testing.T) {
	fake := newFakeDukascopy(t)
	dir := t.TempDir()
	output := filepath.Join(dir, "out.jsonl")
	args := append(append([]string{}, threeDayArgs...), "--output", output, "--cache-dir", filepath.Join(dir, "cache"), "--timestamp-format", "2006-01-02 15:04")
	if logs, err := runTestDownload(t, fake, args...); err != nil {
		t.Fatalf("download: %v\n%s", err, logs)
	}
	data, err := os.ReadFile(output)
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	if len(lines) != 9 {
		t.Fatalf("expected 9 JSON lines, got %d:\n%s", len(lines), data)
	}
	if !strings.HasPrefix(lines[0], `{"timestamp":"2024-01-02 00:00","open":`) {
		t.Fatalf("unexpected first JSON line %q", lines[0])
	}
}

func TestMergeChunksKeepsExistingOutputOnFailure(t *testing.T) {
	dir := t.TempDir()
	output := filepath.Join(dir, "out.csv")
	if err := os.WriteFile(output, []byte("previous good data\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	header := "timestamp,open"
	good := writeChunk(t, dir, "a.csv", header, "2024-01-02T00:00:00Z,1", "2024-01-02T00:01:00Z,2")
	bad := writeChunk(t, dir, "b.csv", header, "2024-01-02T00:02:00Z") // short row

	_, err := mergeChunks(context.Background(), nil, mergeOptions{
		outputPath:    output,
		partPaths:     []string{good, bad},
		columns:       []string{"timestamp", "open"},
		from:          time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC),
		to:            time.Date(2024, 1, 3, 0, 0, 0, 0, time.UTC),
		partitionMode: partitionDay,
		strictTimes:   true,
	})
	if err == nil || !strings.Contains(err.Error(), "malformed row in chunk") || !strings.Contains(err.Error(), "b.csv") {
		t.Fatalf("expected malformed row error naming the chunk, got %v", err)
	}
	data, _ := os.ReadFile(output)
	if string(data) != "previous good data\n" {
		t.Fatalf("existing output was modified: %q", data)
	}
	for _, name := range listDir(t, dir) {
		if strings.Contains(name, ".merge-") {
			t.Fatalf("temp file left behind: %s", name)
		}
		if strings.HasPrefix(name, "out_") {
			t.Fatalf("partition output created on failure: %s", name)
		}
	}
}

func TestMergeChunksWritesPartitionsAtomically(t *testing.T) {
	dir := t.TempDir()
	output := filepath.Join(dir, "out.csv")
	header := "timestamp,open"
	a := writeChunk(t, dir, "a.csv", header, "2024-01-02T00:00:00Z,1")
	b := writeChunk(t, dir, "b.csv", header, "2024-01-03T00:00:00Z,2")
	rows, err := mergeChunks(context.Background(), nil, mergeOptions{
		outputPath:    output,
		partPaths:     []string{a, b},
		columns:       []string{"timestamp", "open"},
		from:          time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC),
		to:            time.Date(2024, 1, 4, 0, 0, 0, 0, time.UTC),
		partitionMode: partitionDay,
		strictTimes:   true,
	})
	if err != nil || rows != 2 {
		t.Fatalf("merge: rows=%d err=%v", rows, err)
	}
	for _, name := range []string{"out.csv", "out_20240102.csv", "out_20240103.csv"} {
		if _, err := os.Stat(filepath.Join(dir, name)); err != nil {
			t.Fatalf("missing %s: %v", name, err)
		}
	}
	for _, name := range listDir(t, dir) {
		if strings.Contains(name, ".merge-") {
			t.Fatalf("temp file left behind: %s", name)
		}
	}
}

func TestMergeChunksRejectsBadChunks(t *testing.T) {
	dir := t.TempDir()
	columns := []string{"timestamp", "open"}
	opts := func(paths ...string) mergeOptions {
		return mergeOptions{
			outputPath: filepath.Join(dir, "out.csv"),
			partPaths:  paths,
			columns:    columns,
			from:       time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
			to:         time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
		}
	}

	missing := filepath.Join(dir, "missing.csv")
	if _, err := mergeChunks(context.Background(), nil, opts(missing)); err == nil || !strings.Contains(err.Error(), "missing.csv") {
		t.Fatalf("expected an error naming the missing chunk, got %v", err)
	}

	wrongHeader := writeChunk(t, dir, "wrong.csv", "timestamp,close", "2024-01-02T00:00:00Z,1")
	if _, err := mergeChunks(context.Background(), nil, opts(wrongHeader)); err == nil || !strings.Contains(err.Error(), "header") {
		t.Fatalf("expected a header mismatch error, got %v", err)
	}

	// A chunk written in a user format (old cache) is not silently misread.
	userFormat := writeChunk(t, dir, "user.csv", "timestamp,open", "2024.01.02 00:00,1")
	if _, err := mergeChunks(context.Background(), nil, opts(userFormat)); err == nil || !strings.Contains(err.Error(), "malformed row in chunk") {
		t.Fatalf("expected a malformed timestamp error, got %v", err)
	}

	// Duplicate bar timestamps across chunks are refused for time bars.
	first := writeChunk(t, dir, "c1.csv", "timestamp,open", "2024-12-30T00:00:00Z,1")
	second := writeChunk(t, dir, "c2.csv", "timestamp,open", "2024-12-30T00:00:00Z,2")
	strict := opts(first, second)
	strict.strictTimes = true
	if _, err := mergeChunks(context.Background(), nil, strict); err == nil || !strings.Contains(err.Error(), "strictly increasing") {
		t.Fatalf("expected a duplicate timestamp error, got %v", err)
	}
	// Equal tick timestamps are legitimate.
	ticks := opts(first, second)
	if rows, err := mergeChunks(context.Background(), nil, ticks); err != nil || rows != 2 {
		t.Fatalf("ticks with equal timestamps: rows=%d err=%v", rows, err)
	}
}

func TestMergeChunksHonoursCancellation(t *testing.T) {
	dir := t.TempDir()
	output := filepath.Join(dir, "out.csv")
	chunk := writeChunk(t, dir, "a.csv", "timestamp,open", "2024-01-02T00:00:00Z,1")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := mergeChunks(ctx, nil, mergeOptions{
		outputPath: output,
		partPaths:  []string{chunk},
		columns:    []string{"timestamp", "open"},
		from:       time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
		to:         time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
	if _, statErr := os.Stat(output); !os.IsNotExist(statErr) {
		t.Fatalf("output must not be created after cancellation: %v", statErr)
	}
	for _, name := range listDir(t, dir) {
		if strings.Contains(name, ".merge-") {
			t.Fatalf("temp file left behind: %s", name)
		}
	}
}

func TestBuildChunkRangesAlignsWeeklyChunks(t *testing.T) {
	from := time.Date(2023, 11, 15, 0, 0, 0, 0, time.UTC)
	to := time.Date(2025, 2, 1, 0, 0, 0, 0, time.UTC)
	chunks, err := buildChunkRanges(from, to, dukascopy.GranularityW1)
	if err != nil {
		t.Fatal(err)
	}
	if len(chunks) != 3 {
		t.Fatalf("expected 3 chunks, got %d: %+v", len(chunks), chunks)
	}
	for i := 1; i < len(chunks); i++ {
		boundary := chunks[i].Start
		if boundary.Weekday() != time.Monday || !boundary.Equal(chunks[i-1].End) {
			t.Fatalf("chunk boundary %s is not a Monday week start", boundary)
		}
	}
	if !chunks[0].Start.Equal(from) || !chunks[len(chunks)-1].End.Equal(to) {
		t.Fatalf("chunks do not cover the range: %+v", chunks)
	}
}

func TestChunkedDownloadWeeklyBarsAreUniqueAcrossYearChunks(t *testing.T) {
	fake := newFakeDukascopy(t)
	dir := t.TempDir()
	output := filepath.Join(dir, "w1.csv")
	if logs, err := runTestDownload(t, fake, "--symbol", "xauusd", "--timeframe", "w1", "--from", "2024-12-02T00:00:00Z", "--to", "2025-01-26T00:00:00Z", "--output", output, "--cache-dir", filepath.Join(dir, "cache")); err != nil {
		t.Fatalf("download: %v\n%s", err, logs)
	}
	rows := readDelimited(t, output, ',')[1:]
	seen := map[string]bool{}
	var previous time.Time
	for _, row := range rows {
		ts, err := time.Parse(time.RFC3339Nano, row[0])
		if err != nil {
			t.Fatal(err)
		}
		if seen[row[0]] || (!previous.IsZero() && !ts.After(previous)) {
			t.Fatalf("duplicate or unordered weekly bar %s in %q", row[0], rows)
		}
		seen[row[0]] = true
		previous = ts
	}
	if !seen["2024-12-30T00:00:00Z"] {
		t.Fatalf("expected the week straddling the year boundary once, got %q", rows)
	}
	// The straddling week holds all 7 daily bars (Dec 30 - Jan 5).
	for _, row := range rows {
		if row[0] == "2024-12-30T00:00:00Z" && row[5] != "7000" {
			t.Fatalf("straddling week volume = %s, want 7000 (7 daily bars)", row[5])
		}
	}
}

func TestChunkCacheIsKeyedBySettings(t *testing.T) {
	fake := newFakeDukascopy(t)
	dir := t.TempDir()
	cache := filepath.Join(dir, "cache")
	output := filepath.Join(dir, "out.csv")
	base := append(append([]string{}, threeDayArgs...), "--output", output, "--cache-dir", cache, "--keep-cache")

	if logs, err := runTestDownload(t, fake, append(base, "--simple")...); err != nil {
		t.Fatalf("simple download: %v\n%s", err, logs)
	}
	if logs, err := runTestDownload(t, fake, append(base, "--full")...); err != nil {
		t.Fatalf("full download: %v\n%s", err, logs)
	}
	rows := readDelimited(t, output, ',')
	if strings.Join(rows[0], ",") != strings.Join(csvout.BarColumnsForProfile(csvout.ProfileFull), ",") || len(rows) != 10 {
		t.Fatalf("full rerun reused simple chunks: header=%q rows=%d", rows[0], len(rows))
	}
	for _, row := range rows {
		if len(row) != len(rows[0]) {
			t.Fatalf("full rerun reused simple chunks: row %q", row)
		}
	}

	// The same settings reuse the cache: no new candle requests.
	before := fake.count("/v1/candles")
	if logs, err := runTestDownload(t, fake, append(base, "--full")...); err != nil {
		t.Fatalf("cached download: %v\n%s", err, logs)
	}
	if after := fake.count("/v1/candles"); after != before {
		t.Fatalf("expected cached chunks to be reused, candle requests %d -> %d", before, after)
	}

	settings := chunkCacheSettings{Symbol: "xauusd", Timeframe: "m1", Side: "bid", Columns: []string{"timestamp", "open"}}
	key := chunkCacheKey(settings)
	for _, mutate := range []func(*chunkCacheSettings){
		func(s *chunkCacheSettings) { s.Columns = []string{"timestamp", "close"} },
		func(s *chunkCacheSettings) { s.CleanOutliers = "auto" },
		func(s *chunkCacheSettings) { s.Engine = "datafeed" },
		func(s *chunkCacheSettings) { s.BarType = "tick" },
		func(s *chunkCacheSettings) { s.FillGaps = "forward" },
	} {
		changed := settings
		mutate(&changed)
		if chunkCacheKey(changed) == key {
			t.Fatalf("cache key did not change for %+v", changed)
		}
	}
}

func TestFeaturesComputedOnceOverMergedSeries(t *testing.T) {
	fake := newFakeDukascopy(t)
	dir := t.TempDir()
	plain := filepath.Join(dir, "plain.csv")
	withFeatures := filepath.Join(dir, "features.csv")
	cache := filepath.Join(dir, "cache")
	common := []string{"--timestamp-format", "2006-01-02 15:04", "--csv-delimiter", ";"}

	if logs, err := runTestDownload(t, fake, append(append(append([]string{}, threeDayArgs...), "--output", plain, "--cache-dir", cache), common...)...); err != nil {
		t.Fatalf("plain download: %v\n%s", err, logs)
	}
	if logs, err := runTestDownload(t, fake, append(append(append([]string{}, threeDayArgs...), "--output", withFeatures, "--cache-dir", cache, "--features", "returns:log"), common...)...); err != nil {
		t.Fatalf("features download: %v\n%s", err, logs)
	}

	plainRows := readDelimited(t, plain, ';')
	rows := readDelimited(t, withFeatures, ';')
	if len(rows) != 10 || len(rows[0]) != 7 {
		t.Fatalf("expected header + 9 rows with one feature column, got %d rows, header %q", len(rows), rows[0])
	}
	for i, row := range rows {
		if len(row) != len(rows[0]) {
			t.Fatalf("row %d has %d fields, header has %d", i, len(row), len(rows[0]))
		}
		// Base columns (format, timezone, price scale) match the plain output.
		if strings.Join(row[:6], ";") != strings.Join(plainRows[i], ";") {
			t.Fatalf("row %d base columns %q differ from plain output %q", i, row[:6], plainRows[i])
		}
	}
	// Warm-up happens once: only the first data row lacks a return, the first
	// rows of the second and third chunks have one.
	for i := 2; i < len(rows); i++ {
		value, err := strconv.ParseFloat(rows[i][6], 64)
		if err != nil || math.IsNaN(value) {
			t.Fatalf("row %d feature value %q is not a number (warm-up restarted per chunk?)", i, rows[i][6])
		}
	}
}

func TestFeaturesRejectedForUnsupportedModes(t *testing.T) {
	fake := newFakeDukascopy(t)
	dir := t.TempDir()
	output := filepath.Join(dir, "out.csv")
	cases := map[string][]string{
		"partition": {"--partition", "day"},
		"tick":      {"--timeframe", "tick"},
		"resume":    {"--resume"},
		"full":      {"--full"},
	}
	for name, extra := range cases {
		args := append(append(append([]string{}, threeDayArgs...), "--output", output, "--cache-dir", filepath.Join(dir, "cache"), "--features", "rsi:14"), extra...)
		_, err := runTestDownload(t, fake, args...)
		if err == nil || !strings.Contains(err.Error(), "--features") {
			t.Fatalf("%s: expected a --features error, got %v", name, err)
		}
	}
	if fake.total.Load() != 0 {
		t.Fatalf("validation must happen before any request, got %d requests", fake.total.Load())
	}
}

func TestCustomBarSamplerMatchesWholeStreamSampling(t *testing.T) {
	rng := rand.New(rand.NewSource(7))
	ticks := make([]dukascopy.Tick, 500)
	start := time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC)
	for i := range ticks {
		ticks[i] = dukascopy.Tick{
			Time:      start.Add(time.Duration(i) * time.Second),
			Bid:       100 + rng.Float64(),
			Ask:       100.2 + rng.Float64(),
			BidVolume: 1 + rng.Float64()*3,
			AskVolume: 1 + rng.Float64()*3,
		}
	}
	for _, tc := range []struct {
		barType dukascopy.BarType
		size    float64
		side    dukascopy.PriceSide
	}{
		{dukascopy.BarTypeTick, 7, dukascopy.PriceSideBid},
		{dukascopy.BarTypeVolume, 25, dukascopy.PriceSideAsk},
		{dukascopy.BarTypeDollar, 2500, dukascopy.PriceSideBid},
	} {
		want, err := dukascopy.SampleTicksToCustomBars(ticks, tc.barType, tc.size, tc.side)
		if err != nil {
			t.Fatal(err)
		}
		sampler, err := newCustomBarSampler(tc.barType, tc.size, tc.side)
		if err != nil {
			t.Fatal(err)
		}
		var got []dukascopy.Bar
		for _, tick := range ticks {
			if bar, ok := sampler.add(tick); ok {
				got = append(got, bar)
			}
		}
		if bar, ok := sampler.flush(); ok {
			got = append(got, bar)
		}
		if len(got) != len(want) {
			t.Fatalf("%s: got %d bars, want %d", tc.barType, len(got), len(want))
		}
		for i := range want {
			if got[i] != want[i] {
				t.Fatalf("%s bar %d: got %+v, want %+v", tc.barType, i, got[i], want[i])
			}
		}
	}
	if _, err := newCustomBarSampler(dukascopy.BarTypeTick, 5, "mid"); err == nil {
		t.Fatal("expected an invalid side error")
	}
}

func TestCustomBarsAreNotCutAtChunkBoundaries(t *testing.T) {
	fake := newFakeDukascopy(t)
	fake.ticksPerHour = 3 // 3 ticks per hourly chunk, volume 1 each
	dir := t.TempDir()
	output := filepath.Join(dir, "bars.csv")
	logs, err := runTestDownload(t, fake, "--symbol", "xauusd", "--timeframe", "tick", "--bar-type", "tick", "--bar-size", "4",
		"--custom-columns", "timestamp,open,close,volume",
		"--from", "2024-01-02T00:00:00Z", "--to", "2024-01-02T02:59:59Z", "--output", output, "--cache-dir", filepath.Join(dir, "cache"))
	if err != nil {
		t.Fatalf("download: %v\n%s", err, logs)
	}
	rows := readDelimited(t, output, ',')
	if strings.Join(rows[0], ",") != "timestamp,open,close,volume" {
		t.Fatalf("custom bar columns were parsed with tick semantics: %q", rows[0])
	}
	rows = rows[1:]
	// 9 ticks in three hourly chunks -> bars of 4, 4 and a final 1.
	if len(rows) != 3 {
		t.Fatalf("expected 3 bars, got %q", rows)
	}
	for i, row := range rows {
		want := "4"
		if i == len(rows)-1 {
			want = "1"
		}
		if row[3] != want {
			t.Fatalf("bar %d volume (tick count) = %s, want %s: %q", i, row[3], want, rows)
		}
	}
}

func TestPartitionManifestStoresAuditedRows(t *testing.T) {
	fake := newFakeDukascopy(t)
	// A gap at minute 2: --fill-gaps forward writes 4 rows from 3 bars.
	fake.minuteOffsets = func(time.Time) []int64 { return []int64{0, 1, 3} }
	dir := t.TempDir()
	output := filepath.Join(dir, "out.csv")
	args := []string{"--symbol", "xauusd", "--timeframe", "m1", "--from", "2024-01-02T00:00:00Z", "--to", "2024-01-03T00:03:00Z",
		"--output", output, "--partition", "day", "--fill-gaps", "forward"}
	if logs, err := runTestDownload(t, fake, args...); err != nil {
		t.Fatalf("download: %v\n%s", err, logs)
	}
	manifest, err := checkpoint.Load(checkpoint.DefaultManifestPath(output))
	if err != nil {
		t.Fatal(err)
	}
	for _, part := range manifest.Parts {
		audit, err := auditChunkFile(filepath.Join(manifest.PartsDir, part.File))
		if err != nil {
			t.Fatal(err)
		}
		if part.Rows != audit.Rows {
			t.Fatalf("part %s stores %d rows, file holds %d", part.ID, part.Rows, audit.Rows)
		}
	}

	before := fake.count("/v1/candles")
	if logs, err := runTestDownload(t, fake, args...); err != nil {
		t.Fatalf("rerun: %v\n%s", err, logs)
	}
	if after := fake.count("/v1/candles"); after != before {
		t.Fatalf("rerun re-downloaded completed parts: candle requests %d -> %d", before, after)
	}
}

func TestDownloadValidation(t *testing.T) {
	fake := newFakeDukascopy(t)
	dir := t.TempDir()
	output := filepath.Join(dir, "out.csv")
	future := time.Now().UTC().Add(48 * time.Hour).Format(time.RFC3339)
	cases := []struct {
		name string
		args []string
		want string
	}{
		{"resume with partition", append(append([]string{}, threeDayArgs...), "--output", output, "--resume", "--partition", "day"), "--resume cannot be combined with --partition"},
		{"multi-symbol without output", []string{"--symbol", "xauusd,eurusd", "--last", "1d"}, "--output is required"},
		{"multi-symbol to stdout", []string{"--symbol", "xauusd,eurusd", "--last", "1d", "--output", "-"}, "cannot be used with more than one symbol"},
		{"cloud with partition", append(append([]string{}, threeDayArgs...), "--output", "s3://bucket/key.csv", "--partition", "day"), "cloud"},
		{"bad fill-gaps", append(append([]string{}, threeDayArgs...), "--output", output, "--fill-gaps", "backward"), "--fill-gaps"},
		{"parallelism too high", append(append([]string{}, threeDayArgs...), "--output", output, "--parallelism", "33"), "--parallelism must be between 1 and 32"},
		{"from with last", []string{"--symbol", "xauusd", "--from", "2024-01-02", "--last", "1d", "--output", output}, "--from and --last cannot be used together"},
		{"future from", []string{"--symbol", "xauusd", "--from", future, "--to", future, "--output", output}, "is in the future"},
		{"custom bars with bid/ask columns", append(append([]string{}, threeDayArgs...), "--output", output, "--bar-type", "tick", "--bar-size", "5", "--full"), "--bar-type tick"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := runTestDownload(t, fake, tc.args...)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("expected error containing %q, got %v", tc.want, err)
			}
		})
	}
	if fake.total.Load() != 0 {
		t.Fatalf("validation errors must not reach the network, got %d requests", fake.total.Load())
	}
}

func TestManifestSaverThrottlesAndReportsErrors(t *testing.T) {
	manifest := checkpoint.Manifest{Parts: []checkpoint.ManifestPart{{ID: "a"}, {ID: "b"}}}
	saves := 0
	var failWith error
	now := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	saver := newManifestSaver("unused", &manifest)
	saver.now = func() time.Time { return now }
	saver.save = func(string, checkpoint.Manifest) error {
		saves++
		return failWith
	}
	if err := saver.saveNow(); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 1000; i++ {
		saver.part("a").Rows = i
		if err := saver.changed(); err != nil {
			t.Fatal(err)
		}
	}
	if saves != 11 { // initial + every 100 updates
		t.Fatalf("expected 11 saves for 1000 updates at one instant, got %d", saves)
	}
	if err := saver.flush(); err != nil || saves != 11 {
		t.Fatalf("flush with nothing dirty must not save: saves=%d err=%v", saves, err)
	}
	saver.part("b").Rows = 1
	now = now.Add(3 * time.Second)
	if err := saver.changed(); err != nil || saves != 12 {
		t.Fatalf("expected a time-based save: saves=%d err=%v", saves, err)
	}
	failWith = errors.New("disk full")
	saver.part("b").Rows = 2
	if err := saver.changed(); err != nil { // throttled: not saved yet
		t.Fatal(err)
	}
	if err := saver.flush(); err == nil || !strings.Contains(err.Error(), "disk full") {
		t.Fatalf("expected the save error to be returned, got %v", err)
	}
	if saver.part("missing") != nil {
		t.Fatal("unknown part id must return nil")
	}
}
