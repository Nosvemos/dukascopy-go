package csvout

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Nosvemos/dukascopy-go/internal/dukascopy"
)

func TestWriterAndFormatterErrorCoverage(t *testing.T) {
	instrument := dukascopy.Instrument{PriceScale: 3}
	bar := dukascopy.Bar{Time: time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC), Open: 1}
	tick := dukascopy.Tick{Time: bar.Time, Bid: 1, Ask: 2}

	if err := WriteBarsToWriter(&filepathErrorWriter{}, instrument, []string{"weird"}, []dukascopy.Bar{bar}, nil, nil); err == nil {
		t.Fatal("expected WriteBarsToWriter primary format error")
	}
	if err := WriteBarsToWriter(&filepathErrorWriter{}, instrument, []string{"mid_open", "weird"}, nil, []dukascopy.Bar{bar}, []dukascopy.Bar{bar}); err == nil {
		t.Fatal("expected WriteBarsToWriter bid/ask format error")
	}
	if err := WriteTicksToWriter(&filepathErrorWriter{}, instrument, []string{"weird"}, []dukascopy.Tick{tick}); err == nil {
		t.Fatal("expected WriteTicksToWriter format error")
	}

	if _, err := formatPrimaryBarColumn("weird", 3, bar); err == nil {
		t.Fatal("expected formatPrimaryBarColumn unknown column error")
	}
	if _, err := formatBarColumn("weird", 3, bar, bar); err == nil {
		t.Fatal("expected formatBarColumn unknown column error")
	}
	if _, err := formatTickColumn("weird", 3, tick); err == nil {
		t.Fatal("expected formatTickColumn unknown column error")
	}

	if _, err := parseColumns("timestamp,weird", map[string]struct{}{"timestamp": {}}); err == nil {
		t.Fatal("expected parseColumns unknown column error")
	}
	if !recordsEqual(nil, nil) {
		t.Fatal("expected nil records to compare equal")
	}
	if recordsEqual([]string{"a"}, []string{"a", "b"}) {
		t.Fatal("expected length mismatch records to compare unequal")
	}
}

type filepathErrorWriter struct{}

func (*filepathErrorWriter) Write(p []byte) (int, error) {
	return len(p), nil
}

func TestCreateCSVWriterCloseErrorBranches(t *testing.T) {
	dir := t.TempDir()

	plainPath := filepath.Join(dir, "plain.csv")
	plainFile, plainWriter, plainClose, err := createCSVWriter(plainPath)
	if err != nil {
		t.Fatalf("createCSVWriter plain returned error: %v", err)
	}
	if err := plainWriter.Write([]string{"timestamp", "open"}); err != nil {
		t.Fatalf("plain writer.Write returned error: %v", err)
	}
	if err := plainFile.Close(); err != nil {
		t.Fatalf("plain file.Close returned error: %v", err)
	}
	if err := plainClose(); err == nil {
		t.Fatal("expected plain close writer error after manual close")
	}

	gzipPath := filepath.Join(dir, "gzip.csv.gz")
	gzipFile, gzipWriter, gzipClose, err := createCSVWriter(gzipPath)
	if err != nil {
		t.Fatalf("createCSVWriter gzip returned error: %v", err)
	}
	if err := gzipWriter.Write([]string{"timestamp", "open"}); err != nil {
		t.Fatalf("gzip writer.Write returned error: %v", err)
	}
	if err := gzipFile.Close(); err != nil {
		t.Fatalf("gzip file.Close returned error: %v", err)
	}
	if err := gzipClose(); err == nil {
		t.Fatal("expected gzip close writer error after manual close")
	}

	badDir := filepath.Join(dir, "bad")
	if err := os.WriteFile(badDir, []byte("x"), 0o644); err != nil {
		t.Fatalf("WriteFile returned error: %v", err)
	}
	if _, err := createAtomicTempPath(filepath.Join(badDir, "x.csv")); err == nil {
		t.Fatal("expected createAtomicTempPath error with file parent")
	}
}
