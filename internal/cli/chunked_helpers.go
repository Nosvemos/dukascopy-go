package cli

import (
	"bufio"
	"crypto/sha256"
	"encoding/csv"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/Nosvemos/dukascopy-go/internal/checkpoint"
	"github.com/Nosvemos/dukascopy-go/pkg/csvout"
	"github.com/Nosvemos/dukascopy-go/pkg/dukascopy"
)

// auditChunkFile counts the data rows of a chunk file (internal format:
// comma-delimited with a header) and hashes it. Unlike csvout.AuditCSV it
// does not depend on the user's --csv-delimiter.
func auditChunkFile(path string) (csvout.FileAudit, error) {
	file, err := os.Open(path)
	if err != nil {
		return csvout.FileAudit{}, err
	}
	defer file.Close()

	hasher := sha256.New()
	reader := csv.NewReader(bufio.NewReaderSize(io.TeeReader(file, hasher), 64*1024))
	reader.FieldsPerRecord = -1
	reader.ReuseRecord = true
	rows := -1 // the header is not a data row
	for {
		_, err := reader.Read()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return csvout.FileAudit{}, err
		}
		rows++
	}
	if rows < 0 {
		rows = 0
	}
	// Drain anything the CSV reader did not consume so the hash covers the file.
	if _, err := io.Copy(hasher, file); err != nil {
		return csvout.FileAudit{}, err
	}
	info, err := file.Stat()
	if err != nil {
		return csvout.FileAudit{}, err
	}
	return csvout.FileAudit{Rows: rows, Bytes: info.Size(), SHA256: hex.EncodeToString(hasher.Sum(nil))}, nil
}

// chunkHeaderMatches reports whether a cached chunk is in the internal format:
// the expected header and, if it has rows, a first row of the right width
// with an RFC3339 timestamp. Chunks written by older versions in the user's
// output format fail the check and are downloaded again.
func chunkHeaderMatches(path string, columns []string) bool {
	file, err := os.Open(path)
	if err != nil {
		return false
	}
	defer file.Close()
	reader := csv.NewReader(bufio.NewReader(file))
	reader.FieldsPerRecord = -1
	header, err := reader.Read()
	if err != nil || !csvout.HeadersMatch(columns, header) {
		return false
	}
	first, err := reader.Read()
	if errors.Is(err, io.EOF) {
		return true
	}
	if err != nil || len(first) != len(columns) {
		return false
	}
	tsIndex := indexOfColumn(columns, "timestamp")
	if tsIndex < 0 {
		return false
	}
	_, err = time.Parse(chunkTimestampLayout, first[tsIndex])
	return err == nil
}

// chunkMode returns the chunk size used for a timeframe.
func chunkModeForGranularity(granularity dukascopy.Granularity) string {
	switch dukascopy.NormalizeGranularity(granularity) {
	case dukascopy.GranularityTick:
		return partitionHour
	case dukascopy.GranularityH1, dukascopy.GranularityH4:
		return partitionMonth
	case dukascopy.GranularityD1, dukascopy.GranularityW1, dukascopy.GranularityMN1:
		return partitionYear
	default:
		return partitionDay
	}
}

// buildChunkRanges splits [from, to) into download chunks. Every chunk
// boundary is also a bucket boundary of the timeframe, so no bar bucket is
// split between two chunks (which would emit two partial bars with the same
// timestamp). Only W1 needs special handling: its year chunks end on the
// Monday that starts the week containing January 1st instead of on
// January 1st itself.
func buildChunkRanges(from time.Time, to time.Time, granularity dukascopy.Granularity) ([]downloadPartition, error) {
	mode := chunkModeForGranularity(granularity)
	if dukascopy.NormalizeGranularity(granularity) != dukascopy.GranularityW1 {
		return buildPartitions(from, to, mode)
	}
	if !from.Before(to) {
		return nil, errors.New("partition range must be non-empty")
	}
	current := from.UTC()
	to = to.UTC()
	var chunks []downloadPartition
	for current.Before(to) {
		next := weekStartForPartition(time.Date(current.Year()+1, 1, 1, 0, 0, 0, 0, time.UTC))
		if !next.After(current) {
			next = weekStartForPartition(time.Date(current.Year()+2, 1, 1, 0, 0, 0, 0, time.UTC))
		}
		end := next
		if end.After(to) {
			end = to
		}
		chunks = append(chunks, downloadPartition{
			ID:    partitionID(current, end),
			Start: current,
			End:   end,
			File:  partitionFileName(current, end),
		})
		current = end
	}
	return chunks, nil
}

// chunkCacheSettings lists every setting that changes the content of a cached
// chunk. Presentation settings (timestamp format, time zone, delimiter,
// header) do not: chunks always use the internal format.
type chunkCacheSettings struct {
	Symbol        string
	Timeframe     string
	Side          string
	ResultKind    string
	Columns       []string
	BarType       string
	CleanOutliers string
	Engine        string
	FillGaps      string
}

// chunkCacheKey returns a short stable hash of settings.
func chunkCacheKey(settings chunkCacheSettings) string {
	hasher := sha256.New()
	fmt.Fprintf(hasher, "v2|%s|%s|%s|%s|%s|%s|%s|%s|%s",
		strings.ToLower(strings.TrimSpace(settings.Symbol)),
		strings.ToLower(settings.Timeframe),
		strings.ToLower(settings.Side),
		settings.ResultKind,
		strings.Join(settings.Columns, ","),
		settings.BarType,
		settings.CleanOutliers,
		settings.Engine,
		settings.FillGaps,
	)
	return hex.EncodeToString(hasher.Sum(nil))[:12]
}

// manifestSaver throttles checkpoint manifest writes: a run with thousands of
// parts would otherwise rewrite the whole manifest after every part
// (O(parts²) I/O). It saves when forced, after saveEvery dirty updates, or
// when saveInterval has passed since the last save. Parts are indexed by ID.
type manifestSaver struct {
	path         string
	manifest     *checkpoint.Manifest
	index        map[string]*checkpoint.ManifestPart
	dirty        int
	lastSave     time.Time
	saveEvery    int
	saveInterval time.Duration
	now          func() time.Time
	save         func(string, checkpoint.Manifest) error
}

func newManifestSaver(path string, manifest *checkpoint.Manifest) *manifestSaver {
	saver := &manifestSaver{
		path:         path,
		manifest:     manifest,
		saveEvery:    100,
		saveInterval: 2 * time.Second,
		now:          time.Now,
		save:         checkpoint.Save,
	}
	saver.reindex()
	return saver
}

// reindex rebuilds the part index; call it if manifest.Parts is replaced.
func (s *manifestSaver) reindex() {
	s.index = make(map[string]*checkpoint.ManifestPart, len(s.manifest.Parts))
	for i := range s.manifest.Parts {
		s.index[s.manifest.Parts[i].ID] = &s.manifest.Parts[i]
	}
}

func (s *manifestSaver) part(id string) *checkpoint.ManifestPart {
	return s.index[id]
}

// changed records an update and saves if the throttle allows.
func (s *manifestSaver) changed() error {
	s.dirty++
	if s.dirty >= s.saveEvery || s.now().Sub(s.lastSave) >= s.saveInterval {
		return s.flush()
	}
	return nil
}

// flush saves now if anything changed since the last save.
func (s *manifestSaver) flush() error {
	if s.dirty == 0 && !s.lastSave.IsZero() {
		return nil
	}
	return s.saveNow()
}

// saveNow saves unconditionally.
func (s *manifestSaver) saveNow() error {
	if err := s.save(s.path, *s.manifest); err != nil {
		return fmt.Errorf("save checkpoint manifest %s: %w", s.path, err)
	}
	s.dirty = 0
	s.lastSave = s.now()
	return nil
}

func getPartitionKey(t time.Time, mode string) string {
	t = t.UTC()
	switch mode {
	case partitionHour:
		return t.Format("20060102T15")
	case partitionDay:
		return t.Format("20060102")
	case partitionWeek:
		year, week := t.ISOWeek()
		return fmt.Sprintf("%04dW%02d", year, week)
	case partitionMonth:
		return t.Format("200601")
	case partitionYear:
		return t.Format("2006")
	default:
		return ""
	}
}

func getPartitionOutputPath(outputPath string, key string) string {
	ext := filepath.Ext(outputPath)
	base := strings.TrimSuffix(outputPath, ext)
	if ext == ".gz" && strings.HasSuffix(strings.ToLower(base), ".csv") {
		ext = ".csv.gz"
		base = strings.TrimSuffix(base, ".csv")
	}
	return fmt.Sprintf("%s_%s%s", base, key, ext)
}

func parquetValueForColumn(column string, value string) (any, error) {
	if strings.EqualFold(strings.TrimSpace(column), "timestamp") {
		return value, nil
	}
	if strings.EqualFold(strings.TrimSpace(column), "tick_count") {
		number, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("parse parquet tick count for column %q: %w", column, err)
		}
		return number, nil
	}
	number, err := strconv.ParseFloat(value, 64)
	if err != nil {
		return nil, fmt.Errorf("parse parquet numeric value for column %q: %w", column, err)
	}
	return number, nil
}

func indexOfColumn(columns []string, name string) int {
	for i, col := range columns {
		if strings.EqualFold(strings.TrimSpace(col), name) {
			return i
		}
	}
	return -1
}

func containsString(values []string, wanted string) bool {
	for _, value := range values {
		if strings.EqualFold(strings.TrimSpace(value), wanted) {
			return true
		}
	}
	return false
}

func getHivePartitionPath(outputPath string, t time.Time, mode string) string {
	ext := filepath.Ext(outputPath)
	baseDir := strings.TrimSuffix(outputPath, ext)
	if ext == ".gz" && strings.HasSuffix(strings.ToLower(baseDir), ".csv") {
		ext = ".csv.gz"
		baseDir = strings.TrimSuffix(baseDir, ".csv")
	}

	t = t.UTC()
	var parts []string
	switch mode {
	case partitionHour:
		parts = []string{
			fmt.Sprintf("year=%04d", t.Year()),
			fmt.Sprintf("month=%02d", t.Month()),
			fmt.Sprintf("day=%02d", t.Day()),
			fmt.Sprintf("hour=%02d", t.Hour()),
		}
	case partitionDay:
		parts = []string{
			fmt.Sprintf("year=%04d", t.Year()),
			fmt.Sprintf("month=%02d", t.Month()),
			fmt.Sprintf("day=%02d", t.Day()),
		}
	case partitionWeek:
		year, week := t.ISOWeek()
		parts = []string{
			fmt.Sprintf("year=%04d", year),
			fmt.Sprintf("week=%02d", week),
		}
	case partitionMonth:
		parts = []string{
			fmt.Sprintf("year=%04d", t.Year()),
			fmt.Sprintf("month=%02d", t.Month()),
		}
	case partitionYear:
		parts = []string{
			fmt.Sprintf("year=%04d", t.Year()),
		}
	default:
		return outputPath
	}

	subDir := filepath.Join(parts...)
	fileName := "data" + ext
	return filepath.Join(baseDir, subDir, fileName)
}
