package cli

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/Nosvemos/dukascopy-go/pkg/csvout"
	"github.com/Nosvemos/dukascopy-go/pkg/dukascopy"
)

// Chunk files in the download cache always use one fixed internal format:
// RFC3339Nano timestamps in UTC, a comma delimiter and a header row. The
// user's output settings (timestamp format, time zone, delimiter, header,
// preset) are applied only when the merger writes the final outputs, so the
// merger can always parse what the workers wrote and cached chunks do not
// depend on presentation settings.
const chunkTimestampLayout = time.RFC3339Nano

// customBarTickColumns are the columns of the raw tick chunks cached for
// --bar-type tick/volume/dollar; bars are sampled from them at merge time.
var customBarTickColumns = csvout.TickColumnsForProfile(csvout.ProfileFull)

// mergeCtxCheckEvery is how many merged rows pass between context checks.
const mergeCtxCheckEvery = 10000

// chunkConfig returns the csvout configuration used for chunk files. Gap
// filling (which changes the rows themselves) is kept from the user's
// settings; every presentation setting is fixed.
func chunkConfig() *csvout.Config {
	cfg := csvout.DefaultConfig()
	cfg.Location = time.UTC
	cfg.TimestampFormat = chunkTimestampLayout
	cfg.CSVDelimiter = ','
	cfg.HideHeader = false
	return cfg
}

// formatOutputTime renders ts the way csvout renders timestamps for cfg.
func formatOutputTime(cfg *csvout.Config, ts time.Time) string {
	loc := time.UTC
	layout := time.RFC3339Nano
	if cfg != nil {
		if cfg.Location != nil {
			loc = cfg.Location
		}
		if cfg.TimestampFormat != "" {
			layout = cfg.TimestampFormat
		}
	}
	return ts.In(loc).Format(layout)
}

// mergeSource yields merged rows (in chunk column order) and their timestamps
// in output order. Next returns io.EOF when exhausted.
type mergeSource interface {
	Next() ([]string, time.Time, error)
	Close() error
}

// chunkFileSource streams the rows of chunk files in order. It verifies each
// chunk's header and row width and filters rows to [from, to) (to only, when
// filterFrom is false: a bar bucket that starts before from but holds
// in-range data is kept, labeled with its bucket start).
type chunkFileSource struct {
	paths      []string
	columns    []string
	tsIndex    int
	from       time.Time
	to         time.Time
	filterFrom bool

	next    int
	current string
	file    *os.File
	reader  *csv.Reader
}

func newChunkFileSource(paths []string, columns []string, from time.Time, to time.Time, filterFrom bool) (*chunkFileSource, error) {
	tsIndex := indexOfColumn(columns, "timestamp")
	if tsIndex < 0 {
		return nil, errors.New("missing timestamp column")
	}
	return &chunkFileSource{
		paths:      paths,
		columns:    columns,
		tsIndex:    tsIndex,
		from:       from,
		to:         to,
		filterFrom: filterFrom,
	}, nil
}

func (s *chunkFileSource) open(path string) error {
	file, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return fmt.Errorf("chunk file %s is missing; rerun the download to fetch it again", path)
		}
		return err
	}
	reader := csv.NewReader(bufio.NewReaderSize(file, 64*1024))
	reader.Comma = ','
	reader.FieldsPerRecord = -1
	header, err := reader.Read()
	if err != nil {
		file.Close()
		if errors.Is(err, io.EOF) {
			return fmt.Errorf("chunk file %s is empty (missing header)", path)
		}
		return fmt.Errorf("read chunk %s header: %w", path, err)
	}
	if !csvout.HeadersMatch(s.columns, header) {
		file.Close()
		return fmt.Errorf("chunk file %s has header %q, expected %q; delete the cache directory and rerun", path, strings.Join(header, ","), strings.Join(s.columns, ","))
	}
	s.file = file
	s.reader = reader
	s.current = path
	return nil
}

func (s *chunkFileSource) closeFile() {
	if s.file != nil {
		_ = s.file.Close()
	}
	s.file = nil
	s.reader = nil
}

func (s *chunkFileSource) Next() ([]string, time.Time, error) {
	for {
		if s.reader == nil {
			if s.next >= len(s.paths) {
				return nil, time.Time{}, io.EOF
			}
			path := s.paths[s.next]
			s.next++
			if err := s.open(path); err != nil {
				return nil, time.Time{}, err
			}
			continue
		}

		record, err := s.reader.Read()
		if errors.Is(err, io.EOF) {
			s.closeFile()
			continue
		}
		if err != nil {
			return nil, time.Time{}, fmt.Errorf("read chunk %s: %w", s.current, err)
		}
		if len(record) != len(s.columns) {
			return nil, time.Time{}, fmt.Errorf("malformed row in chunk %s: got %d fields, expected %d", s.current, len(record), len(s.columns))
		}
		ts, err := time.Parse(chunkTimestampLayout, record[s.tsIndex])
		if err != nil {
			return nil, time.Time{}, fmt.Errorf("malformed row in chunk %s: timestamp %q: %w", s.current, record[s.tsIndex], err)
		}
		ts = ts.UTC()
		if !ts.Before(s.to) || (s.filterFrom && ts.Before(s.from)) {
			continue
		}
		return record, ts, nil
	}
}

func (s *chunkFileSource) Close() error {
	s.closeFile()
	return nil
}

// customBarSampler is an incremental version of
// dukascopy.SampleTicksToCustomBars: fed the whole tick stream in order, it
// emits exactly the bars that function returns for the concatenated stream,
// so an unfinished bar is carried across chunk boundaries instead of being
// emitted (short) at the end of every chunk.
type customBarSampler struct {
	barType     dukascopy.BarType
	size        float64
	ticksPerBar int
	ask         bool

	current dukascopy.Bar
	open    bool
	count   int
	cum     float64
}

func newCustomBarSampler(barType dukascopy.BarType, size float64, side dukascopy.PriceSide) (*customBarSampler, error) {
	// Validate the size, type and side exactly as the library does.
	if _, err := dukascopy.SampleTicksToCustomBars(nil, barType, size, side); err != nil {
		return nil, err
	}
	switch barType {
	case dukascopy.BarTypeTick, dukascopy.BarTypeVolume, dukascopy.BarTypeDollar:
	default:
		return nil, fmt.Errorf("unsupported sampling bar type: %s", barType)
	}
	return &customBarSampler{
		barType:     barType,
		size:        size,
		ticksPerBar: int(size),
		ask:         strings.EqualFold(strings.TrimSpace(string(side)), string(dukascopy.PriceSideAsk)),
	}, nil
}

// add feeds one tick and returns a bar when it completes one.
func (s *customBarSampler) add(tick dukascopy.Tick) (dukascopy.Bar, bool) {
	price := tick.Bid
	volume := tick.BidVolume
	if s.ask {
		price = tick.Ask
		volume = tick.AskVolume
	}
	metric := volume
	if s.barType == dukascopy.BarTypeDollar {
		metric = price * volume
	}

	if !s.open {
		s.current = dukascopy.Bar{Time: tick.Time, Open: price, High: price, Low: price, Close: price, Volume: volume}
		s.open = true
		s.count = 1
		s.cum = metric
	} else {
		if price > s.current.High {
			s.current.High = price
		}
		if price < s.current.Low {
			s.current.Low = price
		}
		s.current.Close = price
		s.current.Volume += volume
		s.count++
		s.cum += metric
	}

	complete := false
	switch s.barType {
	case dukascopy.BarTypeTick:
		complete = s.count >= s.ticksPerBar
	default:
		complete = s.cum >= s.size
	}
	if !complete {
		return dukascopy.Bar{}, false
	}
	bar := s.current
	s.open = false
	s.count = 0
	s.cum = 0
	return bar, true
}

// flush returns the unfinished bar at the end of the stream, if any.
func (s *customBarSampler) flush() (dukascopy.Bar, bool) {
	if !s.open {
		return dukascopy.Bar{}, false
	}
	s.open = false
	return s.current, true
}

// customBarSource samples bars from cached raw tick chunks in order.
type customBarSource struct {
	ticks      *chunkFileSource
	sampler    *customBarSampler
	instrument dukascopy.Instrument
	columns    []string
	cfg        *csvout.Config
	tsIndex    int

	pending []dukascopy.Bar
	records [][]string
	times   []time.Time
	done    bool
}

const customBarBatch = 1024

func newCustomBarSource(tickPaths []string, from time.Time, to time.Time, barType dukascopy.BarType, barSize float64, side dukascopy.PriceSide, instrument dukascopy.Instrument, columns []string) (*customBarSource, error) {
	ticks, err := newChunkFileSource(tickPaths, customBarTickColumns, from, to, true)
	if err != nil {
		return nil, err
	}
	sampler, err := newCustomBarSampler(barType, barSize, side)
	if err != nil {
		return nil, err
	}
	tsIndex := indexOfColumn(columns, "timestamp")
	if tsIndex < 0 {
		return nil, errors.New("missing timestamp column")
	}
	cfg := chunkConfig()
	cfg.FillGaps = "none"
	return &customBarSource{
		ticks:      ticks,
		sampler:    sampler,
		instrument: instrument,
		columns:    columns,
		cfg:        cfg,
		tsIndex:    tsIndex,
	}, nil
}

func parseChunkTick(record []string, ts time.Time) (dukascopy.Tick, error) {
	values := make([]float64, 4)
	for i := range values {
		value, err := strconv.ParseFloat(record[i+1], 64)
		if err != nil {
			return dukascopy.Tick{}, fmt.Errorf("parse tick column %q: %w", customBarTickColumns[i+1], err)
		}
		values[i] = value
	}
	return dukascopy.Tick{Time: ts, Bid: values[0], Ask: values[1], BidVolume: values[2], AskVolume: values[3]}, nil
}

func (s *customBarSource) fill() error {
	for len(s.pending) < customBarBatch && !s.done {
		record, ts, err := s.ticks.Next()
		if errors.Is(err, io.EOF) {
			if bar, ok := s.sampler.flush(); ok {
				s.pending = append(s.pending, bar)
			}
			s.done = true
			break
		}
		if err != nil {
			return err
		}
		tick, err := parseChunkTick(record, ts)
		if err != nil {
			return fmt.Errorf("malformed row in chunk %s: %w", s.ticks.current, err)
		}
		if bar, ok := s.sampler.add(tick); ok {
			s.pending = append(s.pending, bar)
		}
	}
	if len(s.pending) == 0 {
		return nil
	}

	var buffer bytes.Buffer
	if err := s.cfg.WriteBarsRowsToWriter(&buffer, s.instrument, s.columns, s.pending, nil, nil); err != nil {
		return err
	}
	reader := csv.NewReader(&buffer)
	reader.FieldsPerRecord = len(s.columns)
	records, err := reader.ReadAll()
	if err != nil {
		return err
	}
	if len(records) != len(s.pending) {
		return fmt.Errorf("sampled %d bars but formatted %d rows", len(s.pending), len(records))
	}
	for i := range records {
		s.records = append(s.records, records[i])
		s.times = append(s.times, s.pending[i].Time.UTC())
	}
	s.pending = s.pending[:0]
	return nil
}

func (s *customBarSource) Next() ([]string, time.Time, error) {
	if len(s.records) == 0 {
		if s.done {
			return nil, time.Time{}, io.EOF
		}
		if err := s.fill(); err != nil {
			return nil, time.Time{}, err
		}
		if len(s.records) == 0 {
			return nil, time.Time{}, io.EOF
		}
	}
	record, ts := s.records[0], s.times[0]
	s.records = s.records[1:]
	s.times = s.times[1:]
	return record, ts, nil
}

func (s *customBarSource) Close() error {
	return s.ticks.Close()
}

// mergeSink writes merged rows to one output file in the user's format.
type mergeSink interface {
	write(record []string, ts time.Time) error
	close() error
}

type outputKind int

const (
	outputCSV outputKind = iota
	outputJSONL
	outputParquet
	outputArrow
)

func outputKindForPath(path string) outputKind {
	lower := strings.ToLower(strings.TrimSpace(path))
	switch {
	case strings.HasSuffix(lower, ".parquet"):
		return outputParquet
	case strings.HasSuffix(lower, ".arrow"), strings.HasSuffix(lower, ".ipc"), strings.HasSuffix(lower, ".feather"):
		return outputArrow
	case csvout.IsJSONLPath(lower):
		return outputJSONL
	default:
		return outputCSV
	}
}

type csvMergeSink struct {
	file    *os.File
	buf     *bufio.Writer
	gz      *gzip.Writer
	writer  *csv.Writer
	cfg     *csvout.Config
	tsIndex int
	scratch []string
}

func newCSVMergeSink(target io.Writer, file *os.File, gzipped bool, cfg *csvout.Config, columns []string, tsIndex int) (*csvMergeSink, error) {
	sink := &csvMergeSink{file: file, cfg: cfg, tsIndex: tsIndex, scratch: make([]string, len(columns))}
	var w io.Writer = target
	if file != nil {
		sink.buf = bufio.NewWriterSize(file, 64*1024)
		w = sink.buf
	}
	if gzipped {
		sink.gz = gzip.NewWriter(w)
		w = sink.gz
	}
	sink.writer = csv.NewWriter(w)
	if cfg.CSVDelimiter != 0 {
		sink.writer.Comma = cfg.CSVDelimiter
	}
	if !cfg.HideHeader {
		if err := sink.writer.Write(columns); err != nil {
			return sink, err
		}
	}
	return sink, nil
}

func (s *csvMergeSink) write(record []string, ts time.Time) error {
	copy(s.scratch, record)
	s.scratch[s.tsIndex] = formatOutputTime(s.cfg, ts)
	return s.writer.Write(s.scratch)
}

func (s *csvMergeSink) close() error {
	var errs []error
	if s.writer != nil {
		s.writer.Flush()
		errs = append(errs, s.writer.Error())
		s.writer = nil
	}
	if s.gz != nil {
		errs = append(errs, s.gz.Close())
		s.gz = nil
	}
	if s.buf != nil {
		errs = append(errs, s.buf.Flush())
		s.buf = nil
	}
	if s.file != nil {
		errs = append(errs, s.file.Close())
		s.file = nil
	}
	return errors.Join(errs...)
}

// typedRow converts a chunk record to the typed values the Parquet, Arrow and
// JSONL writers use: the timestamp as a string in the user's format,
// tick_count as an integer and every other column as a float.
func typedRow(cfg *csvout.Config, columns []string, tsIndex int, record []string, ts time.Time) ([]any, error) {
	values := make([]any, len(columns))
	for i, column := range columns {
		if i == tsIndex {
			values[i] = formatOutputTime(cfg, ts)
			continue
		}
		value, err := parquetValueForColumn(column, record[i])
		if err != nil {
			return nil, err
		}
		values[i] = value
	}
	return values, nil
}

type jsonlMergeSink struct {
	file    *os.File
	buf     *bufio.Writer
	gz      *gzip.Writer
	out     io.Writer
	cfg     *csvout.Config
	columns []string
	keys    [][]byte
	tsIndex int
	line    bytes.Buffer
}

func newJSONLMergeSink(file *os.File, gzipped bool, cfg *csvout.Config, columns []string, tsIndex int) (*jsonlMergeSink, error) {
	sink := &jsonlMergeSink{file: file, cfg: cfg, columns: columns, tsIndex: tsIndex}
	sink.buf = bufio.NewWriterSize(file, 64*1024)
	sink.out = sink.buf
	if gzipped {
		sink.gz = gzip.NewWriter(sink.buf)
		sink.out = sink.gz
	}
	for _, column := range columns {
		key, err := json.Marshal(column)
		if err != nil {
			return sink, err
		}
		sink.keys = append(sink.keys, key)
	}
	return sink, nil
}

func (s *jsonlMergeSink) write(record []string, ts time.Time) error {
	s.line.Reset()
	s.line.WriteByte('{')
	for i, column := range s.columns {
		if i > 0 {
			s.line.WriteByte(',')
		}
		s.line.Write(s.keys[i])
		s.line.WriteByte(':')
		var value any
		if i == s.tsIndex {
			value = formatOutputTime(s.cfg, ts)
		} else if typed, err := parquetValueForColumn(column, record[i]); err == nil {
			value = typed
			if f, ok := typed.(float64); ok && (math.IsNaN(f) || math.IsInf(f, 0)) {
				value = nil
			}
		}
		encoded, err := json.Marshal(value)
		if err != nil {
			return err
		}
		s.line.Write(encoded)
	}
	s.line.WriteString("}\n")
	_, err := s.out.Write(s.line.Bytes())
	return err
}

func (s *jsonlMergeSink) close() error {
	var errs []error
	if s.gz != nil {
		errs = append(errs, s.gz.Close())
		s.gz = nil
	}
	if s.buf != nil {
		errs = append(errs, s.buf.Flush())
		s.buf = nil
	}
	if s.file != nil {
		errs = append(errs, s.file.Close())
		s.file = nil
	}
	return errors.Join(errs...)
}

type batchWriter interface {
	WriteBatch(records []map[string]any) error
	Close() error
}

const columnarBatchSize = 50000

type columnarMergeSink struct {
	writer  batchWriter
	cfg     *csvout.Config
	columns []string
	tsIndex int
	batch   []map[string]any
}

func (s *columnarMergeSink) write(record []string, ts time.Time) error {
	values, err := typedRow(s.cfg, s.columns, s.tsIndex, record, ts)
	if err != nil {
		return err
	}
	row := make(map[string]any, len(s.columns))
	for i, column := range s.columns {
		row[column] = values[i]
	}
	s.batch = append(s.batch, row)
	if len(s.batch) >= columnarBatchSize {
		if err := s.writer.WriteBatch(s.batch); err != nil {
			return err
		}
		s.batch = s.batch[:0]
	}
	return nil
}

func (s *columnarMergeSink) close() error {
	if s.writer == nil {
		return nil
	}
	var flushErr error
	if len(s.batch) > 0 {
		flushErr = s.writer.WriteBatch(s.batch)
		s.batch = nil
	}
	closeErr := s.writer.Close()
	s.writer = nil
	return errors.Join(flushErr, closeErr)
}

// createMergeTemp reserves a temp file next to finalPath. Outputs are written
// there and renamed over finalPath only after the whole merge succeeded, so a
// failed or interrupted merge never truncates a previous good file.
func createMergeTemp(finalPath string) (string, error) {
	dir := filepath.Dir(finalPath)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", err
	}
	file, err := os.CreateTemp(dir, "."+filepath.Base(finalPath)+".merge-*.tmp")
	if err != nil {
		return "", err
	}
	name := file.Name()
	if err := file.Close(); err != nil {
		_ = os.Remove(name)
		return "", err
	}
	return name, nil
}

// openMergeSink opens a sink writing tempPath in the format implied by
// finalPath.
func openMergeSink(finalPath string, tempPath string, cfg *csvout.Config, columns []string, tsIndex int) (mergeSink, error) {
	lower := strings.ToLower(strings.TrimSpace(finalPath))
	gzipped := strings.HasSuffix(lower, ".gz")
	switch outputKindForPath(finalPath) {
	case outputParquet:
		writer, err := cfg.CreateParquetStreamWriter(tempPath, columns)
		if err != nil {
			return nil, err
		}
		return &columnarMergeSink{writer: writer, cfg: cfg, columns: columns, tsIndex: tsIndex}, nil
	case outputArrow:
		writer, err := cfg.CreateArrowStreamWriter(tempPath, columns)
		if err != nil {
			return nil, err
		}
		return &columnarMergeSink{writer: writer, cfg: cfg, columns: columns, tsIndex: tsIndex}, nil
	case outputJSONL:
		file, err := os.Create(tempPath)
		if err != nil {
			return nil, err
		}
		sink, err := newJSONLMergeSink(file, gzipped, cfg, columns, tsIndex)
		if err != nil {
			_ = sink.close()
			return nil, err
		}
		return sink, nil
	default:
		file, err := os.Create(tempPath)
		if err != nil {
			return nil, err
		}
		sink, err := newCSVMergeSink(nil, file, gzipped, cfg, columns, tsIndex)
		if err != nil {
			_ = sink.close()
			return nil, err
		}
		return sink, nil
	}
}

// mergeOptions describes one merge of cached chunks into the final output(s).
type mergeOptions struct {
	outputPath    string
	partPaths     []string
	columns       []string
	from          time.Time
	to            time.Time
	partitionMode string
	hive          bool
	// filterFrom drops rows before from (ticks). Bar buckets that start before
	// from are kept, labeled with their bucket start.
	filterFrom bool
	// strictTimes requires strictly increasing timestamps (time bars). Ticks
	// and custom bars may legitimately share timestamps.
	strictTimes bool
	// outCfg holds the user's output format.
	outCfg *csvout.Config
	// source overrides reading partPaths (custom bar sampling).
	source mergeSource
}

// mergeChunks streams all chunk rows, in order, to the output path and, when
// partitioning, to per-partition outputs. Every file output is written to a
// temp file in its directory and renamed into place only when the merge
// completes; on error or cancellation the temps are removed and existing
// outputs are left untouched.
func mergeChunks(ctx context.Context, stdout io.Writer, opts mergeOptions) (totalRows int, retErr error) {
	columns := opts.columns
	tsIndex := indexOfColumn(columns, "timestamp")
	if tsIndex < 0 {
		return 0, errors.New("missing timestamp column")
	}
	cfg := opts.outCfg
	if cfg == nil {
		cfg = chunkConfig()
	}

	source := opts.source
	if source == nil {
		chunkSource, err := newChunkFileSource(opts.partPaths, columns, opts.from, opts.to, opts.filterFrom)
		if err != nil {
			return 0, err
		}
		source = chunkSource
	}
	defer source.Close()

	type pendingRename struct {
		temp  string
		final string
	}
	var renames []pendingRename
	var mainSink, partSink mergeSink

	defer func() {
		if retErr == nil {
			return
		}
		if partSink != nil {
			_ = partSink.close()
		}
		if mainSink != nil {
			_ = mainSink.close()
		}
		for _, rename := range renames {
			_ = os.Remove(rename.temp)
		}
	}()

	if strings.TrimSpace(opts.outputPath) == "-" {
		sink, err := newCSVMergeSink(stdout, nil, false, cfg, columns, tsIndex)
		mainSink = sink
		if err != nil {
			return 0, err
		}
	} else {
		temp, err := createMergeTemp(opts.outputPath)
		if err != nil {
			return 0, err
		}
		renames = append(renames, pendingRename{temp: temp, final: opts.outputPath})
		mainSink, err = openMergeSink(opts.outputPath, temp, cfg, columns, tsIndex)
		if err != nil {
			return 0, err
		}
	}

	partitioned := opts.partitionMode != "" && opts.partitionMode != partitionNone
	currentKey := ""
	var previous time.Time
	havePrevious := false

	for {
		if totalRows%mergeCtxCheckEvery == 0 {
			if err := ctx.Err(); err != nil {
				return 0, err
			}
		}
		record, ts, err := source.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return 0, err
		}
		if opts.strictTimes && havePrevious && !ts.After(previous) {
			return 0, fmt.Errorf("bar timestamps are not strictly increasing at %s; delete the cache directory and rerun", ts.Format(time.RFC3339))
		}
		previous = ts
		havePrevious = true

		if partitioned {
			key := getPartitionKey(ts, opts.partitionMode)
			if key != currentKey || partSink == nil {
				if partSink != nil {
					sink := partSink
					partSink = nil
					if err := sink.close(); err != nil {
						return 0, err
					}
				}
				final := getPartitionOutputPath(opts.outputPath, key)
				if opts.hive {
					final = getHivePartitionPath(opts.outputPath, ts, opts.partitionMode)
				}
				temp, err := createMergeTemp(final)
				if err != nil {
					return 0, err
				}
				renames = append(renames, pendingRename{temp: temp, final: final})
				partSink, err = openMergeSink(final, temp, cfg, columns, tsIndex)
				if err != nil {
					return 0, err
				}
				currentKey = key
			}
		}

		if err := mainSink.write(record, ts); err != nil {
			return 0, err
		}
		if partSink != nil {
			if err := partSink.write(record, ts); err != nil {
				return 0, err
			}
		}
		totalRows++
	}

	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if partSink != nil {
		sink := partSink
		partSink = nil
		if err := sink.close(); err != nil {
			return 0, err
		}
	}
	sink := mainSink
	mainSink = nil
	if err := sink.close(); err != nil {
		return 0, err
	}

	for index, rename := range renames {
		if err := os.Rename(rename.temp, rename.final); err != nil {
			renames = renames[index:]
			return 0, fmt.Errorf("finalize %s: %w", rename.final, err)
		}
	}
	renames = nil
	return totalRows, nil
}
