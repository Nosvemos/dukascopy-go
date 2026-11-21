package cli

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"regexp"
	"strings"
	"time"
)

const (
	dbClickHouse = "clickhouse"
	dbInfluxDB   = "influxdb"
	dbPostgres   = "postgres"
	dbQuestDB    = "questdb"

	defaultClickHouseBatchRows = 10000
	defaultInfluxDBBatchRows   = 5000
	defaultQuestDBBatchRows    = 5000
)

// runDBLoad is the CLI entry-point for the `db-load` command.
func runDBLoad(args []string, stdout io.Writer, stderr io.Writer) error {
	fs := flag.NewFlagSet("db-load", flag.ContinueOnError)
	fs.SetOutput(stdout)
	fs.Usage = func() {
		fmt.Fprintf(stdout, "%sdb-load:%s Ingest a CSV or Parquet file directly into ClickHouse, InfluxDB, PostgreSQL, or QuestDB\n\n", colorize(colorCyan), colorize(colorReset))
		fmt.Fprint(stdout, "Usage:\n  dukascopy-go db-load [options]\n\nOptions:\n")
		fs.PrintDefaults()
		fmt.Fprint(stdout, "\nExamples:\n  dukascopy-go db-load --input ./eurusd_m1.csv --db clickhouse --url http://localhost:8123 --table eurusd_m1\n  dukascopy-go db-load --input ./eurusd_tick.csv --db influxdb --url http://localhost:8086 --org myorg --bucket mybucket --token mytoken --table eurusd_tick --symbol eurusd\n  dukascopy-go db-load --input ./xauusd.csv --db questdb --url tcp://localhost:9009 --table xauusd\n  dukascopy-go db-load --input ./eurusd_m1.csv --db postgres --url postgres://user:pass@localhost:5432/db --table eurusd_m1 --create-hypertable\n")
	}

	input := fs.String("input", "", "path to the local CSV or Parquet file to ingest (required)")
	dbType := fs.String("db", "", "target database: clickhouse, influxdb, postgres, or questdb (required)")
	dbURL := fs.String("url", "", "database URL, e.g. http://localhost:8123 or tcp://localhost:9009 or postgres://user:pass@localhost:5432/dbname (required)")
	table := fs.String("table", "", "target table or measurement name (required)")
	user := fs.String("user", "default", "ClickHouse user (optional)")
	password := fs.String("password", "", "ClickHouse password or InfluxDB token (optional)")
	token := fs.String("token", "", "InfluxDB API token (takes precedence over --password)")
	org := fs.String("org", "", "InfluxDB organization (required for InfluxDB)")
	bucket := fs.String("bucket", "", "InfluxDB bucket (required for InfluxDB)")
	symbol := fs.String("symbol", "", "instrument symbol hint for tagging rows (InfluxDB, QuestDB)")
	batch := fs.Int("batch", 0, "rows per HTTP batch (0 = use default for target db)")
	timeout := fs.Duration("timeout", 120*time.Second, "HTTP request timeout for each batch")
	ilpPort := fs.Int("ilp-port", 0, "QuestDB ILP TCP port override (0 = auto; default 9009 for TCP, 9000 for HTTP)")
	createHypertable := fs.Bool("create-hypertable", true, "auto-create TimescaleDB hypertable when detected in PostgreSQL")
	chunkInterval := fs.String("chunk-interval", "", "TimescaleDB hypertable chunk interval override (e.g. '1 day', '7 days'; empty = auto-detect)")

	if err := fs.Parse(args); err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	return DBLoad(ctx, stdout, stderr, DBLoadOptions{
		DBType:           *dbType,
		DBURL:            *dbURL,
		Table:            *table,
		InputPath:        *input,
		User:             *user,
		Password:         *password,
		Token:            *token,
		Org:              *org,
		Bucket:           *bucket,
		SymbolTag:        *symbol,
		BatchSize:        *batch,
		Timeout:          *timeout,
		ILPPort:          *ilpPort,
		CreateHypertable: *createHypertable,
		ChunkInterval:    *chunkInterval,
	})
}

// buildColumnIndex returns a map from column name → position for fast lookup.
func buildColumnIndex(header []string) map[string]int {
	idx := make(map[string]int, len(header))
	for i, col := range header {
		idx[strings.TrimSpace(strings.ToLower(col))] = i
	}
	return idx
}

type DBLoadOptions struct {
	DBType           string
	DBURL            string
	Table            string
	InputPath        string
	User             string
	Password         string
	Token            string
	Org              string
	Bucket           string
	SymbolTag        string
	BatchSize        int
	Timeout          time.Duration
	ILPPort          int
	CreateHypertable bool
	ChunkInterval    string
}

// DBLoad validates opt, applies defaults and loads the input file into the
// selected database. It is the single dispatch point used by the CLI as well.
func DBLoad(ctx context.Context, stdout io.Writer, stderr io.Writer, opt DBLoadOptions) error {
	opt, err := normalizeDBLoadOptions(opt)
	if err != nil {
		return err
	}

	switch opt.DBType {
	case dbClickHouse:
		return ingestClickHouse(ctx, stdout, stderr, opt.InputPath, opt.DBURL, opt.Table, opt.User, opt.Password, opt.Timeout)
	case dbPostgres:
		return ingestPostgres(ctx, stdout, stderr, opt.InputPath, opt.DBURL, opt.Table, opt.CreateHypertable, opt.ChunkInterval)
	case dbInfluxDB:
		return ingestInfluxDB(ctx, stdout, stderr, opt.InputPath, opt.DBURL, opt.Table, opt.Org, opt.Bucket, opt.Token, opt.SymbolTag, opt.BatchSize, opt.Timeout)
	case dbQuestDB:
		return ingestQuestDB(ctx, stdout, stderr, opt.InputPath, opt.DBURL, opt.Table, opt.ILPPort, opt.SymbolTag, opt.BatchSize, opt.Timeout)
	default:
		return fmt.Errorf("unknown --db %q (supported: clickhouse, influxdb, postgres, questdb)", opt.DBType)
	}
}

// defaultDBLoadTimeout applies when DBLoadOptions.Timeout is not positive.
const defaultDBLoadTimeout = 120 * time.Second

// normalizeDBLoadOptions trims and validates the options and fills in defaults
// (per-database batch size, timeout, InfluxDB token fallback). Both the CLI
// and library callers go through it.
func normalizeDBLoadOptions(opt DBLoadOptions) (DBLoadOptions, error) {
	opt.InputPath = strings.TrimSpace(opt.InputPath)
	opt.DBURL = strings.TrimSpace(opt.DBURL)
	opt.Org = strings.TrimSpace(opt.Org)
	opt.Bucket = strings.TrimSpace(opt.Bucket)

	if opt.InputPath == "" {
		return opt, errors.New("--input is required")
	}
	if strings.TrimSpace(opt.DBType) == "" {
		return opt, errors.New("--db is required (clickhouse, influxdb, postgres, or questdb)")
	}
	if opt.DBURL == "" {
		return opt, errors.New("--url is required")
	}
	if strings.TrimSpace(opt.Table) == "" {
		return opt, errors.New("--table is required")
	}

	sanitizedTable, err := sanitizeMeasurementName(opt.Table)
	if err != nil {
		return opt, fmt.Errorf("invalid --table: %w", err)
	}
	opt.Table = sanitizedTable

	dbType := strings.ToLower(strings.TrimSpace(opt.DBType))
	switch dbType {
	case dbClickHouse, dbInfluxDB, dbPostgres, dbQuestDB:
	default:
		return opt, fmt.Errorf("unknown --db %q (supported: clickhouse, influxdb, postgres, questdb)", opt.DBType)
	}
	opt.DBType = dbType

	info, err := os.Stat(opt.InputPath)
	if err != nil {
		return opt, fmt.Errorf("cannot access --input file %q: %w", opt.InputPath, err)
	}
	if info.IsDir() {
		return opt, fmt.Errorf("--input must be a file, not a directory: %q", opt.InputPath)
	}

	if opt.BatchSize <= 0 {
		switch dbType {
		case dbClickHouse:
			opt.BatchSize = defaultClickHouseBatchRows
		case dbQuestDB:
			opt.BatchSize = defaultQuestDBBatchRows
		default:
			opt.BatchSize = defaultInfluxDBBatchRows
		}
	}
	if opt.Timeout <= 0 {
		opt.Timeout = defaultDBLoadTimeout
	}
	if opt.ILPPort < 0 || opt.ILPPort > 65535 {
		return opt, fmt.Errorf("--ilp-port must be between 0 and 65535, got %d", opt.ILPPort)
	}

	if dbType == dbInfluxDB {
		opt.Token = strings.TrimSpace(opt.Token)
		if opt.Token == "" {
			opt.Token = strings.TrimSpace(opt.Password)
		}
		if opt.Token == "" {
			return opt, errors.New("--token (or --password) is required for InfluxDB")
		}
		if opt.Org == "" {
			return opt, errors.New("--org is required for InfluxDB")
		}
		if opt.Bucket == "" {
			return opt, errors.New("--bucket is required for InfluxDB")
		}
	}

	return opt, nil
}

// nonFiniteFieldError is returned by the line-protocol encoders for NaN or
// ±Inf values, which line protocol cannot represent. Unlike other malformed
// rows these abort the load instead of being skipped.
type nonFiniteFieldError struct {
	Column string
	Value  string
}

func (e *nonFiniteFieldError) Error() string {
	return fmt.Sprintf("column %q has non-finite value %q, which line protocol cannot encode", e.Column, e.Value)
}

func isNonFiniteFieldError(err error) bool {
	var target *nonFiniteFieldError
	return errors.As(err, &target)
}

// partialLoadError reports a failure after some batches were already
// committed, so the caller knows a plain rerun would duplicate those rows.
type partialLoadError struct {
	Target        string
	CommittedRows int
	Err           error
}

func (e *partialLoadError) Error() string {
	return fmt.Sprintf("%s load failed after %d rows were already committed (rerunning the same file will duplicate them): %v", e.Target, e.CommittedRows, e.Err)
}

func (e *partialLoadError) Unwrap() error { return e.Err }

// validMeasurementNameRe allows alphanumeric characters, underscores, hyphens,
// dots, and forward slashes.  This covers standard table names and
// schema-qualified names (e.g. "public.eurusd_m1") while rejecting characters
// that could break InfluxDB/QuestDB Line Protocol (spaces, commas, newlines,
// backslashes) or SQL queries.
var validMeasurementNameRe = regexp.MustCompile(`^[a-zA-Z0-9_.\-/]+$`)

// sanitizeMeasurementName validates and trims a table/measurement name.
// It rejects empty names and names containing characters that could break
// InfluxDB/QuestDB Line Protocol or SQL queries.
func sanitizeMeasurementName(name string) (string, error) {
	name = strings.TrimSpace(name)
	if name == "" {
		return "", errors.New("table/measurement name cannot be empty")
	}
	if !validMeasurementNameRe.MatchString(name) {
		return "", fmt.Errorf("table/measurement name %q contains invalid characters; only alphanumeric, underscore, hyphen, dot, and forward slash are allowed", name)
	}
	return name, nil
}
