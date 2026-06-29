package cli

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
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

	// Validation
	if strings.TrimSpace(*input) == "" {
		return errors.New("--input is required")
	}
	if strings.TrimSpace(*dbType) == "" {
		return errors.New("--db is required (clickhouse, influxdb, postgres, or questdb)")
	}
	if strings.TrimSpace(*dbURL) == "" {
		return errors.New("--url is required")
	}
	if strings.TrimSpace(*table) == "" {
		return errors.New("--table is required")
	}

	sanitizedTable, err := sanitizeMeasurementName(*table)
	if err != nil {
		return fmt.Errorf("invalid --table: %w", err)
	}

	dbTypeLower := strings.ToLower(strings.TrimSpace(*dbType))
	if dbTypeLower != dbClickHouse && dbTypeLower != dbInfluxDB && dbTypeLower != dbPostgres && dbTypeLower != dbQuestDB {
		return fmt.Errorf("unknown --db %q (supported: clickhouse, influxdb, postgres, questdb)", *dbType)
	}

	inputPath := strings.TrimSpace(*input)
	info, err := os.Stat(inputPath)
	if err != nil {
		return fmt.Errorf("cannot access --input file %q: %w", inputPath, err)
	}
	if info.IsDir() {
		return fmt.Errorf("--input must be a file, not a directory: %q", inputPath)
	}

	// Choose batch size
	batchSize := *batch
	if batchSize <= 0 {
		switch dbTypeLower {
		case dbClickHouse:
			batchSize = defaultClickHouseBatchRows
		case dbQuestDB:
			batchSize = defaultQuestDBBatchRows
		default:
			batchSize = defaultInfluxDBBatchRows
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	switch dbTypeLower {
	case dbClickHouse:
		return ingestClickHouse(ctx, stdout, stderr, inputPath, *dbURL, sanitizedTable, *user, *password, *timeout)
	case dbPostgres:
		return ingestPostgres(ctx, stdout, stderr, inputPath, *dbURL, sanitizedTable, *createHypertable, *chunkInterval)
	case dbInfluxDB:
		authToken := strings.TrimSpace(*token)
		if authToken == "" {
			authToken = strings.TrimSpace(*password)
		}
		if authToken == "" {
			return errors.New("--token (or --password) is required for InfluxDB")
		}
		if strings.TrimSpace(*org) == "" {
			return errors.New("--org is required for InfluxDB")
		}
		if strings.TrimSpace(*bucket) == "" {
			return errors.New("--bucket is required for InfluxDB")
		}
		return ingestInfluxDB(ctx, stdout, stderr, inputPath, *dbURL, sanitizedTable, *org, *bucket, authToken, *symbol, batchSize, *timeout)
	case dbQuestDB:
		return ingestQuestDB(ctx, stdout, stderr, inputPath, *dbURL, sanitizedTable, *ilpPort, *symbol, batchSize, *timeout)
	}
	return nil
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

func DBLoad(ctx context.Context, stdout io.Writer, stderr io.Writer, opt DBLoadOptions) error {
	sanitizedTable, err := sanitizeMeasurementName(opt.Table)
	if err != nil {
		return fmt.Errorf("invalid table name: %w", err)
	}

	dbTypeLower := strings.ToLower(strings.TrimSpace(opt.DBType))
	switch dbTypeLower {
	case "clickhouse":
		return ingestClickHouse(ctx, stdout, stderr, opt.InputPath, opt.DBURL, sanitizedTable, opt.User, opt.Password, opt.Timeout)
	case "postgres":
		return ingestPostgres(ctx, stdout, stderr, opt.InputPath, opt.DBURL, sanitizedTable, opt.CreateHypertable, opt.ChunkInterval)
	case "influxdb":
		authToken := strings.TrimSpace(opt.Token)
		if authToken == "" {
			authToken = strings.TrimSpace(opt.Password)
		}
		return ingestInfluxDB(ctx, stdout, stderr, opt.InputPath, opt.DBURL, sanitizedTable, opt.Org, opt.Bucket, authToken, opt.SymbolTag, opt.BatchSize, opt.Timeout)
	case "questdb":
		return ingestQuestDB(ctx, stdout, stderr, opt.InputPath, opt.DBURL, sanitizedTable, opt.ILPPort, opt.SymbolTag, opt.BatchSize, opt.Timeout)
	default:
		return fmt.Errorf("unknown --db %q (supported: clickhouse, influxdb, postgres, questdb)", opt.DBType)
	}
}

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
