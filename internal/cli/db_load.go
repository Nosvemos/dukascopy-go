package cli

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"time"
)

const (
	dbClickHouse = "clickhouse"
	dbInfluxDB   = "influxdb"
	dbPostgres   = "postgres"

	defaultClickHouseBatchRows = 10000
	defaultInfluxDBBatchRows   = 5000
)

// runDBLoad is the CLI entry-point for the `db-load` command.
func runDBLoad(args []string, stdout io.Writer, stderr io.Writer) error {
	fs := flag.NewFlagSet("db-load", flag.ContinueOnError)
	fs.SetOutput(stdout)
	fs.Usage = func() {
		fmt.Fprintf(stdout, "%sdb-load:%s Ingest a CSV or Parquet file directly into ClickHouse or InfluxDB\n\n", colorize(colorCyan), colorize(colorReset))
		fmt.Fprint(stdout, "Usage:\n  dukascopy-go db-load [options]\n\nOptions:\n")
		fs.PrintDefaults()
		fmt.Fprint(stdout, "\nExamples:\n  dukascopy-go db-load --input ./eurusd_m1.csv --db clickhouse --url http://localhost:8123 --table eurusd_m1\n  dukascopy-go db-load --input ./eurusd_tick.csv --db influxdb --url http://localhost:8086 --org myorg --bucket mybucket --token mytoken --table eurusd_tick --symbol eurusd\n")
	}

	input := fs.String("input", "", "path to the local CSV or Parquet file to ingest (required)")
	dbType := fs.String("db", "", "target database: clickhouse, influxdb, or postgres (required)")
	dbURL := fs.String("url", "", "database URL, e.g. http://localhost:8123 or postgres://user:pass@localhost:5432/dbname (required)")
	table := fs.String("table", "", "target table or InfluxDB measurement name (required)")
	user := fs.String("user", "default", "ClickHouse user (optional)")
	password := fs.String("password", "", "ClickHouse password or InfluxDB token (optional)")
	token := fs.String("token", "", "InfluxDB API token (takes precedence over --password)")
	org := fs.String("org", "", "InfluxDB organization (required for InfluxDB)")
	bucket := fs.String("bucket", "", "InfluxDB bucket (required for InfluxDB)")
	symbol := fs.String("symbol", "", "instrument symbol hint for tagging InfluxDB rows")
	batch := fs.Int("batch", 0, "rows per HTTP batch (0 = use default for target db)")
	timeout := fs.Duration("timeout", 120*time.Second, "HTTP request timeout for each batch")

	if err := fs.Parse(args); err != nil {
		return err
	}

	// Validation
	if strings.TrimSpace(*input) == "" {
		return errors.New("--input is required")
	}
	if strings.TrimSpace(*dbType) == "" {
		return errors.New("--db is required (clickhouse or influxdb)")
	}
	if strings.TrimSpace(*dbURL) == "" {
		return errors.New("--url is required")
	}
	if strings.TrimSpace(*table) == "" {
		return errors.New("--table is required")
	}

	dbTypeLower := strings.ToLower(strings.TrimSpace(*dbType))
	if dbTypeLower != dbClickHouse && dbTypeLower != dbInfluxDB && dbTypeLower != dbPostgres {
		return fmt.Errorf("unknown --db %q (supported: clickhouse, influxdb, postgres)", *dbType)
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
		if dbTypeLower == dbClickHouse {
			batchSize = defaultClickHouseBatchRows
		} else {
			batchSize = defaultInfluxDBBatchRows
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	switch dbTypeLower {
	case dbClickHouse:
		return ingestClickHouse(ctx, stdout, stderr, inputPath, *dbURL, *table, *user, *password, *timeout)
	case dbPostgres:
		return ingestPostgres(ctx, stdout, stderr, inputPath, *dbURL, *table)
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
		return ingestInfluxDB(ctx, stdout, stderr, inputPath, *dbURL, *table, *org, *bucket, authToken, *symbol, batchSize, *timeout)
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
	DBType    string
	DBURL     string
	Table     string
	InputPath string
	User      string
	Password  string
	Token     string
	Org       string
	Bucket    string
	SymbolTag string
	BatchSize int
	Timeout   time.Duration
}

func DBLoad(ctx context.Context, stdout io.Writer, stderr io.Writer, opt DBLoadOptions) error {
	dbTypeLower := strings.ToLower(strings.TrimSpace(opt.DBType))
	switch dbTypeLower {
	case "clickhouse":
		return ingestClickHouse(ctx, stdout, stderr, opt.InputPath, opt.DBURL, opt.Table, opt.User, opt.Password, opt.Timeout)
	case "postgres":
		return ingestPostgres(ctx, stdout, stderr, opt.InputPath, opt.DBURL, opt.Table)
	case "influxdb":
		authToken := strings.TrimSpace(opt.Token)
		if authToken == "" {
			authToken = strings.TrimSpace(opt.Password)
		}
		return ingestInfluxDB(ctx, stdout, stderr, opt.InputPath, opt.DBURL, opt.Table, opt.Org, opt.Bucket, authToken, opt.SymbolTag, opt.BatchSize, opt.Timeout)
	default:
		return fmt.Errorf("unknown --db %q (supported: clickhouse, influxdb, postgres)", opt.DBType)
	}
}
