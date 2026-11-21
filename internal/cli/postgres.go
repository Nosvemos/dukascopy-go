package cli

import (
	"compress/gzip"
	"context"
	"database/sql"
	"encoding/csv"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/lib/pq"
)

// postgresSetupTimeout bounds the connection ping and the TimescaleDB setup
// queries so an unreachable host fails fast instead of hanging.
var postgresSetupTimeout = 15 * time.Second

// ingestPostgres streams the local CSV file directly to PostgreSQL using COPY.
// When TimescaleDB extension is detected and createHypertable is true, it creates
// a hypertable on the target table before COPY ingestion.
func ingestPostgres(
	ctx context.Context,
	stdout io.Writer,
	stderr io.Writer,
	inputPath string,
	dbURL string,
	table string,
	createHypertable bool,
	chunkInterval string,
) error {
	lower := strings.ToLower(inputPath)
	if !strings.HasSuffix(lower, ".csv") && !strings.HasSuffix(lower, ".csv.gz") {
		return fmt.Errorf("unsupported file type for postgres ingestion: %q (supported: .csv, .csv.gz)", inputPath)
	}

	db, err := sql.Open("postgres", withPostgresConnectTimeout(dbURL, postgresSetupTimeout))
	if err != nil {
		return fmt.Errorf("failed to open postgres connection: %w", err)
	}
	defer db.Close()

	pingCtx, cancelPing := context.WithTimeout(ctx, postgresSetupTimeout)
	err = db.PingContext(pingCtx)
	cancelPing()
	if err != nil {
		return fmt.Errorf("failed to connect to postgres: %w", err)
	}

	// Detect TimescaleDB and optionally create hypertable
	if createHypertable {
		setupCtx, cancelSetup := context.WithTimeout(ctx, postgresSetupTimeout)
		defer cancelSetup()
		hasTimescale, detectErr := detectTimescaleDB(setupCtx, db)
		if detectErr != nil {
			fmt.Fprintf(stderr, "%sdb-load%s warning: could not detect TimescaleDB extension: %v\n",
				colorize(colorYellow), colorize(colorReset), detectErr)
		} else if hasTimescale {
			fmt.Fprintf(stderr, "%sdb-load%s TimescaleDB extension detected\n",
				colorize(colorCyan), colorize(colorReset))

			// Auto-detect chunk interval from the input file name or use provided value
			chunk := strings.TrimSpace(chunkInterval)
			if chunk == "" {
				chunk = inferChunkIntervalFromPath(inputPath)
			}

			if htErr := createHypertablePG(setupCtx, db, table, chunk); htErr != nil {
				fmt.Fprintf(stderr, "%sdb-load%s warning: hypertable creation skipped: %v\n",
					colorize(colorYellow), colorize(colorReset), htErr)
			} else {
				fmt.Fprintf(stderr, "%sdb-load%s hypertable %q ready (chunk interval: %s)\n",
					colorize(colorGreen), colorize(colorReset), table, chunk)
			}
		}
	}

	f, err := os.Open(inputPath)
	if err != nil {
		return fmt.Errorf("cannot open input file: %w", err)
	}
	defer f.Close()

	info, _ := f.Stat()
	sizeMB := float64(info.Size()) / (1024 * 1024)

	var r io.Reader = f
	if strings.HasSuffix(lower, ".gz") {
		gz, err := gzip.NewReader(f)
		if err != nil {
			return fmt.Errorf("failed to open gzip reader: %w", err)
		}
		defer gz.Close()
		r = gz
	}

	reader := csv.NewReader(r)
	header, err := reader.Read()
	if err != nil {
		return fmt.Errorf("failed to read CSV header: %w", err)
	}

	fmt.Fprintf(stderr, "%sdb-load%s streaming %.1f MB to PostgreSQL table %q [CSV COPY]...\n",
		colorize(colorCyan), colorize(colorReset), sizeMB, table)

	txn, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer txn.Rollback()

	stmt, err := txn.PrepareContext(ctx, buildCopyInStatement(table, header))
	if err != nil {
		return fmt.Errorf("failed to prepare COPY statement: %w", err)
	}
	defer stmt.Close()

	rowCount := 0
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("failed to read CSV record at row %d: %w", rowCount+1, err)
		}

		args := make([]any, len(record))
		for i, v := range record {
			args[i] = v
		}

		if _, err := stmt.ExecContext(ctx, args...); err != nil {
			return fmt.Errorf("failed to exec COPY at row %d: %w", rowCount+1, err)
		}
		rowCount++
	}

	if _, err := stmt.ExecContext(ctx); err != nil {
		return fmt.Errorf("failed to flush COPY statement: %w", err)
	}

	if err := txn.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	fmt.Fprintf(stdout, "%sdb-load%s PostgreSQL: successfully ingested %d rows from %q into table %q\n",
		colorize(colorGreen), colorize(colorReset), rowCount, inputPath, table)
	return nil
}

// detectTimescaleDB checks whether the TimescaleDB extension is installed.
func detectTimescaleDB(ctx context.Context, db *sql.DB) (bool, error) {
	var exists bool
	err := db.QueryRowContext(ctx,
		`SELECT EXISTS(SELECT 1 FROM pg_extension WHERE extname = 'timescaledb')`,
	).Scan(&exists)
	if err != nil {
		return false, err
	}
	return exists, nil
}

// createHypertablePG creates a TimescaleDB hypertable on the given table.
// The hypertable creation runs in its own transaction, separate from COPY.
func createHypertablePG(ctx context.Context, db *sql.DB, table string, chunkInterval string) error {
	txn, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin hypertable transaction: %w", err)
	}
	defer txn.Rollback()

	if chunkInterval != "" {
		_, err = txn.ExecContext(ctx,
			`SELECT create_hypertable($1, 'timestamp', chunk_time_interval => $2::interval, if_not_exists => TRUE)`,
			table, chunkInterval,
		)
	} else {
		_, err = txn.ExecContext(ctx,
			`SELECT create_hypertable($1, 'timestamp', if_not_exists => TRUE)`,
			table,
		)
	}
	if err != nil {
		return fmt.Errorf("create hypertable: %w", err)
	}

	return txn.Commit()
}

// withPostgresConnectTimeout adds connect_timeout to a DSN that has none.
// lib/pq only applies the context to the TCP dial, so without it a server that
// accepts the connection but never answers the startup handshake would hang
// the ping forever.
func withPostgresConnectTimeout(dsn string, timeout time.Duration) string {
	if timeout <= 0 || strings.Contains(strings.ToLower(dsn), "connect_timeout") {
		return dsn
	}
	seconds := int((timeout + time.Second - 1) / time.Second)
	if seconds < 1 {
		seconds = 1
	}
	trimmed := strings.TrimSpace(dsn)
	lower := strings.ToLower(trimmed)
	if strings.HasPrefix(lower, "postgres://") || strings.HasPrefix(lower, "postgresql://") {
		u, err := url.Parse(trimmed)
		if err != nil {
			return dsn
		}
		q := u.Query()
		q.Set("connect_timeout", strconv.Itoa(seconds))
		u.RawQuery = q.Encode()
		return u.String()
	}
	if trimmed == "" {
		return fmt.Sprintf("connect_timeout=%d", seconds)
	}
	return fmt.Sprintf("%s connect_timeout=%d", trimmed, seconds)
}

// inferChunkIntervalFromPath derives a sensible chunk interval from the input filename.
// It looks for common timeframe tokens (m1, h1, d1, tick, etc.) in the path.
func inferChunkIntervalFromPath(path string) string {
	// Match on the file name only: directory names such as "/home/tick-data/"
	// must not influence the result.
	lower := strings.ToLower(filepath.Base(path))

	// Check for known timeframe patterns in the filename
	switch {
	case strings.Contains(lower, "tick"):
		return "1 hour"
	case strings.Contains(lower, "m1"), strings.Contains(lower, "m3"),
		strings.Contains(lower, "m5"), strings.Contains(lower, "m15"),
		strings.Contains(lower, "m30"):
		return "1 day"
	case strings.Contains(lower, "h1"), strings.Contains(lower, "h4"):
		return "7 days"
	case strings.Contains(lower, "d1"), strings.Contains(lower, "w1"),
		strings.Contains(lower, "mn1"), strings.Contains(lower, "month"):
		return "30 days"
	default:
		return "1 day"
	}
}

// buildCopyInStatement returns a "COPY ... FROM STDIN" statement for table
// (optionally schema-qualified as schema.table) and columns, quoting every
// identifier. It replaces the deprecated pq.CopyIn / pq.CopyInSchema helpers
// and produces the same SQL they did.
func buildCopyInStatement(table string, columns []string) string {
	var b strings.Builder
	b.WriteString("COPY ")
	if parts := strings.Split(table, "."); len(parts) == 2 {
		schema := strings.Trim(strings.TrimSpace(parts[0]), `"`)
		tbl := strings.Trim(strings.TrimSpace(parts[1]), `"`)
		b.WriteString(pq.QuoteIdentifier(schema))
		b.WriteByte('.')
		b.WriteString(pq.QuoteIdentifier(tbl))
	} else {
		b.WriteString(pq.QuoteIdentifier(table))
	}
	if len(columns) > 0 {
		b.WriteString(" (")
		for i, col := range columns {
			if i > 0 {
				b.WriteString(", ")
			}
			b.WriteString(pq.QuoteIdentifier(col))
		}
		b.WriteByte(')')
	}
	b.WriteString(" FROM STDIN")
	return b.String()
}
