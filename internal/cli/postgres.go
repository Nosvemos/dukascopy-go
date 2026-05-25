package cli

import (
	"context"
	"database/sql"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/lib/pq"
)

// ingestPostgres streams the local CSV file directly to PostgreSQL using COPY.
func ingestPostgres(
	ctx context.Context,
	stdout io.Writer,
	stderr io.Writer,
	inputPath string,
	dbURL string,
	table string,
) error {
	if !strings.HasSuffix(strings.ToLower(inputPath), ".csv") {
		return fmt.Errorf("postgres ingestion currently only supports .csv files")
	}

	db, err := sql.Open("postgres", dbURL)
	if err != nil {
		return fmt.Errorf("failed to open postgres connection: %w", err)
	}
	defer db.Close()

	if err := db.PingContext(ctx); err != nil {
		return fmt.Errorf("failed to connect to postgres: %w", err)
	}

	f, err := os.Open(inputPath)
	if err != nil {
		return fmt.Errorf("cannot open input file: %w", err)
	}
	defer f.Close()

	info, _ := f.Stat()
	sizeMB := float64(info.Size()) / (1024 * 1024)

	reader := csv.NewReader(f)
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

	stmt, err := txn.Prepare(pq.CopyIn(table, header...))
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

		if _, err := stmt.Exec(args...); err != nil {
			return fmt.Errorf("failed to exec COPY at row %d: %w", rowCount+1, err)
		}
		rowCount++
	}

	if _, err := stmt.Exec(); err != nil {
		return fmt.Errorf("failed to flush COPY statement: %w", err)
	}

	if err := txn.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	fmt.Fprintf(stdout, "%sdb-load%s PostgreSQL: successfully ingested %d rows from %q into table %q\n",
		colorize(colorGreen), colorize(colorReset), rowCount, inputPath, table)
	return nil
}
