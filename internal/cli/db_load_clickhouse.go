package cli

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
)

// ingestClickHouse streams the local file directly to ClickHouse via HTTP.
// ClickHouse accepts CSV and Parquet natively at millions of rows/second.
func ingestClickHouse(
	ctx context.Context,
	stdout io.Writer,
	stderr io.Writer,
	inputPath string,
	rawURL string,
	table string,
	user string,
	password string,
	timeout time.Duration,
) error {
	format, err := detectClickHouseFormat(inputPath)
	if err != nil {
		return err
	}

	f, err := os.Open(inputPath)
	if err != nil {
		return fmt.Errorf("cannot open input file: %w", err)
	}
	defer f.Close()

	info, _ := f.Stat()
	sizeMB := float64(info.Size()) / (1024 * 1024)

	query := fmt.Sprintf("INSERT INTO `%s` FORMAT %s", strings.ReplaceAll(table, "`", "\\`"), format)
	endpoint := buildClickHouseURL(rawURL, query, user, password)

	fmt.Fprintf(stderr, "%sdb-load%s streaming %.1f MB to ClickHouse table %q [%s]...\n",
		colorize(colorCyan), colorize(colorReset), sizeMB, table, format)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, f)
	if err != nil {
		return fmt.Errorf("failed to build ClickHouse request: %w", err)
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	if format == "CSV" || format == "CSVWithNames" {
		req.Header.Set("Content-Type", "text/csv")
	} else {
		req.Header.Set("Content-Type", "application/octet-stream")
	}
	if strings.HasSuffix(strings.ToLower(inputPath), ".gz") {
		req.Header.Set("Content-Encoding", "gzip")
	}

	httpClient := &http.Client{Timeout: timeout}
	resp, err := httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("ClickHouse HTTP request failed: %w", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("ClickHouse returned %s: %s", resp.Status, strings.TrimSpace(string(body)))
	}

	fmt.Fprintf(stdout, "%sdb-load%s ClickHouse: successfully ingested %q into table %q\n",
		colorize(colorGreen), colorize(colorReset), inputPath, table)
	return nil
}

func buildClickHouseURL(rawURL string, query string, user string, password string) string {
	base := strings.TrimRight(strings.TrimSpace(rawURL), "/")
	u, err := url.Parse(base)
	if err != nil {
		return base + "/?query=" + url.QueryEscape(query)
	}
	q := u.Query()
	q.Set("query", query)
	if user != "" && user != "default" {
		q.Set("user", user)
	}
	if password != "" {
		q.Set("password", password)
	}
	u.RawQuery = q.Encode()
	return u.String()
}

func detectClickHouseFormat(path string) (string, error) {
	lower := strings.ToLower(path)
	switch {
	case strings.HasSuffix(lower, ".parquet"):
		return "Parquet", nil
	case strings.HasSuffix(lower, ".csv.gz"):
		return "CSVWithNames", nil
	case strings.HasSuffix(lower, ".csv"):
		return "CSVWithNames", nil
	default:
		return "", fmt.Errorf("unsupported file type for ClickHouse ingestion: %q (supported: .csv, .csv.gz, .parquet)", path)
	}
}
