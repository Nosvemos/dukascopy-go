package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
)

// ingestClickHouse streams the local file directly to ClickHouse via HTTP.
// ClickHouse accepts CSV and Parquet natively at millions of rows/second.
//
// Credentials travel in the X-ClickHouse-User / X-ClickHouse-Key headers, never
// in the URL, so they do not end up in server or proxy access logs or in error
// messages. The whole file is sent as one request, so the client has no overall
// timeout: `timeout` bounds connecting, the TLS handshake and waiting for the
// response headers once the upload has finished.
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

	quotedTable, err := quoteClickHouseTable(table)
	if err != nil {
		return err
	}

	endpointURL, urlUser, urlPassword, err := parseClickHouseEndpoint(rawURL)
	if err != nil {
		return err
	}
	if urlUser != "" && (strings.TrimSpace(user) == "" || user == "default") {
		user = urlUser
	}
	if password == "" {
		password = urlPassword
	}

	f, err := os.Open(inputPath)
	if err != nil {
		return fmt.Errorf("cannot open input file: %w", err)
	}
	defer f.Close()

	info, err := f.Stat()
	if err != nil {
		return fmt.Errorf("cannot stat input file: %w", err)
	}
	sizeMB := float64(info.Size()) / (1024 * 1024)

	query := fmt.Sprintf("INSERT INTO %s FORMAT %s", quotedTable, format)
	endpoint := buildClickHouseURL(endpointURL, query)

	fmt.Fprintf(stderr, "%sdb-load%s streaming %.1f MB to ClickHouse table %q [%s]...\n",
		colorize(colorCyan), colorize(colorReset), sizeMB, table, format)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, f)
	if err != nil {
		return fmt.Errorf("failed to build ClickHouse request: %s", redactSecret(err.Error(), password))
	}
	req.ContentLength = info.Size()
	if format == "CSV" || format == "CSVWithNames" {
		req.Header.Set("Content-Type", "text/csv")
	} else {
		req.Header.Set("Content-Type", "application/octet-stream")
	}
	if strings.HasSuffix(strings.ToLower(inputPath), ".gz") {
		req.Header.Set("Content-Encoding", "gzip")
	}
	if user != "" {
		req.Header.Set("X-ClickHouse-User", user)
	}
	if password != "" {
		req.Header.Set("X-ClickHouse-Key", password)
	}

	httpClient := newClickHouseHTTPClient(timeout)
	defer httpClient.CloseIdleConnections()
	resp, err := httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("ClickHouse HTTP request failed: %s", sanitizeClickHouseError(err, password))
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("ClickHouse returned %s: %s", resp.Status, redactSecret(strings.TrimSpace(string(body)), password))
	}

	fmt.Fprintf(stdout, "%sdb-load%s ClickHouse: successfully ingested %q into table %q\n",
		colorize(colorGreen), colorize(colorReset), inputPath, table)
	return nil
}

// newClickHouseHTTPClient returns a client without an overall request timeout
// (which would also cap the upload of a large file) but with transport-level
// dial, TLS handshake and response-header timeouts.
func newClickHouseHTTPClient(timeout time.Duration) *http.Client {
	if timeout <= 0 {
		timeout = defaultDBLoadTimeout
	}
	transport := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   timeout,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		TLSHandshakeTimeout:   timeout,
		ResponseHeaderTimeout: timeout,
		ExpectContinueTimeout: time.Second,
		ForceAttemptHTTP2:     true,
	}
	return &http.Client{Transport: transport}
}

// parseClickHouseEndpoint validates the base URL and strips any user:password
// userinfo from it, returning those credentials separately so they can be sent
// as headers instead.
func parseClickHouseEndpoint(rawURL string) (string, string, string, error) {
	base := strings.TrimRight(strings.TrimSpace(rawURL), "/")
	u, err := url.Parse(base)
	if err != nil || u.Scheme == "" || u.Host == "" {
		return "", "", "", fmt.Errorf("invalid ClickHouse --url %q: expected http(s)://host:port", redactURLUserinfo(base))
	}
	var user, password string
	if u.User != nil {
		user = u.User.Username()
		password, _ = u.User.Password()
		u.User = nil
	}
	return u.String(), user, password, nil
}

// buildClickHouseURL appends the INSERT query to the endpoint. Credentials are
// deliberately not part of the URL.
func buildClickHouseURL(rawURL string, query string) string {
	base := strings.TrimRight(strings.TrimSpace(rawURL), "/")
	u, err := url.Parse(base)
	if err != nil {
		return base + "/?query=" + url.QueryEscape(query)
	}
	u.User = nil
	q := u.Query()
	q.Set("query", query)
	u.RawQuery = q.Encode()
	return u.String()
}

// quoteClickHouseTable quotes a possibly database-qualified table name
// ("db.table") as `db`.`table`.
func quoteClickHouseTable(table string) (string, error) {
	parts := strings.Split(strings.TrimSpace(table), ".")
	quoted := make([]string, 0, len(parts))
	for _, part := range parts {
		if part == "" {
			return "", fmt.Errorf("invalid ClickHouse table name %q: empty identifier", table)
		}
		quoted = append(quoted, "`"+strings.ReplaceAll(strings.ReplaceAll(part, `\`, `\\`), "`", "\\`")+"`")
	}
	if len(quoted) > 2 {
		return "", fmt.Errorf("invalid ClickHouse table name %q: expected table or database.table", table)
	}
	return strings.Join(quoted, "."), nil
}

// sanitizeClickHouseError renders a transport error without the request URL
// (which carries the query) and without any credential.
func sanitizeClickHouseError(err error, password string) string {
	var urlErr *url.Error
	if errors.As(err, &urlErr) {
		err = urlErr.Err
	}
	return redactSecret(err.Error(), password)
}

func redactSecret(message string, secret string) string {
	if secret == "" {
		return message
	}
	message = strings.ReplaceAll(message, secret, "***")
	if escaped := url.QueryEscape(secret); escaped != secret {
		message = strings.ReplaceAll(message, escaped, "***")
	}
	return message
}

func redactURLUserinfo(raw string) string {
	if at := strings.LastIndex(raw, "@"); at >= 0 {
		if scheme := strings.Index(raw, "://"); scheme >= 0 && scheme < at {
			return raw[:scheme+3] + "***@" + raw[at+1:]
		}
	}
	return raw
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
