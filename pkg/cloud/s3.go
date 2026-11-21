package cloud

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strings"
	"time"
)

// MaxSinglePutSize is the largest object S3 accepts in a single PUT request
// (5 GiB). Larger uploads need multipart upload, which is not implemented.
const MaxSinglePutSize int64 = 5 << 30

// DefaultGCSEndpoint is used for gs:// and gcs:// URIs when no endpoint
// environment variable is set (Cloud Storage XML API with HMAC keys).
const DefaultGCSEndpoint = "https://storage.googleapis.com"

// S3Config holds connection parameters for S3-compatible cloud storage
// (AWS S3, Cloudflare R2, MinIO, Wasabi, Google Cloud Storage S3 API).
type S3Config struct {
	Endpoint  string // e.g. "https://s3.amazonaws.com" or "http://localhost:9000" or Cloudflare R2
	Region    string // e.g. "us-east-1"
	AccessKey string // AWS_ACCESS_KEY_ID
	SecretKey string // AWS_SECRET_ACCESS_KEY
	Bucket    string
	Key       string // object key, used verbatim (not percent-decoded)

	// SessionToken is the optional AWS_SESSION_TOKEN for temporary
	// credentials; when set it is sent (and signed) as x-amz-security-token.
	SessionToken string
}

// nowFunc is the clock used for request signing; tests replace it.
var nowFunc = time.Now

// uploadClient has per-phase timeouts but no overall deadline, so large
// uploads on slow links are bounded only by the caller's context.
var uploadClient = &http.Client{
	Transport: &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		ForceAttemptHTTP2:     true,
		TLSHandshakeTimeout:   15 * time.Second,
		ResponseHeaderTimeout: 2 * time.Minute,
		ExpectContinueTimeout: 1 * time.Second,
		MaxIdleConns:          10,
		IdleConnTimeout:       90 * time.Second,
	},
}

// IsCloudPath returns true if path starts with s3://, r2://, gcs://, or gs://.
func IsCloudPath(path string) bool {
	p := strings.ToLower(strings.TrimSpace(path))
	return strings.HasPrefix(p, "s3://") ||
		strings.HasPrefix(p, "r2://") ||
		strings.HasPrefix(p, "gcs://") ||
		strings.HasPrefix(p, "gs://")
}

// ParseCloudURI parses scheme://bucket/path/to/key into S3Config.
//
// The URI is split literally: everything after the first "/" following the
// bucket is the object key, so characters such as '#', '?', '%', '+' and
// spaces are part of the key (no URL decoding).
//
// The endpoint comes from AWS_ENDPOINT_URL or S3_ENDPOINT. Without one,
// s3:// uses AWS (https://s3.<region>.amazonaws.com), gs:// and gcs:// use
// https://storage.googleapis.com, and r2:// is rejected because an R2
// endpoint is account specific (https://<account_id>.r2.cloudflarestorage.com).
// Region comes from AWS_REGION or AWS_DEFAULT_REGION (default "us-east-1" for
// s3://, "auto" for r2:// and gs://). Credentials come from AWS_ACCESS_KEY_ID,
// AWS_SECRET_ACCESS_KEY and the optional AWS_SESSION_TOKEN.
func ParseCloudURI(rawURI string) (S3Config, error) {
	trimmed := strings.TrimSpace(rawURI)
	scheme, rest, ok := strings.Cut(trimmed, "://")
	scheme = strings.ToLower(scheme)
	if !ok {
		return S3Config{}, fmt.Errorf("invalid cloud URI %q: missing scheme", rawURI)
	}
	switch scheme {
	case "s3", "r2", "gs", "gcs":
	default:
		return S3Config{}, fmt.Errorf("invalid cloud URI %q: unsupported scheme %q (supported: s3, r2, gs, gcs)", rawURI, scheme)
	}

	bucket, key, _ := strings.Cut(rest, "/")
	if bucket == "" || key == "" {
		return S3Config{}, fmt.Errorf("cloud URI must be in format %s://bucket/key, got %q", scheme, rawURI)
	}
	if strings.ContainsAny(bucket, "?#%@ \\") {
		return S3Config{}, fmt.Errorf("invalid bucket name %q in cloud URI %q", bucket, rawURI)
	}

	region := os.Getenv("AWS_REGION")
	if region == "" {
		region = os.Getenv("AWS_DEFAULT_REGION")
	}

	endpoint := os.Getenv("AWS_ENDPOINT_URL")
	if endpoint == "" {
		endpoint = os.Getenv("S3_ENDPOINT")
	}

	switch scheme {
	case "s3":
		if region == "" {
			region = "us-east-1"
		}
		if endpoint == "" {
			endpoint = fmt.Sprintf("https://s3.%s.amazonaws.com", region)
		}
	case "r2":
		if region == "" {
			region = "auto"
		}
		if endpoint == "" {
			return S3Config{}, fmt.Errorf("r2:// requires an explicit endpoint: set AWS_ENDPOINT_URL (or S3_ENDPOINT) to https://<account_id>.r2.cloudflarestorage.com")
		}
	case "gs", "gcs":
		if region == "" {
			region = "auto"
		}
		if endpoint == "" {
			endpoint = DefaultGCSEndpoint
		}
	}

	return S3Config{
		Endpoint:     endpoint,
		Region:       region,
		AccessKey:    os.Getenv("AWS_ACCESS_KEY_ID"),
		SecretKey:    os.Getenv("AWS_SECRET_ACCESS_KEY"),
		SessionToken: os.Getenv("AWS_SESSION_TOKEN"),
		Bucket:       bucket,
		Key:          key,
	}, nil
}

// UploadFile uploads a local file to S3-compatible cloud storage using SigV4.
// The file is streamed from disk (read once to compute the SHA-256 payload
// hash, then again for the upload); it is never loaded into memory. Files
// larger than MaxSinglePutSize (5 GiB) are rejected.
func UploadFile(ctx context.Context, cfg S3Config, localFilePath string) error {
	f, err := os.Open(localFilePath)
	if err != nil {
		return fmt.Errorf("read local file %s: %w", localFilePath, err)
	}
	defer f.Close()

	info, err := f.Stat()
	if err != nil {
		return fmt.Errorf("stat local file %s: %w", localFilePath, err)
	}
	size := info.Size()
	if err := checkUploadSize(size); err != nil {
		return fmt.Errorf("%s: %w", localFilePath, err)
	}

	h := sha256.New()
	if _, err := io.Copy(h, io.LimitReader(f, size)); err != nil {
		return fmt.Errorf("hash local file %s: %w", localFilePath, err)
	}
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("rewind local file %s: %w", localFilePath, err)
	}

	return upload(ctx, cfg, io.LimitReader(f, size), size, hex.EncodeToString(h.Sum(nil)))
}

// UploadBytes uploads a byte buffer directly to S3-compatible cloud storage.
func UploadBytes(ctx context.Context, cfg S3Config, payload []byte) error {
	size := int64(len(payload))
	if err := checkUploadSize(size); err != nil {
		return err
	}
	return upload(ctx, cfg, bytes.NewReader(payload), size, sha256Hex(payload))
}

func checkUploadSize(size int64) error {
	if size > MaxSinglePutSize {
		return fmt.Errorf("object is %d bytes, larger than the 5 GiB single-PUT limit; multipart upload is not supported", size)
	}
	return nil
}

// requestTarget returns the request URL (scheme+host) and the escaped request
// path (which is also the SigV4 canonical URI) for cfg.
func requestTarget(cfg S3Config) (base *url.URL, escapedPath string, err error) {
	endpoint := strings.TrimRight(strings.TrimSpace(cfg.Endpoint), "/")
	if endpoint == "" {
		return nil, "", errors.New("s3 upload: empty endpoint")
	}
	if strings.Contains(endpoint, "amazonaws.com") && !strings.Contains(cfg.Bucket, ".") {
		// Virtual-hosted style for AWS (bucket names with dots stay
		// path-style so the TLS certificate still matches).
		base = &url.URL{Scheme: "https", Host: fmt.Sprintf("%s.s3.%s.amazonaws.com", cfg.Bucket, cfg.Region)}
		return base, "/" + escapeKeyPath(cfg.Key), nil
	}

	base, err = url.Parse(endpoint)
	if err != nil || base.Host == "" || (base.Scheme != "http" && base.Scheme != "https") {
		return nil, "", fmt.Errorf("s3 upload: invalid endpoint %q (want http(s)://host[:port])", cfg.Endpoint)
	}
	prefix := escapeKeyPath(strings.Trim(base.Path, "/"))
	if prefix != "" {
		prefix = "/" + prefix
	}
	base = &url.URL{Scheme: base.Scheme, Host: base.Host}
	return base, prefix + "/" + escapeSegment(cfg.Bucket) + "/" + escapeKeyPath(cfg.Key), nil
}

func upload(ctx context.Context, cfg S3Config, body io.Reader, size int64, payloadHash string) error {
	if cfg.AccessKey == "" || cfg.SecretKey == "" {
		return errors.New("s3 upload: no credentials; set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY (and AWS_SESSION_TOKEN for temporary credentials)")
	}

	base, escapedPath, err := requestTarget(cfg)
	if err != nil {
		return err
	}
	decodedPath, err := url.PathUnescape(escapedPath)
	if err != nil {
		return fmt.Errorf("s3 upload: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPut, base.String(), nil)
	if err != nil {
		return err
	}
	// Path and RawPath must agree so net/http sends exactly escapedPath,
	// which is also the canonical URI that gets signed.
	req.URL.Path = decodedPath
	req.URL.RawPath = escapedPath
	if req.URL.EscapedPath() != escapedPath {
		return fmt.Errorf("s3 upload: cannot encode object key %q", cfg.Key)
	}
	if size == 0 {
		req.Body = http.NoBody
	} else {
		req.Body = io.NopCloser(body)
	}
	req.ContentLength = size

	now := nowFunc().UTC()
	amzDate := now.Format("20060102T150405Z")

	headers := map[string]string{
		"host":                 req.URL.Host,
		"x-amz-content-sha256": payloadHash,
		"x-amz-date":           amzDate,
	}
	if cfg.SessionToken != "" {
		headers["x-amz-security-token"] = cfg.SessionToken
	}
	for k, v := range headers {
		if k != "host" {
			req.Header.Set(k, v)
		}
	}
	req.Header.Set("Authorization", signV4(http.MethodPut, escapedPath, "", headers, payloadHash,
		cfg.AccessKey, cfg.SecretKey, cfg.Region, "s3", now))

	resp, err := uploadClient.Do(req)
	if err != nil {
		return fmt.Errorf("s3 upload request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
		return fmt.Errorf("s3 upload failed with status %d: %s", resp.StatusCode, strings.TrimSpace(string(respBody)))
	}
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10))

	return nil
}

// signV4 returns the AWS Signature Version 4 Authorization header value.
// headers maps lower-case header names to values; all of them are signed.
// canonicalURI must already be URI-encoded exactly as sent on the wire.
func signV4(method, canonicalURI, canonicalQuery string, headers map[string]string, payloadHash,
	accessKey, secretKey, region, service string, t time.Time,
) string {
	t = t.UTC()
	amzDate := t.Format("20060102T150405Z")
	dateStamp := t.Format("20060102")

	names := make([]string, 0, len(headers))
	for k := range headers {
		names = append(names, strings.ToLower(k))
	}
	sort.Strings(names)
	lower := make(map[string]string, len(headers))
	for k, v := range headers {
		lower[strings.ToLower(k)] = strings.Join(strings.Fields(v), " ")
	}
	var canonicalHeaders strings.Builder
	for _, k := range names {
		canonicalHeaders.WriteString(k + ":" + lower[k] + "\n")
	}
	signedHeaders := strings.Join(names, ";")

	canonicalRequest := strings.Join([]string{
		method,
		canonicalURI,
		canonicalQuery,
		canonicalHeaders.String(),
		signedHeaders,
		payloadHash,
	}, "\n")

	credentialScope := dateStamp + "/" + region + "/" + service + "/aws4_request"
	stringToSign := "AWS4-HMAC-SHA256\n" + amzDate + "\n" + credentialScope + "\n" + sha256Hex([]byte(canonicalRequest))

	signingKey := getSignatureKey(secretKey, dateStamp, region, service)
	signature := hex.EncodeToString(hmacSHA256(signingKey, []byte(stringToSign)))

	return fmt.Sprintf("AWS4-HMAC-SHA256 Credential=%s/%s, SignedHeaders=%s, Signature=%s",
		accessKey, credentialScope, signedHeaders, signature)
}

// escapeKeyPath URI-encodes each "/"-separated segment of an object key
// (S3 canonical URIs are encoded once, slashes kept).
func escapeKeyPath(key string) string {
	segments := strings.Split(key, "/")
	for i, s := range segments {
		segments[i] = escapeSegment(s)
	}
	return strings.Join(segments, "/")
}

// escapeSegment percent-encodes every byte outside the RFC 3986 unreserved
// set (A-Z a-z 0-9 - _ . ~) with upper-case hex, as SigV4 requires.
func escapeSegment(s string) string {
	const hexDigits = "0123456789ABCDEF"
	var sb strings.Builder
	sb.Grow(len(s))
	for i := 0; i < len(s); i++ {
		c := s[i]
		if ('A' <= c && c <= 'Z') || ('a' <= c && c <= 'z') || ('0' <= c && c <= '9') ||
			c == '-' || c == '_' || c == '.' || c == '~' {
			sb.WriteByte(c)
			continue
		}
		sb.WriteByte('%')
		sb.WriteByte(hexDigits[c>>4])
		sb.WriteByte(hexDigits[c&0x0F])
	}
	return sb.String()
}

func sha256Hex(data []byte) string {
	hash := sha256.Sum256(data)
	return hex.EncodeToString(hash[:])
}

func hmacSHA256(key []byte, data []byte) []byte {
	h := hmac.New(sha256.New, key)
	h.Write(data)
	return h.Sum(nil)
}

func getSignatureKey(key, dateStamp, regionName, serviceName string) []byte {
	kDate := hmacSHA256([]byte("AWS4"+key), []byte(dateStamp))
	kRegion := hmacSHA256(kDate, []byte(regionName))
	kService := hmacSHA256(kRegion, []byte(serviceName))
	kSigning := hmacSHA256(kService, []byte("aws4_request"))
	return kSigning
}
