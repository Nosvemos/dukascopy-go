package cloud

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
)

// S3Config holds connection parameters for S3-compatible cloud storage
// (AWS S3, Cloudflare R2, MinIO, Wasabi, Google Cloud Storage S3 API).
type S3Config struct {
	Endpoint  string // e.g. "https://s3.amazonaws.com" or "http://localhost:9000" or Cloudflare R2
	Region    string // e.g. "us-east-1"
	AccessKey string // AWS_ACCESS_KEY_ID
	SecretKey string // AWS_SECRET_ACCESS_KEY
	Bucket    string
	Key       string
}

// IsCloudPath returns true if path starts with s3://, r2://, gcs://, or gs://.
func IsCloudPath(path string) bool {
	p := strings.ToLower(strings.TrimSpace(path))
	return strings.HasPrefix(p, "s3://") ||
		strings.HasPrefix(p, "r2://") ||
		strings.HasPrefix(p, "gcs://") ||
		strings.HasPrefix(p, "gs://")
}

// ParseCloudURI parses s3://bucket/path/to/key into S3Config.
func ParseCloudURI(rawURI string) (S3Config, error) {
	u, err := url.Parse(rawURI)
	if err != nil {
		return S3Config{}, fmt.Errorf("invalid cloud URI %q: %w", rawURI, err)
	}

	bucket := u.Host
	key := strings.TrimPrefix(u.Path, "/")
	if bucket == "" || key == "" {
		return S3Config{}, fmt.Errorf("cloud URI must be in format s3://bucket/key, got %q", rawURI)
	}

	region := os.Getenv("AWS_REGION")
	if region == "" {
		region = os.Getenv("AWS_DEFAULT_REGION")
	}
	if region == "" {
		region = "us-east-1"
	}

	endpoint := os.Getenv("AWS_ENDPOINT_URL")
	if endpoint == "" {
		endpoint = os.Getenv("S3_ENDPOINT")
	}
	if endpoint == "" {
		endpoint = fmt.Sprintf("https://s3.%s.amazonaws.com", region)
	}

	accessKey := os.Getenv("AWS_ACCESS_KEY_ID")
	secretKey := os.Getenv("AWS_SECRET_ACCESS_KEY")

	return S3Config{
		Endpoint:  endpoint,
		Region:    region,
		AccessKey: accessKey,
		SecretKey: secretKey,
		Bucket:    bucket,
		Key:       key,
	}, nil
}

// UploadFile uploads a local file to S3-compatible cloud storage using SigV4.
func UploadFile(ctx context.Context, cfg S3Config, localFilePath string) error {
	data, err := os.ReadFile(localFilePath)
	if err != nil {
		return fmt.Errorf("read local file %s: %w", localFilePath, err)
	}

	return UploadBytes(ctx, cfg, data)
}

// UploadBytes uploads a byte buffer directly to S3-compatible cloud storage.
func UploadBytes(ctx context.Context, cfg S3Config, payload []byte) error {
	endpoint := strings.TrimRight(cfg.Endpoint, "/")
	var reqURL string
	if strings.Contains(endpoint, "amazonaws.com") {
		reqURL = fmt.Sprintf("https://%s.s3.%s.amazonaws.com/%s", cfg.Bucket, cfg.Region, cfg.Key)
	} else {
		reqURL = fmt.Sprintf("%s/%s/%s", endpoint, cfg.Bucket, cfg.Key)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPut, reqURL, bytes.NewReader(payload))
	if err != nil {
		return err
	}

	now := time.Now().UTC()
	amzDate := now.Format("20060102T150405Z")
	dateStamp := now.Format("20060102")

	payloadHash := sha256Hex(payload)
	req.Header.Set("x-amz-date", amzDate)
	req.Header.Set("x-amz-content-sha256", payloadHash)
	req.Header.Set("Content-Length", fmt.Sprintf("%d", len(payload)))

	// Sign request if credentials are provided
	if cfg.AccessKey != "" && cfg.SecretKey != "" {
		canonicalURI := "/" + cfg.Key
		if !strings.Contains(endpoint, "amazonaws.com") {
			canonicalURI = "/" + cfg.Bucket + "/" + cfg.Key
		}

		canonicalHeaders := fmt.Sprintf("host:%s\nx-amz-content-sha256:%s\nx-amz-date:%s\n",
			req.URL.Host, payloadHash, amzDate)
		signedHeaders := "host;x-amz-content-sha256;x-amz-date"

		canonicalRequest := fmt.Sprintf("%s\n%s\n%s\n%s\n%s\n%s",
			http.MethodPut,
			canonicalURI,
			"", // query string
			canonicalHeaders,
			signedHeaders,
			payloadHash,
		)

		credentialScope := fmt.Sprintf("%s/%s/s3/aws4_request", dateStamp, cfg.Region)
		stringToSign := fmt.Sprintf("AWS4-HMAC-SHA256\n%s\n%s\n%s",
			amzDate,
			credentialScope,
			sha256Hex([]byte(canonicalRequest)),
		)

		signingKey := getSignatureKey(cfg.SecretKey, dateStamp, cfg.Region, "s3")
		signature := hex.EncodeToString(hmacSHA256(signingKey, []byte(stringToSign)))

		authHeader := fmt.Sprintf("AWS4-HMAC-SHA256 Credential=%s/%s, SignedHeaders=%s, Signature=%s",
			cfg.AccessKey, credentialScope, signedHeaders, signature)
		req.Header.Set("Authorization", authHeader)
	}

	client := &http.Client{Timeout: 60 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("s3 upload request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("s3 upload failed with status %d: %s", resp.StatusCode, string(body))
	}

	return nil
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
