package cloud

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
)

func TestParseCloudURI(t *testing.T) {
	uri := "s3://my-quant-bucket/forex/eurusd_m1.parquet"
	cfg, err := ParseCloudURI(uri)
	if err != nil {
		t.Fatalf("ParseCloudURI: %v", err)
	}

	if cfg.Bucket != "my-quant-bucket" {
		t.Errorf("expected bucket my-quant-bucket, got %s", cfg.Bucket)
	}
	if cfg.Key != "forex/eurusd_m1.parquet" {
		t.Errorf("expected key forex/eurusd_m1.parquet, got %s", cfg.Key)
	}
}

func TestS3MockUpload(t *testing.T) {
	var receivedBody []byte
	var receivedAuth string

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		receivedAuth = r.Header.Get("Authorization")
		data, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		receivedBody = data
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	cfg := S3Config{
		Endpoint:  server.URL,
		Region:    "us-east-1",
		AccessKey: "TESTKEY",
		SecretKey: "TESTSECRET",
		Bucket:    "test-bucket",
		Key:       "data.parquet",
	}

	dir := t.TempDir()
	testFile := filepath.Join(dir, "local.parquet")
	if err := os.WriteFile(testFile, []byte("PARQUET_TEST_DATA"), 0o644); err != nil {
		t.Fatalf("write temp file: %v", err)
	}

	if err := UploadFile(context.Background(), cfg, testFile); err != nil {
		t.Fatalf("UploadFile failed: %v", err)
	}

	if string(receivedBody) != "PARQUET_TEST_DATA" {
		t.Errorf("unexpected uploaded content: %s", string(receivedBody))
	}
	if receivedAuth == "" {
		t.Errorf("expected SigV4 Authorization header, got empty")
	}

	// Server error status
	errServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "access denied", http.StatusForbidden)
	}))
	defer errServer.Close()

	cfg.Endpoint = errServer.URL
	if err := UploadBytes(context.Background(), cfg, []byte("data")); err == nil {
		t.Errorf("expected error on 403 response")
	}

	// Nonexistent file
	if err := UploadFile(context.Background(), cfg, filepath.Join(dir, "nonexistent.parquet")); err == nil {
		t.Errorf("expected error reading nonexistent file")
	}
}

func TestIsCloudPath(t *testing.T) {
	if !IsCloudPath("s3://bucket/key") {
		t.Errorf("expected true for s3://")
	}
	if !IsCloudPath("r2://bucket/key") {
		t.Errorf("expected true for r2://")
	}
	if !IsCloudPath("gcs://bucket/key") {
		t.Errorf("expected true for gcs://")
	}
	if !IsCloudPath("gs://bucket/key") {
		t.Errorf("expected true for gs://")
	}
	if IsCloudPath("/local/path/file.csv") {
		t.Errorf("expected false for local path")
	}
}

func TestParseCloudURIExtended(t *testing.T) {
	t.Setenv("AWS_REGION", "")
	t.Setenv("AWS_DEFAULT_REGION", "eu-central-1")
	t.Setenv("AWS_ENDPOINT_URL", "")
	t.Setenv("S3_ENDPOINT", "http://minio:9000")
	t.Setenv("AWS_ACCESS_KEY_ID", "MYKEY")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "MYSECRET")

	cfg, err := ParseCloudURI("s3://quant-bucket/data/bars.parquet")
	if err != nil {
		t.Fatalf("ParseCloudURI failed: %v", err)
	}
	if cfg.Region != "eu-central-1" || cfg.Endpoint != "http://minio:9000" || cfg.AccessKey != "MYKEY" {
		t.Errorf("unexpected config: %+v", cfg)
	}

	// Invalid URIs
	if _, err := ParseCloudURI("s3://"); err == nil {
		t.Errorf("expected error for empty bucket/key")
	}
	if _, err := ParseCloudURI("s3://bucket_only"); err == nil {
		t.Errorf("expected error for missing key")
	}
}
