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
}
