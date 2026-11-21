package cloud

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// AWS documentation example credentials (not real secrets).
const (
	exampleAccessKey = "AKIAIOSFODNN7EXAMPLE"
	exampleSecretKey = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
)

// TestSignV4AWSPublishedExamples checks the signer against the worked examples
// in the Amazon S3 API reference ("Signature Calculations for the
// Authorization Header: Transferring Payload in a Single Chunk", bucket
// examplebucket, region us-east-1, 2013-05-24T00:00:00Z).
func TestSignV4AWSPublishedExamples(t *testing.T) {
	ts := time.Date(2013, 5, 24, 0, 0, 0, 0, time.UTC)

	// Example: GET Object (first 10 bytes of test.txt).
	get := signV4(http.MethodGet, "/test.txt", "", map[string]string{
		"host":                 "examplebucket.s3.amazonaws.com",
		"range":                "bytes=0-9",
		"x-amz-content-sha256": "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
		"x-amz-date":           "20130524T000000Z",
	}, "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
		exampleAccessKey, exampleSecretKey, "us-east-1", "s3", ts)
	wantGet := "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request, " +
		"SignedHeaders=host;range;x-amz-content-sha256;x-amz-date, " +
		"Signature=f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41"
	if get != wantGet {
		t.Errorf("GET example:\n got  %s\n want %s", get, wantGet)
	}

	// Example: PUT Object "test$file.text" -- the '$' must be encoded as %24
	// in the canonical URI.
	put := signV4(http.MethodPut, "/"+escapeKeyPath("test$file.text"), "", map[string]string{
		"date":                 "Fri, 24 May 2013 00:00:00 GMT",
		"host":                 "examplebucket.s3.amazonaws.com",
		"x-amz-content-sha256": "44ce7dd67c959e0d3524ffac1771dfbba87d2b6b4b4e99e42034a8b803f8b072",
		"x-amz-date":           "20130524T000000Z",
		"x-amz-storage-class":  "REDUCED_REDUNDANCY",
	}, "44ce7dd67c959e0d3524ffac1771dfbba87d2b6b4b4e99e42034a8b803f8b072",
		exampleAccessKey, exampleSecretKey, "us-east-1", "s3", ts)
	wantPut := "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request, " +
		"SignedHeaders=date;host;x-amz-content-sha256;x-amz-date;x-amz-storage-class, " +
		"Signature=98ad721746da40c64f1a55b78f14c238d841ea1380cd77a1b5971af0ece108bd"
	if put != wantPut {
		t.Errorf("PUT example:\n got  %s\n want %s", put, wantPut)
	}
}

func TestEscapeKeyPath(t *testing.T) {
	cases := map[string]string{
		"a b.csv":           "a%20b.csv",
		"a+b.csv":           "a%2Bb.csv",
		"dir/sub/x.parquet": "dir/sub/x.parquet",
		"ü/日.csv":           "%C3%BC/%E6%97%A5.csv",
		"x#y?z%41~_-.csv":   "x%23y%3Fz%2541~_-.csv",
		"test$file.text":    "test%24file.text",
		"a//b":              "a//b",
		"(1)!*'=;:@,&.csv":  "%281%29%21%2A%27%3D%3B%3A%40%2C%26.csv",
	}
	for in, want := range cases {
		if got := escapeKeyPath(in); got != want {
			t.Errorf("escapeKeyPath(%q) = %q, want %q", in, got, want)
		}
	}
}

// verifyingS3 is a fake S3 endpoint that independently recomputes the SigV4
// signature from what actually arrived on the wire (raw request URI, Host,
// headers) and rejects mismatches with SignatureDoesNotMatch, like S3 does.
type verifyingS3 struct {
	t       *testing.T
	secret  string
	region  string
	gotPath atomic.Value // decoded path
	gotBody atomic.Value
	hits    atomic.Int32
}

func (v *verifyingS3) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	v.hits.Add(1)
	body, _ := io.ReadAll(r.Body)
	v.gotBody.Store(string(body))
	v.gotPath.Store(r.URL.Path)

	if r.ContentLength != int64(len(body)) || len(r.TransferEncoding) != 0 {
		http.Error(w, "MissingContentLength", http.StatusLengthRequired)
		return
	}
	sum := sha256.Sum256(body)
	if r.Header.Get("x-amz-content-sha256") != hex.EncodeToString(sum[:]) {
		http.Error(w, "XAmzContentSHA256Mismatch", http.StatusBadRequest)
		return
	}

	auth := r.Header.Get("Authorization")
	_, signed, ok := strings.Cut(auth, "SignedHeaders=")
	if !ok {
		http.Error(w, "AccessDenied", http.StatusForbidden)
		return
	}
	signed, _, _ = strings.Cut(signed, ",")
	headers := map[string]string{}
	for _, h := range strings.Split(signed, ";") {
		if h == "host" {
			headers[h] = r.Host
		} else {
			headers[h] = r.Header.Get(h)
		}
	}
	ts, err := time.Parse("20060102T150405Z", r.Header.Get("x-amz-date"))
	if err != nil {
		http.Error(w, "bad date", http.StatusBadRequest)
		return
	}
	akid := strings.TrimPrefix(strings.SplitN(auth, "/", 2)[0], "AWS4-HMAC-SHA256 Credential=")
	rawPath, _, _ := strings.Cut(r.RequestURI, "?")
	want := signV4(r.Method, rawPath, "", headers, r.Header.Get("x-amz-content-sha256"), akid, v.secret, v.region, "s3", ts)
	if want != auth {
		http.Error(w, "SignatureDoesNotMatch", http.StatusForbidden)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func withFixedClock(t *testing.T, ts time.Time) {
	t.Helper()
	old := nowFunc
	nowFunc = func() time.Time { return ts }
	t.Cleanup(func() { nowFunc = old })
}

func TestUploadSpecialCharacterKeysAreSignedAsSent(t *testing.T) {
	ts := time.Date(2024, 3, 5, 6, 7, 8, 0, time.UTC)
	withFixedClock(t, ts)

	v := &verifyingS3{t: t, secret: exampleSecretKey, region: "us-east-1"}
	srv := httptest.NewServer(v)
	defer srv.Close()

	t.Setenv("AWS_ENDPOINT_URL", srv.URL)
	t.Setenv("S3_ENDPOINT", "")
	t.Setenv("AWS_REGION", "us-east-1")
	t.Setenv("AWS_ACCESS_KEY_ID", exampleAccessKey)
	t.Setenv("AWS_SECRET_ACCESS_KEY", exampleSecretKey)
	t.Setenv("AWS_SESSION_TOKEN", "")

	dir := t.TempDir()
	local := filepath.Join(dir, "in.csv")
	if err := os.WriteFile(local, []byte("ts,bid\n1,1.08\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	for _, key := range []string{
		"plain/eurusd.csv",
		"with space/eur usd.csv",
		"plus+sign.csv",
		"ünïcödé/日本.csv",
		"hash#frag?query%41.csv",
	} {
		cfg, err := ParseCloudURI("s3://my-bucket/" + key)
		if err != nil {
			t.Fatalf("ParseCloudURI(%q): %v", key, err)
		}
		if cfg.Key != key {
			t.Fatalf("key truncated/altered: got %q want %q", cfg.Key, key)
		}
		if err := UploadFile(context.Background(), cfg, local); err != nil {
			t.Errorf("upload %q: %v", key, err)
			continue
		}
		if got := v.gotPath.Load().(string); got != "/my-bucket/"+key {
			t.Errorf("server saw path %q, want %q", got, "/my-bucket/"+key)
		}
		if got := v.gotBody.Load().(string); got != "ts,bid\n1,1.08\n" {
			t.Errorf("body = %q", got)
		}
	}
}

func TestUploadSendsSignedSessionToken(t *testing.T) {
	withFixedClock(t, time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC))
	var token, auth, date string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		token = r.Header.Get("x-amz-security-token")
		auth = r.Header.Get("Authorization")
		date = r.Header.Get("x-amz-date")
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	cfg := S3Config{Endpoint: srv.URL, Region: "us-east-1", AccessKey: "AK", SecretKey: "SK", SessionToken: "FwoGZXIvYXdzEXAMPLE", Bucket: "b", Key: "k"}
	if err := UploadBytes(context.Background(), cfg, []byte("x")); err != nil {
		t.Fatal(err)
	}
	if token != "FwoGZXIvYXdzEXAMPLE" {
		t.Errorf("x-amz-security-token = %q", token)
	}
	if !strings.Contains(auth, "SignedHeaders=host;x-amz-content-sha256;x-amz-date;x-amz-security-token,") {
		t.Errorf("session token not signed: %s", auth)
	}
	if date != "20240102T030405Z" {
		t.Errorf("x-amz-date = %q (clock not injected?)", date)
	}

	t.Setenv("AWS_SESSION_TOKEN", "tok123")
	t.Setenv("AWS_ENDPOINT_URL", "http://127.0.0.1:1")
	parsed, err := ParseCloudURI("s3://b/k")
	if err != nil || parsed.SessionToken != "tok123" {
		t.Errorf("ParseCloudURI session token = %q, %v", parsed.SessionToken, err)
	}
}

func TestUploadWithoutCredentialsSendsNothing(t *testing.T) {
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	cfg := S3Config{Endpoint: srv.URL, Region: "us-east-1", Bucket: "b", Key: "k"}
	err := UploadBytes(context.Background(), cfg, []byte("x"))
	if err == nil || !strings.Contains(err.Error(), "AWS_ACCESS_KEY_ID") {
		t.Fatalf("expected missing-credentials error, got %v", err)
	}
	if hits.Load() != 0 {
		t.Fatalf("an unsigned request was sent")
	}
}

func TestParseCloudURISchemesAndEndpoints(t *testing.T) {
	for _, k := range []string{"AWS_ENDPOINT_URL", "S3_ENDPOINT", "AWS_REGION", "AWS_DEFAULT_REGION"} {
		t.Setenv(k, "")
	}

	if _, err := ParseCloudURI("r2://bucket/key.csv"); err == nil || !strings.Contains(err.Error(), "endpoint") {
		t.Errorf("r2 without endpoint: expected error, got %v", err)
	}
	for _, uri := range []string{"gs://bucket/key.csv", "gcs://bucket/key.csv", "GS://bucket/key.csv"} {
		cfg, err := ParseCloudURI(uri)
		if err != nil {
			t.Fatalf("%s: %v", uri, err)
		}
		if cfg.Endpoint != DefaultGCSEndpoint || cfg.Region != "auto" {
			t.Errorf("%s: endpoint %q region %q", uri, cfg.Endpoint, cfg.Region)
		}
	}
	cfg, err := ParseCloudURI("s3://bucket/key.csv")
	if err != nil || cfg.Endpoint != "https://s3.us-east-1.amazonaws.com" {
		t.Errorf("s3 default endpoint: %+v %v", cfg, err)
	}

	t.Setenv("AWS_ENDPOINT_URL", "https://acct.r2.cloudflarestorage.com")
	cfg, err = ParseCloudURI("r2://bucket/dir/key.csv")
	if err != nil || cfg.Endpoint != "https://acct.r2.cloudflarestorage.com" || cfg.Region != "auto" || cfg.Key != "dir/key.csv" {
		t.Errorf("r2 with endpoint: %+v %v", cfg, err)
	}

	for _, bad := range []string{"http://bucket/key", "bucket/key", "s3://bu?cket/key", "s3:///key"} {
		if _, err := ParseCloudURI(bad); err == nil {
			t.Errorf("ParseCloudURI(%q) expected error", bad)
		}
	}
}

func TestRequestTargetStyles(t *testing.T) {
	base, p, err := requestTarget(S3Config{Endpoint: "https://s3.eu-west-1.amazonaws.com", Region: "eu-west-1", Bucket: "bkt", Key: "a b/c"})
	if err != nil || base.String() != "https://bkt.s3.eu-west-1.amazonaws.com" || p != "/a%20b/c" {
		t.Errorf("virtual-hosted: %v %q %v", base, p, err)
	}
	base, p, err = requestTarget(S3Config{Endpoint: "https://s3.eu-west-1.amazonaws.com", Region: "eu-west-1", Bucket: "my.dotted.bkt", Key: "k"})
	if err != nil || base.String() != "https://s3.eu-west-1.amazonaws.com" || p != "/my.dotted.bkt/k" {
		t.Errorf("dotted bucket: %v %q %v", base, p, err)
	}
	base, p, err = requestTarget(S3Config{Endpoint: "http://minio:9000/prefix/", Bucket: "bkt", Key: "k+1"})
	if err != nil || base.String() != "http://minio:9000" || p != "/prefix/bkt/k%2B1" {
		t.Errorf("path-style with prefix: %v %q %v", base, p, err)
	}
	if _, _, err := requestTarget(S3Config{Endpoint: "minio:9000", Bucket: "b", Key: "k"}); err == nil {
		t.Errorf("expected error for endpoint without scheme")
	}
}

func TestUploadLimitsAndTimeouts(t *testing.T) {
	if err := checkUploadSize(MaxSinglePutSize); err != nil {
		t.Errorf("5 GiB exactly should be allowed: %v", err)
	}
	if err := checkUploadSize(MaxSinglePutSize + 1); err == nil || !strings.Contains(err.Error(), "5 GiB") {
		t.Errorf("expected 5 GiB limit error, got %v", err)
	}
	if uploadClient.Timeout != 0 {
		t.Errorf("upload client must not have an overall timeout (got %v); rely on ctx", uploadClient.Timeout)
	}
	tr := uploadClient.Transport.(*http.Transport)
	if tr.TLSHandshakeTimeout == 0 || tr.ResponseHeaderTimeout == 0 {
		t.Errorf("transport phase timeouts not set")
	}

	// Error bodies are bounded.
	big := strings.Repeat("x", 1<<20)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.WriteHeader(http.StatusForbidden)
		_, _ = io.WriteString(w, big)
	}))
	defer srv.Close()
	err := UploadBytes(context.Background(), S3Config{Endpoint: srv.URL, Region: "r", AccessKey: "a", SecretKey: "s", Bucket: "b", Key: "k"}, []byte("x"))
	if err == nil || len(err.Error()) > 70<<10 {
		t.Errorf("error body not bounded (len %d)", len(err.Error()))
	}

	// Empty files upload with Content-Length: 0 (not chunked).
	var cl int64 = -2
	var te []string
	srv2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cl, te = r.ContentLength, r.TransferEncoding
		w.WriteHeader(http.StatusOK)
	}))
	defer srv2.Close()
	empty := filepath.Join(t.TempDir(), "empty.csv")
	if err := os.WriteFile(empty, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := UploadFile(context.Background(), S3Config{Endpoint: srv2.URL, Region: "r", AccessKey: "a", SecretKey: "s", Bucket: "b", Key: "k"}, empty); err != nil {
		t.Fatal(err)
	}
	if cl != 0 || len(te) != 0 {
		t.Errorf("empty upload: content-length %d, transfer-encoding %v", cl, te)
	}
}
