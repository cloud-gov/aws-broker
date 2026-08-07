package elasticsearch

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"testing"

	"github.com/cloud-gov/aws-broker/testutil"
	"github.com/opensearch-project/opensearch-go/v2"
)

var bucket = "mys3bucket"
var path = "foo/bar/baz"

var repoName = "my-snapshots"

var snapshotName = "backup"
var region = "us-east-1"
var rolearn = "arn:aws:iam::123456789012:role/snapshot-role"

type recordedRequest struct {
	method string
	path   string
	body   string
}

type MockRoundTripper struct {
	getResponse string
	statusCode  int
	err         error
	requests    []recordedRequest
}

// RoundTrip implements the http.RoundTripper interface
func (m *MockRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	if m.err != nil {
		return nil, m.err
	}

	rec := recordedRequest{method: req.Method, path: req.URL.Path}
	if req.Body != nil {
		b, _ := io.ReadAll(req.Body)
		rec.body = string(b)
	}
	m.requests = append(m.requests, rec)

	status := m.statusCode
	if status == 0 {
		status = http.StatusOK
	}

	body := `{"status":"OK"}`
	if req.Method == http.MethodGet {
		body = m.getResponse
	}

	header := make(http.Header)
	header.Set("Content-Type", "application/json")
	return &http.Response{
		StatusCode: status,
		Body:       io.NopCloser(bytes.NewBufferString(body)),
		Header:     header,
	}, nil
}

func NewTestEsAPIHandler(client *opensearch.Client, logger *slog.Logger) *EsApiHandler {
	return &EsApiHandler{
		ctx:              context.Background(),
		opensearchClient: client,
		logger:           logger,
	}
}

func TestAuditApiBasePath(t *testing.T) {
	cases := map[string]string{
		"OpenSearch_2.3":    "/_plugins/_security/api/audit",
		"OpenSearch_1.3":    "/_plugins/_security/api/audit",
		"openSearch_1.3":    "/_plugins/_security/api/audit",
		"Elasticsearch_7.4": "/_opendistro/_security/api/audit",
		"elasticsearch_7.4": "/_opendistro/_security/api/audit",
		"7.4":               "/_opendistro/_security/api/audit",
	}

	for version, exp := range cases {
		if got := auditApiBasePath(version); got != exp {
			t.Errorf("auditApiBasePath(%q) = %q, exp %q", version, got, exp)
		}
	}
}

func TestEnableAuditLogging(t *testing.T) {
	getConfig := `{"config": {"enabled": false, "audit": {"enable_rest": false, "ignore_users": ["kibanaserver"]}, "compliance": {"enabled": false}}}`

	tripper := &MockRoundTripper{getResponse: getConfig}
	client, err := opensearch.NewClient(opensearch.Config{
		Addresses: []string{"https://fake-domain"},
		Transport: tripper,
	})
	if err != nil {
		t.Fatal(err)
	}

	es := NewTestEsAPIHandler(client, slog.New(&testutil.MockLogHandler{}))

	if err := es.EnableAuditLogging("OpenSearch_2.3"); err != nil {
		t.Fatal(err)
	}

	if len(tripper.requests) != 2 {
		t.Fatalf("expected a 2 requests (GET, PUT), got %d: %+v", len(tripper.requests), tripper.requests)
	}

	get := tripper.requests[0]
	if get.method != http.MethodGet || get.path != "/_plugins/_security/api/audit" {
		t.Errorf("expected GET /_plugins/_security/api/audit, got %s %s", get.method, get.path)
	}

	put := tripper.requests[1]
	if put.method != http.MethodPut || put.path != "/_plugins/_security/api/audit/config" {
		t.Errorf("expected PUT /_plugins/_security/api/audit/config, got %s %s", put.method, put.path)
	}

	var body map[string]any
	if err := json.Unmarshal([]byte(put.body), &body); err != nil {
		t.Fatalf("PUT body is not valid JSON: %s", err)
	}
	if body["enabled"] != true {
		t.Errorf("expected enabled=true in PUT body, got %v", body["enabled"])
	}
	auditSection, ok := body["audit"].(map[string]any)
	if !ok {
		t.Fatalf("expected audit section in PUT body, got %v", body["audit"])
	}
	if auditSection["enable_rest"] != true {
		t.Errorf("expected audit.enable_rest = true, got %v", auditSection["enable_rest"])
	}
}

func TestNewSnapShotRepo(t *testing.T) {
	snaprepo := NewSnapshotRepo(bucket, path, region, rolearn)

	if snaprepo != nil {
		if snaprepo.Settings.BasePath != path {
			t.Errorf("Expected %s path but got %s", path, snaprepo.Settings.BasePath)
		}
		if snaprepo.Settings.Bucket != bucket {
			t.Errorf("Expected %s bucket but got %s", bucket, snaprepo.Settings.Bucket)
		}
	} else {
		t.Error("Snaprepo is nil")
	}
}

func TestCreateSnapshotRepoSuccess(t *testing.T) {
	client, err := opensearch.NewClient(opensearch.Config{
		Addresses: []string{"fake://url"},
		Transport: &MockRoundTripper{},
	})
	if err != nil {
		t.Fatal(err)
	}

	es := NewTestEsAPIHandler(client, slog.New(&testutil.MockLogHandler{}))

	_, err = es.CreateSnapshotRepo(repoName, bucket, path, region, rolearn)
	if err != nil {
		t.Errorf("Err is not nil: %v", err)
	}
}

func TestCreateSnapshot(t *testing.T) {
	client, err := opensearch.NewClient(opensearch.Config{
		Addresses: []string{"fake://url"},
		Transport: &MockRoundTripper{},
	})
	if err != nil {
		t.Fatal(err)
	}

	es := NewTestEsAPIHandler(client, slog.New(&testutil.MockLogHandler{}))
	_, err = es.CreateSnapshot(repoName, snapshotName)
	if err != nil {
		t.Errorf("Err is not nil: %v", err)
	}
}

func TestGetSnapshotStatus(t *testing.T) {
	client, err := opensearch.NewClient(opensearch.Config{
		Addresses: []string{"fake://url"},
		Transport: &MockRoundTripper{getResponse: `{"snapshots": [{ "state": "SUCCESS" }]}`},
	})
	if err != nil {
		t.Fatal(err)
	}

	es := NewTestEsAPIHandler(client, slog.New(&testutil.MockLogHandler{}))

	resp, err := es.GetSnapshotStatus(repoName, snapshotName)
	if err != nil {
		t.Errorf("Err is not nil: %v", err)
	}

	if resp != "SUCCESS" {
		t.Errorf("Response is %s, not SUCCESS", resp)
	}
}

func TestGetSnapshotStatusNotFound(t *testing.T) {
	client, err := opensearch.NewClient(opensearch.Config{
		Addresses: []string{"fake://url"},
		Transport: &MockRoundTripper{statusCode: http.StatusNotFound, getResponse: `{}`},
	})
	if err != nil {
		t.Fatal(err)
	}

	es := NewTestEsAPIHandler(client, slog.New(&testutil.MockLogHandler{}))

	resp, err := es.GetSnapshotStatus(repoName, snapshotName)
	if err != nil {
		t.Errorf("Err is not nil: %v", err)
	}

	if resp != "" {
		t.Errorf("Expected empty status, got %s", resp)
	}
}

func TestGetSnapshotStatusNoSnapshots(t *testing.T) {
	client, err := opensearch.NewClient(opensearch.Config{
		Addresses: []string{"fake://url"},
		Transport: &MockRoundTripper{getResponse: `{"snapshots": []}`},
	})
	if err != nil {
		t.Fatal(err)
	}

	es := NewTestEsAPIHandler(client, slog.New(&testutil.MockLogHandler{}))

	_, err = es.GetSnapshotStatus(repoName, snapshotName)
	if err == nil {
		t.Fatal("err is nil, but should be received")
	}
}
