package elasticsearch

import (
	"bytes"
	"io"
	"net/http"
	"testing"

	"github.com/opensearch-project/opensearch-go/v2"
)

var bucket = "mys3bucket"
var path = "foo/bar/baz"

var repoName = "my-snapshots"

var snapshotName = "backup"
var region = "us-east-1"
var rolearn = "arn:aws:iam::123456789012:role/snapshot-role"

// MockRoundTripper is a mock implementation of http.RoundTripper
type MockRoundTripper struct {
	Response *http.Response
	Err      error
}

// RoundTrip implements the http.RoundTripper interface
func (m *MockRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	return m.Response, m.Err
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

func TestSnapshotRepoToString(t *testing.T) {
	expected := "{\"type\":\"s3\",\"settings\":{\"bucket\":\"" + bucket + "\",\"base_path\":\"" + path + "\",\"server_side_encryption\":true,\"region\":\"" + region + "\",\"role_arn\":\"" + rolearn + "\"}}"
	snaprepo := NewSnapshotRepo(bucket, path, region, rolearn)
	result, err := snaprepo.ToString()
	if err != nil {
		t.Error("Got non-nil error in ToString")
	}
	if result != expected {
		t.Errorf("Got %s but expected %s", result, expected)
	}
}

func TestCreateSnapshotRepoSuccess(t *testing.T) {
	mockResponse := &http.Response{
		StatusCode: http.StatusOK,
		Body:       io.NopCloser(bytes.NewBufferString(`{"message": "success"}`)),
		Header:     make(http.Header),
	}
	mockResponse.Header.Set("Content-Type", "application/json")

	client, err := opensearch.NewClient(opensearch.Config{
		Addresses: []string{"https://localhost:9200"},
		Transport: &MockRoundTripper{Response: mockResponse, Err: nil},
	})
	if err != nil {
		t.Fatal(err)
	}

	es := &EsApiHandler{
		opensearchClient: client,
	}

	_, err = es.CreateSnapshotRepo(repoName, bucket, path, region, rolearn)
	if err != nil {
		t.Errorf("Err is not nil: %v", err)
	}
}

func TestCreateSnapshot(t *testing.T) {
	mockResponse := &http.Response{
		StatusCode: http.StatusOK,
		Body:       io.NopCloser(bytes.NewBufferString(`{"message": "success"}`)),
		Header:     make(http.Header),
	}
	mockResponse.Header.Set("Content-Type", "application/json")

	client, err := opensearch.NewClient(opensearch.Config{
		Addresses: []string{"https://localhost:9200"},
		Transport: &MockRoundTripper{Response: mockResponse, Err: nil},
	})
	if err != nil {
		t.Fatal(err)
	}

	es := &EsApiHandler{
		opensearchClient: client,
	}
	_, err = es.CreateSnapshot(repoName, snapshotName)
	if err != nil {
		t.Errorf("Err is not nil: %v", err)
	}
}

func TestGetSnapshotStatus(t *testing.T) {
	mockResponse := &http.Response{
		StatusCode: http.StatusOK,
		Body:       io.NopCloser(bytes.NewBufferString(`{"snapshots": [{ "state": "SUCCESS" }]}`)),
		Header:     make(http.Header),
	}
	mockResponse.Header.Set("Content-Type", "application/json")

	client, err := opensearch.NewClient(opensearch.Config{
		Addresses: []string{"https://localhost:9200"},
		Transport: &MockRoundTripper{Response: mockResponse, Err: nil},
	})
	if err != nil {
		t.Fatal(err)
	}

	es := &EsApiHandler{
		opensearchClient: client,
	}

	resp, err := es.GetSnapshotStatus(repoName, snapshotName)
	if err != nil {
		t.Errorf("Err is not nil: %v", err)
	}

	if resp != "SUCCESS" {
		t.Errorf("Response is %s, not SUCCESS", resp)
	}
}

func TestGetSnapshotStatusNoSnapshots(t *testing.T) {
	mockResponse := &http.Response{
		StatusCode: http.StatusOK,
		Body:       io.NopCloser(bytes.NewBufferString(`{"snapshots": []}`)),
		Header:     make(http.Header),
	}
	mockResponse.Header.Set("Content-Type", "application/json")

	client, err := opensearch.NewClient(opensearch.Config{
		Addresses: []string{"https://localhost:9200"},
		Transport: &MockRoundTripper{Response: mockResponse, Err: nil},
	})
	if err != nil {
		t.Fatal(err)
	}

	es := &EsApiHandler{
		opensearchClient: client,
	}

	_, err = es.GetSnapshotStatus(repoName, snapshotName)
	if err == nil {
		t.Fatal("err is nil, but should be received")
	}
}
