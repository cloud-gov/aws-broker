package elasticsearch

import (
	"fmt"
	"net/http"
	"os"
	"testing"
)

var bucket = "mys3bucket"
var path = "foo/bar/baz"
var reponame = "my-snapshots"
var policyname = "daily-snaps"
var svcInfo = map[string]string{
	"access_key": os.Getenv("AWS_ACCESS_KEY_ID"),
	"secret_key": os.Getenv("AWS_SECRET_ACCESS_KEY"),
	"host":       "myesdomain.amazonws.com",
}

// we need to mock the body interface io.Reader to make close testable
type mockBody struct{}

func (b *mockBody) Close() error {
	return nil
}

func (b *mockBody) Read(p []byte) (n int, err error) {
	return 0, nil
}

// and we mock the http.Client interface to make Do testable.
type mockClient struct {
}

func (c *mockClient) Do(req *http.Request) (*http.Response, error) {
	return &http.Response{
		Body: &mockBody{},
	}, nil
}

func createMockESHandler() *EsApiHandler {
	var es EsApiHandler
	es.Init(svcInfo, "us-east-1")
	es.client = &mockClient{}
	return &es
}

func TestNewSnapShotRepo(t *testing.T) {

	snaprepo := NewSnapshotRepo(bucket, path)

	if snaprepo != nil {
		if snaprepo.Settings.BasePath != path {
			t.Errorf("Expected %s path but got %s", path, snaprepo.Settings.BasePath)
		}
		if snaprepo.Settings.Bucket != bucket {
			t.Errorf("Expected %s bucket but got %s", bucket, snaprepo.Settings.Bucket)
		}
		fmt.Printf("%+v", snaprepo)
	} else {
		t.Error("Snapreop is nil")
	}
}

func TestSnapshotRepoToString(t *testing.T) {
	expected := "{\"type\":\"s3\",\"settings\":{\"bucket\":\"" + bucket + "\",\"base_path\":\"" + path + "\",\"server_side_encryption\":true}}"
	snaprepo := NewSnapshotRepo(bucket, path)
	result, err := snaprepo.ToString()
	if err != nil {
		t.Error("Got non-nil error in ToString")
	}
	if result != expected {
		t.Errorf("Got %s but expected %s", result, expected)
	}
}

func TestNewSnapShotPolicy(t *testing.T) {
	snappol := NewSnapshotPolicy(reponame, policyname, "")
	name := "<" + policyname + "{now/d}>"
	if snappol != nil {
		if snappol.Name != name {
			t.Errorf("Expected %s path but got %s", name, snappol.Name)
		}
		if snappol.Repository != reponame {
			t.Errorf("Expected %s reponame but got %s", reponame, snappol.Repository)
		}
		fmt.Printf("%+v", snappol)
	} else {
		t.Error("Snapreop is nil")
	}
}

func TestSnapshotPolicyToString(t *testing.T) {
	expected := "{\"schedule\":\"0 0 3 * * *\",\"name\":\"\\u003c" + policyname + "{now/d}\\u003e\",\"repository\":\"" + reponame + "\",\"config\":{\"indices\":[\"*\"]}}"
	snappol := NewSnapshotPolicy(reponame, policyname, "")
	result, err := snappol.ToString()
	if err != nil {
		t.Error("Got non-nil error in ToString")
	}
	if result != expected {
		t.Errorf("Got %s but expected %s", result, expected)
	}
}

func TestCreateSnapshotRepo(t *testing.T) {
	es := createMockESHandler()
	err := es.CreateSnapshotRepo(reponame, bucket, path)
	if err != nil {
		t.Errorf("Err is not nil: %v", err)
	}
}

func TestCreateSnapshotPolicy(t *testing.T) {
	es := createMockESHandler()
	err := es.CreateSnapshotPolicy(policyname, reponame, "")
	if err != nil {
		t.Errorf("Err is not nil: %v", err)
	}
}

func TestGetSnapshotRepo(t *testing.T) {
	es := createMockESHandler()
	err := es.GetSnapshotRepo(reponame)
	if err != nil {
		t.Errorf("Err is not nil: %v", err)
	}
}

func TestGetSnapshotPolicy(t *testing.T) {
	es := createMockESHandler()
	err := es.GetSnapshotPolicy(policyname)
	if err != nil {
		t.Errorf("Err is not nil: %v", err)
	}
}
