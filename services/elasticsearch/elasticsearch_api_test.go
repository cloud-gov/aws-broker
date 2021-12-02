package elasticsearch

import (
	"fmt"
	"io"
	"net/http"
	"testing"
)

var bucket = "mys3bucket"
var path = "foo/bar/baz"
var reponame = "my-snapshots"
var snapshotname = "backup"
var policyname = "daily-snaps"
var region = "us-east-1"
var rolearn = "arn:aws:iam::123456789012:role/snapshot-role"
var svcInfo = map[string]string{
	"access_key": "foo", //os.Getenv("AWS_ACCESS_KEY_ID"),
	"secret_key": "bar", //os.Getenv("AWS_SECRET_ACCESS_KEY"),
	"host":       "myesdomain.amazonws.com",
}

// we need to mock the body interface io.Reader to make close testable
type mockBody struct{}

func (b *mockBody) Close() error {
	return nil
}

func (b *mockBody) Read(p []byte) (n int, err error) {
	return 0, io.EOF
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

	snaprepo := NewSnapshotRepo(bucket, path, region, rolearn)

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

/* func TestNewSnapShotPolicy(t *testing.T) {
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
} */
/*
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
} */

func TestCreateSnapshotRepo(t *testing.T) {
	es := createMockESHandler()
	_, err := es.CreateSnapshotRepo(reponame, bucket, path, region, rolearn)
	if err != nil {
		t.Errorf("Err is not nil: %v", err)
	}
}

func TestCreateSnapshot(t *testing.T) {
	es := createMockESHandler()
	_, err := es.CreateSnapshot(reponame, snapshotname)
	if err != nil {
		t.Errorf("Err is not nil: %v", err)
	}
}

func TestGetSnapshotRepo(t *testing.T) {
	es := createMockESHandler()
	_, err := es.GetSnapshotRepo(reponame)
	if err != nil {
		t.Errorf("Err is not nil: %v", err)
	}
}

func TestGetSnapshotPolicy(t *testing.T) {
	es := createMockESHandler()
	_, err := es.GetSnapshotStatus(reponame, snapshotname)
	if err != nil {
		t.Errorf("Err is not nil: %v", err)
	}
}
