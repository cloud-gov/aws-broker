package elasticsearch

import (
	"testing"
)

var bucket = "mys3bucket"
var path = "foo/bar/baz"
var reponame = "my-snapshots"
var snapshotname = "backup"
var region = "us-east-1"
var rolearn = "arn:aws:iam::123456789012:role/snapshot-role"
var svcInfo = map[string]string{
	"host": "myesdomain.amazonws.com",
}

var snapshotstatus = "{\"snapshots\":[{\"snapshot\":\"backup3\",\"uuid\":\"kKUia17LT2iJ1nKdhrVgsw\",\"version_id\":7070099,\"version\":\"7.7.0\",\"indices\":[\".kibana_2\",\"test\",\".kibana_1\",\".opendistro-job-scheduler-lock\",\"movies\"],\"include_global_state\":true,\"state\":\"SUCCESS\",\"start_time\":\"2021-12-06T22:16:03.090Z\",\"start_time_in_millis\":1638828963090,\"end_time\":\"2021-12-06T22:16:04.891Z\",\"end_time_in_millis\":1638828964891,\"duration_in_millis\":1801,\"failures\":[],\"shards\":{\"total\":17,\"failed\":0,\"successful\":17}}]}"

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

// func TestCreateSnapshotRepo(t *testing.T) {
// 	es := createMockESHandler("")
// 	_, err := es.CreateSnapshotRepo(reponame, bucket, path, region, rolearn)
// 	if err != nil {
// 		t.Errorf("Err is not nil: %v", err)
// 	}
// }

// func TestCreateSnapshot(t *testing.T) {
// 	es := createMockESHandler("")
// 	_, err := es.CreateSnapshot(reponame, snapshotname)
// 	if err != nil {
// 		t.Errorf("Err is not nil: %v", err)
// 	}
// }

// func TestGetSnapshotStatus(t *testing.T) {
// 	es := createMockESHandler(snapshotstatus)
// 	resp, err := es.GetSnapshotStatus(reponame, snapshotname)
// 	if err != nil {
// 		t.Errorf("Err is not nil: %v", err)
// 	}
// 	if resp != "SUCCESS" {
// 		t.Errorf("Response is %s, not SUCCESS", resp)
// 	}
// }
