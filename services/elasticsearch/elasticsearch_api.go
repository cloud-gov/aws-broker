package elasticsearch

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"

	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
)

// abstract this method for mocking
type HttpClient interface {
	Do(req *http.Request) (*http.Response, error)
}

type EsApiHandler struct {
	client              HttpClient
	credentialsProvider aws.CredentialsProvider
	signer              *v4.Signer
	domain_uri          string
	region              string
	service             string
}

type SnapshotRepo struct {
	Type     string               `json:"type"`
	Settings SnapshotRepoSettings `json:"settings"`
}

type SnapshotRepoSettings struct {
	Bucket   string `json:"bucket"`
	BasePath string `json:"base_path"`              //omit leading '/'
	SSE      bool   `json:"server_side_encryption"` //we set this to true, default is false
	Region   string `json:"region"`
	RoleArn  string `json:"role_arn"`
}

type Snapshot struct {
	Snapshot  string   `json:"snapshot"`
	Version   string   `json:"version"`
	State     string   `json:"state"`
	Indicies  []string `json:"indicies"`
	StartTime string   `json:"start_time"`
	EndTime   string   `json:"end_time"`
}

type Snapshots struct {
	Snapshots []Snapshot `json:"snapshots"`
}

func NewSnapshotRepo(bucketname string, path string, region string, rolearn string) *SnapshotRepo {
	sr := &SnapshotRepo{}
	sr.Type = "s3"
	sr.Settings = SnapshotRepoSettings{
		Bucket:   bucketname,
		BasePath: path,
		Region:   region,
		RoleArn:  rolearn,
		SSE:      true,
	}
	return sr
}

func (sr *SnapshotRepo) ToString() (string, error) {
	bytestr, err := json.Marshal(sr)
	if err != nil {
		fmt.Print(err)
		return "", err
	}
	repo := string(bytestr)
	return repo, nil
}

// This will take a Credentials mapping from an ElasticSearchInstance and the region info
// to create an API handler.
func (es *EsApiHandler) Init(svcInfo map[string]string, region string) error {
	id := svcInfo["access_key"]
	secret := svcInfo["secret_key"]
	es.domain_uri = "https://" + svcInfo["host"]
	es.credentialsProvider = credentials.NewStaticCredentialsProvider(id, secret, "")
	es.signer = v4.NewSigner()
	es.client = &http.Client{}
	es.service = "es"
	es.region = region
	return nil
}

// makes the api request with v4 signing and then returns the body of the response as string
func (es *EsApiHandler) Send(method string, endpoint string, content string) ([]byte, error) {
	endpoint = es.domain_uri + endpoint
	body := strings.NewReader(content)
	result := []byte{}
	// form new request
	req, err := http.NewRequest(method, endpoint, body)
	if err != nil {
		fmt.Print(err)
		return result, err
	}
	req.Header.Add("Content-Type", "application/json")

	credentials, err := es.credentialsProvider.Retrieve(context.TODO())
	if err != nil {
		return result, err
	}

	// Sign the request, send it, and print the response
	err = es.signer.SignHTTP(context.TODO(), credentials, req, content, es.service, es.region, time.Now())
	if err != nil {
		return result, err
	}
	resp, err := es.client.Do(req)

	if err != nil {
		return result, err
	}
	defer resp.Body.Close()
	result, err = io.ReadAll(resp.Body)
	if err != nil {
		fmt.Print(err)
	}
	return result, err
}

func (es *EsApiHandler) CreateSnapshotRepo(reponame string, bucketname string, path string, region string, rolearn string) (string, error) {
	// the repo request cannot have a leading slash in the path
	path = strings.TrimPrefix(path, "/")

	snaprepo, err := NewSnapshotRepo(bucketname, path, region, rolearn).ToString()
	if err != nil {
		fmt.Print(err)
		return "", err

	}
	endpoint := "/_snapshot/" + reponame
	resp, err := es.Send(http.MethodPut, endpoint, snaprepo)
	fmt.Printf("es_api: CreateSnapshotRepo response  %+v\n", resp)
	if err != nil {
		fmt.Print(err)
		return "", err
	}
	return string(resp), err
}

func (es *EsApiHandler) CreateSnapshot(reponame string, snapshotname string) (string, error) {
	endpoint := "/_snapshot/" + reponame + "/" + snapshotname
	resp, err := es.Send(http.MethodPut, endpoint, "")
	fmt.Printf("es_api: CreateSnapshot response  %+v\n", resp)
	if err != nil {
		fmt.Printf("es_api createsnapshot error: %v\n", err)
	}
	return string(resp), err
}

func (es *EsApiHandler) GetSnapshotStatus(reponame string, snapshotname string) (string, error) {
	endpoint := "/_snapshot/" + reponame + "/" + snapshotname
	resp, err := es.Send(http.MethodGet, endpoint, "")
	fmt.Printf("es_api: GetSnapshotStatus response  %+v\n", resp)
	if err != nil {
		fmt.Printf("es_api getsnapshot status error %v\n", err)
		return "", err
	}
	snapshots := Snapshots{}
	err = json.Unmarshal(resp, &snapshots)
	if err != nil {
		fmt.Printf("es_api unmarshall reply error: %v\n", err)
		return "", err
	}
	if len(snapshots.Snapshots) == 0 {
		fmt.Printf("GetSnapshotStatus - Snapshot Response: %v\n", snapshots)
		return "FAILED", errors.New("SnapshotStatus returned empty")
	}
	return snapshots.Snapshots[0].State, nil
}
