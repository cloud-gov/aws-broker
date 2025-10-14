package elasticsearch

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsConfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"

	"github.com/opensearch-project/opensearch-go/v2"
	opensearchapi "github.com/opensearch-project/opensearch-go/v2/opensearchapi"
	requestsigner "github.com/opensearch-project/opensearch-go/v2/signer/awsv2"

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
	opensearchClient    *opensearch.Client
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
	Indices   []string `json:"indices"`
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

	cfg, _ := awsConfig.LoadDefaultConfig(
		context.TODO(),
		awsConfig.WithRegion(region),
	)
	signer, _ := requestsigner.NewSigner(cfg)

	client, _ := opensearch.NewClient(opensearch.Config{
		Addresses: []string{fmt.Sprintf("https://%s", svcInfo["host"])},
		Signer:    signer,
	})
	es.opensearchClient = client

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

func (es *EsApiHandler) CreateSnapshotRepo(repositoryName string, bucketname string, path string, region string, roleArn string) (string, error) {
	// the repo request cannot have a leading slash in the path
	path = strings.TrimPrefix(path, "/")

	// snaprepo, err := NewSnapshotRepo(bucketname, path, region, roleArn).ToString()
	// if err != nil {
	// 	fmt.Print(err)
	// 	return "", err
	// }

	repositorySettings := map[string]interface{}{
		"type": "s3",
		"settings": map[string]string{
			"bucket":    bucketname,
			"region":    region,
			"base_path": path,
			"role_arn":  roleArn,
		},
	}

	// Marshal the map to JSON
	jsonData, err := json.Marshal(repositorySettings)
	if err != nil {
		return "", fmt.Errorf("error marshaling JSON: %s", err)
	}

	// Create the repository using the OpenSearch API client
	req := opensearchapi.SnapshotCreateRepositoryRequest{
		Repository: repositoryName,
		Body:       bytes.NewReader(jsonData),
	}

	res, err := req.Do(context.Background(), es.opensearchClient)
	if err != nil {
		return "", fmt.Errorf("error creating snapshot repository: %s", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		return "", fmt.Errorf("failed to create snapshot repository %s: %s", repositoryName, res.String())
	}

	fmt.Printf("Snapshot repository '%s' created successfully.\n", repositoryName)

	// resp, err = es.opensearchClient.Snapshot.CreateRepository(reponame, req.Body())
	// endpoint := "/_snapshot/" + reponame
	// resp, err := es.Send(http.MethodPut, endpoint, snaprepo)
	// fmt.Printf("es_api: CreateSnapshotRepo response  %s\n", string(resp))
	// if err != nil {
	// 	fmt.Print(err)
	// 	return "", err
	// }
	return res.String(), err
}

func (es *EsApiHandler) CreateSnapshot(repositoryName string, snapshotName string) (string, error) {
	req := opensearchapi.SnapshotCreateRequest{
		Repository: repositoryName,
		Snapshot:   snapshotName,
	}

	res, err := req.Do(context.Background(), es.opensearchClient)
	if err != nil {
		return "", fmt.Errorf("error creating snapshot: %s", err)
	}

	defer res.Body.Close()

	if res.IsError() {
		return "", fmt.Errorf("failed to create snapshot %s: %s", repositoryName, res.String())
	}

	return res.String(), err

	// endpoint := "/_snapshot/" + reponame + "/" + snapshotname
	// resp, err := es.Send(http.MethodPut, endpoint, "")
	// fmt.Printf("es_api: CreateSnapshot response  %s\n", string(resp))
	// if err != nil {
	// 	fmt.Printf("es_api createsnapshot error: %v\n", err)
	// }
	// return string(resp), err
}

func (es *EsApiHandler) GetSnapshotStatus(repositoryName string, snapshotName string) (string, error) {
	req := opensearchapi.SnapshotGetRequest{
		Repository: repositoryName,
		Snapshot:   []string{snapshotName},
	}

	res, err := req.Do(context.Background(), es.opensearchClient)
	if err != nil {
		return "", fmt.Errorf("error getting snapshot: %s", err)
	}

	defer res.Body.Close()

	if res.IsError() {
		return "", fmt.Errorf("failed to get snapshot %s: %s", repositoryName, res.String())
	}

	fmt.Printf("GetSnapshotStatus response: %s.\n", res.String())

	bodyBytes, err := io.ReadAll(res.Body)
	if err != nil {
		return "", fmt.Errorf("GetSnapshotStatus: failed to read response %s", res.String())
	}

	snapshots := Snapshots{}
	err = json.Unmarshal(bodyBytes, &snapshots)
	if err != nil {
		fmt.Printf("GetSnapshotStatus JSON unmarshal error: %v\n", err)
		return "", err
	}

	if len(snapshots.Snapshots) == 0 {
		fmt.Printf("GetSnapshotStatus response: %s.\n", res.String())
		return "", errors.New("SnapshotStatus returned empty")
	}

	return snapshots.Snapshots[0].State, nil

	// endpoint := "/_snapshot/" + reponame + "/" + snapshotname
	// resp, err := es.Send(http.MethodGet, endpoint, "")
	// fmt.Printf("es_api: GetSnapshotStatus response  %s\n", string(resp))
	// if err != nil {
	// 	fmt.Printf("es_api getsnapshot status error %v\n", err)
	// 	return "", err
	// }
	// snapshots := Snapshots{}
	// err = json.Unmarshal(resp, &snapshots)
	// if err != nil {
	// 	fmt.Printf("es_api unmarshall reply error: %v\n", err)
	// 	return "", err
	// }
	// if len(snapshots.Snapshots) == 0 {
	// 	fmt.Printf("GetSnapshotStatus - Snapshot Response: %v\n", snapshots)
	// 	return "FAILED", errors.New("SnapshotStatus returned empty")
	// }
	// return snapshots.Snapshots[0].State, nil
}
