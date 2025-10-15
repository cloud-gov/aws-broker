package elasticsearch

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"

	"code.cloudfoundry.org/lager"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"

	"github.com/opensearch-project/opensearch-go/v2"
	opensearchapi "github.com/opensearch-project/opensearch-go/v2/opensearchapi"
	requestsigner "github.com/opensearch-project/opensearch-go/v2/signer/awsv2"
)

type EsApiHandler struct {
	opensearchClient *opensearch.Client
	logger           lager.Logger
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

// This will take a Credentials mapping from an ElasticSearchInstance and the region info
// to create an API handler.
func NewEsApiHandler(svcInfo map[string]string, region string, logger lager.Logger) (*EsApiHandler, error) {
	cfg, err := config.LoadDefaultConfig(
		context.TODO(),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(svcInfo["access_key"], svcInfo["secret_key"], "")),
	)
	if err != nil {
		return nil, err
	}

	signer, err := requestsigner.NewSigner(cfg)
	if err != nil {
		return nil, err
	}

	client, err := opensearch.NewClient(opensearch.Config{
		Addresses: []string{fmt.Sprintf("https://%s", svcInfo["host"])},
		Signer:    signer,
	})
	if err != nil {
		return nil, err
	}

	return &EsApiHandler{
		opensearchClient: client,
		logger:           logger,
	}, nil
}

func (es *EsApiHandler) CreateSnapshotRepo(repositoryName string, bucketName string, path string, region string, roleArn string) (string, error) {
	// the repo request cannot have a leading slash in the path
	path = strings.TrimPrefix(path, "/")

	repositorySettings := NewSnapshotRepo(bucketName, path, region, roleArn)

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
		return "", fmt.Errorf("CreateSnapshotRepo: error creating snapshot repository: %w", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		return "", fmt.Errorf("CreateSnapshotRepo: failed to create snapshot repository %s: %s", repositoryName, res.String())
	}

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

	bodyBytes, err := io.ReadAll(res.Body)
	if err != nil {
		return "", fmt.Errorf("GetSnapshotStatus: failed to read response %s", res.String())
	}

	snapshots := Snapshots{}
	err = json.Unmarshal(bodyBytes, &snapshots)
	if err != nil {
		es.logger.Error("GetSnapshotStatus JSON unmarshal error", err)
		return "", err
	}

	if len(snapshots.Snapshots) == 0 {
		es.logger.Debug(fmt.Sprintf("GetSnapshotStatus response: %s", res.String()))
		return "", errors.New("SnapshotStatus returned empty")
	}

	return snapshots.Snapshots[0].State, nil
}
