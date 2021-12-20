package elasticsearch

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws/credentials"
	v4 "github.com/aws/aws-sdk-go/aws/signer/v4"
)

//abstract this method for mocking
type HttpClient interface {
	Do(req *http.Request) (*http.Response, error)
}

type EsApiHandler struct {
	client HttpClient
	//svcInfo map[string]string
	credentials *credentials.Credentials
	signer      *v4.Signer
	domain_uri  string
	region      string
	service     string
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

/* type SnapshotPolicy struct {
	Schedule   string               `json:"schedule"`
	Name       string               `json:"name"`
	Repository string               `json:"repository"`
	Config     SnapshotPolicyConfig `json:"config"`
}

type SnapshotPolicyConfig struct {
	Indices []string `json:"indices"`
}

func NewSnapshotPolicy(reponame string, policyname string, cron string) *SnapshotPolicy {
	defcron := "0 0 3 * * *" // nightly at 3am every dow
	if cron != "" {
		defcron = cron
	}
	sp := &SnapshotPolicy{}
	sp.Schedule = defcron
	sp.Name = "<" + policyname + "{now/d}>"
	sp.Repository = reponame
	sp.Config.Indices = []string{"*"}
	return sp
}

func (sp *SnapshotPolicy) ToString() (string, error) {
	bytestr, err := json.Marshal(sp)
	if err != nil {
		fmt.Print(err)
		return "", err
	}
	policy := string(bytestr)
	return policy, nil
} */

// This will take a Credentials mapping from an ElasticSearchInstance and the region info
// to create an API handler.
func (es *EsApiHandler) Init(svcInfo map[string]string, region string) error {
	id := svcInfo["access_key"]
	secret := svcInfo["secret_key"]
	es.domain_uri = "https://" + svcInfo["host"]
	es.credentials = credentials.NewStaticCredentials(id, secret, "")
	es.signer = v4.NewSigner(es.credentials)
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

	// Sign the request, send it, and print the response
	_, err = es.signer.Sign(req, body, es.service, es.region, time.Now())
	if err != nil {
		fmt.Println("ESAPI -- Send -- Signing Error:")
		fmt.Print(err)
		return result, err
	}
	resp, err := es.client.Do(req)

	if err != nil {
		fmt.Println("ESAPI -- Send -- Do Error:")
		fmt.Print(err)
		return result, err
	}
	defer resp.Body.Close()
	result, err = io.ReadAll(resp.Body)
	if err != nil {
		fmt.Print(err)
	}
	fmt.Printf("ESAPI -- Send -- Result: %v", result)
	return result, err
}

func (es *EsApiHandler) CreateSnapshotRepo(reponame string, bucketname string, path string, region string, rolearn string) (string, error) {
	snaprepo, err := NewSnapshotRepo(bucketname, path, region, rolearn).ToString()
	if err != nil {
		fmt.Print(err)
		return "", err

	}
	endpoint := "/_snapshot/" + reponame
	resp, err := es.Send(http.MethodPut, endpoint, snaprepo)
	if err != nil {
		fmt.Print(err)
		return "", err
	}
	fmt.Printf("CreateSnapshotRepo: \n\tEndpoint: %s\n\tResponse %v", endpoint, string(resp))
	return string(resp), err
}

func (es *EsApiHandler) CreateSnapshot(reponame string, snapshotname string) (string, error) {

	endpoint := "/_snapshot/" + reponame + "/" + snapshotname
	resp, err := es.Send(http.MethodPut, endpoint, "")
	if err != nil {
		fmt.Printf("es_api createsnapshot error:%v", err)
	}
	fmt.Printf("CreateSnapshot: \n\tEndpoint: %s\n\tResponse %v", endpoint, string(resp))
	return string(resp), err
}

func (es *EsApiHandler) GetSnapshotRepo(reponame string) (string, error) {
	endpoint := "/_snapshot/" + reponame
	resp, err := es.Send(http.MethodGet, endpoint, "")
	if err != nil {
		fmt.Print(err)
	}
	fmt.Printf("GetSnapshotRepo: \n\tEndpoint: %s\n\tResponse %v", endpoint, string(resp))
	return string(resp), err
}

func (es *EsApiHandler) GetSnapshotStatus(reponame string, snapshotname string) (string, error) {

	endpoint := "/_snapshot/" + reponame + "/" + snapshotname + "/_status"
	resp, err := es.Send(http.MethodGet, endpoint, "")
	if err != nil {
		fmt.Printf("es_api getsnapshot status error %v", err)
		return "", err
	}
	snapshots := Snapshots{}
	err = json.Unmarshal(resp, &snapshots)
	if err != nil {
		fmt.Printf("es_api unmarshall reply error: %v", err)
		return "", err
	}
	if len(snapshots.Snapshots) == 0 {
		fmt.Printf("GetSnapshotStatus - Snapshot Response: %v", snapshots)
		return "FAILED", errors.New("SnapshotStatus returned empty")
	}
	fmt.Printf("GetSnapshotSnapshot: \n\tEndpoint: %s\n\tResponse %v", endpoint, string(resp))
	return snapshots.Snapshots[0].State, nil
}
