package elasticsearch

import (
	"crypto/aes"
	"encoding/base64"
	"errors"
	"fmt"
	"strconv"

	"github.com/18F/aws-broker/base"
	"github.com/18F/aws-broker/helpers"

	"github.com/18F/aws-broker/catalog"
	"github.com/18F/aws-broker/config"
)

// ElasticsearchInstance represents the information of an Elasticsearch Service instance.
type ElasticsearchInstance struct {
	base.Instance

	Description string `sql:"size(255)"`

	Password             string `sql:"size(255)"`
	Salt                 string `sql:"size(255)"`
	AccessKey            string `sql:"size(255)"`
	SecretKey            string `sql:"size(255)"`
	IamPolicy            string `sql:"size(255)"`
	IamPolicyARN         string `sql:"size(255)"`
	AccessControlPolicy  string `sql:"size(255)"`
	ElasticsearchVersion string `sql:"size(255)"`
	MasterCount          int    `sql:"size(255)"`
	DataCount            int    `sql:"size(255)"`
	InstanceType         string `sql:"size(255)"`
	MasterInstanceType   string `sql:"size(255)"`
	VolumeSize           int    `sql:"size(255)"`
	VolumeType           string `sql:"size(255)"`

	ClearPassword string `sql:"-"`

	Domain string `sql:"size(255)"`
	ARN    string `sql:"size(255)"`

	Tags          map[string]string `sql:"-"`
	DbSubnetGroup string            `sql:"-"`
	SecGroup      string            `sql:"-"`
}

func (i *ElasticsearchInstance) setPassword(password, key string) error {
	if i.Salt == "" {
		return errors.New("Salt has to be set before writing the password")
	}

	iv, _ := base64.StdEncoding.DecodeString(i.Salt)

	encrypted, err := helpers.Encrypt(password, key, iv)
	if err != nil {
		return err
	}

	i.Password = encrypted
	i.ClearPassword = password

	return nil
}

func (i *ElasticsearchInstance) getPassword(key string) (string, error) {
	if i.Salt == "" || i.Password == "" {
		return "", errors.New("Salt and password has to be set before writing the password")
	}

	iv, _ := base64.StdEncoding.DecodeString(i.Salt)

	decrypted, err := helpers.Decrypt(i.Password, key, iv)
	if err != nil {
		return "", err
	}

	return decrypted, nil
}

func (i *ElasticsearchInstance) getCredentials(password string) (map[string]string, error) {
	var credentials map[string]string

	uri := fmt.Sprintf("https://%s:443",
		i.Host)

	credentials = map[string]string{
		"uri":        uri,
		"access_key": i.AccessKey,
		"secret_key": i.SecretKey,
		"host":       i.Host,
		"port":       strconv.FormatInt(i.Port, 10),
	}
	return credentials, nil
}

func (i *ElasticsearchInstance) init(uuid string,
	orgGUID string,
	spaceGUID string,
	serviceID string,
	plan catalog.ElasticsearchPlan,
	options ElasticsearchOptions,
	s *config.Settings) error {

	i.Uuid = uuid
	i.ServiceID = serviceID
	i.PlanID = plan.ID
	i.OrganizationGUID = orgGUID
	i.SpaceGUID = spaceGUID
	// Load tags
	i.Tags = plan.Tags
	i.Description = plan.Description

	i.Domain = s.DbNamePrefix + "-" + helpers.RandStr(6)
	i.Salt = helpers.GenerateSalt(aes.BlockSize)
	password := helpers.RandStr(25)
	if err := i.setPassword(password, s.EncryptionKey); err != nil {
		return err
	}

	i.ElasticsearchVersion = plan.ElasticsearchVersion
	i.MasterCount, _ = strconv.Atoi(plan.MasterCount)
	i.DataCount, _ = strconv.Atoi(plan.DataCount)
	i.InstanceType = plan.InstanceType
	i.MasterInstanceType = plan.MasterInstanceType
	i.VolumeSize, _ = strconv.Atoi(plan.VolumeSize)
	i.VolumeType = plan.VolumeType

	// Tag instance with broker details
	i.Tags["Instance GUID"] = uuid
	i.Tags["Space GUID"] = spaceGUID
	i.Tags["Organization GUID"] = orgGUID
	i.Tags["Plan GUID"] = plan.ID
	i.Tags["Service GUID"] = serviceID

	return nil
}
