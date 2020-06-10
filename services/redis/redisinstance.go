package redis

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

// RedisInstance represents the information of a Redis Service instance.
type RedisInstance struct {
	base.Instance

	Description string `sql:"size(255)"`

	Password string `sql:"size(255)"`
	Salt     string `sql:"size(255)"`

	ClearPassword string `sql:"-"`

	EngineVersion              string `sql:"size(255)"`
	ClusterID                  string `sql:"size(255)"`
	CacheNodeType              string `sql:"size(255)"`
	NumCacheClusters           int    `sql:"size(255)"`
	ParameterGroup             string `sql:"size(255)"`
	PreferredMaintenanceWindow string `sql:"size(255)"`
	SnapshotWindow             string `sql:"size(255)"`
	SnapshotRetentionLimit     int    `sql:"size(255)"`
	AutomaticFailoverEnabled   bool   `sql:"size(255)"`

	Tags          map[string]string `sql:"-"`
	DbSubnetGroup string            `sql:"-"`
	SecGroup      string            `sql:"-"`
}

func (i *RedisInstance) setPassword(password, key string) error {
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

func (i *RedisInstance) getPassword(key string) (string, error) {
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

func (i *RedisInstance) getCredentials(password string) (map[string]string, error) {
	var credentials map[string]string

	uri := fmt.Sprintf("redis://:%s@%s:%d",
		password,
		i.Host,
		i.Port)

	credentials = map[string]string{
		"uri":      uri,
		"password": password,
		"host":     i.Host,
		"hostname": i.Host,
		"port":     strconv.FormatInt(i.Port, 10),
	}
	return credentials, nil
}

func (i *RedisInstance) init(uuid string,
	orgGUID string,
	spaceGUID string,
	serviceID string,
	plan catalog.RedisPlan,
	options RedisOptions,
	s *config.Settings) error {

	i.Uuid = uuid
	i.ServiceID = serviceID
	i.PlanID = plan.ID
	i.OrganizationGUID = orgGUID
	i.SpaceGUID = spaceGUID

	// Load AWS values
	i.DbSubnetGroup = plan.SubnetGroup
	i.SecGroup = plan.SecurityGroup

	// Load tags
	i.Tags = plan.Tags
	i.Description = plan.Description

	i.ClusterID = s.DbShorthandPrefix + "-" + uuid
	i.Salt = helpers.GenerateSalt(aes.BlockSize)
	password := helpers.RandStr(25)
	if err := i.setPassword(password, s.EncryptionKey); err != nil {
		return err
	}

	i.EngineVersion = plan.EngineVersion
	i.NumCacheClusters = plan.NumCacheClusters
	i.CacheNodeType = plan.CacheNodeType
	i.ParameterGroup = plan.ParameterGroup
	i.PreferredMaintenanceWindow = plan.PreferredMaintenanceWindow
	i.SnapshotWindow = plan.SnapshotWindow
	i.SnapshotRetentionLimit = plan.SnapshotRetentionLimit
	i.AutomaticFailoverEnabled = plan.AutomaticFailoverEnabled

	// Tag instance with broker details
	i.Tags["Instance GUID"] = uuid
	i.Tags["Space GUID"] = spaceGUID
	i.Tags["Organization GUID"] = orgGUID
	i.Tags["Plan GUID"] = plan.ID
	i.Tags["Service GUID"] = serviceID

	return nil
}
