package redis

import (
	"crypto/aes"
	"encoding/base64"
	"errors"
	"fmt"
	"strconv"

	"github.com/cloud-gov/aws-broker/base"
	"github.com/cloud-gov/aws-broker/helpers"

	"github.com/cloud-gov/aws-broker/catalog"
	"github.com/cloud-gov/aws-broker/config"
)

// RedisInstance represents the information of a Redis Service instance.
type RedisInstance struct {
	base.Instance

	Description string `sql:"size(255)"`

	Password string `sql:"size(255)" deep:"-"`
	Salt     string `sql:"size(255)" deep:"-"`

	ClearPassword string `gorm:"-" deep:"-"`

	EngineVersion              string `sql:"size(255)"`
	ClusterID                  string `sql:"size(255)"`
	CacheNodeType              string `sql:"size(255)"`
	NumCacheClusters           int    `sql:"size(255)"`
	ParameterGroup             string `sql:"size(255)"`
	PreferredMaintenanceWindow string `sql:"size(255)"`
	SnapshotWindow             string `sql:"size(255)"`
	SnapshotRetentionLimit     int    `sql:"size(255)"`
	AutomaticFailoverEnabled   bool   `sql:"size(255)"`

	Tags          map[string]string `gorm:"-"`
	DbSubnetGroup string            `gorm:"-"`
	SecGroup      string            `gorm:"-"`

	ParameterGroupFamily string `gorm:"-"`
	ParameterGroupName   string `sql:"size(255)"`

	EngineLogsGroupName string `sql:"size(512)"`
	SlowLogsGroupName   string `sql:"size(512)"`
}

func (i *RedisInstance) setPassword(password, key string) error {
	if i.Salt == "" {
		return errors.New("salt has to be set before writing the password")
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
		return "", errors.New("salt and password has to be set before writing the password")
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

	uri := fmt.Sprintf("rediss://:%s@%s:%d",
		password,
		i.Host,
		i.Port)

	credentials = map[string]string{
		"uri":                          uri,
		"password":                     password,
		"host":                         i.Host,
		"hostname":                     i.Host,
		"current_redis_engine_version": i.EngineVersion,
		"port":                         strconv.FormatInt(i.Port, 10),
	}
	return credentials, nil
}

func (i *RedisInstance) init(
	uuid string,
	orgGUID string,
	spaceGUID string,
	serviceID string,
	plan catalog.RedisPlan,
	options RedisOptions,
	s *config.Settings,
	tags map[string]string,
) error {

	i.Uuid = uuid
	i.ServiceID = serviceID
	i.OrganizationGUID = orgGUID
	i.SpaceGUID = spaceGUID

	// Load AWS values
	setInstanceParameters(i, options, plan)

	i.ClusterID = s.DbShorthandPrefix + "-" + uuid
	i.Salt = helpers.GenerateSalt(aes.BlockSize)
	password := helpers.RandStr(25)
	if err := i.setPassword(password, s.EncryptionKey); err != nil {
		return err
	}

	i.setTags(plan, tags)

	return nil
}

func (i RedisInstance) modify(
	options RedisOptions, newPlan *catalog.RedisPlan, tags map[string]string,
) *RedisInstance {
	// Copy the existing instance so that we can return a modified instance rather than mutating the instance
	modifiedInstance := i

	setInstanceParameters(&modifiedInstance, options, *newPlan)

	modifiedInstance.setTags(*newPlan, tags)

	return &modifiedInstance
}

func setInstanceParameters(i *RedisInstance, options RedisOptions, plan catalog.RedisPlan) {
	i.PlanID = plan.ID
	i.DbSubnetGroup = plan.SubnetGroup
	i.SecGroup = plan.SecurityGroup

	i.Description = plan.Description

	// Set the DB Version
	if options.EngineVersion != "" {
		i.EngineVersion = options.EngineVersion
	} else {
		// Default to the version provided by the plan chosen in catalog.
		i.EngineVersion = plan.EngineVersion
	}

	i.NumCacheClusters = plan.NumCacheClusters
	i.CacheNodeType = plan.CacheNodeType
	i.PreferredMaintenanceWindow = plan.PreferredMaintenanceWindow
	i.SnapshotWindow = plan.SnapshotWindow
	i.SnapshotRetentionLimit = plan.SnapshotRetentionLimit
	i.AutomaticFailoverEnabled = plan.AutomaticFailoverEnabled
}

func (i *RedisInstance) setTags(
	plan catalog.RedisPlan,
	tags map[string]string,
) error {
	i.Tags = plan.Tags

	for k, v := range tags {
		i.Tags[k] = v
	}

	return nil
}
