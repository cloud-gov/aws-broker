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

	Engine                     string `sql:"size(255)"`
	EngineVersion              string `sql:"size(255)"`
	ClusterID                  string `sql:"size(255)"`
	CacheNodeType              string `sql:"size(255)"`
	NumCacheClusters           int    `sql:"size(255)"`
	ParameterGroup             string `sql:"size(255)"`
	PreferredMaintenanceWindow string `sql:"size(255)"`
	SnapshotWindow             string `sql:"size(255)"`
	SnapshotRetentionLimit     int    `sql:"size(255)"`
	AutomaticFailoverEnabled   bool   `sql:"size(255)"`
	NumNodeGroups              int    `sql:"size(255)"`
	ReplicasPerNodeGroup       int    `sql:"size(255)"`
	NewNumNodeGroups           int    `gorm:"-"`
	MigratingToClusterMode     bool   `gorm:"-"`

	Tags          map[string]string `gorm:"-"`
	DbSubnetGroup string            `gorm:"-"`
	SecGroup      string            `gorm:"-"`

	ParameterGroupFamily string `gorm:"-"`
	ParameterGroupName   string `sql:"size(255)"`

	EngineLogsGroupName string `sql:"size(512)"`
	SlowLogsGroupName   string `sql:"size(512)"`

	NewReplicaCount int `gorm:"-"`
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

func (i *RedisInstance) decryptCredential(key string) (string, error) {
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
		"cluster_mode_enabled":         strconv.FormatBool(i.ClusterModeEnabled()),
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

	i.setTags(plan, tags) //nolint:errcheck // decide fail-vs-best-effort on tagging failure

	return nil
}

func (i RedisInstance) modify(
	options RedisOptions, newPlan *catalog.RedisPlan, tags map[string]string,
) *RedisInstance {
	// Copy the existing instance so that we can return a modified instance rather than mutating the instance
	modifiedInstance := i

	setInstanceParameters(&modifiedInstance, options, *newPlan)

	modifiedInstance.setTags(*newPlan, tags) //nolint:errcheck // decide fail-vs-best-effort on tagging failure

	return &modifiedInstance
}

func setInstanceParameters(i *RedisInstance, options RedisOptions, plan catalog.RedisPlan) {
	i.PlanID = plan.ID
	i.DbSubnetGroup = plan.SubnetGroup
	i.SecGroup = plan.SecurityGroup

	i.Description = plan.Description

	// Without
	// this, an existing Redis instance would be silently migrated to the
	// plan's Valkey engine by an unrelated plan change, and would be left on
	// a version its new engine does not offer.
	if options.Engine != "" {
		i.Engine = options.Engine
	} else if i.Engine == "" {
		i.Engine = plan.Engine
	}

	// Set the DB Version
	if options.EngineVersion != "" {
		i.EngineVersion = options.EngineVersion
	} else if i.EngineVersion == "" {
		// Default to the version provided by the plan chosen in catalog.
		i.EngineVersion = plan.EngineVersion
	}

	if plan.NumCacheClusters > i.NumCacheClusters {
		// If we are increasing the number of cluster nodes, we need
		// to increase the number of replica nodes
		if i.NumCacheClusters > 0 {
			i.NewReplicaCount = plan.NumCacheClusters - i.NumCacheClusters
		}
		i.NumCacheClusters = plan.NumCacheClusters
	}

	// Capture pre-modify cluster-mode state before NumNodeGroups is mutated
	// below, so a disabled -> enabled change is recognized as a migration.
	wasClusterModeEnabled := i.NumNodeGroups > 1
	isExistingInstance := i.ClusterID != ""

	planNumNodeGroups := plan.NumNodeGroups
	if planNumNodeGroups <= 0 {
		planNumNodeGroups = 1
	}

	if planNumNodeGroups > i.NumNodeGroups {
		if isExistingInstance {
			i.NewNumNodeGroups = planNumNodeGroups
		}
		i.NumNodeGroups = planNumNodeGroups
	}

	if plan.ClusterModeEnabled() && plan.ReplicasPerNodeGroup != i.ReplicasPerNodeGroup {
		if i.ReplicasPerNodeGroup > 0 {
			i.NewReplicaCount = plan.ReplicasPerNodeGroup
		}
		i.ReplicasPerNodeGroup = plan.ReplicasPerNodeGroup
	}

	if isExistingInstance && !wasClusterModeEnabled && plan.ClusterModeEnabled() {
		i.MigratingToClusterMode = true
		i.NewReplicaCount = plan.ReplicasPerNodeGroup
	}

	i.CacheNodeType = plan.CacheNodeType
	i.PreferredMaintenanceWindow = plan.PreferredMaintenanceWindow
	i.SnapshotWindow = plan.SnapshotWindow
	i.SnapshotRetentionLimit = plan.SnapshotRetentionLimit
	if plan.ClusterModeEnabled() {
		i.AutomaticFailoverEnabled = true
	} else {
		i.AutomaticFailoverEnabled = plan.AutomaticFailoverEnabled
	}
}

func (i *RedisInstance) ClusterModeEnabled() bool {
	return i.NumNodeGroups > 1
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
