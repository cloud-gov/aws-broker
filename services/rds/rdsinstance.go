package rds

import (
	"fmt"

	"github.com/cloud-gov/aws-broker/base"
	"github.com/lib/pq"

	"errors"

	"github.com/cloud-gov/aws-broker/catalog"
	"github.com/cloud-gov/aws-broker/config"
)

// RDSInstance represents the information of a RDS Service instance.
type RDSInstance struct {
	base.Instance

	dbUtils DatabaseUtils `sql:"-"`

	Database string `sql:"size(255)"`
	Username string `sql:"size(255)"`
	Password string `sql:"size(255)"`
	Salt     string `sql:"size(255)"`

	ClearPassword string `sql:"-"`

	Tags                  map[string]string `sql:"-"`
	BackupRetentionPeriod int64             `sql:"size(255)"`
	DbSubnetGroup         string            `sql:"-"`
	AllocatedStorage      int64             `sql:"size(255)"`
	SecGroup              string            `sql:"-"`
	EnableFunctions       bool              `sql:"-"`
	PubliclyAccessible    bool              `sql:"-"`

	Adapter string `sql:"size(255)"`

	DbType       string `sql:"size(255)"`
	DbVersion    string `sql:"size(255)"`
	LicenseModel string `sql:"size(255)"`

	BinaryLogFormat      string `sql:"size(255)"`
	EnablePgCron         *bool  `sql:"size(255)"`
	ParameterGroupFamily string `sql:"-"`
	ParameterGroupName   string `sql:"size(255)"`

	EnabledCloudwatchLogGroupExports pq.StringArray `sql:"type:text[]"`

	StorageType string `sql:"size(255)"`

	AddReadReplica      bool   `sql:"-"`
	ReplicaDatabase     string `sql:"size(255)"`
	ReplicaDatabaseHost string `sql:"size(255)"`
}

func NewRDSInstance() *RDSInstance {
	return &RDSInstance{
		dbUtils: &RDSDatabaseUtils{},
	}
}

func (i *RDSInstance) FormatDBName() string {
	return i.dbUtils.FormatDBName(i.DbType, i.Database)
}

func (i *RDSInstance) getCredentials(password string) (map[string]string, error) {
	return i.dbUtils.getCredentials(i, password)
}

func (i *RDSInstance) generateCredentials(settings *config.Settings) error {
	salt, encrypted, password, err := i.dbUtils.generateCredentials(settings)
	if err != nil {
		return err
	}
	i.Salt = salt
	i.Password = encrypted
	i.ClearPassword = password
	return nil
}

func (i *RDSInstance) modify(options Options, plan catalog.RDSPlan, settings *config.Settings) error {
	// needed to create an RDS read replica
	i.SecGroup = plan.SecurityGroup

	// Check to see if there is a storage size change and if so, check to make sure it's a valid change.
	if options.AllocatedStorage > 0 {
		// Check that we are not decreasing the size of the instance.
		if options.AllocatedStorage < i.AllocatedStorage {
			return errors.New("cannot decrease the size of an existing instance. If you need to do this, you'll need to create a new instance with the smaller size amount, backup and restore the data into that instance, and delete this instance")
		}

		// Update the existing instance with the new allocated storage.
		i.AllocatedStorage = options.AllocatedStorage
	}

	if options.StorageType == "gp3" && i.AllocatedStorage < 20 {
		return errors.New("the database must have at least 20 GB of storage to use gp3 storage volumes. Please update the \"storage\" value in your update-service command")
	}

	if options.StorageType != i.StorageType {
		i.StorageType = options.StorageType
	}

	// Check if there is a backup retention change
	if options.BackupRetentionPeriod != nil && *options.BackupRetentionPeriod > 0 {
		i.BackupRetentionPeriod = *options.BackupRetentionPeriod
	}

	// There may be some instances which were previously updated to have
	// i.BackupRetentionPeriod = 0. Make sure those instances get updated
	// to the minimum backup retention period, since 0 will disable backups
	// on the database.
	if i.BackupRetentionPeriod < settings.MinBackupRetention {
		i.BackupRetentionPeriod = settings.MinBackupRetention
	}

	// Check if there is a binary log format change and if so, apply it
	if options.BinaryLogFormat != "" {
		i.BinaryLogFormat = options.BinaryLogFormat
	}

	if options.EnablePgCron != i.EnablePgCron {
		i.EnablePgCron = options.EnablePgCron
	}

	if options.EnableFunctions != i.EnableFunctions {
		i.EnableFunctions = options.EnableFunctions
	}

	if options.RotateCredentials != nil && *options.RotateCredentials {
		err := i.generateCredentials(settings)
		if err != nil {
			return err
		}
	}

	i.setEnabledCloudwatchLogGroupExports(options.EnableCloudWatchLogGroupExports)

	fmt.Printf("rds: before modify instance, instance: %s, plan read replica: %t, replica database: %s\n", i.Uuid, plan.ReadReplica, i.ReplicaDatabase)
	if plan.ReadReplica && i.ReplicaDatabase == "" {
		i.AddReadReplica = true
		i.ReplicaDatabase = i.generateDatabaseReplicaName()
	}
	fmt.Printf("rds: after modify instance, instance: %s, plan read replica: %t, replica database: %s\n", i.Uuid, plan.ReadReplica, i.ReplicaDatabase)

	return nil
}

func (i *RDSInstance) generateDatabaseReplicaName() string {
	return i.Database + "-replica"
}

func (i *RDSInstance) init(
	uuid string,
	orgGUID string,
	spaceGUID string,
	serviceID string,
	plan catalog.RDSPlan,
	options Options,
	settings *config.Settings,
	tags map[string]string,
) error {
	i.Uuid = uuid
	i.ServiceID = serviceID
	i.PlanID = plan.ID
	i.OrganizationGUID = orgGUID
	i.SpaceGUID = spaceGUID

	i.Adapter = plan.Adapter

	// Load AWS values
	i.DbType = plan.DbType

	// Set the DB Version
	// Currently only supported for MySQL and PostgreSQL instances.
	if (i.DbType == "postgres" || i.DbType == "mysql") && options.Version != "" {
		i.DbVersion = options.Version
	} else {
		// Default to the version provided by the plan chosen in catalog.
		i.DbVersion = plan.DbVersion
	}

	if options.BackupRetentionPeriod != nil {
		i.BackupRetentionPeriod = *options.BackupRetentionPeriod
	}

	if i.BackupRetentionPeriod == 0 {
		i.BackupRetentionPeriod = plan.BackupRetentionPeriod
	}

	i.DbSubnetGroup = plan.SubnetGroup
	i.SecGroup = plan.SecurityGroup
	i.LicenseModel = plan.LicenseModel

	// Build random values
	i.Database = i.dbUtils.generateDatabaseName(settings)
	i.Username = i.dbUtils.buildUsername()

	err := i.generateCredentials(settings)
	if err != nil {
		return err
	}

	i.setTags(plan, tags)

	i.StorageType = plan.StorageType

	i.AllocatedStorage = options.AllocatedStorage
	if i.AllocatedStorage == 0 {
		i.AllocatedStorage = plan.AllocatedStorage
	}
	i.EnableFunctions = options.EnableFunctions
	i.PubliclyAccessible = options.PubliclyAccessible
	i.BinaryLogFormat = options.BinaryLogFormat
	i.EnablePgCron = options.EnablePgCron

	i.setEnabledCloudwatchLogGroupExports(options.EnableCloudWatchLogGroupExports)

	if plan.ReadReplica {
		i.AddReadReplica = true
		i.ReplicaDatabase = i.generateDatabaseReplicaName()
	}

	return nil
}

func (i *RDSInstance) setTags(
	plan catalog.RDSPlan,
	tags map[string]string,
) error {
	// Load tags
	i.Tags = plan.Tags
	if i.Tags == nil {
		i.Tags = make(map[string]string)
	}
	for k, v := range tags {
		i.Tags[k] = v
	}
	return nil
}

func (i *RDSInstance) setEnabledCloudwatchLogGroupExports(enabledLogGroups []string) error {
	// TODO: update this to set the enabled log groups when
	// enabling log groups is supported by the broker
	if len(enabledLogGroups) > 0 {
		i.EnabledCloudwatchLogGroupExports = enabledLogGroups
	}
	return nil
}
