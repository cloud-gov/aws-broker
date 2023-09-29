package rds

import (
	"github.com/18F/aws-broker/base"

	"crypto/aes"
	"encoding/base64"
	"errors"
	"fmt"
	"regexp"
	"strconv"

	"github.com/18F/aws-broker/catalog"
	"github.com/18F/aws-broker/config"
	"github.com/18F/aws-broker/helpers"
)

// RDSInstance represents the information of a RDS Service instance.
type RDSInstance struct {
	base.Instance

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

	StorageType string `sql:"size(255)"`
}

func (i *RDSInstance) FormatDBName() string {
	switch i.DbType {
	case "oracle-se1", "oracle-se2":
		return "ORCL"
	default:
		re, _ := regexp.Compile("(i?)[^a-z0-9]")
		return re.ReplaceAllString(i.Database, "")
	}
}

func (i *RDSInstance) setPassword(password, key string) error {
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

func (i *RDSInstance) getPassword(key string) (string, error) {
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

func (i *RDSInstance) getCredentials(password string) (map[string]string, error) {
	var dbScheme string
	var credentials map[string]string
	switch i.DbType {
	case "postgres", "mysql":
		dbScheme = i.DbType
	case "oracle-se1", "oracle-se2", "oracle-ee":
		dbScheme = "oracle"
	default:
		return nil, errors.New("Cannot generate credentials for unsupported db type: " + i.DbType)
	}
	uri := fmt.Sprintf("%s://%s:%s@%s:%d/%s",
		dbScheme,
		i.Username,
		password,
		i.Host,
		i.Port,
		i.FormatDBName())

	credentials = map[string]string{
		"uri":      uri,
		"username": i.Username,
		"password": password,
		"host":     i.Host,
		"port":     strconv.FormatInt(i.Port, 10),
		"db_name":  i.FormatDBName(),
		"name":     i.FormatDBName(),
	}
	return credentials, nil
}

func (i *RDSInstance) setCredentials(settings *config.Settings) error {
	i.Salt = helpers.GenerateSalt(aes.BlockSize)
	password := helpers.RandStrNoCaps(25)
	err := i.setPassword(password, settings.EncryptionKey)
	return err
}

func (i *RDSInstance) modify(options Options, plan catalog.RDSPlan, settings *config.Settings) error {
	// Check to see if there is a storage size change and if so, check to make sure it's a valid change.
	if options.AllocatedStorage > 0 {
		// Check that we are not decreasing the size of the instance.
		if options.AllocatedStorage < i.AllocatedStorage {
			return errors.New("cannot decrease the size of an existing instance. If you need to do this, you'll need to create a new instance with the smaller size amount, backup and restore the data into that instance, and delete this instance")
		}

		// Update the existing instance with the new allocated storage.
		i.AllocatedStorage = options.AllocatedStorage
	}

	if options.StorageType != "" {
		if options.StorageType == "gp3" && i.AllocatedStorage < 20 {
			return errors.New("the database must have at least 20 GB of storage to use gp3 storage volumes. Please update the \"storage\" value in your update-service command")
		}
		i.StorageType = options.StorageType
	}

	// Check if there is a backup retention change:
	if options.BackupRetentionPeriod > 0 {
		i.BackupRetentionPeriod = options.BackupRetentionPeriod
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

	// Set the DB Version if it is not already set
	// Currently only supported for MySQL and PostgreSQL instances.
	if i.DbVersion == "" && options.Version == "" {
		// Default to the version provided by the plan chosen in catalog.
		i.DbVersion = plan.DbVersion
	}

	if options.RotateCredentials != nil && *options.RotateCredentials {
		if err := i.setCredentials(settings); err != nil {
			return err
		}
	}

	return nil
}

func (i *RDSInstance) init(uuid string,
	orgGUID string,
	spaceGUID string,
	serviceID string,
	plan catalog.RDSPlan,
	options Options,
	s *config.Settings) error {

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

	i.BackupRetentionPeriod = options.BackupRetentionPeriod
	if options.BackupRetentionPeriod == 0 {
		i.BackupRetentionPeriod = plan.BackupRetentionPeriod
	}

	i.DbSubnetGroup = plan.SubnetGroup
	i.SecGroup = plan.SecurityGroup
	i.LicenseModel = plan.LicenseModel

	// Build random values
	i.Database = s.DbNamePrefix + helpers.RandStrNoCaps(15)
	i.Username = "u" + helpers.RandStrNoCaps(15)
	if err := i.setCredentials(s); err != nil {
		return err
	}

	// Load tags
	i.Tags = plan.Tags

	// Tag instance with broker details
	i.Tags["Instance GUID"] = uuid
	i.Tags["Space GUID"] = spaceGUID
	i.Tags["Organization GUID"] = orgGUID
	i.Tags["Plan GUID"] = plan.ID
	i.Tags["Service GUID"] = serviceID

	i.AllocatedStorage = options.AllocatedStorage
	if i.AllocatedStorage == 0 {
		i.AllocatedStorage = plan.AllocatedStorage
	}
	i.EnableFunctions = options.EnableFunctions
	i.PubliclyAccessible = options.PubliclyAccessible
	i.BinaryLogFormat = options.BinaryLogFormat
	i.EnablePgCron = options.EnablePgCron

	return nil
}
