package rds

import (
	"github.com/18F/aws-broker/base"

	"crypto/aes"
	"encoding/base64"
	"errors"
	"fmt"
	"github.com/18F/aws-broker/catalog"
	"github.com/18F/aws-broker/config"
	"github.com/18F/aws-broker/helpers"
	"strconv"
)

var (
	ErrNoSaltSet = errors.New("No salt set for instance")
	ErrNoPassword = errors.New("No password set for instance")
)

// Instance represents the information of a RDS Service instance.
type Instance struct {
	base.Instance

	Database string `sql:"size(255)"`
	Username string `sql:"size(255)"`
	Password string `sql:"size(255)"`
	Salt     string `sql:"size(255)"`

	ClearPassword string `sql:"-"`

	Tags          map[string]string `sql:"-"`
	DbSubnetGroup string            `sql:"-"`
	SecGroup      string            `sql:"-"`

	Adapter string `sql:"size(255)"`

	DbType string `sql:"size(255)"`
}

func (i *Instance) setPassword(password, key string) error {
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

func (i *Instance) getPassword(key string) (string, error) {
	if i.Salt == "" {
		return "", ErrNoSaltSet
	}
	if i.Password == "" {
		return "", ErrNoPassword
	}

	iv, _ := base64.StdEncoding.DecodeString(i.Salt)

	decrypted, err := helpers.Decrypt(i.Password, key, iv)
	if err != nil {
		return "", err
	}

	return decrypted, nil
}

func (i *Instance) getCredentials(password string) (map[string]string, error) {
	var credentials map[string]string
	switch i.DbType {
	case "postgres", "mysql":
		uri := fmt.Sprintf("%s://%s:%s@%s:%d/%s",
			i.DbType,
			i.Username,
			password,
			i.Host,
			i.Port,
			i.Database)

		credentials = map[string]string{
			"uri":      uri,
			"username": i.Username,
			"password": password,
			"host":     i.Host,
			"port":     strconv.FormatInt(i.Port, 10),
			"db_name":  i.Database,
		}
	default:
		return nil, errors.New("Cannot generate credentials for unsupported db type: " + i.DbType)
	}
	return credentials, nil
}

func (i *Instance) init(uuid string,
	orgGUID string,
	spaceGUID string,
	serviceID string,
	plan catalog.RDSPlan,
	s *config.Settings) error {

	i.UUID = uuid
	i.ServiceID = serviceID
	i.PlanID = plan.ID
	i.OrganizationGUID = orgGUID
	i.SpaceGUID = spaceGUID

	i.Adapter = plan.Adapter

	// Build random values
	i.Database = "db" + helpers.RandStr(15)
	i.Username = "u" + helpers.RandStr(15)
	i.Salt = helpers.GenerateSalt(aes.BlockSize)
	password := helpers.RandStr(25)
	if err := i.setPassword(password, s.EncryptionKey); err != nil {
		return err
	}

	// Load tags
	i.Tags = plan.Tags

	// Load AWS values
	i.DbType = plan.DbType
	i.DbSubnetGroup = plan.SubnetGroup
	i.SecGroup = plan.SecurityGroup

	return nil
}

// TableName is a getter function used by GORM to specify the Table Name for the struct.
func (i Instance) TableName() string {
	// Older versions of the code had the table name as this because the struct name was "RDSInstance"
	// In the future, we can add migrations to check and rename the database table.
	return "r_d_s_instances"
}
