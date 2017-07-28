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

	Tags             map[string]string `sql:"-"`
	DbSubnetGroup    string            `sql:"-"`
	AllocatedStorage int64             `sql:"-"`
	SecGroup         string            `sql:"-"`

	Adapter string `sql:"size(255)"`

	DbType       string `sql:"size(255)"`
	LicenseModel string `sql:"size(255)"`
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
	/*
		What is the correct format for Oracle?
			https://msdn.microsoft.com/en-us/library/dd787819.aspx
			oracledb://User=[USER_NAME];Password=[PASSWORD]@[NET_SERVICE_NAME]?PollingId=[POLLING_ID]
			http://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/USER_ConnectToOracleInstance.html
			'mydbusr@(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=<dns name of db instance>)
			(PORT=<listener port>))(CONNECT_DATA=(SID=<database name>)))

			sqlplus 'mydbusr@(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=<dns name of db instance>)(PORT=<listener port>))(CONNECT_DATA=(SID=<database name>)))'


			https://www.connectionstrings.com/oracle/
			SERVER=(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=MyHost)(PORT=MyPort))(CONNECT_DATA=(SERVICE_NAME=MyOracleSID)));
			uid=myUsername;pwd=myPassword;
	*/
	var credentials map[string]string
	switch i.DbType {
	case "postgres", "mysql":
		uri := fmt.Sprintf("%s://%s:%s@%s:%d/%s",
			i.DbType,
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
		}
	case "oracle-se1", "oracle-se2", "oracle-ee":
		uri := fmt.Sprintf("oracledb://User=%s;Password=%s@%s:%d/%s",
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
			"sid":      i.FormatDBName(),
		}
	default:
		return nil, errors.New("Cannot generate credentials for unsupported db type: " + i.DbType)
	}
	return credentials, nil
}

func (i *RDSInstance) init(uuid string,
	orgGUID string,
	spaceGUID string,
	serviceID string,
	plan catalog.RDSPlan,
	options RDSOptions,
	s *config.Settings) error {

	i.Uuid = uuid
	i.ServiceID = serviceID
	i.PlanID = plan.ID
	i.OrganizationGUID = orgGUID
	i.SpaceGUID = spaceGUID

	i.Adapter = plan.Adapter

	// Load AWS values
	i.DbType = plan.DbType
	i.DbSubnetGroup = plan.SubnetGroup
	i.SecGroup = plan.SecurityGroup
	i.LicenseModel = plan.LicenseModel

	// Build random values
	i.Database = s.DbNamePrefix + helpers.RandStr(15)
	i.Username = "u" + helpers.RandStr(15)
	i.Salt = helpers.GenerateSalt(aes.BlockSize)
	password := helpers.RandStr(25)
	if err := i.setPassword(password, s.EncryptionKey); err != nil {
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

	return nil
}
