package rds

import (
	"crypto/aes"
	"encoding/base64"
	"errors"
	"fmt"
	"regexp"
	"strconv"

	"github.com/cloud-gov/aws-broker/config"
	"github.com/cloud-gov/aws-broker/helpers"
)

type CredentialUtils interface {
	generatePassword(password string, salt string, key string) (string, []byte, error)
	getPassword(salt string, password string, key string, nonce []byte) (string, error)
	getCredentials(i *RDSInstance, password string) (map[string]string, error)
	generateCredentials(settings *config.Settings) (string, string, []byte, error)
	generateDatabaseName(settings *config.Settings) string
	buildUsername() string
}

func formatDBName(database string) string {
	re, _ := regexp.Compile("(i?)[^a-z0-9]")
	return re.ReplaceAllString(database, "")
}

type RDSCredentialUtils struct {
}

func (u *RDSCredentialUtils) generatePassword(password string, salt string, key string) (string, []byte, error) {
	if salt == "" {
		return "", nil, errors.New("salt has to be set before writing the password")
	}

	iv, _ := base64.StdEncoding.DecodeString(salt)

	encrypted, nonce, err := helpers.Encrypt(password, iv, key)
	if err != nil {
		return "", nil, err
	}

	return encrypted, nonce, nil
}

func (u *RDSCredentialUtils) getPassword(salt string, password string, key string, nonce []byte) (string, error) {
	if salt == "" || password == "" {
		return "", errors.New("salt and password has to be set before getting the password")
	}

	iv, _ := base64.StdEncoding.DecodeString(salt)

	decrypted, err := helpers.Decrypt(password, iv, nonce, key)
	if err != nil {
		return "", err
	}

	return decrypted, nil
}

func (u *RDSCredentialUtils) getCredentials(i *RDSInstance, password string) (map[string]string, error) {
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

	dbName := formatDBName(i.Database)
	uri := fmt.Sprintf(
		"%s://%s:%s@%s:%d/%s",
		dbScheme,
		i.Username,
		password,
		i.Host,
		i.Port,
		dbName,
	)

	credentials = map[string]string{
		"uri":      uri,
		"username": i.Username,
		"password": password,
		"host":     i.Host,
		"port":     strconv.FormatInt(i.Port, 10),
		"db_name":  dbName,
		"name":     dbName,
	}

	if i.ReplicaDatabaseHost != "" {
		credentials["replica_host"] = i.ReplicaDatabaseHost
		credentials["replica_uri"] = fmt.Sprintf(
			"%s://%s:%s@%s:%d/%s",
			dbScheme,
			i.Username,
			password,
			i.ReplicaDatabaseHost,
			i.Port,
			dbName,
		)
	}

	return credentials, nil
}

func (u *RDSCredentialUtils) generateCredentials(
	settings *config.Settings,
) (string, string, []byte, error) {
	salt, err := helpers.GenerateSalt(aes.BlockSize)
	if err != nil {
		return "", "", nil, err
	}
	password := helpers.RandStrNoCaps(25)
	encrypted, nonce, err := u.generatePassword(password, salt, settings.EncryptionKey)
	if err != nil {
		return "", "", nil, err
	}
	return salt, encrypted, nonce, err
}

func (u *RDSCredentialUtils) generateDatabaseName(
	settings *config.Settings,
) string {
	return settings.DbNamePrefix + helpers.RandStrNoCaps(15)
}

func (u *RDSCredentialUtils) buildUsername() string {
	return "u" + helpers.RandStrNoCaps(15)
}
