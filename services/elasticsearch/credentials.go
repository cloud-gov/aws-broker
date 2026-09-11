package elasticsearch

import (
	"crypto/aes"
	"encoding/base64"
	"errors"

	"github.com/cloud-gov/aws-broker/helpers"
)

type CredentialUtils interface {
	encryptCredential(salt string, credential string, key string) (string, error)
	decryptCredential(salt string, credential string, key string) (string, error)
	generateSalt() string
}

type ElasticsearchCredentialUtils struct {
}

func (u *ElasticsearchCredentialUtils) encryptCredential(salt string, credential string, key string) (string, error) {
	if salt == "" {
		return "", errors.New("salt has to be set before writing the credential")
	}

	iv, _ := base64.StdEncoding.DecodeString(salt)

	encrypted, err := helpers.Encrypt(credential, key, iv)
	if err != nil {
		return "", err
	}

	return encrypted, nil
}

func (u *ElasticsearchCredentialUtils) decryptCredential(salt string, credential string, key string) (string, error) {
	if salt == "" || credential == "" {
		return "", errors.New("salt and msg has to be set before getting the decrypted msg")
	}

	iv, _ := base64.StdEncoding.DecodeString(salt)

	decrypted, err := helpers.Decrypt(credential, key, iv)
	if err != nil {
		return "", err
	}

	return decrypted, nil
}

func (u *ElasticsearchCredentialUtils) generateSalt() string {
	return helpers.GenerateSalt(aes.BlockSize)
}
