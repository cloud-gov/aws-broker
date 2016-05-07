package helpers

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
)

// RandStr will generate a random alphanumeric string of the specified length.
func RandStr(strSize int) string {

	var dictionary string
	dictionary = "0123456789abcdefghijklmnopqrstuvwxyz"

	bytes := generateIv(strSize)
	for k, v := range bytes {
		bytes[k] = dictionary[v%byte(len(dictionary))]
	}
	return string(bytes)
}

// Encrypt will encrypt the given plain text string.
func Encrypt(msg, key string, iv []byte) (string, error) {
	src := []byte(msg)
	dst := make([]byte, len(src))

	aesBlockEncrypter, err := aes.NewCipher([]byte(key))
	if err != nil {
		return "", err
	}

	aesEncrypter := cipher.NewCFBEncrypter(aesBlockEncrypter, iv)
	aesEncrypter.XORKeyStream(dst, src)

	return base64.StdEncoding.EncodeToString(dst), nil
}

// Decrypt will decrypt the given encrypted string.
func Decrypt(msg, key string, iv []byte) (string, error) {
	src, _ := base64.StdEncoding.DecodeString(msg)
	dst := make([]byte, len(src))

	aesBlockDecrypter, err := aes.NewCipher([]byte(key))
	if err != nil {
		return "", err
	}

	aesDecrypter := cipher.NewCFBDecrypter(aesBlockDecrypter, iv)
	aesDecrypter.XORKeyStream(dst, src)

	return string(dst), nil
}

func generateIv(size int) []byte {
	var bytes = make([]byte, size)
	rand.Read(bytes)

	return bytes
}

// GenerateSalt will generate a salt with the given size.
func GenerateSalt(size int) string {
	iv := generateIv(size)

	return base64.StdEncoding.EncodeToString(iv)
}
