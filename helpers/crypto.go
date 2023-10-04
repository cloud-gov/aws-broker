package helpers

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"math/big"
)

type CryptoUtils interface {
	RandStr(strSize int) string
	RandStrNoCaps(strSize int) string
	StringWithCharset(length int, charset string) string
	Encrypt(msg, key string, iv []byte) (string, error)
	Decrypt(msg, key string, iv []byte) (string, error)
	generateIv(size int) []byte
	GenerateSalt(size int) string
}

// RandStr will generate a random alphanumeric string of the specified length.
func RandStr(strSize int) string {
	dictionary := "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	return StringWithCharset(strSize, dictionary)
}

// RandStrNoCaps will generate a random alphanumeric string of the specified length.
func RandStrNoCaps(strSize int) string {
	dictionary := "abcdefghijklmnopqrstuvwxyz0123456789"
	return StringWithCharset(strSize, dictionary)
}

func StringWithCharset(length int, charset string) string {
	b := make([]byte, length)
	charsetLen := big.NewInt(int64(len(charset)))

	for i := range b {
		idx, _ := rand.Int(rand.Reader, charsetLen)
		idx64 := idx.Int64()
		b[i] = charset[idx64]
	}
	return string(b)
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
