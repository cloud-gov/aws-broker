package helpers

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"io"
	"math/big"

	"golang.org/x/crypto/argon2"
)

const (
	Memory      = 64 * 1024 // 64 MB
	Iterations  = 3
	Parallelism = 2
	KeyLength   = 32 // For AES-256
)

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
func Encrypt(msg string, salt []byte, encKey string) (string, []byte, error) {
	src := []byte(msg)

	key := argon2.IDKey([]byte(encKey), salt, Iterations, Memory, Parallelism, KeyLength)

	aesBlockEncrypter, err := aes.NewCipher([]byte(key))
	if err != nil {
		return "", nil, err
	}

	gcm, err := cipher.NewGCM(aesBlockEncrypter)
	if err != nil {
		return "", nil, err
	}

	nonceSize := gcm.NonceSize()
	nonce := make([]byte, nonceSize)
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return "", nil, err
	}

	ciphertext := gcm.Seal(nil, nonce, src, nil)

	return base64.StdEncoding.EncodeToString(ciphertext), nonce, nil
}

// Decrypt will decrypt the given encrypted string.
func Decrypt(msg string, salt []byte, nonce []byte, encKey string) (string, error) {
	src, _ := base64.StdEncoding.DecodeString(msg)

	// Re-derive the same key using the original salt
	key := argon2.IDKey([]byte(encKey), salt, Iterations, Memory, Parallelism, KeyLength)

	aesBlockDecrypter, err := aes.NewCipher([]byte(key))
	if err != nil {
		return "", err
	}

	gcm, err := cipher.NewGCM(aesBlockDecrypter)
	if err != nil {
		return "", err
	}

	plaintext, err := gcm.Open(nil, nonce, src, nil)
	if err != nil {
		return "", fmt.Errorf("decryption failed: %v", err)
	}

	return string(plaintext), nil
}

func generateIv(size int) ([]byte, error) {
	var bytes = make([]byte, size)
	rand.Read(bytes)
	if _, err := io.ReadFull(rand.Reader, bytes); err != nil {
		return nil, err
	}
	return bytes, nil
}

// GenerateSalt will generate a salt with the given size.
func GenerateSalt(size int) (string, error) {
	iv, err := generateIv(size)
	if err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(iv), nil
}
