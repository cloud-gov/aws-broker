package helpers

import (
	"crypto/aes"
	"testing"
)

func TestEncryption(t *testing.T) {
	msg := "Very secure message"
	key := "12345678901234567890123456789012"
	iv := generateIv(aes.BlockSize)

	encrypted, _ := Encrypt(msg, key, iv)

	if encrypted == msg {
		t.Error("encrypted and original can't be the same")
	}

	decrypted, _ := Decrypt(encrypted, key, iv)

	if decrypted != msg {
		t.Error("decrypted should be the same as the original")
	}
}

func TestIvChangesEncryption(t *testing.T) {
	msg := "Very secure message"
	key := "12345678901234567890123456789012"
	iv1 := generateIv(aes.BlockSize)
	iv2 := generateIv(aes.BlockSize)

	encrypted1, _ := Encrypt(msg, key, iv1)
	encrypted2, _ := Encrypt(msg, key, iv2)

	if encrypted1 == encrypted2 {
		t.Error("different ivs should return different strings")
	}
}

func TestKeyChangesEncryption(t *testing.T) {
	msg := "Very secure message"
	key1 := "12345678901234567890123456789012"
	key2 := "21098765432109876543210987654321"
	iv := generateIv(aes.BlockSize)

	encrypted1, _ := Encrypt(msg, key1, iv)
	encrypted2, _ := Encrypt(msg, key2, iv)

	if encrypted1 == encrypted2 {
		t.Error("different ivs should return different strings")
	}
}
