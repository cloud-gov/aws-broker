package helpers

import (
	"crypto/aes"
	"fmt"
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

func TestRandStringDistribution(t *testing.T) {
	dict := make(map[string]int)
	for i := 0; i < 1000000; i++ {
		randstring := RandStr(1)
		dict[randstring] = dict[randstring] + 1
	}
	fmt.Println(dict)
	min := 1000000
	max := 0
	for _, value := range dict {
		if value > max {
			max = value
		}
		if value < min {
			min = value
		}
	}

	fmt.Println("Min: ", min)
	fmt.Println("Max: ", max)
	if float32(min)/float32(max) < float32(.94) {
		t.Error("The Deviation of random characters is too high", float32(min)/float32(max), float32(.95))
	}
}
