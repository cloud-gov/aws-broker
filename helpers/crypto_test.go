package helpers

import (
	"crypto/aes"
	"fmt"
	"testing"
)

func TestEncryption(t *testing.T) {
	msg := "Very secure message"
	key := "12345678901234567890123456789012"
	iv, err := generateIv(aes.BlockSize)
	if err != nil {
		t.Fatal(err)
	}

	encrypted, nonce, err := Encrypt(msg, iv, key)
	if err != nil {
		t.Fatal(err)
	}

	if encrypted == msg {
		t.Error("encrypted and original can't be the same")
	}

	decrypted, err := Decrypt(encrypted, iv, nonce, key)
	if err != nil {
		t.Fatal(err)
	}

	if decrypted != msg {
		t.Error("decrypted should be the same as the original")
	}
}

func TestIvChangesEncryption(t *testing.T) {
	msg := "Very secure message"
	key := "12345678901234567890123456789012"
	iv1, err := generateIv(aes.BlockSize)
	if err != nil {
		t.Fatal(err)
	}
	iv2, err := generateIv(aes.BlockSize)
	if err != nil {
		t.Fatal(err)
	}

	encrypted1, _, _ := Encrypt(msg, iv1, key)
	encrypted2, _, _ := Encrypt(msg, iv2, key)

	if encrypted1 == encrypted2 {
		t.Error("different ivs should return different strings")
	}
}

func TestKeyChangesEncryption(t *testing.T) {
	msg := "Very secure message"
	key1 := "12345678901234567890123456789012"
	key2 := "21098765432109876543210987654321"
	iv, err := generateIv(aes.BlockSize)
	if err != nil {
		t.Fatal(err)
	}

	encrypted1, _, _ := Encrypt(msg, iv, key1)
	encrypted2, _, _ := Encrypt(msg, iv, key2)

	if encrypted1 == encrypted2 {
		t.Error("different ivs should return different strings")
	}
}

func TestRandStringGeneration(t *testing.T) {
	randstrings := []string{}

	for i := 0; i < 1000000; i++ {
		randstrings = append(randstrings, RandStr(10))
	}

	fmt.Println(randstrings[1])

	dict := make(map[string]bool)
	duplicatestrings := []string{}

	for _, randstring := range randstrings {
		if _, value := dict[randstring]; value {
			dict[randstring] = true
			duplicatestrings = append(duplicatestrings, randstring)
		}
	}

	if len(duplicatestrings) != 0 {
		t.Error("One or more strings were generated with the same value more than once: ", duplicatestrings)
	}

}

func TestRandStringDistribution(t *testing.T) {
	dict := make(map[string]int)
	for i := 0; i < 1000000; i++ {
		randstring := RandStr(1)
		dict[randstring]++
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
