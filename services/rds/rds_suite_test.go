package rds_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"testing"
)

func TestRds(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Rds Suite")
}
