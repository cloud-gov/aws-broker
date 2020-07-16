package awsiam_test

import (
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestAWSIAM(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "AWS IAM Suite")
}
