package config

import (
	"os"
	"testing"

	"github.com/cloud-gov/aws-broker/common"
	"github.com/go-test/deep"
)

func TestSettings(t *testing.T) {
	os.Setenv("AWS_DEFAULT_REGION", "region-1")
	os.Setenv("ENC_KEY", "fake-key")
	os.Setenv("CF_API_URL", "fake-api")
	os.Setenv("CF_API_CLIENT_ID", "fake-client-id")
	os.Setenv("CF_API_CLIENT_SECRET", "fake-client-secret")
	// ensure that this is set to empty to override any test environment variables in .env
	os.Setenv("DB_SSLMODE", "")

	settings := &Settings{}
	err := settings.LoadFromEnv()
	if err != nil {
		t.Fatal(err)
	}

	expectedSettings := &Settings{
		DbConfig: &common.DBConfig{
			Port:    5432,
			Sslmode: "require",
		},
		Region:                    "region-1",
		EncryptionKey:             "fake-key",
		DbNamePrefix:              "db",
		DbShorthandPrefix:         "db",
		MaxAllocatedStorage:       1024,
		PubliclyAccessibleFeature: false,
		EnableFunctionsFeature:    false,
		SnapshotsRepoName:         "cg-archive",
		LastSnapshotName:          "cg-last-snapshot",
		MaxBackupRetention:        35,
		MinBackupRetention:        14,
		CfApiUrl:                  "fake-api",
		CfApiClientId:             "fake-client-id",
		CfApiClientSecret:         "fake-client-secret",
		PollAwsMaxRetries:         120,
		PollAwsRetryDelaySeconds:  30,
	}
	if diff := deep.Equal(settings, expectedSettings); diff != nil {
		t.Error(diff)
	}
}
