package config

import (
	"testing"
	"time"

	"github.com/cloud-gov/aws-broker/common"
	"github.com/go-test/deep"
)

func TestSettings(t *testing.T) {
	t.Setenv("AWS_DEFAULT_REGION", "region-1")
	t.Setenv("ENC_KEY", "fake-key")
	t.Setenv("CF_API_URL", "fake-api")
	t.Setenv("CF_API_CLIENT_ID", "fake-client-id")
	t.Setenv("CF_API_CLIENT_SECRET", "fake-client-secret")
	// ensure that these are set to empty to override any test environment variables in .env
	t.Setenv("DB_SSLMODE", "")
	t.Setenv("DB_TYPE", "")

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
		Region:                       "region-1",
		EncryptionKey:                "fake-key",
		DbNamePrefix:                 "db",
		DbShorthandPrefix:            "db",
		MaxAllocatedStorage:          1024,
		PubliclyAccessibleFeature:    false,
		EnableFunctionsFeature:       false,
		SnapshotsRepoName:            "cg-archive",
		LastSnapshotName:             "cg-last-snapshot",
		MaxBackupRetention:           35,
		MinBackupRetention:           14,
		CfApiUrl:                     "fake-api",
		CfApiClientId:                "fake-client-id",
		CfApiClientSecret:            "fake-client-secret",
		PollAwsMaxDurationMultiplier: 1,
		PollAwsMinDelay:              30 * time.Second,
		PollAwsMaxDuration:           3600 * time.Second,
	}
	if diff := deep.Equal(settings, expectedSettings); diff != nil {
		t.Error(diff)
	}
}
