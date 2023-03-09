package rds

import (
	"reflect"
	"testing"

	"github.com/18F/aws-broker/catalog"
	"github.com/18F/aws-broker/helpers"
	"github.com/aws/aws-sdk-go/aws"
)

func TestFormatDBName(t *testing.T) {
	i := RDSInstance{
		Database: "db" + helpers.RandStrNoCaps(15),
	}
	dbName1 := i.FormatDBName()
	dbName2 := i.FormatDBName()
	if dbName1 != dbName2 {
		t.Fatalf("database names should be the same")
	}
}

func TestModifyInstance(t *testing.T) {
	testCases := map[string]struct {
		options          Options
		existingInstance *RDSInstance
		expectedInstance *RDSInstance
		expectErr        bool
		plan             catalog.RDSPlan
	}{
		"update allocated storage": {
			options: Options{
				AllocatedStorage: 20,
			},
			existingInstance: &RDSInstance{
				AllocatedStorage: 10,
			},
			expectedInstance: &RDSInstance{
				AllocatedStorage: 20,
			},
			plan: catalog.RDSPlan{},
		},
		"allocated storage option less than existing, does not update": {
			options: Options{
				AllocatedStorage: 10,
			},
			existingInstance: &RDSInstance{
				AllocatedStorage: 20,
			},
			expectedInstance: &RDSInstance{
				AllocatedStorage: 20,
			},
			expectErr: true,
			plan:      catalog.RDSPlan{},
		},
		"allocated storage empty, does not update": {
			options: Options{
				AllocatedStorage: 0,
			},
			existingInstance: &RDSInstance{
				AllocatedStorage: 20,
			},
			expectedInstance: &RDSInstance{
				AllocatedStorage: 20,
			},
			plan: catalog.RDSPlan{},
		},
		"update backup retention period": {
			options: Options{
				BackupRetentionPeriod: 20,
			},
			existingInstance: &RDSInstance{
				BackupRetentionPeriod: 10,
			},
			expectedInstance: &RDSInstance{
				BackupRetentionPeriod: 20,
			},
			plan: catalog.RDSPlan{},
		},
		"does not update backup retention period": {
			options: Options{
				BackupRetentionPeriod: 0,
			},
			existingInstance: &RDSInstance{
				BackupRetentionPeriod: 20,
			},
			expectedInstance: &RDSInstance{
				BackupRetentionPeriod: 20,
			},
			plan: catalog.RDSPlan{},
		},
		"update binary log format": {
			options: Options{
				BinaryLogFormat: "ROW",
			},
			existingInstance: &RDSInstance{},
			expectedInstance: &RDSInstance{
				BinaryLogFormat: "ROW",
			},
			plan: catalog.RDSPlan{},
		},
		"enable PG cron": {
			options: Options{
				EnablePgCron: aws.Bool(true),
			},
			existingInstance: &RDSInstance{},
			expectedInstance: &RDSInstance{
				EnablePgCron: aws.Bool(true),
			},
			plan: catalog.RDSPlan{},
		},
		"enable PG cron not specified": {
			options:          Options{},
			existingInstance: &RDSInstance{},
			expectedInstance: &RDSInstance{},
			plan:             catalog.RDSPlan{},
		},
		"enable PG cron not specified on options, true on existing instance": {
			options: Options{},
			existingInstance: &RDSInstance{
				EnablePgCron: aws.Bool(true),
			},
			expectedInstance: &RDSInstance{},
			plan:             catalog.RDSPlan{},
		},
	}

	for name, test := range testCases {
		t.Run(name, func(t *testing.T) {
			err := test.existingInstance.modify(test.options, test.plan)
			if !test.expectErr && err != nil {
				t.Fatalf("unexpected error: %s", err)
			}
			if !reflect.DeepEqual(test.existingInstance, test.expectedInstance) {
				t.Fatalf("expected instance: %+v, got instance: %+v", test.expectedInstance, test.existingInstance)
			}
		})
	}
}
