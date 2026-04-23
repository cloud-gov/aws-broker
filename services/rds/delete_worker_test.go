package rds

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	rdsTypes "github.com/aws/aws-sdk-go-v2/service/rds/types"
	"github.com/cloud-gov/aws-broker/base"
	"github.com/cloud-gov/aws-broker/config"
	"github.com/cloud-gov/aws-broker/helpers"
	"github.com/cloud-gov/aws-broker/helpers/request"
	jobs "github.com/cloud-gov/aws-broker/jobs"
)

func TestAsyncDeleteDB(t *testing.T) {
	dbInstanceNotFoundErr := &rdsTypes.DBInstanceNotFoundFault{
		Message: aws.String("not found"),
	}
	brokerDB, err := testDBInit()
	if err != nil {
		t.Fatal(err)
	}

	testCases := map[string]struct {
		ctx                 context.Context
		dbInstance          *RDSInstance
		worker              *DeleteWorker
		expectedState       base.InstanceState
		expectedRecordCount int64
		expectErr           bool
	}{
		"success without replica": {
			ctx: context.Background(),
			worker: NewDeleteWorker(
				brokerDB,
				&config.Settings{
					PollAwsMinDelay:    1 * time.Millisecond,
					PollAwsMaxDuration: 1 * time.Millisecond,
				},
				&mockRDSClient{
					describeDbInstancesErrs: []error{dbInstanceNotFoundErr},
				},
				slog.New(&mockLogHandler{}),
				&mockParameterGroupClient{},
				&mockCredentialUtils{},
			),
			dbInstance: &RDSInstance{
				Instance: base.Instance{
					Request: request.Request{
						ServiceID: helpers.RandStr(10),
					},
					Uuid: helpers.RandStr(10),
				},
				Database: helpers.RandStr(10),
			},
			expectedState: base.InstanceGone,
		},
		"success with replica": {
			ctx: context.Background(),
			worker: NewDeleteWorker(
				brokerDB,
				&config.Settings{
					PollAwsMinDelay:    1 * time.Millisecond,
					PollAwsMaxDuration: 1 * time.Millisecond,
				},
				&mockRDSClient{
					describeDbInstancesErrs: []error{dbInstanceNotFoundErr, dbInstanceNotFoundErr},
				},
				slog.New(&mockLogHandler{}),
				&mockParameterGroupClient{},
				&mockCredentialUtils{},
			),
			dbInstance: &RDSInstance{
				Instance: base.Instance{
					Request: request.Request{
						ServiceID: helpers.RandStr(10),
					},
					Uuid: helpers.RandStr(10),
				},
				Database:        helpers.RandStr(10),
				ReplicaDatabase: helpers.RandStr(10),
			},
			expectedState: base.InstanceGone,
		},
		"error checking database status": {
			ctx:       context.Background(),
			expectErr: true,
			worker: NewDeleteWorker(
				brokerDB,
				&config.Settings{
					PollAwsMinDelay:    1 * time.Millisecond,
					PollAwsMaxDuration: 1 * time.Millisecond,
				},
				&mockRDSClient{
					describeDbInstancesErrs: []error{errors.New("error describing database instances")},
				},
				slog.New(&mockLogHandler{}),
				&mockParameterGroupClient{},
				&mockCredentialUtils{},
			),
			dbInstance: &RDSInstance{
				Instance: base.Instance{
					Request: request.Request{
						ServiceID: helpers.RandStr(10),
					},
					Uuid: helpers.RandStr(10),
				},
				Database: helpers.RandStr(10),
			},
			expectedState:       base.InstanceNotGone,
			expectedRecordCount: 1,
		},
		"error checking replica database status": {
			ctx:       context.Background(),
			expectErr: true,
			worker: NewDeleteWorker(
				brokerDB,
				&config.Settings{
					PollAwsMinDelay:    1 * time.Millisecond,
					PollAwsMaxDuration: 1 * time.Millisecond,
				},
				&mockRDSClient{
					describeDbInstancesErrs: []error{errors.New("error describing database instances")},
				},
				slog.New(&mockLogHandler{}),
				&mockParameterGroupClient{},
				&mockCredentialUtils{},
			),
			dbInstance: &RDSInstance{
				Instance: base.Instance{
					Request: request.Request{
						ServiceID: helpers.RandStr(10),
					},
					Uuid: helpers.RandStr(10),
				},
				Database:        helpers.RandStr(10),
				ReplicaDatabase: helpers.RandStr(10),
			},
			expectedState:       base.InstanceNotGone,
			expectedRecordCount: 1,
		},
		"error deleting database": {
			ctx:       context.Background(),
			expectErr: true,
			worker: NewDeleteWorker(
				brokerDB,
				&config.Settings{
					PollAwsMinDelay:    1 * time.Millisecond,
					PollAwsMaxDuration: 1 * time.Millisecond,
				},
				&mockRDSClient{
					deleteDbInstancesErrs: []error{errors.New("failed to delete database")},
				},
				slog.New(&mockLogHandler{}),
				&mockParameterGroupClient{},
				&mockCredentialUtils{},
			),
			dbInstance: &RDSInstance{
				Instance: base.Instance{
					Request: request.Request{
						ServiceID: helpers.RandStr(10),
					},
					Uuid: helpers.RandStr(10),
				},
				Database: helpers.RandStr(10),
			},
			expectedState:       base.InstanceNotGone,
			expectedRecordCount: 1,
		},
		"error deleting replica database": {
			ctx:       context.Background(),
			expectErr: true,
			worker: NewDeleteWorker(
				brokerDB,
				&config.Settings{
					PollAwsMinDelay:    1 * time.Millisecond,
					PollAwsMaxDuration: 1 * time.Millisecond,
				},
				&mockRDSClient{
					deleteDbInstancesErrs: []error{errors.New("failed to delete database")},
				},
				slog.New(&mockLogHandler{}),
				&mockParameterGroupClient{},
				&mockCredentialUtils{},
			),
			dbInstance: &RDSInstance{
				Instance: base.Instance{
					Request: request.Request{
						ServiceID: helpers.RandStr(10),
					},
					Uuid: helpers.RandStr(10),
				},
				Database:        helpers.RandStr(10),
				ReplicaDatabase: helpers.RandStr(10),
			},
			expectedState:       base.InstanceNotGone,
			expectedRecordCount: 1,
		},
		"database already deleted": {
			ctx: context.Background(),
			worker: NewDeleteWorker(
				brokerDB,
				&config.Settings{
					PollAwsMinDelay:    1 * time.Millisecond,
					PollAwsMaxDuration: 1 * time.Millisecond,
				},
				&mockRDSClient{
					deleteDbInstancesErrs: []error{dbInstanceNotFoundErr},
				},
				slog.New(&mockLogHandler{}),
				&mockParameterGroupClient{},
				&mockCredentialUtils{},
			),
			dbInstance: &RDSInstance{
				Instance: base.Instance{
					Request: request.Request{
						ServiceID: helpers.RandStr(10),
					},
					Uuid: helpers.RandStr(10),
				},
				Database: helpers.RandStr(10),
			},
			expectedState: base.InstanceGone,
		},
		"replica and database already deleted": {
			ctx: context.Background(),
			worker: NewDeleteWorker(
				brokerDB,
				&config.Settings{
					PollAwsMinDelay:    1 * time.Millisecond,
					PollAwsMaxDuration: 1 * time.Millisecond,
				},
				&mockRDSClient{
					deleteDbInstancesErrs: []error{dbInstanceNotFoundErr, dbInstanceNotFoundErr},
				},
				slog.New(&mockLogHandler{}),
				&mockParameterGroupClient{},
				&mockCredentialUtils{},
			),
			dbInstance: &RDSInstance{
				Instance: base.Instance{
					Request: request.Request{
						ServiceID: helpers.RandStr(10),
					},
					Uuid: helpers.RandStr(10),
				},
				Database:        helpers.RandStr(10),
				ReplicaDatabase: helpers.RandStr(10),
			},
			expectedState: base.InstanceGone,
		},
	}

	for name, test := range testCases {
		t.Run(name, func(t *testing.T) {
			err := brokerDB.Create(test.dbInstance).Error
			if err != nil {
				t.Fatal(err)
			}

			var count int64
			brokerDB.Where("uuid = ?", test.dbInstance.Uuid).First(test.dbInstance).Count(&count)
			if count == 0 {
				t.Fatal("The instance should be in the DB")
			}

			err = test.worker.asyncDeleteDB(test.ctx, test.dbInstance)
			if err != nil && !test.expectErr {
				t.Fatalf("unexpected error: %s", err)
			}
			if err == nil && test.expectErr {
				t.Fatal("expected error but received none")
			}

			asyncJobMsg, err := jobs.GetLastAsyncJobMessage(brokerDB, test.dbInstance.ServiceID, test.dbInstance.Uuid, base.DeleteOp)
			if err != nil {
				t.Fatal(err)
			}

			if asyncJobMsg.JobState.State != test.expectedState {
				t.Fatalf("expected state: %s, got: %s", test.expectedState, asyncJobMsg.JobState.State)
			}

			brokerDB.Where("uuid = ?", test.dbInstance.Uuid).First(test.dbInstance).Count(&count)
			if count != test.expectedRecordCount {
				t.Fatalf("expected %d records, found %d", test.expectedRecordCount, count)
			}
		})
	}
}
