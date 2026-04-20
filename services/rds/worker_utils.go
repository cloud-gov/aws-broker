package rds

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/rds"
	"github.com/cloud-gov/aws-broker/base"
	"github.com/cloud-gov/aws-broker/config"
	jobs "github.com/cloud-gov/aws-broker/jobs"
	"gorm.io/gorm"
)

type dbContextKey struct{}

func createContextWithDb(ctx context.Context, db *gorm.DB) context.Context {
	return context.WithValue(ctx, dbContextKey{}, db)
}

func getDbConnectionFromContext(ctx context.Context) *gorm.DB {
	return ctx.Value(dbContextKey{}).(*gorm.DB)
}

func waitForDbReady(
	ctx context.Context,
	rdsClient RDSClientInterface,
	db *gorm.DB,
	logger *slog.Logger,
	settings *config.Settings,
	operation base.Operation,
	i *RDSInstance,
	database string,
) error {
	logger.Debug(fmt.Sprintf("Waiting for DB instance %s to be available", database))

	// Create a waiter
	waiter := rds.NewDBInstanceAvailableWaiter(rdsClient, func(dawo *rds.DBInstanceAvailableWaiterOptions) {
		dawo.MinDelay = settings.PollAwsMinDelay
		dawo.LogWaitAttempts = true
	})

	// Define the waiting strategy
	maxWaitTime := getPollAwsMaxWaitTime(i.AllocatedStorage, settings.PollAwsMaxDuration)

	waiterInput := &rds.DescribeDBInstancesInput{
		DBInstanceIdentifier: &database,
	}
	err := waiter.Wait(ctx, waiterInput, maxWaitTime)

	if err != nil {
		updateErr := jobs.WriteAsyncJobMessage(db, i.ServiceID, i.Uuid, operation, base.InstanceNotCreated, fmt.Sprintf("Failed waiting for database to become available: %s", err))
		if updateErr != nil {
			err = fmt.Errorf("while handling error %w, error updating async job message: %w", err, updateErr)
		}
		return fmt.Errorf("waitForDbReady: %w", err)
	}

	return nil
}

func updateDBTags(ctx context.Context, rdsClient RDSClientInterface, i *RDSInstance, dbInstanceARN string) error {
	_, err := rdsClient.AddTagsToResource(ctx, &rds.AddTagsToResourceInput{
		ResourceName: aws.String(dbInstanceARN),
		Tags:         ConvertTagsToRDSTags(i.getTags()),
	})
	return err
}
