package rds

import (
	"errors"
	"fmt"
	"log"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	awsRds "github.com/aws/aws-sdk-go/service/rds"
	"github.com/aws/aws-sdk-go/service/rds/rdsiface"
	"github.com/cloud-gov/aws-broker/services/rds"
	"gorm.io/gorm"
)

func reconcileDbParameterGroup(rdsClient rdsiface.RDSAPI, rdsInstance rds.RDSInstance) error {
	resp, err := rdsClient.DescribeDBInstances(&awsRds.DescribeDBInstancesInput{
		DBInstanceIdentifier: aws.String(rdsInstance.Database),
	})

	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok {
			if awsErr.Code() == awsRds.ErrCodeDBInstanceNotFoundFault {
				log.Printf("Could not find database %s, continuing", rdsInstance.Database)
				return nil
			} else {
				return fmt.Errorf("could not describe database instance: %s", err)
			}
		} else {
			return fmt.Errorf("could not describe database instance: %s", err)
		}
	}

	if len(resp.DBInstances) == 0 {
		return fmt.Errorf("could not find database instance info for %s", rdsInstance.Database)
	}

	instanceInfo := resp.DBInstances[0]

	if rdsInstance.ParameterGroupName == "" && len(instanceInfo.DBParameterGroups) > 0 {
		log.Printf("Database %s has parameter groups, but none are recorded in the broker database", rdsInstance.Database)
	}

	if len(instanceInfo.DBParameterGroups) == 0 && rdsInstance.ParameterGroupName != "" {
		log.Printf("Database %s has no parameter groups, but one is recorded in the broker database", rdsInstance.Database)
	}

	return nil
}

func ReconcileRDSParameterGroups(rdsClient rdsiface.RDSAPI, db *gorm.DB) error {
	rows, err := db.Model(&rds.RDSInstance{}).Rows()
	if err != nil {
		return err
	}

	var errs error

	for rows.Next() {
		var rdsInstance rds.RDSInstance
		db.ScanRows(rows, &rdsInstance)

		err := reconcileDbParameterGroup(rdsClient, rdsInstance)
		if err != nil {
			errs = errors.Join(errs, err)
			continue
		}
	}

	return errs
}
