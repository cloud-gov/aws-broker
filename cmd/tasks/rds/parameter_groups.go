package rds

import (
	"errors"
	"fmt"
	"log"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	awsRds "github.com/aws/aws-sdk-go/service/rds"
	"github.com/aws/aws-sdk-go/service/rds/rdsiface"
	"github.com/cloud-gov/aws-broker/services/rds"
	"gorm.io/gorm"
)

func updateParameterGroupInBrokerDatabase(rdsInstance rds.RDSInstance, parameterGroupName string, db *gorm.DB) error {
	log.Printf("Database %s has custom parameter group %s, but none is recorded in the broker database", rdsInstance.Database, parameterGroupName)
	rdsInstance.ParameterGroupName = parameterGroupName
	err := db.Save(rdsInstance).Error
	if err == nil {
		log.Printf("Updated database %s to have custom parameter group %s in broker database", rdsInstance.Database, parameterGroupName)
	}
	return err
}

func reconcileDbParameterGroup(rdsClient rdsiface.RDSAPI, rdsInstance rds.RDSInstance, db *gorm.DB) error {
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
		for _, parameterGroup := range instanceInfo.DBParameterGroups {
			if strings.HasPrefix(*parameterGroup.DBParameterGroupName, "cg-aws-broker-") {
				err := updateParameterGroupInBrokerDatabase(rdsInstance, *parameterGroup.DBParameterGroupName, db)
				if err != nil {
					log.Printf("Error updating parameter group for %s, continuing", rdsInstance.Database)
					continue
				}
			}
		}
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

		err := reconcileDbParameterGroup(rdsClient, rdsInstance, db)
		if err != nil {
			errs = errors.Join(errs, err)
			continue
		}
	}

	return errs
}

func FindUnusedParameterGroups(rdsClient rdsiface.RDSAPI, dbNamePrefix string, dbConfigUrl string) error {
	err := rdsClient.DescribeDBInstancesPages(&awsRds.DescribeDBInstancesInput{}, func(page *awsRds.DescribeDBInstancesOutput, lastPage bool) bool {
		for _, dbInstance := range page.DBInstances {
			instanceName := *dbInstance.DBInstanceIdentifier
			if !strings.Contains(instanceName, dbNamePrefix) {
				log.Printf("database %s is not a brokered database for this environment, continuing", instanceName)
				continue
			}
			if strings.Contains(dbConfigUrl, instanceName) {
				log.Printf("database %s is the database for the broker itself, continuing", instanceName)
				continue
			}

			// collect all parameter groups for active brokered databases

			continue
		}
		return !lastPage // Continue iterating until the last page is reached.
	})

	if err != nil {
		return err
	}

	return nil
}
