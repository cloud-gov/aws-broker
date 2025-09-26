package rds

import (
	"errors"
	"log"

	awsRds "github.com/aws/aws-sdk-go/service/rds"
	"github.com/aws/aws-sdk-go/service/rds/rdsiface"
	"github.com/cloud-gov/aws-broker/services/rds"
	"gorm.io/gorm"
)

func FindOrphanedInstances(rdsClient rdsiface.RDSAPI, db *gorm.DB) error {
	err := rdsClient.DescribeDBInstancesPages(&awsRds.DescribeDBInstancesInput{}, func(page *awsRds.DescribeDBInstancesOutput, lastPage bool) bool {
		for _, dbInstance := range page.DBInstances {
			var rdsDatabase rds.RDSInstance
			err := db.Where(&rds.RDSInstance{Database: *dbInstance.DBName}).First(&rdsDatabase).Error
			if errors.Is(err, gorm.ErrRecordNotFound) {
				log.Printf("database %s does not exist in the broker database", *dbInstance.DBName)
			} else {
				log.Printf("encountered error trying to fetch record from database: %s", err)
			}
			continue
		}
		return !lastPage // Continue iterating until the last page is reached.
	})

	if err != nil {
		return err
	}

	return nil
}
