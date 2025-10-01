package rds

import (
	"errors"
	"log"
	"strings"

	awsRds "github.com/aws/aws-sdk-go/service/rds"
	"github.com/aws/aws-sdk-go/service/rds/rdsiface"
	"github.com/cloud-gov/aws-broker/services/rds"
	"gorm.io/gorm"
)

func FindOrphanedInstances(rdsClient rdsiface.RDSAPI, db *gorm.DB, dbNamePrefix string) error {
	err := rdsClient.DescribeDBInstancesPages(&awsRds.DescribeDBInstancesInput{}, func(page *awsRds.DescribeDBInstancesOutput, lastPage bool) bool {
		for _, dbInstance := range page.DBInstances {
			instanceName := *dbInstance.DBInstanceIdentifier
			if !strings.Contains(instanceName, dbNamePrefix) {
				log.Printf("database %s is not a brokered database, continuing", instanceName)
				continue
			}
			var rdsDatabase rds.RDSInstance
			err := db.Where(&rds.RDSInstance{Database: instanceName}).First(&rdsDatabase).Error
			if errors.Is(err, gorm.ErrRecordNotFound) {
				log.Printf("database %s does not exist in the broker database", instanceName)
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
