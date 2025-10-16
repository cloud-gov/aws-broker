package rds

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"

	awsRds "github.com/aws/aws-sdk-go-v2/service/rds"
	"github.com/cloud-gov/aws-broker/services/rds"
	"gorm.io/gorm"
)

func FindOrphanedInstances(rdsClient RDSClientInterface, db *gorm.DB, dbNamePrefix string, dbConfigUrl string) error {
	input := &awsRds.DescribeDBInstancesInput{}
	paginator := awsRds.NewDescribeDBInstancesPaginator(rdsClient, input)

	for paginator.HasMorePages() {
		output, err := paginator.NextPage(context.TODO())
		if err != nil {
			return fmt.Errorf("CleanupCustomParameterGroups: error handling next page: %w", err)
		}

		for _, dbInstance := range output.DBInstances {
			instanceName := *dbInstance.DBInstanceIdentifier
			if !strings.Contains(instanceName, dbNamePrefix) {
				log.Printf("database %s is not a brokered database for this environment, continuing", instanceName)
				continue
			}
			if strings.Contains(dbConfigUrl, instanceName) {
				log.Printf("database %s is the database for the broker itself, continuing", instanceName)
				continue
			}
			var rdsDatabase rds.RDSInstance
			err := db.Where("database = ? OR replica_database = ?", instanceName, instanceName).First(&rdsDatabase).Error
			if err != nil {
				if errors.Is(err, gorm.ErrRecordNotFound) {
					log.Printf("database %s does not exist in the broker database", instanceName)
				} else {
					log.Printf("encountered error trying to fetch record from database: %s", err)
				}
			}
			continue
		}
	}

	return nil
}
