package logs

import "github.com/aws/aws-sdk-go/service/rds/rdsiface"

func ReconcileRDSCloudwatchLogGroups(rdsClient rdsiface.RDSAPI) error {
	return nil
}
