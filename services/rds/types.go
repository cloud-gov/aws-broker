package rds

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/service/rds"
	rdsTypes "github.com/aws/aws-sdk-go-v2/service/rds/types"
)

type RDSClientInterface interface {
	CreateDBParameterGroup(ctx context.Context, params *rds.CreateDBParameterGroupInput, optFns ...func(*rds.Options)) (*rds.CreateDBParameterGroupOutput, error)
	DeleteDBParameterGroup(ctx context.Context, params *rds.DeleteDBParameterGroupInput, optFns ...func(*rds.Options)) (*rds.DeleteDBParameterGroupOutput, error)
	DescribeDBEngineVersions(ctx context.Context, params *rds.DescribeDBEngineVersionsInput, optFns ...func(*rds.Options)) (*rds.DescribeDBEngineVersionsOutput, error)
	DescribeDBInstances(ctx context.Context, params *rds.DescribeDBInstancesInput, optFns ...func(*rds.Options)) (*rds.DescribeDBInstancesOutput, error)
	DescribeDBParameterGroups(ctx context.Context, params *rds.DescribeDBParameterGroupsInput, optFns ...func(*rds.Options)) (*rds.DescribeDBParameterGroupsOutput, error)
	DescribeDBParameters(ctx context.Context, params *rds.DescribeDBParametersInput, optFns ...func(*rds.Options)) (*rds.DescribeDBParametersOutput, error)
	DescribeEngineDefaultParameters(ctx context.Context, params *rds.DescribeEngineDefaultParametersInput, optFns ...func(*rds.Options)) (*rds.DescribeEngineDefaultParametersOutput, error)
	ModifyDBParameterGroup(ctx context.Context, params *rds.ModifyDBParameterGroupInput, optFns ...func(*rds.Options)) (*rds.ModifyDBParameterGroupOutput, error)
}

var rdsApplyMethodMap = map[string]rdsTypes.ApplyMethod{
	"immediate":      rdsTypes.ApplyMethodImmediate,
	"pending-reboot": rdsTypes.ApplyMethodPendingReboot,
}

func getRdsApplyMethodEnum(applyMethodString string) (*rdsTypes.ApplyMethod, error) {
	if volumeType, ok := rdsApplyMethodMap[applyMethodString]; ok {
		return &volumeType, nil
	}
	return nil, fmt.Errorf("invalid volume type: %s", applyMethodString)
}
