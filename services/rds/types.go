package rds

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/service/rds"
	rdsTypes "github.com/aws/aws-sdk-go-v2/service/rds/types"
)

type RDSClientInterface interface {
	AddTagsToResource(ctx context.Context, params *rds.AddTagsToResourceInput, optFns ...func(*rds.Options)) (*rds.AddTagsToResourceOutput, error)
	CreateDBInstance(ctx context.Context, params *rds.CreateDBInstanceInput, optFns ...func(*rds.Options)) (*rds.CreateDBInstanceOutput, error)
	CreateDBInstanceReadReplica(ctx context.Context, params *rds.CreateDBInstanceReadReplicaInput, optFns ...func(*rds.Options)) (*rds.CreateDBInstanceReadReplicaOutput, error)
	CreateDBParameterGroup(ctx context.Context, params *rds.CreateDBParameterGroupInput, optFns ...func(*rds.Options)) (*rds.CreateDBParameterGroupOutput, error)
	DeleteDBInstance(ctx context.Context, params *rds.DeleteDBInstanceInput, optFns ...func(*rds.Options)) (*rds.DeleteDBInstanceOutput, error)
	DeleteDBParameterGroup(ctx context.Context, params *rds.DeleteDBParameterGroupInput, optFns ...func(*rds.Options)) (*rds.DeleteDBParameterGroupOutput, error)
	DescribeDBEngineVersions(ctx context.Context, params *rds.DescribeDBEngineVersionsInput, optFns ...func(*rds.Options)) (*rds.DescribeDBEngineVersionsOutput, error)
	DescribeDBInstances(ctx context.Context, params *rds.DescribeDBInstancesInput, optFns ...func(*rds.Options)) (*rds.DescribeDBInstancesOutput, error)
	DescribeDBParameterGroups(ctx context.Context, params *rds.DescribeDBParameterGroupsInput, optFns ...func(*rds.Options)) (*rds.DescribeDBParameterGroupsOutput, error)
	DescribeDBParameters(ctx context.Context, params *rds.DescribeDBParametersInput, optFns ...func(*rds.Options)) (*rds.DescribeDBParametersOutput, error)
	DescribeEngineDefaultParameters(ctx context.Context, params *rds.DescribeEngineDefaultParametersInput, optFns ...func(*rds.Options)) (*rds.DescribeEngineDefaultParametersOutput, error)
	ModifyDBInstance(ctx context.Context, params *rds.ModifyDBInstanceInput, optFns ...func(*rds.Options)) (*rds.ModifyDBInstanceOutput, error)
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
