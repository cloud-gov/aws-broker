package rds

import (
	"context"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"

	"github.com/aws/aws-sdk-go-v2/service/rds"
	rdsTypes "github.com/aws/aws-sdk-go-v2/service/rds/types"
	"github.com/cloud-gov/aws-broker/config"
)

// Prefix for AWS managed default option groups
const defaultOptionGroupPrefix = "default:"

type optionGroupClient interface {
	ProvisionOrModifyCustomOptionGroup(i *RDSInstance, rdsTags []rdsTypes.Tag) (bool, error)
	CleanupCustomOptionGroups() error
	DeleteOptionGroup(optionGroupName string) error
	IsCustomOptionGroup(optionGroupName string) bool
	IsBrokerOptionGroup(optionGroupName string) bool
	ReconcileRDSInstanceOptionGroup(dbInstanceState *rdsTypes.DBInstance, i RDSInstance) (*RDSInstance, error)
}

type awsOptionsGroupClient struct {
	ctx               context.Context
	rds               RDSClientInterface
	settings          *config.Settings
	optionGroupPrefix string
	logger            *slog.Logger
}

func NewAwsOptionGroupClient(ctx context.Context, rds RDSClientInterface, settings *config.Settings, logger *slog.Logger) *awsOptionsGroupClient {
	return &awsOptionsGroupClient{
		ctx:               ctx,
		rds:               rds,
		settings:          settings,
		optionGroupPrefix: "cg-aws-broker-",
		logger:            logger,
	}
}

func (o *awsOptionsGroupClient) IsCustomOptionGroup(optionGroupName string) bool {
	return optionGroupName != "" && !strings.HasPrefix(optionGroupName, defaultOptionGroupPrefix)
}

func (o *awsOptionsGroupClient) IsBrokerOptionGroup(optionGroupName string) bool {
	return strings.HasPrefix(optionGroupName, o.optionGroupPrefix)
}

func (o *awsOptionsGroupClient) getOptionGroupName(i *RDSInstance, majorEngineVersion string) string {
	return o.optionGroupPrefix + formatDBName(i.Database) + "-option-" + formatDBVersion(majorEngineVersion)
}

func (o *awsOptionsGroupClient) getMajorEngineVersion(i *RDSInstance) (string, error) {
	if i.DbVersion == "" {
		return "", errors.New("database version is required to determine major engine version")
	}

	dbEngineVersionsInput := &rds.DescribeDBEngineVersionsInput{
		Engine:        aws.String(i.DbType),
		EngineVersion: aws.String(i.DbVersion),
	}

	defaultEngineInfo, err := o.rds.DescribeDBEngineVersions(o.ctx, dbEngineVersionsInput)
	if err != nil {
		return "", err
	}
	if len(defaultEngineInfo.DBEngineVersions) == 0 || defaultEngineInfo.DBEngineVersions[0].MajorEngineVersion == nil {
		return "", fmt.Errorf("could not determine major engine version for %s, %s", i.DbType, i.DbVersion)
	}

	return *defaultEngineInfo.DBEngineVersions[0].MajorEngineVersion, nil
}

// Captures the instance's currently attached option group from live AWS state,
// so the broker can recreate the group for the target version upon a major version upgrade.
// Default groups are ignored
func (o *awsOptionsGroupClient) ReconcileRDSInstanceOptionGroup(dbInstanceState *rdsTypes.DBInstance, i RDSInstance) (*RDSInstance, error) {
	reconciledInstance := i

	if len(*&dbInstanceState.OptionGroupMemberships) == 0 {
		return &reconciledInstance, nil
	}

	optionGroupName := dbInstanceState.OptionGroupMemberships[0].OptionGroupName
	if optionGroupName == nil || !o.IsCustomOptionGroup(*optionGroupName) {
		return &reconciledInstance, nil
	}

	reconciledInstance.OptionGroupName = *optionGroupName
	return &reconciledInstance, nil
}

// Returns the named option group
func (o *awsOptionsGroupClient) describeOptionGroup(optionGroupName string) (*rdsTypes.OptionGroup, error) {
	output, err := o.rds.DescribeOptionGroups(o.ctx, &rds.DescribeOptionGroupsInput{
		OptionGroupName: aws.String(optionGroupName),
	})
	if err != nil {
		var notFoundErr *rdsTypes.OptionGroupNotFoundFault
		if errors.As(err, &notFoundErr) {
			return nil, nil
		}
		return nil, fmt.Errorf("describeOptionGroup: error describing option group %s: %w", optionGroupName, err)
	}
	if len(output.OptionGroupsList) == 0 {
		return nil, nil
	}
	return &output.OptionGroupsList[0], nil
}

// Converts the options configured on an existing option group into the configuration needed to
// replicate them into a new group
func optionsFromGroup(optionGroup *rdsTypes.OptionGroup) []rdsTypes.OptionConfiguration {
	optionConfigs := []rdsTypes.OptionConfiguration{}
	for _, option := range optionGroup.Options {
		if option.OptionName == nil {
			continue
		}
		optionConfig := rdsTypes.OptionConfiguration{
			OptionName: option.OptionName,
		}
		// Coppy the setting name/value
		for _, setting := range option.OptionSettings {
			if setting.Name == nil || setting.Value == nil {
				continue
			}
			if setting.IsModifiable != nil && !*setting.IsModifiable {
				continue
			}
			// Only copy options that aren't the default value
			if aws.ToString(setting.Value) == aws.ToString(setting.DefaultValue) {
				continue
			}

			optionConfig.OptionSettings = append(optionConfig.OptionSettings, rdsTypes.OptionSetting{
				Name:  setting.Name,
				Value: setting.Value,
			})
		}
		optionConfigs = append(optionConfigs, optionConfig)
	}
	return optionConfigs
}

// Ensures that an instance with a custom option group keeps a valid one for its target database version.
// On a major version upgrade, we create a new option group with the new target version carrying the same options.
func (o *awsOptionsGroupClient) ProvisionOrModifyCustomOptionGroup(i *RDSInstance, rdsTags []rdsTypes.Tag) (bool, error) {
	// For now, we only manage an option group for instances that
	// already have a custom one attached (which is captured during the reconcile step)
	if !o.IsCustomOptionGroup(i.OptionGroupName) {
		return false, nil
	}

	existingOptionGroupName := i.OptionGroupName

	existingOptionGroup, err := o.describeOptionGroup(existingOptionGroupName)
	if err != nil {
		return false, fmt.Errorf("ProvisionOrModifyCustomOptionGroup: %w", err)
	}
	if existingOptionGroup == nil {
		return false, nil
	}
	existingMajorVersion := aws.ToString(existingOptionGroup.MajorEngineVersion)

	targetMajorVersion, err := o.getMajorEngineVersion(i)
	if err != nil {
		return false, fmt.Errorf("ProvisionOrModifyCustomOptionGroup: %w", err)
	}

	// If major version is unchanged, the existing group is still valid
	if existingMajorVersion == targetMajorVersion {
		return false, nil
	}

	targetOptionGroupName := o.getOptionGroupName(i, targetMajorVersion)
	if targetOptionGroupName == existingOptionGroupName {
		return false, nil
	}

	targetOptionGroup, err := o.describeOptionGroup(targetOptionGroupName)
	if err != nil {
		return false, fmt.Errorf("ProvisionOrModifyCustomOptionGroup: %w", err)
	}

	createdOptionGroup := false
	if targetOptionGroup == nil {
		log.Printf("creating option group %s for %s %s", targetOptionGroupName, i.DbType, targetMajorVersion)
		_, err = o.rds.CreateOptionGroup(o.ctx, &rds.CreateOptionGroupInput{
			OptionGroupName:        aws.String(targetOptionGroupName),
			EngineName:             aws.String(i.DbType),
			MajorEngineVersion:     aws.String(targetMajorVersion),
			OptionGroupDescription: aws.String("aws broker option group for " + formatDBName(i.Database)),
			Tags:                   rdsTags,
		})
		if err != nil {
			return false, fmt.Errorf("ProvisionOrModifyCustomOptionGroup: error creating option group: %w", err)
		}
		createdOptionGroup = true
	}

	existingOptions := optionsFromGroup(existingOptionGroup)
	if len(existingOptions) > 0 {
		_, err = o.rds.ModifyOptionGroup(o.ctx, &rds.ModifyOptionGroupInput{
			OptionGroupName:  aws.String(targetOptionGroupName),
			OptionsToInclude: existingOptions,
			ApplyImmediately: aws.Bool(true),
		})
		if err != nil {
			return false, fmt.Errorf("ProvisionOrModifyCustomOptionGroup: error adding options to option group: %w", err)
		}
	}

	i.OptionGroupName = targetOptionGroupName
	return createdOptionGroup, nil
}

// Deletes a broker-created option group. Retries deletion while it is still in use because
// a recently detached group takes time to become deletable
func (o *awsOptionsGroupClient) DeleteOptionGroup(optionGroupName string) error {
	if optionGroupName == "" {
		return nil
	}

	// Never delete a group the broker did not create.
	if !o.IsBrokerOptionGroup(optionGroupName) {
		o.logger.Info(fmt.Sprintf("skipping deletion of non-broker option group %s", optionGroupName))
		return nil
	}

	existingOptionGroup, err := o.describeOptionGroup(optionGroupName)
	if err != nil {
		return err
	}
	if existingOptionGroup == nil {
		return nil
	}

	_, err = o.rds.DeleteOptionGroup(o.ctx, &rds.DeleteOptionGroupInput{
		OptionGroupName: &optionGroupName,
	})
	if err == nil {
		return nil
	}

	return err
}

// Deletes any broker-created option groups that are no longer in use. Groups still attached
// to an instance fail to delete and are skipped.
func (o *awsOptionsGroupClient) CleanupCustomOptionGroups() error {
	input := &rds.DescribeOptionGroupsInput{}
	paginator := rds.NewDescribeOptionGroupsPaginator(o.rds, input)
	for paginator.HasMorePages() {
		output, err := paginator.NextPage(o.ctx)
		if err != nil {
			return fmt.Errorf("CleanupCustomOptionGroups: error handling next page: %w", err)
		}

		for _, optionGroup := range output.OptionGroupsList {
			if optionGroup.OptionGroupName == nil || !o.IsBrokerOptionGroup(*optionGroup.OptionGroupName) {
				continue
			}

			_, err := o.rds.DeleteOptionGroup(o.ctx, &rds.DeleteOptionGroupInput{
				OptionGroupName: optionGroup.OptionGroupName,
			})
			if err != nil {
				var invalidStateErr *rdsTypes.InvalidOptionGroupStateFault
				if errors.As(err, &invalidStateErr) {
					o.logger.Debug(fmt.Sprintf("could not clean up option group %s: %s", *optionGroup.OptionGroupName, err))
					continue
				}
				var notFoundErr *rdsTypes.OptionGroupNotFoundFault
				if errors.As(err, &notFoundErr) {
					continue
				}
				return fmt.Errorf("CleanupCustomOptionGroups: DeleteOptionGroup err %w", err)
			}

			log.Printf("cleaned up %s option group", *optionGroup.OptionGroupName)
		}
	}

	return nil
}
