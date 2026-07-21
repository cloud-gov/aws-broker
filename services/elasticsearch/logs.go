package elasticsearch

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	cloudwatchTypes "github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs/types"
	opensearchTypes "github.com/aws/aws-sdk-go-v2/service/opensearch/types"
)

// logGroupPrefix is the common path prefix for every log group the broker
// creates for OpenSearch domains. The shared CloudWatch Logs resource policy that lets
// OpenSearch  service to write to these groups is managed in terraform-provision (elasticsearch-log-publishing-policy)
const logGroupPrefix = "/aws/OpenSearchService/domains"

type logTypeState struct {
	logType opensearchTypes.LogType
	suffix  string
	enabled bool
	arn     *string
}

// logTypeStates returns the per-log-type state for an instance. The arn pointers
// allow ensureLogGroups to persist created group ARNs back onto the instance
func (i *ElasticsearchInstance) logTypeStates() []logTypeState {
	return []logTypeState{
		{logType: opensearchTypes.LogTypeAuditLogs, suffix: "audit-logs", enabled: i.AuditLogsEnabled, arn: &i.AuditLogsGroupARN},
		{logType: opensearchTypes.LogTypeEsApplicationLogs, suffix: "application-logs", enabled: i.ErrorLogsEnabled, arn: &i.ErrorLogsGroupARN},
		{logType: opensearchTypes.LogTypeSearchSlowLogs, suffix: "search-slow-logs", enabled: i.SearchSlowLogsEnabled, arn: &i.SearchSlowLogsGroupARN},
		{logType: opensearchTypes.LogTypeIndexSlowLogs, suffix: "index-slow-logs", enabled: i.IndexSlowLogsEnabled, arn: &i.IndexSlowLogsGroupARN},
	}
}

func (i *ElasticsearchInstance) anyLogsEnabled() bool {
	for _, s := range i.logTypeStates() {
		if s.enabled {
			return true
		}
	}
	return false
}

// logGroupName returns the Cloudwatch log group name for a domain + log type
func logGroupName(domain string, suffix string) string {
	return fmt.Sprintf("%s/%s/%s", logGroupPrefix, domain, suffix)
}

// logGroupArn returns the full ARN of a log group, which is what LogPublishingOptions expects
func logGroupArn(region string, accountID string, name string) string {
	return fmt.Sprintf("arn:aws-us-gov:logs:%s:%s:log-group:%s", region, accountID, name)
}

// ensureLogGroups creates a Cloudwatch log group (with retention) for every enabled log type
// that does not yet have one. The group ARNs are persisted onto the instance.
func ensureLogGroups(
	ctx context.Context,
	logsClient CloudwatchLogsClientInterface,
	logger *slog.Logger,
	i *ElasticsearchInstance,
	retentionDays int32,
	region string,
	accountID string,
) error {
	for _, state := range i.logTypeStates() {
		if !state.enabled || *state.arn != "" {
			continue
		}

		name := logGroupName(i.Domain, state.suffix)
		_, err := logsClient.CreateLogGroup(ctx, &cloudwatchlogs.CreateLogGroupInput{
			LogGroupName: aws.String(name),
		})
		if err != nil {
			var alreadyExists *cloudwatchTypes.ResourceAlreadyExistsException
			if !errors.As(err, &alreadyExists) {
				logger.Error("ensureLogGroups: CreateLogGroup err", "err", err, "logGroup", name)
				return err
			}
		}

		if retentionDays > 0 {
			_, err = logsClient.PutRetentionPolicy(ctx, &cloudwatchlogs.PutRetentionPolicyInput{
				LogGroupName:    aws.String(name),
				RetentionInDays: aws.Int32(retentionDays),
			})
			if err != nil {
				logger.Error("ensureLogGroups: PutRetentionPolicy err", "err", err, "logGroup", name)
			}
		}

		*state.arn = logGroupArn(region, accountID, name)
	}

	return nil
}

// advancedSecurityOptionsForAudit returns the FGAC config required for audit logs. Assigns the domain
// IAM user as the master user.
func advancedSecurityOptionsForAudit(i *ElasticsearchInstance) *opensearchTypes.AdvancedSecurityOptionsInput {
	if !i.AdvancedSecurityEnabled {
		return nil
	}

	return &opensearchTypes.AdvancedSecurityOptionsInput{
		Enabled:                     aws.Bool(true),
		InternalUserDatabaseEnabled: aws.Bool(false),
		MasterUserOptions: &opensearchTypes.MasterUserOptions{
			MasterUserARN: aws.String(i.IamUserARN),
		},
	}
}

// buildLogPublishingOptions builds the LogPublishingOptions map for
// CreateDomain/UpdateDomainConfig. Only log types that have a created group ARN are included; the enabled flag is the desired state
// so a disabled type whose group still exists is published with Enabled=false.
func buildLogPublishingOptions(i *ElasticsearchInstance) map[string]opensearchTypes.LogPublishingOption {
	options := make(map[string]opensearchTypes.LogPublishingOption)
	for _, state := range i.logTypeStates() {
		if *state.arn == "" {
			continue
		}
		options[string(state.logType)] = opensearchTypes.LogPublishingOption{
			CloudWatchLogsLogGroupArn: aws.String(*state.arn),
			Enabled:                   aws.Bool(state.enabled),
		}
	}
	return options
}

func cleanupLogGroups(
	ctx context.Context,
	logsClient CloudwatchLogsClientInterface,
	logger *slog.Logger,
	i *ElasticsearchInstance) error {

	for _, state := range i.logTypeStates() {
		if *state.arn == "" {
			continue
		}
		name := logGroupName(i.Domain, state.suffix)
		_, err := logsClient.DeleteLogGroup(ctx, &cloudwatchlogs.DeleteLogGroupInput{
			LogGroupName: aws.String(name),
		})
		if err != nil {
			var notFound *cloudwatchTypes.ResourceNotFoundException
			if errors.As(err, &notFound) {
				continue
			}
			logger.Error("cleanupLogGroups: DeleteLogGroup err", "err", err, "logGroup", name)
			return err
		}
	}
	return nil
}
