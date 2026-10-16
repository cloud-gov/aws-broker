package rds

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"

	"github.com/cloud-gov/aws-broker/asyncmessage"
	"github.com/cloud-gov/aws-broker/base"
	"github.com/cloud-gov/aws-broker/common"
	"github.com/cloud-gov/aws-broker/config"
	"github.com/riverqueue/river"
	"gorm.io/gorm"
)

var (
	ErrDeletingDB                = errors.New("deleting database")
	ErrDeletingDBInstance        = errors.New("deleting database instance")
	ErrDeletingDBOptionGroups    = errors.New("deleting databse option groups")
	ErrDeletingDBParameterGroups = errors.New("deleting database parameter groups")
)

const (
	DeleteKind = "rds-delete"
)

type DeleteArgs struct {
	InstanceID string       `json:"instance_id"`
	Instance   *RDSInstance `json:"instance,omitempty"`
}

// MarshalJSON prevents newly queued jobs from carrying the legacy instance
// payload. Unmarshal remains compatible with jobs queued by older releases.
func (a DeleteArgs) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		InstanceID string `json:"instance_id"`
	}{InstanceID: a.InstanceID})
}

func (DeleteArgs) Kind() string { return DeleteKind }

type DeleteWorker struct {
	river.WorkerDefaults[DeleteArgs]
	db                    *gorm.DB
	settings              *config.Settings
	rds                   RDSClientInterface
	logger                *slog.Logger
	parameterGroupClient  parameterGroupClient
	optionGroupClient     optionGroupClient
	credentialUtils       CredentialUtils
	postgresBindingClient postgresBindingClient
}

func NewDeleteWorker(
	db *gorm.DB,
	settings *config.Settings,
	rds RDSClientInterface,
	logger *slog.Logger,
	parameterGroupClient parameterGroupClient,
	optionGroupClient optionGroupClient,
	credentialUtils CredentialUtils,
) *DeleteWorker {
	return &DeleteWorker{
		db:                    db,
		settings:              settings,
		rds:                   rds,
		logger:                logger,
		parameterGroupClient:  parameterGroupClient,
		optionGroupClient:     optionGroupClient,
		credentialUtils:       credentialUtils,
		postgresBindingClient: pqPostgresBindingClient{sslRootCert: settings.PostgresSSLRootCert},
	}
}

func (w *DeleteWorker) Work(ctx context.Context, job *river.Job[DeleteArgs]) error {
	operation := base.DeleteOp
	instanceID := job.Args.InstanceID
	if instanceID == "" && job.Args.Instance != nil {
		instanceID = job.Args.Instance.Uuid
	}
	if instanceID == "" {
		return errors.New("delete job is missing an RDS instance ID")
	}
	i := NewRDSInstance()
	if err := w.db.Where("uuid = ?", instanceID).First(i).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			// Jobs queued before the ID-only payload change carry the complete
			// instance. Keep them executable until the queue drains.
			if job.Args.Instance != nil {
				i = job.Args.Instance
			} else {
				return nil
			}
		} else {
			return fmt.Errorf("load RDS instance for deletion: %w", err)
		}
	}
	err := w.asyncDeleteDB(ctx, i, operation)
	if err != nil {
		w.logger.Error("error deleting database", "err", err)
		asyncmessage.WriteAsyncJobMessageAndLogError(w.db, w.logger, i.ServiceID, i.Uuid, operation, base.InstanceNotGone, err.Error())
		w.clearPostgresDeleteRequested(i)
		return river.JobCancel(err)
	}
	return nil
}

func (w *DeleteWorker) clearPostgresDeleteRequested(i *RDSInstance) {
	if i.DbType != "postgres" {
		return
	}
	if err := w.db.Model(&RDSInstance{}).Where("uuid = ?", i.Uuid).Update("delete_requested", false).Error; err != nil {
		w.logger.Error("error clearing PostgreSQL deletion fence", "err", err)
	}
}

func (w *DeleteWorker) asyncDeleteDB(ctx context.Context, i *RDSInstance, operation base.Operation) error {
	if i.DbType == "postgres" {
		if err := withPostgresBindingLock(ctx, w.db, i.Uuid, func(tx *gorm.DB) error {
			return cleanupPostgresBindings(ctx, tx, i, w.settings, w.credentialUtils, w.postgresBindingClient)
		}); err != nil {
			return err
		}
	}

	if i.ReplicaDatabase != "" {
		asyncmessage.WriteAsyncJobMessageAndLogError(w.db, w.logger, i.ServiceID, i.Uuid, operation, base.InstanceInProgress, "Deleting database replica")
		err := deleteDatabaseReadReplica(ctx, w.db, w.settings, w.rds, w.logger, i, operation)
		if err != nil {
			return common.FmtErr(ErrDeletingDBReplica, err)
		}
	}

	asyncmessage.WriteAsyncJobMessageAndLogError(w.db, w.logger, i.ServiceID, i.Uuid, operation, base.InstanceInProgress, "Deleting database")
	err := deleteDatabaseInstance(ctx, w.db, w.settings, w.rds, w.logger, i, operation, i.Database)
	if err != nil {
		return common.FmtErr(ErrDeletingDB, err)
	}

	asyncmessage.WriteAsyncJobMessageAndLogError(w.db, w.logger, i.ServiceID, i.Uuid, operation, base.InstanceInProgress, "Deleting parameter group")
	err = w.parameterGroupClient.DeleteParameterGroup(i.ParameterGroupName)
	if err != nil {
		return common.FmtErr(ErrDeletingDBParameterGroup, err)
	}

	asyncmessage.WriteAsyncJobMessageAndLogError(w.db, w.logger, i.ServiceID, i.Uuid, operation, base.InstanceInProgress, "Cleaning up parameter groups")
	err = w.parameterGroupClient.CleanupCustomParameterGroups()
	if err != nil {
		return common.FmtErr(ErrDeletingDBParameterGroups, err)
	}

	asyncmessage.WriteAsyncJobMessageAndLogError(w.db, w.logger, i.ServiceID, i.Uuid, operation, base.InstanceInProgress, "Deleting option group")
	err = w.optionGroupClient.DeleteOptionGroup(i.OptionGroupName)
	if err != nil {
		return common.FmtErr(ErrDeletingDBOptionGroup, err)
	}

	asyncmessage.WriteAsyncJobMessageAndLogError(w.db, w.logger, i.ServiceID, i.Uuid, operation, base.InstanceInProgress, "Cleaning up option groups")
	err = w.optionGroupClient.CleanupCustomOptionGroups()
	if err != nil {
		return common.FmtErr(ErrDeletingDBOptionGroups, err)
	}

	err = w.db.Unscoped().Delete(i).Error
	if err != nil {
		return common.FmtErr(ErrDeletingDBInstance, err)
	}

	asyncmessage.WriteAsyncJobMessageAndLogError(w.db, w.logger, i.ServiceID, i.Uuid, operation, base.InstanceGone, "Successfully deleted database resources")
	return nil
}

func cleanupPostgresBindings(ctx context.Context, db *gorm.DB, i *RDSInstance, settings *config.Settings, credentialUtils CredentialUtils, client postgresBindingClient) error {
	bindings := []RDSBinding{}
	if err := db.Where("instance_id = ?", i.Uuid).Find(&bindings).Error; err != nil {
		return fmt.Errorf("load PostgreSQL binding credentials: %w", err)
	}
	if len(bindings) == 0 {
		return nil
	}
	masterPassword, err := credentialUtils.decryptCredential(i.Salt, i.Password, settings.EncryptionKey)
	if err != nil {
		return fmt.Errorf("decrypt PostgreSQL master credential: %w", err)
	}
	for _, binding := range bindings {
		binding.Profile = normalizedStoredBindingProfile(binding)
		if binding.Profile == postgresBindingProfileOwner {
			continue
		}
		if err := client.dropRole(ctx, i, masterPassword, binding.Username); err != nil {
			return fmt.Errorf("remove PostgreSQL binding principal: %w", err)
		}
		if err := db.Delete(&binding).Error; err != nil {
			return fmt.Errorf("remove PostgreSQL binding credential: %w", err)
		}
	}
	for _, binding := range bindings {
		binding.Profile = normalizedStoredBindingProfile(binding)
		if binding.Profile != postgresBindingProfileOwner {
			continue
		}
		resource := RDSSchemaResource{}
		if err := db.Where("instance_id = ? AND schema_name = ?", i.Uuid, binding.SchemaName).First(&resource).Error; err != nil {
			return fmt.Errorf("load PostgreSQL schema resource: %w", err)
		}
		if resource.Provenance == postgresSchemaProvenanceBrokerCreated && resource.OwnerUnbindPolicy == postgresOwnerUnbindDrop {
			if resource.State != postgresSchemaStateReady {
				return errors.New("PostgreSQL broker-created schema resource is not ready")
			}
			if err := db.Model(&resource).Update("state", postgresSchemaStateDeleting).Error; err != nil {
				return fmt.Errorf("mark PostgreSQL schema resource deleting: %w", err)
			}
			resource.State = postgresSchemaStateDeleting
			if err := client.dropSchema(ctx, i, masterPassword, &resource); err != nil {
				return fmt.Errorf("remove PostgreSQL schema resource: %w", err)
			}
		} else if resource.Provenance != postgresSchemaProvenanceAdoptedExternal && !(resource.Provenance == postgresSchemaProvenanceBrokerCreated && resource.OwnerUnbindPolicy == postgresOwnerUnbindKeep) {
			return errors.New("PostgreSQL schema resource has unknown provenance")
		} else if err := client.disableRole(ctx, i, masterPassword, binding.Username); err != nil {
			return fmt.Errorf("disable retained PostgreSQL owner principal: %w", err)
		} else {
			// The retained schema remains owned by this role. Preserve both until
			// AWS deletes the whole RDS instance.
			continue
		}
		if err := db.Where("schema_resource_id = ?", resource.ID).Delete(&RDSSchemaAttachment{}).Error; err != nil {
			return fmt.Errorf("remove PostgreSQL schema attachments: %w", err)
		}
		if err := db.Delete(&resource).Error; err != nil {
			return fmt.Errorf("remove PostgreSQL schema claim: %w", err)
		}
		if err := client.dropRole(ctx, i, masterPassword, binding.Username); err != nil {
			return fmt.Errorf("remove PostgreSQL binding principal: %w", err)
		}
		if err := db.Delete(&binding).Error; err != nil {
			return fmt.Errorf("remove PostgreSQL binding credential: %w", err)
		}
	}
	return nil
}
