package rds

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/lib/pq"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

var postgresRoleName = regexp.MustCompile(`^[a-z][a-z0-9_]{0,62}$`)

const postgresBindingQueryTimeout = 30 * time.Second

type postgresBindingClient interface {
	createRole(context.Context, *RDSInstance, string, string, string, string) error
	disableRole(context.Context, *RDSInstance, string, string) error
	dropRole(context.Context, *RDSInstance, string, string) error
	provisionSchema(context.Context, *RDSInstance, string, *RDSBinding, *RDSSchemaResource, *RDSSchemaAttachment) (string, error)
	setRoleDefaultPrivileges(context.Context, *RDSInstance, string, string, *RDSSchemaResource, *RDSSchemaAttachment) error
	revokeRoleDefaultPrivileges(context.Context, *RDSInstance, string, string, *RDSSchemaResource, *RDSSchemaAttachment) error
	revokeSchemaAttachment(context.Context, *RDSInstance, string, *RDSSchemaResource, *RDSSchemaAttachment) error
	dropSchema(context.Context, *RDSInstance, string, *RDSSchemaResource) error
}

type pqPostgresBindingClient struct {
	sslRootCert string
}

type postgresRoleAttributes struct {
	CreateDB    bool
	CreateRole  bool
	Replication bool
	BypassRLS   bool
}

// noopPostgresBindingClient prevents the HTTP test broker from opening a tenant
// database connection. Production always uses pqPostgresBindingClient.
type noopPostgresBindingClient struct{}

func (noopPostgresBindingClient) createRole(context.Context, *RDSInstance, string, string, string, string) error {
	return nil
}

func (noopPostgresBindingClient) disableRole(context.Context, *RDSInstance, string, string) error {
	return nil
}

func (noopPostgresBindingClient) dropRole(context.Context, *RDSInstance, string, string) error {
	return nil
}

func (c pqPostgresBindingClient) disableRole(ctx context.Context, i *RDSInstance, masterPassword, username string) error {
	if !postgresRoleName.MatchString(username) || !postgresRoleName.MatchString(i.Username) {
		return errors.New("invalid generated PostgreSQL role name")
	}
	db, err := openPostgresAdminConnection(ctx, i, masterPassword, c.sslRootCert)
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()
	queryCtx, cancel := context.WithTimeout(ctx, postgresBindingQueryTimeout)
	defer cancel()

	if _, err := db.ExecContext(queryCtx, fmt.Sprintf("ALTER ROLE %s NOLOGIN", pq.QuoteIdentifier(username))); err != nil {
		return fmt.Errorf("disable binding role: %w", err)
	}
	return nil
}

func (noopPostgresBindingClient) provisionSchema(context.Context, *RDSInstance, string, *RDSBinding, *RDSSchemaResource, *RDSSchemaAttachment) (string, error) {
	return postgresSchemaProvenanceBrokerCreated, nil
}

func (noopPostgresBindingClient) revokeSchemaAttachment(context.Context, *RDSInstance, string, *RDSSchemaResource, *RDSSchemaAttachment) error {
	return nil
}

func (noopPostgresBindingClient) setRoleDefaultPrivileges(context.Context, *RDSInstance, string, string, *RDSSchemaResource, *RDSSchemaAttachment) error {
	return nil
}

func (noopPostgresBindingClient) revokeRoleDefaultPrivileges(context.Context, *RDSInstance, string, string, *RDSSchemaResource, *RDSSchemaAttachment) error {
	return nil
}

func (noopPostgresBindingClient) dropSchema(context.Context, *RDSInstance, string, *RDSSchemaResource) error {
	return nil
}

func (c pqPostgresBindingClient) createRole(ctx context.Context, i *RDSInstance, masterPassword, username, password, profile string) error {
	if !postgresRoleName.MatchString(username) || !postgresRoleName.MatchString(i.Username) {
		return errors.New("invalid generated PostgreSQL role name")
	}
	db, err := openPostgresAdminConnection(ctx, i, masterPassword, c.sslRootCert)
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()
	queryCtx, cancel := context.WithTimeout(ctx, postgresBindingQueryTimeout)
	defer cancel()

	quotedUsername := pq.QuoteIdentifier(username)
	quotedMasterUsername := pq.QuoteIdentifier(i.Username)
	roleAttributes := "NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS"
	if profile == postgresBindingProfileCompatibility {
		masterAttributes, err := postgresRoleAttributesFor(queryCtx, db, i.Username)
		if err != nil {
			return err
		}
		roleAttributes = masterAttributes.sql()
	}
	var exists bool
	if err := db.QueryRowContext(queryCtx, "SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = $1)", username).Scan(&exists); err != nil {
		return fmt.Errorf("check binding role: %w", err)
	}
	inherit := "NOINHERIT"
	if profile == postgresBindingProfileCompatibility {
		inherit = "INHERIT"
	}
	if !exists {
		if _, err := db.ExecContext(queryCtx, fmt.Sprintf("CREATE ROLE %s LOGIN %s %s PASSWORD %s", quotedUsername, inherit, roleAttributes, pq.QuoteLiteral(password))); err != nil {
			return fmt.Errorf("create binding role: %w", err)
		}
	} else if _, err := db.ExecContext(queryCtx, fmt.Sprintf("ALTER ROLE %s LOGIN %s %s PASSWORD %s", quotedUsername, inherit, roleAttributes, pq.QuoteLiteral(password))); err != nil {
		return fmt.Errorf("reconcile binding role password: %w", err)
	}

	if profile == postgresBindingProfileCompatibility {
		// Membership preserves the current master-credential capability set without
		// inventing a new default grant policy. PostgreSQL/RDS support is verified
		// before rollout; failure must block rather than narrow the default profile.
		if _, err := db.ExecContext(queryCtx, fmt.Sprintf("GRANT %s TO %s", quotedMasterUsername, quotedUsername)); err != nil {
			return fmt.Errorf("grant master role membership to binding role: %w", err)
		}
	}
	return nil
}

func postgresRoleAttributesFor(ctx context.Context, db *sql.DB, username string) (postgresRoleAttributes, error) {
	attributes := postgresRoleAttributes{}
	err := db.QueryRowContext(ctx, `SELECT rolcreatedb, rolcreaterole, rolreplication, rolbypassrls FROM pg_roles WHERE rolname = $1`, username).Scan(
		&attributes.CreateDB,
		&attributes.CreateRole,
		&attributes.Replication,
		&attributes.BypassRLS,
	)
	if err != nil {
		return postgresRoleAttributes{}, fmt.Errorf("load master role attributes: %w", err)
	}
	return attributes, nil
}

func (a postgresRoleAttributes) sql() string {
	return strings.Join([]string{
		postgresRoleAttributeSQL("CREATEDB", a.CreateDB),
		postgresRoleAttributeSQL("CREATEROLE", a.CreateRole),
		postgresRoleAttributeSQL("REPLICATION", a.Replication),
		postgresRoleAttributeSQL("BYPASSRLS", a.BypassRLS),
	}, " ")
}

func postgresRoleAttributeSQL(name string, enabled bool) string {
	if enabled {
		return name
	}
	return "NO" + name
}

func (c pqPostgresBindingClient) dropRole(ctx context.Context, i *RDSInstance, masterPassword, username string) error {
	if !postgresRoleName.MatchString(username) || !postgresRoleName.MatchString(i.Username) {
		return errors.New("invalid generated PostgreSQL role name")
	}
	db, err := openPostgresAdminConnection(ctx, i, masterPassword, c.sslRootCert)
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()
	queryCtx, cancel := context.WithTimeout(ctx, postgresBindingQueryTimeout)
	defer cancel()

	quotedUsername := pq.QuoteIdentifier(username)
	quotedMasterUsername := pq.QuoteIdentifier(i.Username)
	var exists bool
	if err := db.QueryRowContext(queryCtx, "SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = $1)", username).Scan(&exists); err != nil {
		return fmt.Errorf("check binding role: %w", err)
	}
	if !exists {
		return nil
	}
	if _, err := db.ExecContext(queryCtx, fmt.Sprintf("REASSIGN OWNED BY %s TO %s", quotedUsername, quotedMasterUsername)); err != nil {
		return fmt.Errorf("reassign binding role objects: %w", err)
	}
	if _, err := db.ExecContext(queryCtx, fmt.Sprintf("DROP OWNED BY %s", quotedUsername)); err != nil {
		return fmt.Errorf("drop binding role privileges: %w", err)
	}
	if _, err := db.ExecContext(queryCtx, fmt.Sprintf("DROP ROLE %s", quotedUsername)); err != nil {
		return fmt.Errorf("drop binding role: %w", err)
	}
	return nil
}

func (c pqPostgresBindingClient) provisionSchema(ctx context.Context, i *RDSInstance, masterPassword string, binding *RDSBinding, resource *RDSSchemaResource, attachment *RDSSchemaAttachment) (string, error) {
	if !postgresRoleName.MatchString(binding.Username) || !postgresSchemaName.MatchString(resource.SchemaName) {
		return "", errors.New("invalid generated PostgreSQL schema resource")
	}
	db, err := openPostgresAdminConnection(ctx, i, masterPassword, c.sslRootCert)
	if err != nil {
		return "", err
	}
	defer func() { _ = db.Close() }()
	queryCtx, cancel := context.WithTimeout(ctx, postgresBindingQueryTimeout)
	defer cancel()
	schema := pq.QuoteIdentifier(resource.SchemaName)
	owner := pq.QuoteIdentifier(resource.OwnerUsername)
	if binding.Profile == postgresBindingProfileOwner {
		var existingOwner string
		err := db.QueryRowContext(queryCtx, `SELECT r.rolname FROM pg_namespace n JOIN pg_roles r ON r.oid = n.nspowner WHERE n.nspname = $1`, resource.SchemaName).Scan(&existingOwner)
		if errors.Is(err, sql.ErrNoRows) {
			if _, err := db.ExecContext(queryCtx, fmt.Sprintf("CREATE SCHEMA %s AUTHORIZATION %s", schema, owner)); err != nil {
				return "", fmt.Errorf("create owner schema: %w", err)
			}
			if _, err := db.ExecContext(queryCtx, fmt.Sprintf("REVOKE ALL ON SCHEMA %s FROM PUBLIC", schema)); err != nil {
				return "", fmt.Errorf("revoke public schema privileges: %w", err)
			}
			return postgresSchemaProvenanceBrokerCreated, nil
		} else if err != nil {
			return "", fmt.Errorf("load schema owner: %w", err)
		} else if existingOwner == resource.OwnerUsername && (resource.Provenance == postgresSchemaProvenanceBrokerCreated || resource.Provenance == postgresSchemaProvenanceAdoptedExternal || resource.Provenance == "") {
			// A retry can reconcile remote DDL that committed before its broker
			// metadata transaction. The binding's immutable adoption flag selects
			// the only permissible provenance for that recovery.
			if binding.AdoptExternal {
				return postgresSchemaProvenanceAdoptedExternal, nil
			}
			return postgresSchemaProvenanceBrokerCreated, nil
		} else if binding.AdoptExternal && existingOwner == i.Username {
			if err := validateAdoptablePostgresSchema(queryCtx, db, resource.SchemaName); err != nil {
				return "", err
			}
			if _, err := db.ExecContext(queryCtx, fmt.Sprintf("ALTER SCHEMA %s OWNER TO %s", schema, owner)); err != nil {
				return "", fmt.Errorf("adopt external schema: %w", err)
			}
			if _, err := db.ExecContext(queryCtx, fmt.Sprintf("REVOKE ALL ON SCHEMA %s FROM PUBLIC", schema)); err != nil {
				return "", fmt.Errorf("revoke public schema privileges: %w", err)
			}
			return postgresSchemaProvenanceAdoptedExternal, nil
		} else if resource.Provenance != postgresSchemaProvenanceBrokerCreated || existingOwner != resource.OwnerUsername {
			return "", errors.New("PostgreSQL schema is not an approved owner resource")
		}
		if _, err := db.ExecContext(queryCtx, fmt.Sprintf("REVOKE ALL ON SCHEMA %s FROM PUBLIC", schema)); err != nil {
			return "", fmt.Errorf("revoke public schema privileges: %w", err)
		}
		return postgresSchemaProvenanceBrokerCreated, nil
	}
	if (binding.Profile != postgresBindingProfileApplication && binding.Profile != postgresBindingProfileMigration) || attachment == nil || !postgresRoleName.MatchString(attachment.Username) {
		return "", errors.New("invalid PostgreSQL schema attachment")
	}
	var existingOwner string
	if err := db.QueryRowContext(queryCtx, `SELECT r.rolname FROM pg_namespace n JOIN pg_roles r ON r.oid = n.nspowner WHERE n.nspname = $1`, resource.SchemaName).Scan(&existingOwner); err != nil {
		return "", fmt.Errorf("load schema owner: %w", err)
	}
	if existingOwner != resource.OwnerUsername {
		return "", errors.New("PostgreSQL schema owner is incompatible with binding claim")
	}
	if binding.Profile == postgresBindingProfileMigration {
		if _, err := db.ExecContext(queryCtx, fmt.Sprintf("GRANT USAGE, CREATE ON SCHEMA %s TO %s", schema, pq.QuoteIdentifier(attachment.Username))); err != nil {
			return "", fmt.Errorf("grant migration schema privileges: %w", err)
		}
		return resource.Provenance, nil
	}
	application := pq.QuoteIdentifier(attachment.Username)
	statements := []string{
		fmt.Sprintf("GRANT USAGE ON SCHEMA %s TO %s", schema, application),
		fmt.Sprintf("GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA %s TO %s", schema, application),
		fmt.Sprintf("GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA %s TO %s", schema, application),
		fmt.Sprintf("GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA %s TO %s", schema, application),
		fmt.Sprintf("GRANT USAGE ON ALL TYPES IN SCHEMA %s TO %s", schema, application),
	}
	for _, statement := range statements {
		if _, err := db.ExecContext(queryCtx, statement); err != nil {
			return "", fmt.Errorf("grant application schema privileges: %w", err)
		}
	}
	return resource.Provenance, nil
}

// validateAdoptablePostgresSchema permits adoption only of an empty schema with
// PostgreSQL's default ACL. This prevents a binding request from taking over a
// customer schema containing objects or a separately managed access boundary.
func validateAdoptablePostgresSchema(ctx context.Context, db *sql.DB, schemaName string) error {
	var hasObjects bool
	if err := db.QueryRowContext(ctx, `SELECT EXISTS (
		SELECT 1
		FROM pg_depend d
		JOIN pg_namespace n ON n.oid = d.refobjid
		WHERE d.refclassid = 'pg_namespace'::regclass
		  AND d.refobjsubid = 0
		  AND d.deptype = 'n'
		  AND n.nspname = $1
	)`, schemaName).Scan(&hasObjects); err != nil {
		return fmt.Errorf("validate adopted schema contents: %w", err)
	}
	if hasObjects {
		return errors.New("PostgreSQL schema is not empty and cannot be adopted")
	}
	var hasCustomACL bool
	if err := db.QueryRowContext(ctx, `SELECT COALESCE(nspacl, acldefault('n', nspowner)) <> acldefault('n', nspowner) FROM pg_namespace WHERE nspname = $1`, schemaName).Scan(&hasCustomACL); err != nil {
		return fmt.Errorf("validate adopted schema ACL: %w", err)
	}
	if hasCustomACL {
		return errors.New("PostgreSQL schema has custom ACLs and cannot be adopted")
	}
	return nil
}

func (c pqPostgresBindingClient) dropSchema(ctx context.Context, i *RDSInstance, masterPassword string, resource *RDSSchemaResource) error {
	if resource.Provenance != postgresSchemaProvenanceBrokerCreated || (resource.State != postgresSchemaStateReady && resource.State != postgresSchemaStateDeleting) || !postgresSchemaName.MatchString(resource.SchemaName) || !postgresRoleName.MatchString(resource.OwnerUsername) {
		return errors.New("invalid generated PostgreSQL schema resource")
	}
	db, err := openPostgresAdminConnection(ctx, i, masterPassword, c.sslRootCert)
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()
	queryCtx, cancel := context.WithTimeout(ctx, postgresBindingQueryTimeout)
	defer cancel()
	var owner string
	err = db.QueryRowContext(queryCtx, `SELECT r.rolname FROM pg_namespace n JOIN pg_roles r ON r.oid = n.nspowner WHERE n.nspname = $1`, resource.SchemaName).Scan(&owner)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("load schema owner: %w", err)
	}
	if owner != resource.OwnerUsername {
		return errors.New("PostgreSQL schema owner is incompatible with binding claim")
	}
	if _, err := db.ExecContext(queryCtx, fmt.Sprintf("DROP SCHEMA %s CASCADE", pq.QuoteIdentifier(resource.SchemaName))); err != nil {
		return fmt.Errorf("drop migration schema: %w", err)
	}
	return nil
}

func (c pqPostgresBindingClient) revokeSchemaAttachment(ctx context.Context, i *RDSInstance, masterPassword string, resource *RDSSchemaResource, attachment *RDSSchemaAttachment) error {
	if !postgresSchemaName.MatchString(resource.SchemaName) || !postgresRoleName.MatchString(resource.OwnerUsername) || !postgresRoleName.MatchString(attachment.Username) {
		return errors.New("invalid generated PostgreSQL schema attachment")
	}
	db, err := openPostgresAdminConnection(ctx, i, masterPassword, c.sslRootCert)
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()
	queryCtx, cancel := context.WithTimeout(ctx, postgresBindingQueryTimeout)
	defer cancel()
	schema := pq.QuoteIdentifier(resource.SchemaName)
	application := pq.QuoteIdentifier(attachment.Username)
	statements := []string{
		fmt.Sprintf("REVOKE ALL ON ALL TABLES IN SCHEMA %s FROM %s", schema, application),
		fmt.Sprintf("REVOKE ALL ON ALL SEQUENCES IN SCHEMA %s FROM %s", schema, application),
		fmt.Sprintf("REVOKE ALL ON ALL FUNCTIONS IN SCHEMA %s FROM %s", schema, application),
		fmt.Sprintf("REVOKE ALL ON ALL TYPES IN SCHEMA %s FROM %s", schema, application),
		fmt.Sprintf("REVOKE ALL ON SCHEMA %s FROM %s", schema, application),
	}
	for _, statement := range statements {
		if _, err := db.ExecContext(queryCtx, statement); err != nil {
			return fmt.Errorf("revoke application schema privileges: %w", err)
		}
	}
	return nil
}

func (c pqPostgresBindingClient) revokeRoleDefaultPrivileges(ctx context.Context, i *RDSInstance, username, password string, resource *RDSSchemaResource, application *RDSSchemaAttachment) error {
	if !postgresRoleName.MatchString(username) || !postgresRoleName.MatchString(application.Username) || !postgresSchemaName.MatchString(resource.SchemaName) {
		return errors.New("invalid PostgreSQL default privilege revocation request")
	}
	db, err := openPostgresConnection(ctx, i, username, password, c.sslRootCert)
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()
	queryCtx, cancel := context.WithTimeout(ctx, postgresBindingQueryTimeout)
	defer cancel()
	schema := pq.QuoteIdentifier(resource.SchemaName)
	applicationRole := pq.QuoteIdentifier(application.Username)
	for _, statement := range []string{
		fmt.Sprintf("ALTER DEFAULT PRIVILEGES IN SCHEMA %s REVOKE ALL ON TABLES FROM %s", schema, applicationRole),
		fmt.Sprintf("ALTER DEFAULT PRIVILEGES IN SCHEMA %s REVOKE ALL ON SEQUENCES FROM %s", schema, applicationRole),
		fmt.Sprintf("ALTER DEFAULT PRIVILEGES IN SCHEMA %s REVOKE ALL ON FUNCTIONS FROM %s", schema, applicationRole),
		fmt.Sprintf("ALTER DEFAULT PRIVILEGES IN SCHEMA %s REVOKE ALL ON TYPES FROM %s", schema, applicationRole),
	} {
		if _, err := db.ExecContext(queryCtx, statement); err != nil {
			return fmt.Errorf("revoke role default privileges: %w", err)
		}
	}
	return nil
}

// setRoleDefaultPrivileges authenticates as the object creator. PostgreSQL
// default privileges are creator-specific, so an admin role cannot safely
// establish them for a restricted migration role without broader membership.
func (c pqPostgresBindingClient) setRoleDefaultPrivileges(ctx context.Context, i *RDSInstance, username, password string, resource *RDSSchemaResource, application *RDSSchemaAttachment) error {
	if !postgresRoleName.MatchString(username) || !postgresRoleName.MatchString(application.Username) || !postgresSchemaName.MatchString(resource.SchemaName) {
		return errors.New("invalid PostgreSQL migration default privilege request")
	}
	db, err := openPostgresConnection(ctx, i, username, password, c.sslRootCert)
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()
	queryCtx, cancel := context.WithTimeout(ctx, postgresBindingQueryTimeout)
	defer cancel()
	schema := pq.QuoteIdentifier(resource.SchemaName)
	applicationRole := pq.QuoteIdentifier(application.Username)
	for _, statement := range []string{
		fmt.Sprintf("ALTER DEFAULT PRIVILEGES IN SCHEMA %s GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO %s", schema, applicationRole),
		fmt.Sprintf("ALTER DEFAULT PRIVILEGES IN SCHEMA %s GRANT USAGE, SELECT ON SEQUENCES TO %s", schema, applicationRole),
		fmt.Sprintf("ALTER DEFAULT PRIVILEGES IN SCHEMA %s GRANT EXECUTE ON FUNCTIONS TO %s", schema, applicationRole),
		fmt.Sprintf("ALTER DEFAULT PRIVILEGES IN SCHEMA %s GRANT USAGE ON TYPES TO %s", schema, applicationRole),
	} {
		if _, err := db.ExecContext(queryCtx, statement); err != nil {
			return fmt.Errorf("set migration default privileges: %w", err)
		}
	}
	return nil
}

func openPostgresAdminConnection(ctx context.Context, i *RDSInstance, password, sslRootCert string) (*sql.DB, error) {
	return openPostgresConnection(ctx, i, i.Username, password, sslRootCert)
}

func openPostgresConnection(ctx context.Context, i *RDSInstance, username, password, sslRootCert string) (*sql.DB, error) {
	if i.Host == "" || i.Port == 0 {
		return nil, errors.New("database endpoint is not available")
	}
	if sslRootCert == "" {
		return nil, errors.New("POSTGRES_SSLROOTCERT is required for PostgreSQL binding administration")
	}
	if _, err := os.Stat(sslRootCert); err != nil {
		return nil, errors.New("PostgreSQL CA bundle is unavailable")
	}
	dsn := (&url.URL{
		Scheme: "postgres",
		User:   url.UserPassword(username, password),
		Host:   fmt.Sprintf("%s:%d", i.Host, i.Port),
		Path:   formatDBName(i.Database, i.DbType),
		RawQuery: url.Values{
			"connect_timeout": {"10"},
			"sslmode":         {"verify-full"},
			"sslrootcert":     {sslRootCert},
		}.Encode(),
	}).String()
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, fmt.Errorf("open PostgreSQL admin connection: %w", err)
	}
	db.SetConnMaxLifetime(time.Minute)
	db.SetMaxOpenConns(1)
	pctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := db.PingContext(pctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("connect PostgreSQL admin connection: %w", err)
	}
	return db, nil
}

func withPostgresBindingLock(ctx context.Context, db *gorm.DB, instanceID string, operation func(*gorm.DB) error) error {
	return db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		lock := RDSBindingLock{InstanceID: instanceID}
		if err := tx.Clauses(clause.OnConflict{DoNothing: true}).Create(&lock).Error; err != nil {
			return fmt.Errorf("create PostgreSQL binding lifecycle lock: %w", err)
		}
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("instance_id = ?", instanceID).First(&lock).Error; err != nil {
			return fmt.Errorf("lock PostgreSQL binding lifecycle: %w", err)
		}
		return operation(tx)
	})
}
