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

type postgresBindingClient interface {
	createRole(context.Context, *RDSInstance, string, string, string) error
	dropRole(context.Context, *RDSInstance, string, string) error
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

func (noopPostgresBindingClient) createRole(context.Context, *RDSInstance, string, string, string) error {
	return nil
}

func (noopPostgresBindingClient) dropRole(context.Context, *RDSInstance, string, string) error {
	return nil
}

func (c pqPostgresBindingClient) createRole(ctx context.Context, i *RDSInstance, masterPassword, username, password string) error {
	if !postgresRoleName.MatchString(username) || !postgresRoleName.MatchString(i.Username) {
		return errors.New("invalid generated PostgreSQL role name")
	}
	db, err := openPostgresAdminConnection(ctx, i, masterPassword, c.sslRootCert)
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()

	quotedUsername := pq.QuoteIdentifier(username)
	quotedMasterUsername := pq.QuoteIdentifier(i.Username)
	masterAttributes, err := postgresRoleAttributesFor(ctx, db, i.Username)
	if err != nil {
		return err
	}
	roleAttributes := masterAttributes.sql()
	var exists bool
	if err := db.QueryRowContext(ctx, "SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = $1)", username).Scan(&exists); err != nil {
		return fmt.Errorf("check binding role: %w", err)
	}
	if !exists {
		if _, err := db.ExecContext(ctx, fmt.Sprintf("CREATE ROLE %s LOGIN INHERIT %s PASSWORD %s", quotedUsername, roleAttributes, pq.QuoteLiteral(password))); err != nil {
			return fmt.Errorf("create binding role: %w", err)
		}
	} else if _, err := db.ExecContext(ctx, fmt.Sprintf("ALTER ROLE %s LOGIN INHERIT %s PASSWORD %s", quotedUsername, roleAttributes, pq.QuoteLiteral(password))); err != nil {
		return fmt.Errorf("reconcile binding role password: %w", err)
	}

	// Membership preserves the current master-credential capability set without
	// inventing a new default grant policy. PostgreSQL/RDS support is verified
	// before rollout; failure must block rather than narrow the default profile.
	if _, err := db.ExecContext(ctx, fmt.Sprintf("GRANT %s TO %s", quotedMasterUsername, quotedUsername)); err != nil {
		return fmt.Errorf("grant master role membership to binding role: %w", err)
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

	quotedUsername := pq.QuoteIdentifier(username)
	quotedMasterUsername := pq.QuoteIdentifier(i.Username)
	var exists bool
	if err := db.QueryRowContext(ctx, "SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = $1)", username).Scan(&exists); err != nil {
		return fmt.Errorf("check binding role: %w", err)
	}
	if !exists {
		return nil
	}
	if _, err := db.ExecContext(ctx, fmt.Sprintf("REASSIGN OWNED BY %s TO %s", quotedUsername, quotedMasterUsername)); err != nil {
		return fmt.Errorf("reassign binding role objects: %w", err)
	}
	if _, err := db.ExecContext(ctx, fmt.Sprintf("DROP OWNED BY %s", quotedUsername)); err != nil {
		return fmt.Errorf("drop binding role privileges: %w", err)
	}
	if _, err := db.ExecContext(ctx, fmt.Sprintf("DROP ROLE %s", quotedUsername)); err != nil {
		return fmt.Errorf("drop binding role: %w", err)
	}
	return nil
}

func openPostgresAdminConnection(ctx context.Context, i *RDSInstance, password, sslRootCert string) (*sql.DB, error) {
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
		User:   url.UserPassword(i.Username, password),
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
