package rds

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/cloud-gov/aws-broker/base"
	"github.com/cloud-gov/aws-broker/config"
	"gorm.io/gorm"
)

const postgresBindingTestEncryptionKey = "test-key-1234" + "567"

type mockPostgresBindingClient struct {
	createCalls  int
	disableCalls int
	dropCalls    int
	createErr    error
	dropErr      error
	username     string
	password     string
	schemaDrops  int
	defaultsFor  []string
	revokedFor   []string
}

func (m *mockPostgresBindingClient) createRole(_ context.Context, _ *RDSInstance, _ string, username, password, _ string) error {
	m.createCalls++
	m.username = username
	m.password = password
	return m.createErr
}

func (m *mockPostgresBindingClient) disableRole(_ context.Context, _ *RDSInstance, _ string, username string) error {
	m.disableCalls++
	m.username = username
	return nil
}

func (m *mockPostgresBindingClient) dropRole(_ context.Context, _ *RDSInstance, _ string, username string) error {
	m.dropCalls++
	m.username = username
	return m.dropErr
}

func (m *mockPostgresBindingClient) provisionSchema(_ context.Context, _ *RDSInstance, _ string, binding *RDSBinding, _ *RDSSchemaResource, _ *RDSSchemaAttachment) (string, error) {
	if binding.Profile == postgresBindingProfileOwner {
		return postgresSchemaProvenanceBrokerCreated, nil
	}
	return "", nil
}

func (m *mockPostgresBindingClient) revokeSchemaAttachment(_ context.Context, _ *RDSInstance, _ string, _ *RDSSchemaResource, _ *RDSSchemaAttachment) error {
	return nil
}

func (m *mockPostgresBindingClient) dropSchema(_ context.Context, _ *RDSInstance, _ string, _ *RDSSchemaResource) error {
	m.schemaDrops++
	return nil
}

func (m *mockPostgresBindingClient) setRoleDefaultPrivileges(_ context.Context, _ *RDSInstance, username, _ string, _ *RDSSchemaResource, _ *RDSSchemaAttachment) error {
	m.defaultsFor = append(m.defaultsFor, username)
	return nil
}

func (m *mockPostgresBindingClient) revokeRoleDefaultPrivileges(_ context.Context, _ *RDSInstance, username, _ string, _ *RDSSchemaResource, _ *RDSSchemaAttachment) error {
	m.revokedFor = append(m.revokedFor, username)
	return nil
}

func TestPostgresBindingPersistsUniqueCredentialsAndRetries(t *testing.T) {
	db, err := testDBInit()
	if err != nil {
		t.Fatal(err)
	}
	settings := &config.Settings{EncryptionKey: postgresBindingTestEncryptionKey}
	instance := postgresBindingTestInstance(t, db, settings, "binding-instance-id")
	client := &mockPostgresBindingClient{}
	broker := &rdsBroker{brokerDB: db, settings: settings, postgresBindingClient: client}
	masterPassword := decryptInstancePassword(t, instance, settings)

	first, err := broker.bindPostgresInstance(t.Context(), instance, "binding-one", masterPassword)
	if err != nil {
		t.Fatal(err)
	}
	second, err := broker.bindPostgresInstance(t.Context(), instance, "binding-two", masterPassword)
	if err != nil {
		t.Fatal(err)
	}
	retry, err := broker.bindPostgresInstance(t.Context(), instance, "binding-one", masterPassword)
	if err != nil {
		t.Fatal(err)
	}

	if first["username"] == instance.Username || first["password"] == masterPassword {
		t.Fatal("binding returned the instance master credential")
	}
	if first["username"] == second["username"] || first["password"] == second["password"] {
		t.Fatal("separate bindings did not receive unique credentials")
	}
	if retry["username"] != first["username"] || retry["password"] != first["password"] || client.createCalls != 2 {
		t.Fatal("bind retry did not return the persisted credential")
	}

	stored := RDSBinding{}
	if err := db.Where("instance_id = ? AND binding_id = ?", instance.Uuid, "binding-one").First(&stored).Error; err != nil {
		t.Fatal(err)
	}
	if stored.Password == first["password"] || stored.Salt == "" || !stored.Provisioned {
		t.Fatal("binding credential was not encrypted and marked provisioned")
	}
}

func TestPostgresBindingRetriesFailedRoleCreation(t *testing.T) {
	db, err := testDBInit()
	if err != nil {
		t.Fatal(err)
	}
	settings := &config.Settings{EncryptionKey: postgresBindingTestEncryptionKey}
	instance := postgresBindingTestInstance(t, db, settings, "failed-binding-instance")
	client := &mockPostgresBindingClient{createErr: errors.New("database unavailable")}
	broker := &rdsBroker{brokerDB: db, settings: settings, postgresBindingClient: client}
	masterPassword := decryptInstancePassword(t, instance, settings)

	if _, err := broker.bindPostgresInstance(t.Context(), instance, "binding-id", masterPassword); err == nil {
		t.Fatal("expected role creation failure")
	}
	stored := RDSBinding{}
	if err := db.Where("instance_id = ? AND binding_id = ?", instance.Uuid, "binding-id").First(&stored).Error; err != nil {
		t.Fatal(err)
	}
	if stored.Provisioned {
		t.Fatal("failed role creation marked binding provisioned")
	}
	username := stored.Username
	client.createErr = nil
	credentials, err := broker.bindPostgresInstance(t.Context(), instance, "binding-id", masterPassword)
	if err != nil {
		t.Fatal(err)
	}
	if credentials["username"] != username || client.createCalls != 2 {
		t.Fatal("retry did not reconcile the persisted credential")
	}
}

func TestPostgresBindingUnbindIsRevocableAndIdempotent(t *testing.T) {
	db, err := testDBInit()
	if err != nil {
		t.Fatal(err)
	}
	settings := &config.Settings{EncryptionKey: postgresBindingTestEncryptionKey}
	instance := postgresBindingTestInstance(t, db, settings, "unbind-instance-id")
	client := &mockPostgresBindingClient{}
	broker := &rdsBroker{brokerDB: db, settings: settings, postgresBindingClient: client}
	masterPassword := decryptInstancePassword(t, instance, settings)
	credentials, err := broker.bindPostgresInstance(t.Context(), instance, "binding-id", masterPassword)
	if err != nil {
		t.Fatal(err)
	}
	if err := broker.UnbindInstance(t.Context(), instance.Uuid, "binding-id"); err != nil {
		t.Fatal(err)
	}
	if client.dropCalls != 1 || client.username != credentials["username"] {
		t.Fatal("unbind did not revoke the binding credential")
	}
	if err := broker.UnbindInstance(t.Context(), instance.Uuid, "binding-id"); err != nil {
		t.Fatal(err)
	}
	if client.dropCalls != 1 {
		t.Fatal("repeat unbind was not idempotent")
	}
}

func TestPostgresBindingSecretsAreNotJSONSerialized(t *testing.T) {
	serializedBytes, err := json.Marshal(RDSBinding{Password: "secret", Salt: "salt"})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(serializedBytes), "secret") || strings.Contains(string(serializedBytes), "salt") {
		t.Fatal("binding secret was JSON serialized")
	}
}

func TestDeleteArgsDoNotSerializeCredentials(t *testing.T) {
	serializedBytes, err := json.Marshal(DeleteArgs{InstanceID: "instance-id", Instance: &RDSInstance{Password: "secret", Salt: "salt"}})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(serializedBytes), "password") || strings.Contains(string(serializedBytes), "salt") {
		t.Fatal("delete job serialized credentials")
	}
	if !strings.Contains(string(serializedBytes), "instance-id") {
		t.Fatal("delete job did not retain its instance ID")
	}
}

func TestPostgresRoleAttributesPreserveDelegableCapabilities(t *testing.T) {
	attributes := postgresRoleAttributes{CreateDB: true, CreateRole: true, Replication: false, BypassRLS: true}
	got := attributes.sql()
	want := "CREATEDB CREATEROLE NOREPLICATION BYPASSRLS"
	if got != want {
		t.Fatalf("role attributes = %q, want %q", got, want)
	}
}

func TestParsePostgresBindParameters(t *testing.T) {
	testCases := map[string]struct {
		raw     string
		profile string
		schema  string
		wantErr bool
	}{
		"compatibility omitted": {profile: postgresBindingProfileCompatibility},
		"migration":             {raw: `{"version":1,"profile":"migration","schema":"orders"}`, profile: postgresBindingProfileMigration, schema: "orders"},
		"owner":                 {raw: `{"version":1,"profile":"owner","schema":"orders"}`, profile: postgresBindingProfileOwner, schema: "orders"},
		"application":           {raw: `{"version":1,"profile":"application","schema":"orders"}`, profile: postgresBindingProfileApplication, schema: "orders"},
		"owner keep":            {raw: `{"version":1,"profile":"owner","schema":"orders","on_owner_unbind":"keep"}`, profile: postgresBindingProfileOwner, schema: "orders"},
		"owner adopts":          {raw: `{"version":1,"profile":"owner","schema":"orders","adopt_external":true,"on_owner_unbind":"keep"}`, profile: postgresBindingProfileOwner, schema: "orders"},
		"unknown field":         {raw: `{"version":1,"profile":"migration","schema":"orders","grant":"all"}`, wantErr: true},
		"migration adoption":    {raw: `{"version":1,"profile":"migration","schema":"orders","adopt_external":true}`, wantErr: true},
		"application policy":    {raw: `{"version":1,"profile":"application","schema":"orders","on_owner_unbind":"keep"}`, wantErr: true},
		"adoption drop":         {raw: `{"version":1,"profile":"owner","schema":"orders","adopt_external":true,"on_owner_unbind":"drop"}`, wantErr: true},
		"invalid JSON":          {raw: `{"version":`, wantErr: true},
		"duplicate field":       {raw: `{"version":1,"version":1,"profile":"migration","schema":"orders"}`, wantErr: true},
		"reserved schema":       {raw: `{"version":1,"profile":"migration","schema":"public"}`, wantErr: true},
		"unsafe schema":         {raw: `{"version":1,"profile":"migration","schema":"Orders"}`, wantErr: true},
		"unsupported profile":   {raw: `{"version":1,"profile":"admin","schema":"orders"}`, wantErr: true},
	}
	for name, test := range testCases {
		t.Run(name, func(t *testing.T) {
			parameters, err := parsePostgresBindParameters([]byte(test.raw))
			if test.wantErr {
				if err == nil {
					t.Fatal("expected error")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if parameters.Profile != test.profile || parameters.Schema != test.schema {
				t.Fatalf("parameters = %#v", parameters)
			}
		})
	}
}

func TestPostgresOwnerKeepNeverDropsSchema(t *testing.T) {
	db, err := testDBInit()
	if err != nil {
		t.Fatal(err)
	}
	settings := &config.Settings{EncryptionKey: postgresBindingTestEncryptionKey}
	instance := postgresBindingTestInstance(t, db, settings, "owner-keep-instance")
	client := &mockPostgresBindingClient{}
	broker := &rdsBroker{brokerDB: db, settings: settings, postgresBindingClient: client}
	owner := postgresBindParameters{Version: 1, Profile: postgresBindingProfileOwner, Schema: "orders", OnOwnerUnbind: postgresOwnerUnbindKeep}

	if err := broker.ensurePostgresBinding(t.Context(), instance.Uuid, "owner", owner); err != nil {
		t.Fatal(err)
	}
	if _, err := broker.bindPostgresProfile(t.Context(), instance, "owner", decryptInstancePassword(t, instance, settings)); err != nil {
		t.Fatal(err)
	}
	if err := broker.UnbindInstance(t.Context(), instance.Uuid, "owner"); err != nil {
		t.Fatal(err)
	}
	if client.schemaDrops != 0 {
		t.Fatalf("schema drops = %d, want 0", client.schemaDrops)
	}
	if client.disableCalls != 1 {
		t.Fatalf("owner disable calls = %d, want 1", client.disableCalls)
	}
	var resource RDSSchemaResource
	if err := db.Where("instance_id = ? AND schema_name = ?", instance.Uuid, "orders").First(&resource).Error; err != nil {
		t.Fatalf("kept schema resource was deleted: %v", err)
	}
	var stored RDSBinding
	if err := db.Where("instance_id = ? AND binding_id = ?", instance.Uuid, "owner").First(&stored).Error; err != nil || stored.Provisioned {
		t.Fatalf("disabled owner binding = %#v, err = %v", stored, err)
	}
}

func TestPostgresOwnerMigrationApplicationLifecycle(t *testing.T) {
	db, err := testDBInit()
	if err != nil {
		t.Fatal(err)
	}
	settings := &config.Settings{EncryptionKey: postgresBindingTestEncryptionKey}
	instance := postgresBindingTestInstance(t, db, settings, "profile-instance")
	client := &mockPostgresBindingClient{}
	broker := &rdsBroker{brokerDB: db, settings: settings, postgresBindingClient: client}
	owner := postgresBindParameters{Version: 1, Profile: postgresBindingProfileOwner, Schema: "orders"}
	migration := postgresBindParameters{Version: 1, Profile: postgresBindingProfileMigration, Schema: "orders"}
	application := postgresBindParameters{Version: 1, Profile: postgresBindingProfileApplication, Schema: "orders"}

	if err := broker.ensurePostgresBinding(t.Context(), instance.Uuid, "owner", owner); err != nil {
		t.Fatal(err)
	}
	password := decryptInstancePassword(t, instance, settings)
	if _, err := broker.bindPostgresProfile(t.Context(), instance, "owner", password); err != nil {
		t.Fatal(err)
	}
	if err := broker.ensurePostgresBinding(t.Context(), instance.Uuid, "second-owner", owner); !errors.Is(err, errPostgresBindingConflict) {
		t.Fatalf("second owner error = %v", err)
	}
	if err := broker.ensurePostgresBinding(t.Context(), instance.Uuid, "migration", migration); err != nil {
		t.Fatal(err)
	}
	if _, err := broker.bindPostgresProfile(t.Context(), instance, "migration", password); err != nil {
		t.Fatal(err)
	}
	if err := broker.ensurePostgresBinding(t.Context(), instance.Uuid, "second-migration", migration); !errors.Is(err, errPostgresBindingConflict) {
		t.Fatalf("second migration error = %v", err)
	}
	if err := broker.ensurePostgresBinding(t.Context(), instance.Uuid, "application", application); err != nil {
		t.Fatal(err)
	}
	if _, err := broker.bindPostgresProfile(t.Context(), instance, "application", password); err != nil {
		t.Fatal(err)
	}
	if len(client.defaultsFor) == 0 {
		t.Fatal("application attachment did not establish creator default privileges")
	}
	if err := broker.UnbindInstance(t.Context(), instance.Uuid, "owner"); !errors.Is(err, errPostgresBindingConflict) {
		t.Fatalf("owner unbind error = %v", err)
	}
	if err := broker.UnbindInstance(t.Context(), instance.Uuid, "application"); err != nil {
		t.Fatal(err)
	}
	if len(client.revokedFor) != 2 {
		t.Fatalf("default privilege revocations = %v, want owner and migration", client.revokedFor)
	}
	if err := broker.UnbindInstance(t.Context(), instance.Uuid, "migration"); err != nil {
		t.Fatal(err)
	}
	if err := broker.UnbindInstance(t.Context(), instance.Uuid, "owner"); err != nil {
		t.Fatal(err)
	}
	if client.schemaDrops != 1 {
		t.Fatalf("schema drops = %d, want 1", client.schemaDrops)
	}
}

func TestCleanupPostgresBindings(t *testing.T) {
	db, err := testDBInit()
	if err != nil {
		t.Fatal(err)
	}
	settings := &config.Settings{EncryptionKey: postgresBindingTestEncryptionKey}
	instance := postgresBindingTestInstance(t, db, settings, "delete-instance-id")
	if err := db.Create(&RDSBinding{InstanceID: instance.Uuid, BindingID: "binding-id", Username: "bbindingrole", Password: "encrypted", Salt: "salt", Provisioned: true}).Error; err != nil {
		t.Fatal(err)
	}
	client := &mockPostgresBindingClient{}
	if err := cleanupPostgresBindings(t.Context(), db, instance, settings, instance.credentialUtils, client); err != nil {
		t.Fatal(err)
	}
	if client.dropCalls != 1 || client.username != "bbindingrole" {
		t.Fatal("instance cleanup did not revoke the binding role")
	}
	var count int64
	if err := db.Model(&RDSBinding{}).Where("instance_id = ?", instance.Uuid).Count(&count).Error; err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatal("instance cleanup retained binding state")
	}
}

func postgresBindingTestInstance(t *testing.T, db *gorm.DB, settings *config.Settings, instanceID string) *RDSInstance {
	t.Helper()
	instance := NewRDSInstance()
	instance.Uuid = instanceID
	instance.DbType = "postgres"
	instance.Database = "database"
	instance.Username = "master"
	instance.Host = "database.example"
	instance.Port = 5432
	instance.State = base.InstanceReady
	if err := instance.generateCredentials(settings); err != nil {
		t.Fatal(err)
	}
	if err := db.Create(instance).Error; err != nil {
		t.Fatal(err)
	}
	return instance
}

func decryptInstancePassword(t *testing.T, instance *RDSInstance, settings *config.Settings) string {
	t.Helper()
	password, err := instance.credentialUtils.decryptCredential(instance.Salt, instance.Password, settings.EncryptionKey)
	if err != nil {
		t.Fatal(err)
	}
	return password
}
