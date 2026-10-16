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

type mockPostgresBindingClient struct {
	createCalls int
	dropCalls   int
	createErr   error
	dropErr     error
	username    string
	password    string
}

func (m *mockPostgresBindingClient) createRole(_ context.Context, _ *RDSInstance, _ string, username, password string) error {
	m.createCalls++
	m.username = username
	m.password = password
	return m.createErr
}

func (m *mockPostgresBindingClient) dropRole(_ context.Context, _ *RDSInstance, _ string, username string) error {
	m.dropCalls++
	m.username = username
	return m.dropErr
}

func TestPostgresBindingPersistsUniqueCredentialsAndRetries(t *testing.T) {
	db, err := testDBInit()
	if err != nil {
		t.Fatal(err)
	}
	settings := &config.Settings{EncryptionKey: "0123456789abcdef"}
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
	settings := &config.Settings{EncryptionKey: "0123456789abcdef"}
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
	settings := &config.Settings{EncryptionKey: "0123456789abcdef"}
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

func TestCleanupPostgresBindings(t *testing.T) {
	db, err := testDBInit()
	if err != nil {
		t.Fatal(err)
	}
	settings := &config.Settings{EncryptionKey: "0123456789abcdef"}
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
