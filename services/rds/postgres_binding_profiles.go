package rds

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strings"
)

const (
	postgresBindingProfileCompatibility     = "compatibility"
	postgresBindingProfileOwner             = "owner"
	postgresBindingProfileMigration         = "migration"
	postgresBindingProfileApplication       = "application"
	postgresBindingProfileVersion           = 1
	postgresSchemaStateProvisioning         = "provisioning"
	postgresSchemaStateReady                = "ready"
	postgresSchemaStateDeleting             = "deleting"
	postgresSchemaProvenanceBrokerCreated   = "broker_created"
	postgresSchemaProvenanceAdoptedExternal = "adopted_external"
	postgresSchemaBaselineVersion           = 1
	postgresOwnerUnbindDrop                 = "drop"
	postgresOwnerUnbindKeep                 = "keep"
)

var postgresSchemaName = regexp.MustCompile(`^[a-z][a-z0-9_]{0,62}$`)

type postgresBindParameters struct {
	Version       int    `json:"version"`
	Profile       string `json:"profile"`
	Schema        string `json:"schema"`
	AdoptExternal bool   `json:"adopt_external"`
	OnOwnerUnbind string `json:"on_owner_unbind"`
}

func parsePostgresBindParameters(raw json.RawMessage) (postgresBindParameters, error) {
	if len(bytes.TrimSpace(raw)) == 0 || bytes.Equal(bytes.TrimSpace(raw), []byte("{}")) {
		return postgresBindParameters{Profile: postgresBindingProfileCompatibility}, nil
	}
	if err := validatePostgresBindParameterObject(raw); err != nil {
		return postgresBindParameters{}, err
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var parameters postgresBindParameters
	if err := decoder.Decode(&parameters); err != nil {
		return postgresBindParameters{}, fmt.Errorf("invalid PostgreSQL binding parameters: %w", err)
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return postgresBindParameters{}, errors.New("invalid PostgreSQL binding parameters")
	}
	if parameters.Version != postgresBindingProfileVersion {
		return postgresBindParameters{}, errors.New("unsupported PostgreSQL binding parameter version")
	}
	if parameters.Profile != postgresBindingProfileOwner && parameters.Profile != postgresBindingProfileMigration && parameters.Profile != postgresBindingProfileApplication {
		return postgresBindParameters{}, errors.New("unsupported PostgreSQL binding profile")
	}
	if !postgresSchemaName.MatchString(parameters.Schema) || isReservedPostgresSchema(parameters.Schema) {
		return postgresBindParameters{}, errors.New("invalid PostgreSQL schema name")
	}
	if parameters.Profile != postgresBindingProfileOwner && parameters.AdoptExternal {
		return postgresBindParameters{}, errors.New("only owner bindings may adopt external schemas")
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		return postgresBindParameters{}, errors.New("invalid PostgreSQL binding parameters")
	}
	for field := range fields {
		if field == "version" || field == "profile" || field == "schema" || (parameters.Profile == postgresBindingProfileOwner && (field == "adopt_external" || field == "on_owner_unbind")) {
			continue
		}
		return postgresBindParameters{}, errors.New("unsupported PostgreSQL binding parameter")
	}
	if parameters.Profile == postgresBindingProfileOwner {
		if parameters.OnOwnerUnbind == "" {
			parameters.OnOwnerUnbind = postgresOwnerUnbindDrop
		}
		if parameters.OnOwnerUnbind != postgresOwnerUnbindDrop && parameters.OnOwnerUnbind != postgresOwnerUnbindKeep {
			return postgresBindParameters{}, errors.New("invalid PostgreSQL owner unbind policy")
		}
		if parameters.AdoptExternal && parameters.OnOwnerUnbind != postgresOwnerUnbindKeep {
			return postgresBindParameters{}, errors.New("adopted PostgreSQL schemas require keep owner unbind policy")
		}
	}
	return parameters, nil
}

func validatePostgresBindParameterObject(raw json.RawMessage) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	token, err := decoder.Token()
	if err != nil {
		return errors.New("invalid PostgreSQL binding parameters")
	}
	if delimiter, ok := token.(json.Delim); !ok || delimiter != '{' {
		return errors.New("PostgreSQL binding parameters must be an object")
	}
	keys := map[string]struct{}{}
	for decoder.More() {
		token, err := decoder.Token()
		if err != nil {
			return errors.New("invalid PostgreSQL binding parameters")
		}
		key, ok := token.(string)
		if !ok {
			return errors.New("invalid PostgreSQL binding parameters")
		}
		if _, exists := keys[key]; exists {
			return errors.New("duplicate PostgreSQL binding parameter")
		}
		keys[key] = struct{}{}
		var value json.RawMessage
		if err := decoder.Decode(&value); err != nil {
			return errors.New("invalid PostgreSQL binding parameters")
		}
	}
	if _, err := decoder.Token(); err != nil {
		return errors.New("invalid PostgreSQL binding parameters")
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return errors.New("invalid PostgreSQL binding parameters")
	}
	return nil
}

func isReservedPostgresSchema(name string) bool {
	return name == "public" || name == "information_schema" || name == "pg_catalog" || strings.HasPrefix(name, "pg_")
}
