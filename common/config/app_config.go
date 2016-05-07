package config

import "github.com/18F/aws-broker/services/rds"

// InitDefaultAppConfig creates an AppConfig with structs to use default.
func InitDefaultAppConfig() AppConfig {
	return AppConfig{DBAdapter:rds.DefaultDBAdapter{}}
}

// AppConfig is a holder of stateless structs.
// This allows us to switch out structs during tests to make easily testable code.
type AppConfig struct {
	DBAdapter rds.DBAdapter
}
