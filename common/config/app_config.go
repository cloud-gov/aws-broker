package config

import "github.com/18F/aws-broker/services/rds"

func InitDefaultAppConfig() AppConfig {
	return AppConfig{DBAdapter:rds.DefaultDBAdapter{}}
}

type AppConfig struct {
	DBAdapter rds.DBAdapter
}
