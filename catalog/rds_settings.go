package catalog

import (
	"errors"
	"github.com/18F/aws-broker/common/db"
	"github.com/jinzhu/gorm"
)

// RDSSetting is the wrapper for
type RDSSetting struct {
	DB     *gorm.DB
	Config db.Config
}

// RDSSettings is a wrapper for all the resources loaded / instantiated.
type RDSSettings struct {
	databases map[string]*RDSSetting
}

// InitRDSSettings tries to construct all the RDSSettings based on the received secrets.
func InitRDSSettings(secrets *Secrets) (*RDSSettings, error) {
	rdsSettings := RDSSettings{databases: make(map[string]*RDSSetting)}
	for _, rdsSecret := range secrets.RdsSecret.RDSDBSecrets {
		db, err := db.Init(&rdsSecret.Config)
		if err == nil {
			rdsSettings.AddRDSSetting(&RDSSetting{DB: db, Config: rdsSecret.Config}, rdsSecret.PlanID)
		} else {
			return nil, err
		}
	}
	return &rdsSettings, nil
}

// AddRDSSetting adds an RDSSetting to the map of RDSSettings with the planID being the key.
func (s *RDSSettings) AddRDSSetting(rdsSetting *RDSSetting, planID string) {
	if s.databases == nil {
		s.databases = make(map[string]*RDSSetting)
	}
	// TODO do additional checks to see if one already exists for that plan id.
	s.databases[planID] = rdsSetting
}

// GetRDSSettingByPlan retrieves the RDS setting based on its planID.
func (s *RDSSettings) GetRDSSettingByPlan(planID string) (*RDSSetting, error) {
	if setting, ok := s.databases[planID]; ok {
		return setting, nil
	}
	return nil, ErrNoRDSSettingForID
}

var (
	// ErrNoRDSSettingForID describes the error when no setting can be found
	ErrNoRDSSettingForID = errors.New("Cannot find rds setting by plan id.")
)
