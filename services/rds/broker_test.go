package rds

import (
	"github.com/18F/aws-broker/config"
	"github.com/jinzhu/gorm"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestInitRDSBroker(t *testing.T) {
	broker := InitRDSBroker(&gorm.DB{}, &config.Settings{})
	assert.IsType(t, &rdsBroker{}, broker)
}

func TestCreateInstance(t *testing.T) {

}
