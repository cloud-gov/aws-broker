package rds

import (
	"github.com/18F/aws-broker/config"
	"github.com/18F/aws-broker/helpers/request"
	"github.com/18F/aws-broker/helpers/response"
	"github.com/jinzhu/gorm"
	. "github.com/onsi/ginkgo"
	"github.com/stretchr/testify/assert"
	"net/http"
)

var _ = Describe("Broker", func() {

	Describe("RDS Broker Initialization", func() {
		It("should create a broker of type rdsBroker", func() {
			broker := InitRDSBroker(&gorm.DB{}, &config.Settings{})
			assert.IsType(GinkgoT(), &rdsBroker{}, broker)
		})
	})

	Describe("Instance Creation", func() {
		It("should fail if there is already an instance with the uuid", func() {
			broker := InitRDSBroker(&gorm.DB{}, &config.Settings{})
			resp := broker.CreateInstance(nil, "id", request.Request{})
			assert.Equal(GinkgoT(), resp, response.NewErrorResponse(http.StatusConflict, "The instance already exists"))
		})
	})
})
