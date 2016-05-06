package rds

import (
	"github.com/18F/aws-broker/common/env"
	"github.com/18F/aws-broker/common/request"
	"github.com/18F/aws-broker/common/response"
	"github.com/jinzhu/gorm"
	. "github.com/onsi/ginkgo"
	"github.com/stretchr/testify/assert"
	"net/http"
)

var _ = Describe("Broker", func() {

	Describe("RDS Broker Initialization", func() {
		It("should create a broker of type rdsBroker", func() {
			broker := InitRDSBroker(&gorm.DB{}, &env.SystemEnv{})
			assert.IsType(GinkgoT(), &rdsBroker{}, broker)
		})
	})

	XDescribe("Instance Creation", func() {
		It("should fail if there is already an instance with the uuid", func() {
			broker := InitRDSBroker(&gorm.DB{}, &env.SystemEnv{})
			resp := broker.CreateInstance(nil, "id", request.Request{})
			assert.Equal(GinkgoT(), resp, response.NewErrorResponse(http.StatusConflict, "The instance already exists"))
		})
	})
})
