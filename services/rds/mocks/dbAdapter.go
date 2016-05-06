package mocks

import "github.com/18F/aws-broker/services/rds"
import "github.com/stretchr/testify/mock"

import "github.com/18F/aws-broker/base"

type dbAdapter struct {
	mock.Mock
}

// createDB provides a mock function with given fields: i, password
func (_m *dbAdapter) createDB(i *rds.Instance, password string) (base.InstanceState, error) {
	ret := _m.Called(i, password)

	var r0 base.InstanceState
	if rf, ok := ret.Get(0).(func(*rds.Instance, string) base.InstanceState); ok {
		r0 = rf(i, password)
	} else {
		r0 = ret.Get(0).(base.InstanceState)
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(*rds.Instance, string) error); ok {
		r1 = rf(i, password)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// bindDBToApp provides a mock function with given fields: i, password
func (_m *dbAdapter) bindDBToApp(i *rds.Instance, password string) (map[string]string, error) {
	ret := _m.Called(i, password)

	var r0 map[string]string
	if rf, ok := ret.Get(0).(func(*rds.Instance, string) map[string]string); ok {
		r0 = rf(i, password)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(map[string]string)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(*rds.Instance, string) error); ok {
		r1 = rf(i, password)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// deleteDB provides a mock function with given fields: i
func (_m *dbAdapter) deleteDB(i *rds.Instance) (base.InstanceState, error) {
	ret := _m.Called(i)

	var r0 base.InstanceState
	if rf, ok := ret.Get(0).(func(*rds.Instance) base.InstanceState); ok {
		r0 = rf(i)
	} else {
		r0 = ret.Get(0).(base.InstanceState)
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(*rds.Instance) error); ok {
		r1 = rf(i)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}
