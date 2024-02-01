// Code generated by MockGen. DO NOT EDIT.
// Source: github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicreaderinternal (interfaces: Pool)
//
// Generated by this command:
//
//	mockgen -destination=pool_interface_mock_test.go -write_package_comment=false -package=topicreaderinternal github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicreaderinternal Pool
package topicreaderinternal

import (
	reflect "reflect"

	gomock "go.uber.org/mock/gomock"
)

// MockPool is a mock of Pool interface.
type MockPool struct {
	ctrl     *gomock.Controller
	recorder *MockPoolMockRecorder
}

// MockPoolMockRecorder is the mock recorder for MockPool.
type MockPoolMockRecorder struct {
	mock *MockPool
}

// NewMockPool creates a new mock instance.
func NewMockPool(ctrl *gomock.Controller) *MockPool {
	mock := &MockPool{ctrl: ctrl}
	mock.recorder = &MockPoolMockRecorder{mock}

	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockPool) EXPECT() *MockPoolMockRecorder {
	return m.recorder
}

// Get mocks base method.
func (m *MockPool) Get() any {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Get")
	ret0, _ := ret[0].(any)

	return ret0
}

// Get indicates an expected call of Get.
func (mr *MockPoolMockRecorder) Get() *gomock.Call {
	mr.mock.ctrl.T.Helper()

	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Get", reflect.TypeOf((*MockPool)(nil).Get))
}

// Put mocks base method.
func (m *MockPool) Put(arg0 any) {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "Put", arg0)
}

// Put indicates an expected call of Put.
func (mr *MockPoolMockRecorder) Put(arg0 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()

	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Put", reflect.TypeOf((*MockPool)(nil).Put), arg0)
}
