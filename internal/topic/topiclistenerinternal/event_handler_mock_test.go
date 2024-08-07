// Code generated by MockGen. DO NOT EDIT.
// Source: event_handler.go
//
// Generated by this command:
//
//	mockgen -source event_handler.go -destination event_handler_mock_test.go --typed -package topiclistenerinternal -write_package_comment=false
package topiclistenerinternal

import (
	context "context"
	reflect "reflect"

	gomock "go.uber.org/mock/gomock"
)

// MockEventHandler is a mock of EventHandler interface.
type MockEventHandler struct {
	ctrl     *gomock.Controller
	recorder *MockEventHandlerMockRecorder
}

// MockEventHandlerMockRecorder is the mock recorder for MockEventHandler.
type MockEventHandlerMockRecorder struct {
	mock *MockEventHandler
}

// NewMockEventHandler creates a new mock instance.
func NewMockEventHandler(ctrl *gomock.Controller) *MockEventHandler {
	mock := &MockEventHandler{ctrl: ctrl}
	mock.recorder = &MockEventHandlerMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockEventHandler) EXPECT() *MockEventHandlerMockRecorder {
	return m.recorder
}

// OnReadMessages mocks base method.
func (m *MockEventHandler) OnReadMessages(ctx context.Context, event *PublicReadMessages) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "OnReadMessages", ctx, event)
	ret0, _ := ret[0].(error)
	return ret0
}

// OnReadMessages indicates an expected call of OnReadMessages.
func (mr *MockEventHandlerMockRecorder) OnReadMessages(ctx, event any) *MockEventHandlerOnReadMessagesCall {
	mr.mock.ctrl.T.Helper()
	call := mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "OnReadMessages", reflect.TypeOf((*MockEventHandler)(nil).OnReadMessages), ctx, event)
	return &MockEventHandlerOnReadMessagesCall{Call: call}
}

// MockEventHandlerOnReadMessagesCall wrap *gomock.Call
type MockEventHandlerOnReadMessagesCall struct {
	*gomock.Call
}

// Return rewrite *gomock.Call.Return
func (c *MockEventHandlerOnReadMessagesCall) Return(arg0 error) *MockEventHandlerOnReadMessagesCall {
	c.Call = c.Call.Return(arg0)
	return c
}

// Do rewrite *gomock.Call.Do
func (c *MockEventHandlerOnReadMessagesCall) Do(f func(context.Context, *PublicReadMessages) error) *MockEventHandlerOnReadMessagesCall {
	c.Call = c.Call.Do(f)
	return c
}

// DoAndReturn rewrite *gomock.Call.DoAndReturn
func (c *MockEventHandlerOnReadMessagesCall) DoAndReturn(f func(context.Context, *PublicReadMessages) error) *MockEventHandlerOnReadMessagesCall {
	c.Call = c.Call.DoAndReturn(f)
	return c
}

// OnStartPartitionSessionRequest mocks base method.
func (m *MockEventHandler) OnStartPartitionSessionRequest(ctx context.Context, event *PublicEventStartPartitionSession) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "OnStartPartitionSessionRequest", ctx, event)
	ret0, _ := ret[0].(error)
	return ret0
}

// OnStartPartitionSessionRequest indicates an expected call of OnStartPartitionSessionRequest.
func (mr *MockEventHandlerMockRecorder) OnStartPartitionSessionRequest(ctx, event any) *MockEventHandlerOnStartPartitionSessionRequestCall {
	mr.mock.ctrl.T.Helper()
	call := mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "OnStartPartitionSessionRequest", reflect.TypeOf((*MockEventHandler)(nil).OnStartPartitionSessionRequest), ctx, event)
	return &MockEventHandlerOnStartPartitionSessionRequestCall{Call: call}
}

// MockEventHandlerOnStartPartitionSessionRequestCall wrap *gomock.Call
type MockEventHandlerOnStartPartitionSessionRequestCall struct {
	*gomock.Call
}

// Return rewrite *gomock.Call.Return
func (c *MockEventHandlerOnStartPartitionSessionRequestCall) Return(arg0 error) *MockEventHandlerOnStartPartitionSessionRequestCall {
	c.Call = c.Call.Return(arg0)
	return c
}

// Do rewrite *gomock.Call.Do
func (c *MockEventHandlerOnStartPartitionSessionRequestCall) Do(f func(context.Context, *PublicEventStartPartitionSession) error) *MockEventHandlerOnStartPartitionSessionRequestCall {
	c.Call = c.Call.Do(f)
	return c
}

// DoAndReturn rewrite *gomock.Call.DoAndReturn
func (c *MockEventHandlerOnStartPartitionSessionRequestCall) DoAndReturn(f func(context.Context, *PublicEventStartPartitionSession) error) *MockEventHandlerOnStartPartitionSessionRequestCall {
	c.Call = c.Call.DoAndReturn(f)
	return c
}

// OnStopPartitionSessionRequest mocks base method.
func (m *MockEventHandler) OnStopPartitionSessionRequest(ctx context.Context, event *PublicEventStopPartitionSession) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "OnStopPartitionSessionRequest", ctx, event)
	ret0, _ := ret[0].(error)
	return ret0
}

// OnStopPartitionSessionRequest indicates an expected call of OnStopPartitionSessionRequest.
func (mr *MockEventHandlerMockRecorder) OnStopPartitionSessionRequest(ctx, event any) *MockEventHandlerOnStopPartitionSessionRequestCall {
	mr.mock.ctrl.T.Helper()
	call := mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "OnStopPartitionSessionRequest", reflect.TypeOf((*MockEventHandler)(nil).OnStopPartitionSessionRequest), ctx, event)
	return &MockEventHandlerOnStopPartitionSessionRequestCall{Call: call}
}

// MockEventHandlerOnStopPartitionSessionRequestCall wrap *gomock.Call
type MockEventHandlerOnStopPartitionSessionRequestCall struct {
	*gomock.Call
}

// Return rewrite *gomock.Call.Return
func (c *MockEventHandlerOnStopPartitionSessionRequestCall) Return(arg0 error) *MockEventHandlerOnStopPartitionSessionRequestCall {
	c.Call = c.Call.Return(arg0)
	return c
}

// Do rewrite *gomock.Call.Do
func (c *MockEventHandlerOnStopPartitionSessionRequestCall) Do(f func(context.Context, *PublicEventStopPartitionSession) error) *MockEventHandlerOnStopPartitionSessionRequestCall {
	c.Call = c.Call.Do(f)
	return c
}

// DoAndReturn rewrite *gomock.Call.DoAndReturn
func (c *MockEventHandlerOnStopPartitionSessionRequestCall) DoAndReturn(f func(context.Context, *PublicEventStopPartitionSession) error) *MockEventHandlerOnStopPartitionSessionRequestCall {
	c.Call = c.Call.DoAndReturn(f)
	return c
}
