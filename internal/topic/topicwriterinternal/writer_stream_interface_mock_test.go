// Code generated by MockGen. DO NOT EDIT.
// Source: writer_stream_interface.go
//
// Generated by this command:
//
//	mockgen -source writer_stream_interface.go --typed -destination writer_stream_interface_mock_test.go -package topicwriterinternal -write_package_comment=false
package topicwriterinternal

import (
	context "context"
	reflect "reflect"

	gomock "go.uber.org/mock/gomock"
)

// MockStreamWriter is a mock of StreamWriter interface.
type MockStreamWriter struct {
	ctrl     *gomock.Controller
	recorder *MockStreamWriterMockRecorder
}

// MockStreamWriterMockRecorder is the mock recorder for MockStreamWriter.
type MockStreamWriterMockRecorder struct {
	mock *MockStreamWriter
}

// NewMockStreamWriter creates a new mock instance.
func NewMockStreamWriter(ctrl *gomock.Controller) *MockStreamWriter {
	mock := &MockStreamWriter{ctrl: ctrl}
	mock.recorder = &MockStreamWriterMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockStreamWriter) EXPECT() *MockStreamWriterMockRecorder {
	return m.recorder
}

// Close mocks base method.
func (m *MockStreamWriter) Close(ctx context.Context) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Close", ctx)
	ret0, _ := ret[0].(error)
	return ret0
}

// Close indicates an expected call of Close.
func (mr *MockStreamWriterMockRecorder) Close(ctx any) *MockStreamWriterCloseCall {
	mr.mock.ctrl.T.Helper()
	call := mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Close", reflect.TypeOf((*MockStreamWriter)(nil).Close), ctx)
	return &MockStreamWriterCloseCall{Call: call}
}

// MockStreamWriterCloseCall wrap *gomock.Call
type MockStreamWriterCloseCall struct {
	*gomock.Call
}

// Return rewrite *gomock.Call.Return
func (c *MockStreamWriterCloseCall) Return(arg0 error) *MockStreamWriterCloseCall {
	c.Call = c.Call.Return(arg0)
	return c
}

// Do rewrite *gomock.Call.Do
func (c *MockStreamWriterCloseCall) Do(f func(context.Context) error) *MockStreamWriterCloseCall {
	c.Call = c.Call.Do(f)
	return c
}

// DoAndReturn rewrite *gomock.Call.DoAndReturn
func (c *MockStreamWriterCloseCall) DoAndReturn(f func(context.Context) error) *MockStreamWriterCloseCall {
	c.Call = c.Call.DoAndReturn(f)
	return c
}

// Flush mocks base method.
func (m *MockStreamWriter) Flush(ctx context.Context) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Flush", ctx)
	ret0, _ := ret[0].(error)
	return ret0
}

// Flush indicates an expected call of Flush.
func (mr *MockStreamWriterMockRecorder) Flush(ctx any) *MockStreamWriterFlushCall {
	mr.mock.ctrl.T.Helper()
	call := mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Flush", reflect.TypeOf((*MockStreamWriter)(nil).Flush), ctx)
	return &MockStreamWriterFlushCall{Call: call}
}

// MockStreamWriterFlushCall wrap *gomock.Call
type MockStreamWriterFlushCall struct {
	*gomock.Call
}

// Return rewrite *gomock.Call.Return
func (c *MockStreamWriterFlushCall) Return(arg0 error) *MockStreamWriterFlushCall {
	c.Call = c.Call.Return(arg0)
	return c
}

// Do rewrite *gomock.Call.Do
func (c *MockStreamWriterFlushCall) Do(f func(context.Context) error) *MockStreamWriterFlushCall {
	c.Call = c.Call.Do(f)
	return c
}

// DoAndReturn rewrite *gomock.Call.DoAndReturn
func (c *MockStreamWriterFlushCall) DoAndReturn(f func(context.Context) error) *MockStreamWriterFlushCall {
	c.Call = c.Call.DoAndReturn(f)
	return c
}

// WaitInit mocks base method.
func (m *MockStreamWriter) WaitInit(ctx context.Context) (InitialInfo, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "WaitInit", ctx)
	ret0, _ := ret[0].(InitialInfo)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// WaitInit indicates an expected call of WaitInit.
func (mr *MockStreamWriterMockRecorder) WaitInit(ctx any) *MockStreamWriterWaitInitCall {
	mr.mock.ctrl.T.Helper()
	call := mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "WaitInit", reflect.TypeOf((*MockStreamWriter)(nil).WaitInit), ctx)
	return &MockStreamWriterWaitInitCall{Call: call}
}

// MockStreamWriterWaitInitCall wrap *gomock.Call
type MockStreamWriterWaitInitCall struct {
	*gomock.Call
}

// Return rewrite *gomock.Call.Return
func (c *MockStreamWriterWaitInitCall) Return(info InitialInfo, err error) *MockStreamWriterWaitInitCall {
	c.Call = c.Call.Return(info, err)
	return c
}

// Do rewrite *gomock.Call.Do
func (c *MockStreamWriterWaitInitCall) Do(f func(context.Context) (InitialInfo, error)) *MockStreamWriterWaitInitCall {
	c.Call = c.Call.Do(f)
	return c
}

// DoAndReturn rewrite *gomock.Call.DoAndReturn
func (c *MockStreamWriterWaitInitCall) DoAndReturn(f func(context.Context) (InitialInfo, error)) *MockStreamWriterWaitInitCall {
	c.Call = c.Call.DoAndReturn(f)
	return c
}

// Write mocks base method.
func (m *MockStreamWriter) Write(ctx context.Context, messages []PublicMessage) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Write", ctx, messages)
	ret0, _ := ret[0].(error)
	return ret0
}

// Write indicates an expected call of Write.
func (mr *MockStreamWriterMockRecorder) Write(ctx, messages any) *MockStreamWriterWriteCall {
	mr.mock.ctrl.T.Helper()
	call := mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Write", reflect.TypeOf((*MockStreamWriter)(nil).Write), ctx, messages)
	return &MockStreamWriterWriteCall{Call: call}
}

// MockStreamWriterWriteCall wrap *gomock.Call
type MockStreamWriterWriteCall struct {
	*gomock.Call
}

// Return rewrite *gomock.Call.Return
func (c *MockStreamWriterWriteCall) Return(arg0 error) *MockStreamWriterWriteCall {
	c.Call = c.Call.Return(arg0)
	return c
}

// Do rewrite *gomock.Call.Do
func (c *MockStreamWriterWriteCall) Do(f func(context.Context, []PublicMessage) error) *MockStreamWriterWriteCall {
	c.Call = c.Call.Do(f)
	return c
}

// DoAndReturn rewrite *gomock.Call.DoAndReturn
func (c *MockStreamWriterWriteCall) DoAndReturn(f func(context.Context, []PublicMessage) error) *MockStreamWriterWriteCall {
	c.Call = c.Call.DoAndReturn(f)
	return c
}
