// Code generated by MockGen. DO NOT EDIT.
// Source: writer_stream_interface.go

package topicwriterinternal

import (
	context "context"
	reflect "reflect"

	gomock "github.com/golang/mock/gomock"
	rawtopicwriter "github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopicwriter"
)

// MockstreamWriter is a mock of streamWriter interface.
type MockstreamWriter struct {
	ctrl     *gomock.Controller
	recorder *MockstreamWriterMockRecorder
}

// MockstreamWriterMockRecorder is the mock recorder for MockstreamWriter.
type MockstreamWriterMockRecorder struct {
	mock *MockstreamWriter
}

// NewMockstreamWriter creates a new mock instance.
func NewMockstreamWriter(ctrl *gomock.Controller) *MockstreamWriter {
	mock := &MockstreamWriter{ctrl: ctrl}
	mock.recorder = &MockstreamWriterMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockstreamWriter) EXPECT() *MockstreamWriterMockRecorder {
	return m.recorder
}

// Close mocks base method.
func (m *MockstreamWriter) Close(ctx context.Context) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Close", ctx)
	ret0, _ := ret[0].(error)
	return ret0
}

// Close indicates an expected call of Close.
func (mr *MockstreamWriterMockRecorder) Close(ctx interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Close", reflect.TypeOf((*MockstreamWriter)(nil).Close), ctx)
}

// Write mocks base method.
func (m *MockstreamWriter) Write(ctx context.Context, messages *messageWithDataContentSlice) (rawtopicwriter.WriteResult, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Write", ctx, messages)
	ret0, _ := ret[0].(rawtopicwriter.WriteResult)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Write indicates an expected call of Write.
func (mr *MockstreamWriterMockRecorder) Write(ctx, messages interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Write", reflect.TypeOf((*MockstreamWriter)(nil).Write), ctx, messages)
}
