// Code generated by MockGen. DO NOT EDIT.
// Source: batched_stream_reader_interface.go
//
// Generated by this command:
//
//	mockgen -source batched_stream_reader_interface.go -destination batched_stream_reader_mock_test.go -package topicreaderinternal -write_package_comment=false
package topicreaderinternal

import (
	context "context"
	reflect "reflect"

	gomock "go.uber.org/mock/gomock"
)

// MockbatchedStreamReader is a mock of batchedStreamReader interface.
type MockbatchedStreamReader struct {
	ctrl     *gomock.Controller
	recorder *MockbatchedStreamReaderMockRecorder
}

// MockbatchedStreamReaderMockRecorder is the mock recorder for MockbatchedStreamReader.
type MockbatchedStreamReaderMockRecorder struct {
	mock *MockbatchedStreamReader
}

// NewMockbatchedStreamReader creates a new mock instance.
func NewMockbatchedStreamReader(ctrl *gomock.Controller) *MockbatchedStreamReader {
	mock := &MockbatchedStreamReader{ctrl: ctrl}
	mock.recorder = &MockbatchedStreamReaderMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockbatchedStreamReader) EXPECT() *MockbatchedStreamReaderMockRecorder {
	return m.recorder
}

// CloseWithError mocks base method.
func (m *MockbatchedStreamReader) CloseWithError(ctx context.Context, err error) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "CloseWithError", ctx, err)
	ret0, _ := ret[0].(error)
	return ret0
}

// CloseWithError indicates an expected call of CloseWithError.
func (mr *MockbatchedStreamReaderMockRecorder) CloseWithError(ctx, err any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "CloseWithError", reflect.TypeOf((*MockbatchedStreamReader)(nil).CloseWithError), ctx, err)
}

// Commit mocks base method.
func (m *MockbatchedStreamReader) Commit(ctx context.Context, commitRange commitRange) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Commit", ctx, commitRange)
	ret0, _ := ret[0].(error)
	return ret0
}

// Commit indicates an expected call of Commit.
func (mr *MockbatchedStreamReaderMockRecorder) Commit(ctx, commitRange any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Commit", reflect.TypeOf((*MockbatchedStreamReader)(nil).Commit), ctx, commitRange)
}

// ReadMessageBatch mocks base method.
func (m *MockbatchedStreamReader) ReadMessageBatch(ctx context.Context, opts ReadMessageBatchOptions) (*PublicBatch, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ReadMessageBatch", ctx, opts)
	ret0, _ := ret[0].(*PublicBatch)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// ReadMessageBatch indicates an expected call of ReadMessageBatch.
func (mr *MockbatchedStreamReaderMockRecorder) ReadMessageBatch(ctx, opts any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ReadMessageBatch", reflect.TypeOf((*MockbatchedStreamReader)(nil).ReadMessageBatch), ctx, opts)
}

// WaitInit mocks base method.
func (m *MockbatchedStreamReader) WaitInit(ctx context.Context) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "WaitInit", ctx)
	ret0, _ := ret[0].(error)
	return ret0
}

// WaitInit indicates an expected call of WaitInit.
func (mr *MockbatchedStreamReaderMockRecorder) WaitInit(ctx any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "WaitInit", reflect.TypeOf((*MockbatchedStreamReader)(nil).WaitInit), ctx)
}
