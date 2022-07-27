// Code generated by MockGen. DO NOT EDIT.
// Source: github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicreaderinternal (interfaces: RawTopicReaderStream)

package topicreaderinternal

import (
	reflect "reflect"

	gomock "github.com/golang/mock/gomock"
	rawtopicreader "github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopicreader"
)

// MockRawTopicReaderStream is a mock of RawTopicReaderStream interface.
type MockRawTopicReaderStream struct {
	ctrl     *gomock.Controller
	recorder *MockRawTopicReaderStreamMockRecorder
}

// MockRawTopicReaderStreamMockRecorder is the mock recorder for MockRawTopicReaderStream.
type MockRawTopicReaderStreamMockRecorder struct {
	mock *MockRawTopicReaderStream
}

// NewMockRawTopicReaderStream creates a new mock instance.
func NewMockRawTopicReaderStream(ctrl *gomock.Controller) *MockRawTopicReaderStream {
	mock := &MockRawTopicReaderStream{ctrl: ctrl}
	mock.recorder = &MockRawTopicReaderStreamMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockRawTopicReaderStream) EXPECT() *MockRawTopicReaderStreamMockRecorder {
	return m.recorder
}

// CloseSend mocks base method.
func (m *MockRawTopicReaderStream) CloseSend() error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "CloseSend")
	ret0, _ := ret[0].(error)
	return ret0
}

// CloseSend indicates an expected call of CloseSend.
func (mr *MockRawTopicReaderStreamMockRecorder) CloseSend() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "CloseSend", reflect.TypeOf((*MockRawTopicReaderStream)(nil).CloseSend))
}

// Recv mocks base method.
func (m *MockRawTopicReaderStream) Recv() (rawtopicreader.ServerMessage, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Recv")
	ret0, _ := ret[0].(rawtopicreader.ServerMessage)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Recv indicates an expected call of Recv.
func (mr *MockRawTopicReaderStreamMockRecorder) Recv() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Recv", reflect.TypeOf((*MockRawTopicReaderStream)(nil).Recv))
}

// Send mocks base method.
func (m *MockRawTopicReaderStream) Send(arg0 rawtopicreader.ClientMessage) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Send", arg0)
	ret0, _ := ret[0].(error)
	return ret0
}

// Send indicates an expected call of Send.
func (mr *MockRawTopicReaderStreamMockRecorder) Send(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Send", reflect.TypeOf((*MockRawTopicReaderStream)(nil).Send), arg0)
}
