package gtrace

import (
	"testing"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/tracefuzz"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func TestCompose(t *testing.T) {
	tracefuzz.TestCompose(t, Compose, WithTopicPanicCallback)
}

func TestComposeReaderKeepsAbsentHooksNil(t *testing.T) {
	composed := ComposeReader(&trace.Topic{}, &trace.Topic{})
	for name, absent := range map[string]bool{
		"OnReaderCommitQueued":           composed.OnReaderCommitQueued == nil,
		"OnListenerCommitQueued":         composed.OnListenerCommitQueued == nil,
		"OnReaderCreditBalanceChanged":   composed.OnReaderCreditBalanceChanged == nil,
		"OnReaderLocalBufferChanged":     composed.OnReaderLocalBufferChanged == nil,
		"OnReaderMessagesReceived":       composed.OnReaderMessagesReceived == nil,
		"OnReaderMessagesDelivered":      composed.OnReaderMessagesDelivered == nil,
		"OnReaderSessionError":           composed.OnReaderSessionError == nil,
		"OnListenerMessagesReceived":     composed.OnListenerMessagesReceived == nil,
		"OnListenerMessagesDelivered":    composed.OnListenerMessagesDelivered == nil,
		"OnListenerCreditBalanceChanged": composed.OnListenerCreditBalanceChanged == nil,
		"OnListenerLocalBufferChanged":   composed.OnListenerLocalBufferChanged == nil,
	} {
		if !absent {
			t.Errorf("%s: composing two traces without the hook produced a non-nil hook", name)
		}
	}

	called := false
	withHook := ComposeReader(&trace.Topic{}, &trace.Topic{
		OnReaderSessionError: func(trace.TopicReaderSessionErrorInfo) { called = true },
	})
	withHook.OnReaderSessionError(trace.TopicReaderSessionErrorInfo{})
	if !called {
		t.Error("OnReaderSessionError of the right trace was not called")
	}
}

func TestHooks(t *testing.T) {
	tracefuzz.TestHooks[trace.Topic](t,
		TopicOnReaderStart,
		TopicOnReaderReconnect,
		TopicOnReaderReconnectRequest,
		TopicOnReaderPartitionReadStartResponse,
		TopicOnReaderPartitionReadStopResponse,
		TopicOnReaderEndPartitionSession,
		TopicOnReaderCommit,
		TopicOnReaderSendCommitMessage,
		TopicOnReaderCommittedNotify,
		TopicOnReaderClose,
		TopicOnReaderInit,
		TopicOnReaderError,
		TopicOnReaderUpdateToken,
		TopicOnReaderPopBatchTx,
		TopicOnReaderStreamPopBatchTx,
		TopicOnReaderUpdateOffsetsInTransaction,
		TopicOnReaderTransactionCompleted,
		TopicOnReaderTransactionRollback,
		TopicOnReaderSentGRPCMessage,
		TopicOnReaderReceiveGRPCMessage,
		TopicOnReaderSentDataRequest,
		TopicOnReaderReceiveDataResponse,
		TopicOnReaderReadMessages,
		TopicOnReaderUnknownGrpcMessage,
		TopicOnWriterReconnect,
		TopicOnWriterInitStream,
		TopicOnWriterClose,
		TopicOnWriterBeforeCommitTransaction,
		TopicOnWriterAfterFinishTransaction,
		TopicOnWriterCompressMessages,
		TopicOnWriterSendMessages,
		TopicOnWriterReceiveResult,
		TopicOnWriterSentGRPCMessage,
		TopicOnWriterReceiveGRPCMessage,
		TopicOnWriterReadUnknownGrpcMessage,
		TopicOnListenerStart,
		TopicOnListenerInit,
		TopicOnListenerReceiveMessage,
		TopicOnListenerRouteMessage,
		TopicOnListenerSplitMessage,
		TopicOnListenerError,
		TopicOnListenerClose,
		TopicOnPartitionWorkerStart,
		TopicOnPartitionWorkerProcessMessage,
		TopicOnPartitionWorkerHandlerCall,
		TopicOnPartitionWorkerStop,
		TopicOnListenerSendDataRequest,
		TopicOnListenerUnknownMessage,
	)
}
