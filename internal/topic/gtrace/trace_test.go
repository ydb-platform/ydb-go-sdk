package gtrace

import (
	"testing"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/tracefuzz"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func TestCompose(t *testing.T) {
	tracefuzz.TestCompose(t, Compose, WithTopicPanicCallback)
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
