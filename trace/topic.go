//nolint:lll
package trace

import (
	"context"
)

// tool gtrace used from ./internal/cmd/gtrace

//go:generate gtrace

type (
	// Topic specified trace of topic reader client activity.
	// gtrace:gen
	// Experimental
	//
	// Notice: This API is EXPERIMENTAL and may be changed or removed in a
	// later release.
	Topic struct {
		// TopicReaderStreamLifeCycleEvents
		OnReaderReconnect        func(startInfo TopicReaderReconnectStartInfo) func(doneInfo TopicReaderReconnectDoneInfo)
		OnReaderReconnectRequest func(info TopicReaderReconnectRequestInfo)

		// TopicReaderPartitionEvents
		OnReaderPartitionReadStartResponse func(startInfo TopicReaderPartitionReadStartResponseStartInfo) func(doneInfo TopicReaderPartitionReadStartResponseDoneInfo)
		OnReaderPartitionReadStopResponse  func(startInfo TopicReaderPartitionReadStopResponseStartInfo) func(doneInfo TopicReaderPartitionReadStopResponseDoneInfo)

		// TopicReaderStreamEvents
		OnReaderCommit            func(startInfo TopicReaderCommitStartInfo) func(doneInfo TopicReaderCommitDoneInfo)
		OnReaderSendCommitMessage func(startInfo TopicReaderSendCommitMessageStartInfo) func(doneInfo TopicReaderSendCommitMessageDoneInfo)
		OnReaderCommittedNotify   func(info TopicReaderCommittedNotifyInfo)
		OnReaderClose             func(startInfo TopicReaderCloseStartInfo) func(doneInfo TopicReaderCloseDoneInfo)
		OnReaderInit              func(startInfo TopicReaderInitStartInfo) func(doneInfo TopicReaderInitDoneInfo)
		OnReaderError             func(info TopicReaderErrorInfo)
		OnReaderUpdateToken       func(startInfo OnReadUpdateTokenStartInfo) func(updateTokenInfo OnReadUpdateTokenMiddleTokenReceivedInfo) func(doneInfo OnReadStreamUpdateTokenDoneInfo)

		// TopicReaderMessageEvents
		OnReaderSentDataRequest     func(startInfo TopicReaderSentDataRequestInfo)
		OnReaderReceiveDataResponse func(startInfo TopicReaderReceiveDataResponseStartInfo) func(doneInfo TopicReaderReceiveDataResponseDoneInfo)
		OnReaderReadMessages        func(startInfo TopicReaderReadMessagesStartInfo) func(doneInfo TopicReaderReadMessagesDoneInfo)
		OnReaderUnknownGrpcMessage  func(info OnReadUnknownGrpcMessageInfo)

		// TopicWriterStreamLifeCycleEvents
		OnWriterReconnect  func(startInfo TopicWriterReconnectStartInfo) func(doneInfo TopicWriterReconnectDoneInfo)
		OnWriterInitStream func(startInfo TopicWriterInitStreamStartInfo) func(doneInfo TopicWriterInitStreamDoneInfo)
		OnWriterClose      func(startInfo TopicWriterCloseStartInfo) func(doneInfo TopicWriterCloseDoneInfo)

		// TopicWriterStreamEvents
		OnWriterCompressMessages       func(startInfo TopicWriterCompressMessagesStartInfo) func(doneInfo TopicWriterCompressMessagesDoneInfo)
		OnWriterSendMessages           func(startInfo TopicWriterSendMessagesStartInfo) func(doneInfo TopicWriterSendMessagesDoneInfo)
		OnWriterReadUnknownGrpcMessage func(info TopicOnWriterReadUnknownGrpcMessageInfo)
	}

	// TopicReaderPartitionReadStartResponseStartInfo
	// Experimental
	//
	// Notice: This API is EXPERIMENTAL and may be changed or removed in a
	// later release.
	TopicReaderPartitionReadStartResponseStartInfo struct {
		ReaderConnectionID string
		PartitionContext   context.Context
		Topic              string
		PartitionID        int64
		PartitionSessionID int64
	}

	// TopicReaderPartitionReadStartResponseDoneInfo
	// Experimental
	//
	// Notice: This API is EXPERIMENTAL and may be changed or removed in a
	// later release.
	TopicReaderPartitionReadStartResponseDoneInfo struct {
		ReadOffset   *int64
		CommitOffset *int64
		Error        error
	}

	// TopicReaderPartitionReadStopResponseStartInfo
	//
	// Experimental
	//
	// Notice: This API is EXPERIMENTAL and may be changed or removed in a
	// later release.
	TopicReaderPartitionReadStopResponseStartInfo struct {
		ReaderConnectionID string
		PartitionContext   context.Context
		Topic              string
		PartitionID        int64
		PartitionSessionID int64
		CommittedOffset    int64
		Graceful           bool
	}

	// TopicReaderPartitionReadStopResponseDoneInfo
	//
	// Experimental
	//
	// Notice: This API is EXPERIMENTAL and may be changed or removed in a
	// later release.
	TopicReaderPartitionReadStopResponseDoneInfo struct {
		Error error
	}

	// TopicReaderSendCommitMessageStartInfo
	//
	// Experimental
	//
	// Notice: This API is EXPERIMENTAL and may be changed or removed in a
	// later release.
	TopicReaderSendCommitMessageStartInfo struct {
		// ReaderConnectionID string unimplemented yet - need some internal changes
		CommitsInfo TopicReaderStreamSendCommitMessageStartMessageInfo
	}

	// TopicReaderStreamSendCommitMessageStartMessageInfo
	//
	// Experimental
	//
	// Notice: This API is EXPERIMENTAL and may be changed or removed in a
	// later release.
	TopicReaderStreamSendCommitMessageStartMessageInfo interface {
		PartitionIDs() []int64
		PartitionSessionIDs() []int64
	}

	// TopicReaderSendCommitMessageDoneInfo
	//
	// Experimental
	//
	// Notice: This API is EXPERIMENTAL and may be changed or removed in a
	// later release.
	TopicReaderSendCommitMessageDoneInfo struct {
		Error error
	}

	// TopicReaderCommittedNotifyInfo
	//
	// Experimental
	//
	// Notice: This API is EXPERIMENTAL and may be changed or removed in a
	// later release.
	TopicReaderCommittedNotifyInfo struct {
		ReaderConnectionID string
		Topic              string
		PartitionID        int64
		PartitionSessionID int64
		CommittedOffset    int64
	}

	// TopicReaderErrorInfo
	//
	// Experimental
	//
	// Notice: This API is EXPERIMENTAL and may be changed or removed in a
	// later release.
	TopicReaderErrorInfo struct {
		ReaderConnectionID string
		Error              error
	}

	// TopicReaderSentDataRequestInfo
	//
	// Experimental
	//
	// Notice: This API is EXPERIMENTAL and may be changed or removed in a
	// later release.
	TopicReaderSentDataRequestInfo struct {
		ReaderConnectionID       string
		RequestBytes             int
		LocalBufferSizeAfterSent int
	}

	// TopicReaderReceiveDataResponseStartInfo
	//
	// Experimental
	//
	// Notice: This API is EXPERIMENTAL and may be changed or removed in a
	// later release.
	TopicReaderReceiveDataResponseStartInfo struct {
		ReaderConnectionID          string
		LocalBufferSizeAfterReceive int
		DataResponse                TopicReaderDataResponseInfo
	}

	TopicReaderDataResponseInfo interface {
		GetBytesSize() int
		GetPartitionBatchMessagesCounts() (partitionCount, batchCount, messagesCount int)
	}

	// TopicReaderReceiveDataResponseDoneInfo
	//
	// Experimental
	//
	// Notice: This API is EXPERIMENTAL and may be changed or removed in a
	// later release.
	TopicReaderReceiveDataResponseDoneInfo struct {
		Error error
	}

	// TopicReaderReadMessagesStartInfo
	//
	// Experimental
	//
	// Notice: This API is EXPERIMENTAL and may be changed or removed in a
	// later release.
	TopicReaderReadMessagesStartInfo struct {
		RequestContext     context.Context
		MinCount           int
		MaxCount           int
		FreeBufferCapacity int
	}

	// TopicReaderReadMessagesDoneInfo
	//
	// Experimental
	//
	// Notice: This API is EXPERIMENTAL and may be changed or removed in a
	// later release.
	TopicReaderReadMessagesDoneInfo struct {
		MessagesCount      int
		Topic              string
		PartitionID        int64
		PartitionSessionID int64
		OffsetStart        int64
		OffsetEnd          int64
		FreeBufferCapacity int
		Error              error
	}

	// OnReadUnknownGrpcMessageInfo
	//
	// Experimental
	//
	// Notice: This API is EXPERIMENTAL and may be changed or removed in a
	// later release.
	OnReadUnknownGrpcMessageInfo struct {
		ReaderConnectionID string
		Error              error
	}

	// TopicReaderReconnectStartInfo
	//
	// Experimental
	//
	// Notice: This API is EXPERIMENTAL and may be changed or removed in a
	// later release.
	TopicReaderReconnectStartInfo struct{}

	// TopicReaderReconnectDoneInfo
	//
	// Experimental
	//
	// Notice: This API is EXPERIMENTAL and may be changed or removed in a
	// later release.
	TopicReaderReconnectDoneInfo struct {
		Error error
	}

	// TopicReaderReconnectRequestInfo
	//
	// Experimental
	//
	// Notice: This API is EXPERIMENTAL and may be changed or removed in a
	// later release.
	TopicReaderReconnectRequestInfo struct {
		Reason  error
		WasSent bool
	}

	// TopicReaderCommitStartInfo
	//
	// Experimental
	//
	// Notice: This API is EXPERIMENTAL and may be changed or removed in a
	// later release.
	TopicReaderCommitStartInfo struct {
		RequestContext     context.Context
		Topic              string
		PartitionID        int64
		PartitionSessionID int64
		StartOffset        int64
		EndOffset          int64
	}

	// TopicReaderCommitDoneInfo
	//
	// Experimental
	//
	// Notice: This API is EXPERIMENTAL and may be changed or removed in a
	// later release.
	TopicReaderCommitDoneInfo struct {
		Error error
	}

	// TopicReaderCloseStartInfo
	//
	// Experimental
	//
	// Notice: This API is EXPERIMENTAL and may be changed or removed in a
	// later release.
	TopicReaderCloseStartInfo struct {
		ReaderConnectionID string
		CloseReason        error
	}

	// TopicReaderCloseDoneInfo
	//
	// Experimental
	//
	// Notice: This API is EXPERIMENTAL and may be changed or removed in a
	// later release.
	TopicReaderCloseDoneInfo struct {
		CloseError error
	}

	// TopicReaderInitStartInfo
	//
	// Experimental
	//
	// Notice: This API is EXPERIMENTAL and may be changed or removed in a
	// later release.
	TopicReaderInitStartInfo struct {
		PreInitReaderConnectionID string
		InitRequestInfo           TopicReadStreamInitRequestInfo
	}

	// TopicReadStreamInitRequestInfo
	//
	// Experimental
	//
	// Notice: This API is EXPERIMENTAL and may be changed or removed in a
	// later release.
	TopicReadStreamInitRequestInfo interface {
		GetConsumer() string
		GetTopics() []string
	}

	// TopicReaderInitDoneInfo
	//
	// Experimental
	//
	// Notice: This API is EXPERIMENTAL and may be changed or removed in a
	// later release.
	TopicReaderInitDoneInfo struct {
		ReaderConnectionID string
		Error              error
	}

	// OnReadUpdateTokenStartInfo
	//
	// Experimental
	//
	// Notice: This API is EXPERIMENTAL and may be changed or removed in a
	// later release.
	OnReadUpdateTokenStartInfo struct {
		ReaderConnectionID string
	}

	// OnReadUpdateTokenMiddleTokenReceivedInfo
	//
	// Experimental
	//
	// Notice: This API is EXPERIMENTAL and may be changed or removed in a
	// later release.
	OnReadUpdateTokenMiddleTokenReceivedInfo struct {
		TokenLen int
		Error    error
	}

	// OnReadStreamUpdateTokenDoneInfo
	//
	// Experimental
	//
	// Notice: This API is EXPERIMENTAL and may be changed or removed in a
	// later release.
	OnReadStreamUpdateTokenDoneInfo struct {
		Error error
	}

	////////////
	//////////// TopicWriter
	////////////

	TopicWriterReconnectStartInfo struct {
		WriterInstanceID string
		Topic            string
		ProducerID       string
		Attempt          int
	}

	TopicWriterReconnectDoneInfo struct {
		Error error
	}

	TopicWriterInitStreamStartInfo struct {
		WriterInstanceID string
		Topic            string
		ProducerID       string
	}

	TopicWriterInitStreamDoneInfo struct {
		SessionID string
		Error     error
	}

	TopicWriterCloseStartInfo struct {
		WriterInstanceID string
		Reason           error
	}

	TopicWriterCloseDoneInfo struct {
		Error error
	}

	TopicWriterCompressMessagesStartInfo struct {
		WriterInstanceID string
		SessionID        string
		Codec            int32
		FirstSeqNo       int64
		MessagesCount    int
		Reason           TopicWriterCompressMessagesReason
	}

	TopicWriterCompressMessagesDoneInfo struct {
		Error error
	}

	TopicWriterSendMessagesStartInfo struct {
		WriterInstanceID string
		SessionID        string
		Codec            int32
		FirstSeqNo       int64
		MessagesCount    int
	}

	TopicWriterSendMessagesDoneInfo struct {
		Error error
	}

	TopicOnWriterReadUnknownGrpcMessageInfo struct {
		WriterInstanceID string
		SessionID        string
		Error            error
	}
)

type TopicWriterCompressMessagesReason string

const (
	TopicWriterCompressMessagesReasonCompressData                = TopicWriterCompressMessagesReason("compress-on-send")
	TopicWriterCompressMessagesReasonCompressDataOnWriteReadData = TopicWriterCompressMessagesReason("compress-on-call-write")
	TopicWriterCompressMessagesReasonCodecsMeasure               = TopicWriterCompressMessagesReason("compress-on-codecs-measure")
)

func (r TopicWriterCompressMessagesReason) String() string {
	return string(r)
}
