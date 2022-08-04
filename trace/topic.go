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
		OnReaderConnect          func(OnReaderConnectStartInfo) func(OnReaderConnectDoneInfo)
		OnReaderReconnect        func(OnReaderReconnectStartInfo) func(OnReaderReconnectDoneInfo)
		OnReaderReconnectRequest func(OnReaderReconnectRequestInfo)
		OnReaderReadMessages     func(OnReaderReadMessagesStartInfo) func(OnReaderReadMessagesDoneInfo)
		OnReaderCommit           func(OnReaderCommitStartInfo) func(OnReaderCommitDoneInfo)

		OnReaderStreamSentCommitMessage   func(OnReaderStreamSentCommitMessageStartInfo) func(OnReaderStreamSentCommitMessageDoneInfo)
		OnReaderStreamCommittedNotify     func(OnReaderStreamCommittedInfo)
		OnReaderStreamPartitionReadStart  func(OnReaderStreamPartitionReadStartInfo)
		OnReaderStreamPartitionReadStop   func(OnReaderStreamPartitionReadStopInfo)
		OnReaderStreamClose               func(OnReaderStreamCloseStartInfo) func(OnReaderStreamCloseDoneInfo)
		OnReaderStreamInit                func(OnReaderStreamInitStartInfo) func(OnReaderStreamInitDoneInfo)
		OnReaderStreamError               func(OnReaderStreamErrorInfo)
		OnReaderStreamSentDataRequest     func(OnReaderStreamSentDataRequestInfo)
		OnReaderStreamReceiveDataResponse func(OnReaderStreamReceiveDataResponseStartInfo) func(OnReaderStreamReceiveDataResponseDoneInfo)
		OnReaderStreamReadMessages        func(OnReaderStreamReadMessagesStartInfo) func(OnReaderStreamReadMessagesDoneInfo)
		OnReaderStreamUnknownGrpcMessage  func(OnReadStreamUnknownGrpcMessageInfo)
		OnReaderStreamUpdateToken         func(OnReadStreamUpdateTokenStartInfo) func(OnReadStreamUpdateTokenMiddleTokenReceivedInfo) func(OnReadStreamUpdateTokenDoneInfo)
	}

	// OnReaderStreamPartitionReadStartInfo
	// Experimental
	//
	// Notice: This API is EXPERIMENTAL and may be changed or removed in a
	// later release.
	OnReaderStreamPartitionReadStartInfo struct {
		ReaderConnectionID string
		PartitionContext   context.Context
		Topic              string
		PartitionID        int64
		ReadOffset         *int64
		CommitOffset       *int64
	}

	// OnReaderStreamPartitionReadStopInfo
	//
	// Experimental
	//
	// Notice: This API is EXPERIMENTAL and may be changed or removed in a
	// later release.
	OnReaderStreamPartitionReadStopInfo struct {
		ReaderConnectionID string
		PartitionContext   context.Context
		Topic              string
		PartitionID        int64
		PartitionSessionID int64
		CommittedOffset    int64
		Graceful           bool
	}

	// OnReaderStreamSentCommitMessageStartInfo
	//
	// Experimental
	//
	// Notice: This API is EXPERIMENTAL and may be changed or removed in a
	// later release.
	OnReaderStreamSentCommitMessageStartInfo struct{}

	// OnReaderStreamSentCommitMessageDoneInfo
	//
	// Experimental
	//
	// Notice: This API is EXPERIMENTAL and may be changed or removed in a
	// later release.
	OnReaderStreamSentCommitMessageDoneInfo struct {
		Error error
	}

	// OnReaderStreamCommittedInfo
	//
	// Experimental
	//
	// Notice: This API is EXPERIMENTAL and may be changed or removed in a
	// later release.
	OnReaderStreamCommittedInfo struct {
		ReaderConnectionID string
		Topic              string
		PartitionID        int64
		CommittedOffset    int64
	}

	// OnReaderStreamErrorInfo
	//
	// Experimental
	//
	// Notice: This API is EXPERIMENTAL and may be changed or removed in a
	// later release.
	OnReaderStreamErrorInfo struct {
		ReaderConnectionID string
		Error              error
	}

	// OnReaderStreamSentDataRequestInfo
	//
	// Experimental
	//
	// Notice: This API is EXPERIMENTAL and may be changed or removed in a
	// later release.
	OnReaderStreamSentDataRequestInfo struct {
		ReaderConnectionID       string
		RequestBytes             int
		LocalBufferSizeAfterSent int
	}

	// OnReaderStreamReceiveDataResponseStartInfo
	//
	// Experimental
	//
	// Notice: This API is EXPERIMENTAL and may be changed or removed in a
	// later release.
	OnReaderStreamReceiveDataResponseStartInfo struct {
		ReaderConnectionID          string
		LocalBufferSizeAfterReceive int
		DataResponse                TopicReaderDataResponseInfo
	}

	TopicReaderDataResponseInfo interface {
		GetBytesSize() int
		GetPartitionBatchMessagesCounts() (partitionCount, batchCount, messagesCount int)
	}

	// OnReaderStreamReceiveDataResponseDoneInfo
	//
	// Experimental
	//
	// Notice: This API is EXPERIMENTAL and may be changed or removed in a
	// later release.
	OnReaderStreamReceiveDataResponseDoneInfo struct {
		Error error
	}

	// OnReaderStreamReadMessagesStartInfo
	//
	// Experimental
	//
	// Notice: This API is EXPERIMENTAL and may be changed or removed in a
	// later release.
	OnReaderStreamReadMessagesStartInfo struct {
		RequestContext     context.Context
		MinCount           int
		MaxCount           int
		FreeBufferCapacity int
	}

	// OnReaderStreamReadMessagesDoneInfo
	//
	// Experimental
	//
	// Notice: This API is EXPERIMENTAL and may be changed or removed in a
	// later release.
	OnReaderStreamReadMessagesDoneInfo struct {
		MessagesCount      int
		Topic              string
		PartitionID        int64
		OffsetStart        int64
		OffsetEnd          int64
		FreeBufferCapacity int
		Error              error
	}

	// OnReadStreamUnknownGrpcMessageInfo
	//
	// Experimental
	//
	// Notice: This API is EXPERIMENTAL and may be changed or removed in a
	// later release.
	OnReadStreamUnknownGrpcMessageInfo struct {
		ReaderConnectionID string
		Error              error
	}

	// OnReaderConnectStartInfo
	//
	// Experimental
	//
	// Notice: This API is EXPERIMENTAL and may be changed or removed in a
	// later release.
	OnReaderConnectStartInfo struct{}

	// OnReaderConnectDoneInfo
	//
	// Experimental
	//
	// Notice: This API is EXPERIMENTAL and may be changed or removed in a
	// later release.
	OnReaderConnectDoneInfo struct {
		Error error
	}

	// OnReaderReconnectStartInfo
	//
	// Experimental
	//
	// Notice: This API is EXPERIMENTAL and may be changed or removed in a
	// later release.
	OnReaderReconnectStartInfo struct{}

	// OnReaderReconnectDoneInfo
	//
	// Experimental
	//
	// Notice: This API is EXPERIMENTAL and may be changed or removed in a
	// later release.
	OnReaderReconnectDoneInfo struct {
		Error error
	}

	// OnReaderReconnectRequestInfo
	//
	// Experimental
	//
	// Notice: This API is EXPERIMENTAL and may be changed or removed in a
	// later release.
	OnReaderReconnectRequestInfo struct {
		Reason  error
		WasSent bool
	}

	// OnReaderReadMessagesStartInfo
	//
	// Experimental
	//
	// Notice: This API is EXPERIMENTAL and may be changed or removed in a
	// later release.
	OnReaderReadMessagesStartInfo struct {
		RequestContext context.Context
		MinCount       int
		MaxCount       int
	}

	// OnReaderCommitStartInfo
	//
	// Experimental
	//
	// Notice: This API is EXPERIMENTAL and may be changed or removed in a
	// later release.
	OnReaderCommitStartInfo struct {
		RequestContext     context.Context
		Topic              string
		PartitionID        int64
		PartitionSessionID int64
		StartOffset        int64
		EndOffset          int64
	}

	// OnReaderCommitDoneInfo
	//
	// Experimental
	//
	// Notice: This API is EXPERIMENTAL and may be changed or removed in a
	// later release.
	OnReaderCommitDoneInfo struct {
		Error error
	}

	// OnReaderReadMessagesDoneInfo
	//
	// Experimental
	//
	// Notice: This API is EXPERIMENTAL and may be changed or removed in a
	// later release.
	OnReaderReadMessagesDoneInfo struct {
		MessagesCount int
		Topic         string
		PartitionID   int64
		OffsetStart   int64
		OffsetEnd     int64
		Error         error
	}

	// OnReaderStreamCloseStartInfo
	//
	// Experimental
	//
	// Notice: This API is EXPERIMENTAL and may be changed or removed in a
	// later release.
	OnReaderStreamCloseStartInfo struct {
		ReaderConnectionID string
		CloseReason        error
	}

	// OnReaderStreamCloseDoneInfo
	//
	// Experimental
	//
	// Notice: This API is EXPERIMENTAL and may be changed or removed in a
	// later release.
	OnReaderStreamCloseDoneInfo struct {
		CloseError error
	}

	// OnReaderStreamInitStartInfo
	//
	// Experimental
	//
	// Notice: This API is EXPERIMENTAL and may be changed or removed in a
	// later release.
	OnReaderStreamInitStartInfo struct {
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

	// OnReaderStreamInitDoneInfo
	//
	// Experimental
	//
	// Notice: This API is EXPERIMENTAL and may be changed or removed in a
	// later release.
	OnReaderStreamInitDoneInfo struct {
		NewReaderConnectionID string
		Error                 error
	}

	// OnReadStreamUpdateTokenStartInfo
	//
	// Experimental
	//
	// Notice: This API is EXPERIMENTAL and may be changed or removed in a
	// later release.
	OnReadStreamUpdateTokenStartInfo struct {
		ReaderConnectionID string
	}

	// OnReadStreamUpdateTokenMiddleTokenReceivedInfo
	//
	// Experimental
	//
	// Notice: This API is EXPERIMENTAL and may be changed or removed in a
	// later release.
	OnReadStreamUpdateTokenMiddleTokenReceivedInfo struct {
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
)
