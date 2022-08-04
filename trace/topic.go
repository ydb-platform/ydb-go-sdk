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
		OnReaderConnect          func(TableReaderConnectStartInfo) func(TableReaderConnectDoneInfo)
		OnReaderReconnect        func(TableReaderReconnectStartInfo) func(TableReaderReconnectDoneInfo)
		OnReaderReconnectRequest func(TableReaderReconnectRequestInfo)
		OnReaderReadMessages     func(TableReaderReadMessagesStartInfo) func(TableReaderReadMessagesDoneInfo)
		OnReaderCommit           func(TableReaderCommitStartInfo) func(TableReaderCommitDoneInfo)

		OnReaderStreamSentCommitMessage   func(TableReaderStreamSentCommitMessageStartInfo) func(TableReaderStreamSentCommitMessageDoneInfo)
		OnReaderStreamCommittedNotify     func(TableReaderStreamCommittedInfo)
		OnReaderStreamPartitionReadStart  func(TableReaderStreamPartitionReadStartInfo)
		OnReaderStreamPartitionReadStop   func(TableReaderStreamPartitionReadStopInfo)
		OnReaderStreamClose               func(TableReaderStreamCloseStartInfo) func(TableReaderStreamCloseDoneInfo)
		OnReaderStreamInit                func(TableReaderStreamInitStartInfo) func(TableReaderStreamInitDoneInfo)
		OnReaderStreamError               func(TableReaderStreamErrorInfo)
		OnReaderStreamSentDataRequest     func(TableReaderStreamSentDataRequestInfo)
		OnReaderStreamReceiveDataResponse func(TableReaderStreamReceiveDataResponseStartInfo) func(TableReaderStreamReceiveDataResponseDoneInfo)
		OnReaderStreamReadMessages        func(TableReaderStreamReadMessagesStartInfo) func(TableReaderStreamReadMessagesDoneInfo)
		OnReaderStreamUnknownGrpcMessage  func(OnReadStreamUnknownGrpcMessageInfo)
		OnReaderStreamUpdateToken         func(OnReadStreamUpdateTokenStartInfo) func(OnReadStreamUpdateTokenMiddleTokenReceivedInfo) func(OnReadStreamUpdateTokenDoneInfo)
	}

	// TableReaderStreamPartitionReadStartInfo
	// Experimental
	//
	// Notice: This API is EXPERIMENTAL and may be changed or removed in a
	// later release.
	TableReaderStreamPartitionReadStartInfo struct {
		ReaderConnectionID string
		PartitionContext   context.Context
		Topic              string
		PartitionID        int64
		ReadOffset         *int64
		CommitOffset       *int64
	}

	// TableReaderStreamPartitionReadStopInfo
	//
	// Experimental
	//
	// Notice: This API is EXPERIMENTAL and may be changed or removed in a
	// later release.
	TableReaderStreamPartitionReadStopInfo struct {
		ReaderConnectionID string
		PartitionContext   context.Context
		Topic              string
		PartitionID        int64
		PartitionSessionID int64
		CommittedOffset    int64
		Graceful           bool
	}

	// TableReaderStreamSentCommitMessageStartInfo
	//
	// Experimental
	//
	// Notice: This API is EXPERIMENTAL and may be changed or removed in a
	// later release.
	TableReaderStreamSentCommitMessageStartInfo struct{}

	// TableReaderStreamSentCommitMessageDoneInfo
	//
	// Experimental
	//
	// Notice: This API is EXPERIMENTAL and may be changed or removed in a
	// later release.
	TableReaderStreamSentCommitMessageDoneInfo struct {
		Error error
	}

	// TableReaderStreamCommittedInfo
	//
	// Experimental
	//
	// Notice: This API is EXPERIMENTAL and may be changed or removed in a
	// later release.
	TableReaderStreamCommittedInfo struct {
		ReaderConnectionID string
		Topic              string
		PartitionID        int64
		CommittedOffset    int64
	}

	// TableReaderStreamErrorInfo
	//
	// Experimental
	//
	// Notice: This API is EXPERIMENTAL and may be changed or removed in a
	// later release.
	TableReaderStreamErrorInfo struct {
		ReaderConnectionID string
		Error              error
	}

	// TableReaderStreamSentDataRequestInfo
	//
	// Experimental
	//
	// Notice: This API is EXPERIMENTAL and may be changed or removed in a
	// later release.
	TableReaderStreamSentDataRequestInfo struct {
		ReaderConnectionID       string
		RequestBytes             int
		LocalBufferSizeAfterSent int
	}

	// TableReaderStreamReceiveDataResponseStartInfo
	//
	// Experimental
	//
	// Notice: This API is EXPERIMENTAL and may be changed or removed in a
	// later release.
	TableReaderStreamReceiveDataResponseStartInfo struct {
		ReaderConnectionID          string
		LocalBufferSizeAfterReceive int
		DataResponse                TopicReaderDataResponseInfo
	}

	TopicReaderDataResponseInfo interface {
		GetBytesSize() int
		GetPartitionBatchMessagesCounts() (partitionCount, batchCount, messagesCount int)
	}

	// TableReaderStreamReceiveDataResponseDoneInfo
	//
	// Experimental
	//
	// Notice: This API is EXPERIMENTAL and may be changed or removed in a
	// later release.
	TableReaderStreamReceiveDataResponseDoneInfo struct {
		Error error
	}

	// TableReaderStreamReadMessagesStartInfo
	//
	// Experimental
	//
	// Notice: This API is EXPERIMENTAL and may be changed or removed in a
	// later release.
	TableReaderStreamReadMessagesStartInfo struct {
		RequestContext     context.Context
		MinCount           int
		MaxCount           int
		FreeBufferCapacity int
	}

	// TableReaderStreamReadMessagesDoneInfo
	//
	// Experimental
	//
	// Notice: This API is EXPERIMENTAL and may be changed or removed in a
	// later release.
	TableReaderStreamReadMessagesDoneInfo struct {
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

	// TableReaderConnectStartInfo
	//
	// Experimental
	//
	// Notice: This API is EXPERIMENTAL and may be changed or removed in a
	// later release.
	TableReaderConnectStartInfo struct{}

	// TableReaderConnectDoneInfo
	//
	// Experimental
	//
	// Notice: This API is EXPERIMENTAL and may be changed or removed in a
	// later release.
	TableReaderConnectDoneInfo struct {
		Error error
	}

	// TableReaderReconnectStartInfo
	//
	// Experimental
	//
	// Notice: This API is EXPERIMENTAL and may be changed or removed in a
	// later release.
	TableReaderReconnectStartInfo struct{}

	// TableReaderReconnectDoneInfo
	//
	// Experimental
	//
	// Notice: This API is EXPERIMENTAL and may be changed or removed in a
	// later release.
	TableReaderReconnectDoneInfo struct {
		Error error
	}

	// TableReaderReconnectRequestInfo
	//
	// Experimental
	//
	// Notice: This API is EXPERIMENTAL and may be changed or removed in a
	// later release.
	TableReaderReconnectRequestInfo struct {
		Reason  error
		WasSent bool
	}

	// TableReaderReadMessagesStartInfo
	//
	// Experimental
	//
	// Notice: This API is EXPERIMENTAL and may be changed or removed in a
	// later release.
	TableReaderReadMessagesStartInfo struct {
		RequestContext context.Context
		MinCount       int
		MaxCount       int
	}

	// TableReaderCommitStartInfo
	//
	// Experimental
	//
	// Notice: This API is EXPERIMENTAL and may be changed or removed in a
	// later release.
	TableReaderCommitStartInfo struct {
		RequestContext     context.Context
		Topic              string
		PartitionID        int64
		PartitionSessionID int64
		StartOffset        int64
		EndOffset          int64
	}

	// TableReaderCommitDoneInfo
	//
	// Experimental
	//
	// Notice: This API is EXPERIMENTAL and may be changed or removed in a
	// later release.
	TableReaderCommitDoneInfo struct {
		Error error
	}

	// TableReaderReadMessagesDoneInfo
	//
	// Experimental
	//
	// Notice: This API is EXPERIMENTAL and may be changed or removed in a
	// later release.
	TableReaderReadMessagesDoneInfo struct {
		MessagesCount int
		Topic         string
		PartitionID   int64
		OffsetStart   int64
		OffsetEnd     int64
		Error         error
	}

	// TableReaderStreamCloseStartInfo
	//
	// Experimental
	//
	// Notice: This API is EXPERIMENTAL and may be changed or removed in a
	// later release.
	TableReaderStreamCloseStartInfo struct {
		ReaderConnectionID string
		CloseReason        error
	}

	// TableReaderStreamCloseDoneInfo
	//
	// Experimental
	//
	// Notice: This API is EXPERIMENTAL and may be changed or removed in a
	// later release.
	TableReaderStreamCloseDoneInfo struct {
		CloseError error
	}

	// TableReaderStreamInitStartInfo
	//
	// Experimental
	//
	// Notice: This API is EXPERIMENTAL and may be changed or removed in a
	// later release.
	TableReaderStreamInitStartInfo struct {
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

	// TableReaderStreamInitDoneInfo
	//
	// Experimental
	//
	// Notice: This API is EXPERIMENTAL and may be changed or removed in a
	// later release.
	TableReaderStreamInitDoneInfo struct {
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
