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
		OnReaderConnect          func(TopicReaderConnectStartInfo) func(TopicReaderConnectDoneInfo)
		OnReaderReconnect        func(TopicReaderReconnectStartInfo) func(TopicReaderReconnectDoneInfo)
		OnReaderReconnectRequest func(TopicReaderReconnectRequestInfo)

		OnReaderPartitionReadStartResponse func(TopicReaderPartitionReadStartResponseStartInfo) func(TopicReaderPartitionReadStartResponseDoneInfo)
		OnReaderPartitionReadStop          func(TopicReaderPartitionReadStopInfo)

		OnReaderStreamCommit              func(TopicReaderStreamCommitStartInfo) func(TopicReaderStreamCommitDoneInfo)
		OnReaderStreamSentCommitMessage   func(TopicReaderStreamSentCommitMessageStartInfo) func(TopicReaderStreamSentCommitMessageDoneInfo)
		OnReaderStreamCommittedNotify     func(TopicReaderStreamCommittedInfo)
		OnReaderStreamClose               func(TopicReaderStreamCloseStartInfo) func(TopicReaderStreamCloseDoneInfo)
		OnReaderStreamInit                func(TopicReaderStreamInitStartInfo) func(TopicReaderStreamInitDoneInfo)
		OnReaderStreamError               func(TopicReaderStreamErrorInfo)
		OnReaderStreamSentDataRequest     func(TopicReaderStreamSentDataRequestInfo)
		OnReaderStreamReceiveDataResponse func(TopicReaderStreamReceiveDataResponseStartInfo) func(TopicReaderStreamReceiveDataResponseDoneInfo)
		OnReaderStreamReadMessages        func(TopicReaderStreamReadMessagesStartInfo) func(TopicReaderStreamReadMessagesDoneInfo)
		OnReaderStreamUnknownGrpcMessage  func(OnReadStreamUnknownGrpcMessageInfo)
		OnReaderStreamUpdateToken         func(OnReadStreamUpdateTokenStartInfo) func(OnReadStreamUpdateTokenMiddleTokenReceivedInfo) func(OnReadStreamUpdateTokenDoneInfo)
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

	// TopicReaderPartitionReadStopInfo
	//
	// Experimental
	//
	// Notice: This API is EXPERIMENTAL and may be changed or removed in a
	// later release.
	TopicReaderPartitionReadStopInfo struct {
		ReaderConnectionID string
		PartitionContext   context.Context
		Topic              string
		PartitionID        int64
		PartitionSessionID int64
		CommittedOffset    int64
		Graceful           bool
	}

	// TopicReaderStreamSentCommitMessageStartInfo
	//
	// Experimental
	//
	// Notice: This API is EXPERIMENTAL and may be changed or removed in a
	// later release.
	TopicReaderStreamSentCommitMessageStartInfo struct{}

	// TopicReaderStreamSentCommitMessageDoneInfo
	//
	// Experimental
	//
	// Notice: This API is EXPERIMENTAL and may be changed or removed in a
	// later release.
	TopicReaderStreamSentCommitMessageDoneInfo struct {
		Error error
	}

	// TopicReaderStreamCommittedInfo
	//
	// Experimental
	//
	// Notice: This API is EXPERIMENTAL and may be changed or removed in a
	// later release.
	TopicReaderStreamCommittedInfo struct {
		ReaderConnectionID string
		Topic              string
		PartitionID        int64
		CommittedOffset    int64
	}

	// TopicReaderStreamErrorInfo
	//
	// Experimental
	//
	// Notice: This API is EXPERIMENTAL and may be changed or removed in a
	// later release.
	TopicReaderStreamErrorInfo struct {
		ReaderConnectionID string
		Error              error
	}

	// TopicReaderStreamSentDataRequestInfo
	//
	// Experimental
	//
	// Notice: This API is EXPERIMENTAL and may be changed or removed in a
	// later release.
	TopicReaderStreamSentDataRequestInfo struct {
		ReaderConnectionID       string
		RequestBytes             int
		LocalBufferSizeAfterSent int
	}

	// TopicReaderStreamReceiveDataResponseStartInfo
	//
	// Experimental
	//
	// Notice: This API is EXPERIMENTAL and may be changed or removed in a
	// later release.
	TopicReaderStreamReceiveDataResponseStartInfo struct {
		ReaderConnectionID          string
		LocalBufferSizeAfterReceive int
		DataResponse                TopicReaderDataResponseInfo
	}

	TopicReaderDataResponseInfo interface {
		GetBytesSize() int
		GetPartitionBatchMessagesCounts() (partitionCount, batchCount, messagesCount int)
	}

	// TopicReaderStreamReceiveDataResponseDoneInfo
	//
	// Experimental
	//
	// Notice: This API is EXPERIMENTAL and may be changed or removed in a
	// later release.
	TopicReaderStreamReceiveDataResponseDoneInfo struct {
		Error error
	}

	// TopicReaderStreamReadMessagesStartInfo
	//
	// Experimental
	//
	// Notice: This API is EXPERIMENTAL and may be changed or removed in a
	// later release.
	TopicReaderStreamReadMessagesStartInfo struct {
		RequestContext     context.Context
		MinCount           int
		MaxCount           int
		FreeBufferCapacity int
	}

	// TopicReaderStreamReadMessagesDoneInfo
	//
	// Experimental
	//
	// Notice: This API is EXPERIMENTAL and may be changed or removed in a
	// later release.
	TopicReaderStreamReadMessagesDoneInfo struct {
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

	// TopicReaderConnectStartInfo
	//
	// Experimental
	//
	// Notice: This API is EXPERIMENTAL and may be changed or removed in a
	// later release.
	TopicReaderConnectStartInfo struct{}

	// TopicReaderConnectDoneInfo
	//
	// Experimental
	//
	// Notice: This API is EXPERIMENTAL and may be changed or removed in a
	// later release.
	TopicReaderConnectDoneInfo struct {
		Error error
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

	// TopicReaderReadMessagesStartInfo
	//
	// Experimental
	//
	// Notice: This API is EXPERIMENTAL and may be changed or removed in a
	// later release.
	TopicReaderReadMessagesStartInfo struct {
		RequestContext context.Context
		MinCount       int
		MaxCount       int
	}

	// TopicReaderStreamCommitStartInfo
	//
	// Experimental
	//
	// Notice: This API is EXPERIMENTAL and may be changed or removed in a
	// later release.
	TopicReaderStreamCommitStartInfo struct {
		RequestContext     context.Context
		Topic              string
		PartitionID        int64
		PartitionSessionID int64
		StartOffset        int64
		EndOffset          int64
	}

	// TopicReaderStreamCommitDoneInfo
	//
	// Experimental
	//
	// Notice: This API is EXPERIMENTAL and may be changed or removed in a
	// later release.
	TopicReaderStreamCommitDoneInfo struct {
		Error error
	}

	// TopicReaderReadMessagesDoneInfo
	//
	// Experimental
	//
	// Notice: This API is EXPERIMENTAL and may be changed or removed in a
	// later release.
	TopicReaderReadMessagesDoneInfo struct {
		MessagesCount int
		Topic         string
		PartitionID   int64
		OffsetStart   int64
		OffsetEnd     int64
		Error         error
	}

	// TopicReaderStreamCloseStartInfo
	//
	// Experimental
	//
	// Notice: This API is EXPERIMENTAL and may be changed or removed in a
	// later release.
	TopicReaderStreamCloseStartInfo struct {
		ReaderConnectionID string
		CloseReason        error
	}

	// TopicReaderStreamCloseDoneInfo
	//
	// Experimental
	//
	// Notice: This API is EXPERIMENTAL and may be changed or removed in a
	// later release.
	TopicReaderStreamCloseDoneInfo struct {
		CloseError error
	}

	// TopicReaderStreamInitStartInfo
	//
	// Experimental
	//
	// Notice: This API is EXPERIMENTAL and may be changed or removed in a
	// later release.
	TopicReaderStreamInitStartInfo struct {
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

	// TopicReaderStreamInitDoneInfo
	//
	// Experimental
	//
	// Notice: This API is EXPERIMENTAL and may be changed or removed in a
	// later release.
	TopicReaderStreamInitDoneInfo struct {
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
