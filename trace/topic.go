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
		OnReaderReconnect        func(startInfo TopicReaderReconnectStartInfo) func(doneInfo TopicReaderReconnectDoneInfo)
		OnReaderReconnectRequest func(info TopicReaderReconnectRequestInfo)

		OnReaderPartitionReadStartResponse func(startInfo TopicReaderPartitionReadStartResponseStartInfo) func(doneInfo TopicReaderPartitionReadStartResponseDoneInfo)
		OnReaderPartitionReadStopResponse  func(startInfo TopicReaderPartitionReadStopResponseStartInfo) func(doneInfo TopicReaderPartitionReadStopResponseDoneInfo)

		OnReaderStreamCommit            func(startInfo TopicReaderStreamCommitStartInfo) func(doneInfo TopicReaderStreamCommitDoneInfo)
		OnReaderStreamSendCommitMessage func(startInfo TopicReaderStreamSendCommitMessageStartInfo) func(doneInfo TopicReaderStreamSendCommitMessageDoneInfo)
		OnReaderStreamCommittedNotify   func(info TopicReaderStreamCommittedNotifyInfo)
		OnReaderStreamClose             func(startInfo TopicReaderStreamCloseStartInfo) func(doneInfo TopicReaderStreamCloseDoneInfo)
		OnReaderStreamInit              func(startInfo TopicReaderStreamInitStartInfo) func(doneInfo TopicReaderStreamInitDoneInfo)
		OnReaderStreamError             func(info TopicReaderStreamErrorInfo)
		OnReaderStreamUpdateToken       func(startInfo OnReadStreamUpdateTokenStartInfo) func(updateTokenInfo OnReadStreamUpdateTokenMiddleTokenReceivedInfo) func(doneInfo OnReadStreamUpdateTokenDoneInfo)

		OnReaderStreamSentDataRequest     func(startInfo TopicReaderStreamSentDataRequestInfo)
		OnReaderStreamReceiveDataResponse func(startInfo TopicReaderStreamReceiveDataResponseStartInfo) func(doneInfo TopicReaderStreamReceiveDataResponseDoneInfo)
		OnReaderStreamReadMessages        func(startInfo TopicReaderStreamReadMessagesStartInfo) func(doneInfo TopicReaderStreamReadMessagesDoneInfo)
		OnReaderStreamUnknownGrpcMessage  func(info OnReadStreamUnknownGrpcMessageInfo)
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

	// TopicReaderStreamSendCommitMessageStartInfo
	//
	// Experimental
	//
	// Notice: This API is EXPERIMENTAL and may be changed or removed in a
	// later release.
	TopicReaderStreamSendCommitMessageStartInfo struct {
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

	// TopicReaderStreamSendCommitMessageDoneInfo
	//
	// Experimental
	//
	// Notice: This API is EXPERIMENTAL and may be changed or removed in a
	// later release.
	TopicReaderStreamSendCommitMessageDoneInfo struct {
		Error error
	}

	// TopicReaderStreamCommittedNotifyInfo
	//
	// Experimental
	//
	// Notice: This API is EXPERIMENTAL and may be changed or removed in a
	// later release.
	TopicReaderStreamCommittedNotifyInfo struct {
		ReaderConnectionID string
		Topic              string
		PartitionID        int64
		PartitionSessionID int64
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
		PartitionSessionID int64
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
		ReaderConnectionID string
		Error              error
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
