package trace

import (
	"context"
)

// tool gtrace used from ./internal/cmd/gtrace

//go:generate gtrace

type (
	// Topic specified trace of topic reader client activity.
	// gtrace:gen
	// Unstable: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#unstable
	Topic struct {
		// TopicReaderCustomerEvents - upper level, on bridge with customer code

		// Unstable: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#unstable
		OnReaderStart func(info TopicReaderStartInfo)

		// TopicReaderStreamLifeCycleEvents

		// Unstable: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#unstable
		OnReaderReconnect func(TopicReaderReconnectStartInfo) func(TopicReaderReconnectDoneInfo)
		// Unstable: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#unstable
		OnReaderReconnectRequest func(TopicReaderReconnectRequestInfo)

		// TopicReaderPartitionEvents

		// Unstable: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#unstable
		OnReaderPartitionReadStartResponse func(
			TopicReaderPartitionReadStartResponseStartInfo,
		) func(
			TopicReaderPartitionReadStartResponseDoneInfo,
		)
		// Unstable: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#unstable
		OnReaderPartitionReadStopResponse func(
			TopicReaderPartitionReadStopResponseStartInfo,
		) func(
			TopicReaderPartitionReadStopResponseDoneInfo,
		)

		// TopicReaderStreamEvents

		// Unstable: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#unstable
		OnReaderCommit func(TopicReaderCommitStartInfo) func(TopicReaderCommitDoneInfo)
		// Unstable: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#unstable
		OnReaderSendCommitMessage func(TopicReaderSendCommitMessageStartInfo) func(TopicReaderSendCommitMessageDoneInfo)
		// Unstable: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#unstable
		OnReaderCommittedNotify func(TopicReaderCommittedNotifyInfo)
		// Unstable: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#unstable
		OnReaderClose func(TopicReaderCloseStartInfo) func(TopicReaderCloseDoneInfo)
		// Unstable: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#unstable
		OnReaderInit func(TopicReaderInitStartInfo) func(TopicReaderInitDoneInfo)
		// Unstable: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#unstable
		OnReaderError func(TopicReaderErrorInfo)
		// Unstable: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#unstable
		OnReaderUpdateToken func(
			OnReadUpdateTokenStartInfo,
		) func(
			OnReadUpdateTokenMiddleTokenReceivedInfo,
		) func(
			OnReadStreamUpdateTokenDoneInfo,
		)

		// TopicReaderMessageEvents

		// Unstable: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#unstable
		OnReaderSentDataRequest func(TopicReaderSentDataRequestInfo)
		// Unstable: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#unstable
		OnReaderReceiveDataResponse func(TopicReaderReceiveDataResponseStartInfo) func(TopicReaderReceiveDataResponseDoneInfo)
		// Unstable: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#unstable
		OnReaderReadMessages func(TopicReaderReadMessagesStartInfo) func(TopicReaderReadMessagesDoneInfo)
		// Unstable: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#unstable
		OnReaderUnknownGrpcMessage func(OnReadUnknownGrpcMessageInfo)

		// TopicWriterStreamLifeCycleEvents

		// Unstable: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#unstable
		OnWriterReconnect func(TopicWriterReconnectStartInfo) func(TopicWriterReconnectDoneInfo)
		// Unstable: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#unstable
		OnWriterInitStream func(TopicWriterInitStreamStartInfo) func(TopicWriterInitStreamDoneInfo)
		// Unstable: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#unstable
		OnWriterClose func(TopicWriterCloseStartInfo) func(TopicWriterCloseDoneInfo)

		// TopicWriterStreamEvents

		OnWriterCompressMessages       func(TopicWriterCompressMessagesStartInfo) func(TopicWriterCompressMessagesDoneInfo)
		OnWriterSendMessages           func(TopicWriterSendMessagesStartInfo) func(TopicWriterSendMessagesDoneInfo)
		OnWriterReadUnknownGrpcMessage func(TopicOnWriterReadUnknownGrpcMessageInfo)
	}

	// Unstable: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#unstable
	TopicReaderPartitionReadStartResponseStartInfo struct {
		ReaderConnectionID string
		PartitionContext   context.Context
		Topic              string
		PartitionID        int64
		PartitionSessionID int64
	}

	// Unstable: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#unstable
	TopicReaderStartInfo struct {
		ReaderID int64
		Consumer string
	}

	// Unstable: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#unstable
	TopicReaderPartitionReadStartResponseDoneInfo struct {
		ReadOffset   *int64
		CommitOffset *int64
		Error        error
	}

	// Unstable: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#unstable
	TopicReaderPartitionReadStopResponseStartInfo struct {
		ReaderConnectionID string
		PartitionContext   context.Context
		Topic              string
		PartitionID        int64
		PartitionSessionID int64
		CommittedOffset    int64
		Graceful           bool
	}

	// Unstable: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#unstable
	TopicReaderPartitionReadStopResponseDoneInfo struct {
		Error error
	}

	// Unstable: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#unstable
	TopicReaderSendCommitMessageStartInfo struct {
		CommitsInfo TopicReaderStreamSendCommitMessageStartMessageInfo
	}

	// Unstable: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#unstable
	TopicReaderStreamCommitInfo struct {
		Topic              string
		PartitionID        int64
		PartitionSessionID int64
		StartOffset        int64
		EndOffset          int64
	}

	TopicReaderStreamSendCommitMessageStartMessageInfo interface {
		GetCommitsInfo() []TopicReaderStreamCommitInfo
	}

	// Unstable: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#unstable
	TopicReaderSendCommitMessageDoneInfo struct {
		Error error
	}

	// Unstable: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#unstable
	TopicReaderCommittedNotifyInfo struct {
		ReaderConnectionID string
		Topic              string
		PartitionID        int64
		PartitionSessionID int64
		CommittedOffset    int64
	}

	// Unstable: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#unstable
	TopicReaderErrorInfo struct {
		ReaderConnectionID string
		Error              error
	}

	// Unstable: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#unstable
	TopicReaderSentDataRequestInfo struct {
		ReaderConnectionID       string
		RequestBytes             int
		LocalBufferSizeAfterSent int
	}

	// Unstable: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#unstable
	TopicReaderReceiveDataResponseStartInfo struct {
		ReaderConnectionID          string
		LocalBufferSizeAfterReceive int
		DataResponse                TopicReaderDataResponseInfo
	}

	TopicReaderDataResponseInfo interface {
		GetBytesSize() int
		GetPartitionBatchMessagesCounts() (partitionCount, batchCount, messagesCount int)
	}

	// Unstable: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#unstable
	TopicReaderReceiveDataResponseDoneInfo struct {
		Error error
	}

	// Unstable: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#unstable
	TopicReaderReadMessagesStartInfo struct {
		RequestContext     context.Context
		MinCount           int
		MaxCount           int
		FreeBufferCapacity int
	}

	// Unstable: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#unstable
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

	// Unstable: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#unstable
	OnReadUnknownGrpcMessageInfo struct {
		ReaderConnectionID string
		Error              error
	}

	// Unstable: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#unstable
	TopicReaderReconnectStartInfo struct {
		Reason error
	}

	// Unstable: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#unstable
	TopicReaderReconnectDoneInfo struct {
		Error error
	}

	// Unstable: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#unstable
	TopicReaderReconnectRequestInfo struct {
		Reason  error
		WasSent bool
	}

	// Unstable: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#unstable
	TopicReaderCommitStartInfo struct {
		RequestContext     context.Context
		Topic              string
		PartitionID        int64
		PartitionSessionID int64
		StartOffset        int64
		EndOffset          int64
	}

	// Unstable: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#unstable
	TopicReaderCommitDoneInfo struct {
		Error error
	}

	// Unstable: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#unstable
	TopicReaderCloseStartInfo struct {
		ReaderConnectionID string
		CloseReason        error
	}

	// Unstable: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#unstable
	TopicReaderCloseDoneInfo struct {
		CloseError error
	}

	// Unstable: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#unstable
	TopicReaderInitStartInfo struct {
		PreInitReaderConnectionID string
		InitRequestInfo           TopicReadStreamInitRequestInfo
	}

	TopicReadStreamInitRequestInfo interface {
		GetConsumer() string
		GetTopics() []string
	}

	// Unstable: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#unstable
	TopicReaderInitDoneInfo struct {
		ReaderConnectionID string
		Error              error
	}

	// Unstable: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#unstable
	OnReadUpdateTokenStartInfo struct {
		ReaderConnectionID string
	}

	// Unstable: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#unstable
	OnReadUpdateTokenMiddleTokenReceivedInfo struct {
		TokenLen int
		Error    error
	}

	// Unstable: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#unstable
	OnReadStreamUpdateTokenDoneInfo struct {
		Error error
	}

	////////////
	//////////// TopicWriter
	////////////

	// Unstable: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#unstable
	TopicWriterReconnectStartInfo struct {
		WriterInstanceID string
		Topic            string
		ProducerID       string
		Attempt          int
	}

	// Unstable: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#unstable
	TopicWriterReconnectDoneInfo struct {
		Error error
	}

	// Unstable: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#unstable
	TopicWriterInitStreamStartInfo struct {
		WriterInstanceID string
		Topic            string
		ProducerID       string
	}

	// Unstable: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#unstable
	TopicWriterInitStreamDoneInfo struct {
		SessionID string
		Error     error
	}

	// Unstable: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#unstable
	TopicWriterCloseStartInfo struct {
		WriterInstanceID string
		Reason           error
	}

	// Unstable: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#unstable
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
	TopicWriterCompressMessagesReasonCompressData                = TopicWriterCompressMessagesReason("compress-on-send")           //nolint:lll
	TopicWriterCompressMessagesReasonCompressDataOnWriteReadData = TopicWriterCompressMessagesReason("compress-on-call-write")     //nolint:lll
	TopicWriterCompressMessagesReasonCodecsMeasure               = TopicWriterCompressMessagesReason("compress-on-codecs-measure") //nolint:lll
)

func (r TopicWriterCompressMessagesReason) String() string {
	return string(r)
}
