package trace

import (
	"context"
)

// tool gtrace used from ./internal/cmd/gtrace

//go:generate gtrace

type (
	// Topic specified trace of topic reader client activity.
	// gtrace:gen
	Topic struct {
		// TopicReaderCustomerEvents - upper level, on bridge with customer code

		OnReaderStart func(info TopicReaderStartInfo)

		// TopicReaderStreamLifeCycleEvents

		OnReaderReconnect        func(TopicReaderReconnectStartInfo) func(TopicReaderReconnectDoneInfo)
		OnReaderReconnectRequest func(TopicReaderReconnectRequestInfo)

		// TopicReaderPartitionEvents

		OnReaderPartitionReadStartResponse func(
			TopicReaderPartitionReadStartResponseStartInfo,
		) func(
			TopicReaderPartitionReadStartResponseDoneInfo,
		)
		OnReaderPartitionReadStopResponse func(
			TopicReaderPartitionReadStopResponseStartInfo,
		) func(
			TopicReaderPartitionReadStopResponseDoneInfo,
		)

		// TopicReaderStreamEvents

		OnReaderCommit            func(TopicReaderCommitStartInfo) func(TopicReaderCommitDoneInfo)
		OnReaderSendCommitMessage func(TopicReaderSendCommitMessageStartInfo) func(TopicReaderSendCommitMessageDoneInfo)
		OnReaderCommittedNotify   func(TopicReaderCommittedNotifyInfo)
		OnReaderClose             func(TopicReaderCloseStartInfo) func(TopicReaderCloseDoneInfo)
		OnReaderInit              func(TopicReaderInitStartInfo) func(TopicReaderInitDoneInfo)
		OnReaderError             func(TopicReaderErrorInfo)
		OnReaderUpdateToken       func(
			OnReadUpdateTokenStartInfo,
		) func(
			OnReadUpdateTokenMiddleTokenReceivedInfo,
		) func(
			OnReadStreamUpdateTokenDoneInfo,
		)

		// TopicReaderMessageEvents

		OnReaderSentDataRequest     func(TopicReaderSentDataRequestInfo)
		OnReaderReceiveDataResponse func(TopicReaderReceiveDataResponseStartInfo) func(TopicReaderReceiveDataResponseDoneInfo)
		OnReaderReadMessages        func(TopicReaderReadMessagesStartInfo) func(TopicReaderReadMessagesDoneInfo)
		OnReaderUnknownGrpcMessage  func(OnReadUnknownGrpcMessageInfo)

		// TopicWriterStreamLifeCycleEvents

		OnWriterReconnect  func(TopicWriterReconnectStartInfo) func(TopicWriterReconnectDoneInfo)
		OnWriterInitStream func(TopicWriterInitStreamStartInfo) func(TopicWriterInitStreamDoneInfo)
		OnWriterClose      func(TopicWriterCloseStartInfo) func(TopicWriterCloseDoneInfo)

		// TopicWriterStreamEvents

		OnWriterCompressMessages       func(TopicWriterCompressMessagesStartInfo) func(TopicWriterCompressMessagesDoneInfo)
		OnWriterSendMessages           func(TopicWriterSendMessagesStartInfo) func(TopicWriterSendMessagesDoneInfo)
		OnWriterReadUnknownGrpcMessage func(TopicOnWriterReadUnknownGrpcMessageInfo)
	}

	TopicReaderPartitionReadStartResponseStartInfo struct {
		ReaderConnectionID string
		PartitionContext   context.Context
		Topic              string
		PartitionID        int64
		PartitionSessionID int64
	}

	TopicReaderStartInfo struct {
		ReaderID int64
		Consumer string
	}

	TopicReaderPartitionReadStartResponseDoneInfo struct {
		ReadOffset   *int64
		CommitOffset *int64
		Error        error
	}

	TopicReaderPartitionReadStopResponseStartInfo struct {
		ReaderConnectionID string
		PartitionContext   context.Context
		Topic              string
		PartitionID        int64
		PartitionSessionID int64
		CommittedOffset    int64
		Graceful           bool
	}

	TopicReaderPartitionReadStopResponseDoneInfo struct {
		Error error
	}

	TopicReaderSendCommitMessageStartInfo struct {
		CommitsInfo TopicReaderStreamSendCommitMessageStartMessageInfo
	}

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

	TopicReaderSendCommitMessageDoneInfo struct {
		Error error
	}

	TopicReaderCommittedNotifyInfo struct {
		ReaderConnectionID string
		Topic              string
		PartitionID        int64
		PartitionSessionID int64
		CommittedOffset    int64
	}

	TopicReaderErrorInfo struct {
		ReaderConnectionID string
		Error              error
	}

	TopicReaderSentDataRequestInfo struct {
		ReaderConnectionID       string
		RequestBytes             int
		LocalBufferSizeAfterSent int
	}

	TopicReaderReceiveDataResponseStartInfo struct {
		ReaderConnectionID          string
		LocalBufferSizeAfterReceive int
		DataResponse                TopicReaderDataResponseInfo
	}

	TopicReaderDataResponseInfo interface {
		GetBytesSize() int
		GetPartitionBatchMessagesCounts() (partitionCount, batchCount, messagesCount int)
	}

	TopicReaderReceiveDataResponseDoneInfo struct {
		Error error
	}

	TopicReaderReadMessagesStartInfo struct {
		RequestContext     context.Context
		MinCount           int
		MaxCount           int
		FreeBufferCapacity int
	}

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

	OnReadUnknownGrpcMessageInfo struct {
		ReaderConnectionID string
		Error              error
	}

	TopicReaderReconnectStartInfo struct {
		Reason error
	}

	TopicReaderReconnectDoneInfo struct {
		Error error
	}

	TopicReaderReconnectRequestInfo struct {
		Reason  error
		WasSent bool
	}

	TopicReaderCommitStartInfo struct {
		RequestContext     context.Context
		Topic              string
		PartitionID        int64
		PartitionSessionID int64
		StartOffset        int64
		EndOffset          int64
	}

	TopicReaderCommitDoneInfo struct {
		Error error
	}

	TopicReaderCloseStartInfo struct {
		ReaderConnectionID string
		CloseReason        error
	}

	TopicReaderCloseDoneInfo struct {
		CloseError error
	}

	TopicReaderInitStartInfo struct {
		PreInitReaderConnectionID string
		InitRequestInfo           TopicReadStreamInitRequestInfo
	}

	TopicReadStreamInitRequestInfo interface {
		GetConsumer() string
		GetTopics() []string
	}

	TopicReaderInitDoneInfo struct {
		ReaderConnectionID string
		Error              error
	}

	OnReadUpdateTokenStartInfo struct {
		ReaderConnectionID string
	}

	OnReadUpdateTokenMiddleTokenReceivedInfo struct {
		TokenLen int
		Error    error
	}

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
	TopicWriterCompressMessagesReasonCompressData                = TopicWriterCompressMessagesReason("compress-on-send")           //nolint:lll
	TopicWriterCompressMessagesReasonCompressDataOnWriteReadData = TopicWriterCompressMessagesReason("compress-on-call-write")     //nolint:lll
	TopicWriterCompressMessagesReasonCodecsMeasure               = TopicWriterCompressMessagesReason("compress-on-codecs-measure") //nolint:lll
)

func (r TopicWriterCompressMessagesReason) String() string {
	return string(r)
}
