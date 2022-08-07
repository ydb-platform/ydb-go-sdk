package rawtopicwriter

import (
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopiccommon"
)

type InitRequest struct {
	//nolint:unused
	clientMessageImpl

	Path             string
	ProducerID       string
	WriteSessionMeta map[string]string

	Partitioning Partitioning

	GetLastSeqNo bool
}

// Partitioning is struct because it included in per-message structure and
// places on hot-path for write messages
// structure will work and compile-optimization better then interface
type Partitioning struct {
	Type           PartitioningType
	MessageGroupID string
	PartitionID    int64
}

type PartitioningType int

const (
	PartitioningUndefined PartitioningType = iota
	PartitioningMessageGroupID
	PartitioningPartitionID
)

type InitResult struct {
	//nolint:unused
	serverMessageImpl
	rawtopiccommon.ServerMessageMetadata

	LastSeqNo       int64
	SessionID       string
	PartitionID     int64
	SupportedCodecs rawtopiccommon.SupportedCodecs
}

type WriteRequest struct {
	//nolint:unused
	clientMessageImpl

	Messages []MessageData
	Codec    rawtopiccommon.Codec
}

type MessageData struct {
	SeqNo            int64
	CreatedAt        time.Time
	UncompressedSize int64
	Partitioning     Partitioning
	Data             []byte
}

type WriteResult struct {
	//nolint:unused
	serverMessageImpl
	rawtopiccommon.ServerMessageMetadata

	Acks            []WriteAck
	PartitionID     int64
	WriteStatistics WriteStatistics
}

type WriteAck struct {
	SeqNo              int64
	MessageWriteStatus MessageWriteStatus
}

// MessageWriteStatus is struct because it included in per-message structure and
// places on hot-path for write messages
// structure will work and compile-optimization better then interface
type MessageWriteStatus struct {
	Type          WriteStatusType
	WrittenOffset int64
	SkippedReason WriteStatusSkipReason
}

type WriteStatusType int

const (
	WriteStatusTypeUnknown WriteStatusType = iota
	WriteStatusTypeWritten
	WriteStatusTypeSkipped
)

type WriteStatusSkipReason int

const (
	WriteStatusSkipReasonUnspecified    WriteStatusSkipReason = 0
	WriteStatusSkipReasonAlreadyWritten WriteStatusSkipReason = 1
)

type WriteStatistics struct {
	PersistingTime     time.Duration
	MinQueueWaitTime   time.Duration
	MaxQueueWaitTime   time.Duration
	TopicQuotaWaitTime time.Duration
}
