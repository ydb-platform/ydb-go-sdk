package topicoptions

import (
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopiccommon"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicwriterinternal"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topictypes"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicwriter"
)

type WriterOption = topicwriterinternal.PublicWriterOption

type WriteSessionMetadata map[string]string

type CreateEncoderFunc = topicwriterinternal.PublicCreateEncoderFunc

// WithWriterAddEncoder add custom codec implementation to writer
//
// # Experimental
//
// Notice: This API is EXPERIMENTAL and may be changed or removed in a later release.
func WithWriterAddEncoder(codec topictypes.Codec, f CreateEncoderFunc) WriterOption {
	return topicwriterinternal.WithAddEncoder(rawtopiccommon.Codec(codec), f)
}

// WithWriterCompressorCount set max count of goroutine for compress messages
// must be more zero
//
// panic if num <= 0
//
// # Experimental
//
// Notice: This API is EXPERIMENTAL and may be changed or removed in a later release.
func WithWriterCompressorCount(num int) WriterOption {
	return topicwriterinternal.WithCompressorCount(num)
}

// WithWriteSessionMeta set session metadata
//
// # Experimental
//
// Notice: This API is EXPERIMENTAL and may be changed or removed in a later release.
func WithWriteSessionMeta(meta map[string]string) WriterOption {
	return topicwriterinternal.WithSessionMeta(meta)
}

// WithMessageGroupID set message groupid on write session level
//
// # Experimental
//
// Notice: This API is EXPERIMENTAL and may be changed or removed in a later release.
func WithMessageGroupID(groupID string) WriterOption {
	return topicwriterinternal.WithPartitioning(topicwriterinternal.NewPartitioningWithMessageGroupID(groupID))
}

// WithPartitionID set direct partition id on write session level
//
// # Experimental
//
// Notice: This API is EXPERIMENTAL and may be changed or removed in a later release.
func WithPartitionID(partitionID int64) WriterOption {
	return topicwriterinternal.WithPartitioning(topicwriterinternal.NewPartitioningWithPartitionID(partitionID))
}

// WithSyncWrite - when enabled every Write call wait ack from server for all messages from the call
// disabled by default
//
// # Experimental
//
// Notice: This API is EXPERIMENTAL and may be changed or removed in a later release.
func WithSyncWrite(sync bool) WriterOption {
	return topicwriterinternal.WithWaitAckOnWrite(sync)
}

// WithWriterPartitioning explicit set partitioning for write session
//
// # Experimental
//
// Notice: This API is EXPERIMENTAL and may be changed or removed in a later release.
func WithWriterPartitioning(partitioning topicwriter.Partitioning) WriterOption {
	return topicwriterinternal.WithPartitioning(partitioning)
}

type (
	// WithOnWriterConnectedInfo present information, received from server
	//
	// # Experimental
	//
	// Notice: This API is EXPERIMENTAL and may be changed or removed in a later release.
	WithOnWriterConnectedInfo = topicwriterinternal.PublicWithOnWriterConnectedInfo

	// OnWriterInitResponseCallback
	//
	// # Experimental
	//
	// Notice: This API is EXPERIMENTAL and may be changed or removed in a later release.
	OnWriterInitResponseCallback = topicwriterinternal.PublicOnWriterInitResponseCallback
)

// WithOnWriterFirstConnected set callback f, which will called once - after first successfully init topic writer stream
//
// # Experimental
//
// Notice: This API is EXPERIMENTAL and may be changed or removed in a later release.
func WithOnWriterFirstConnected(f OnWriterInitResponseCallback) WriterOption {
	return func(cfg *topicwriterinternal.WriterReconnectorConfig) {
		cfg.OnWriterInitResponseCallback = f
	}
}

// WithCodec disable codec auto select and force set codec for the write session
//
// # Experimental
//
// Notice: This API is EXPERIMENTAL and may be changed or removed in a later release.
func WithCodec(codec topictypes.Codec) WriterOption {
	return topicwriterinternal.WithCodec(rawtopiccommon.Codec(codec))
}

// WithCodecAutoSelect - auto select best codec for messages stream
// enabled by default
// if option enabled - send a batch of messages for every allowed codec (for prevent delayed bad codec accident)
// then from time to time measure all codecs and select codec with the smallest result messages size
//
// # Experimental
//
// Notice: This API is EXPERIMENTAL and may be changed or removed in a later release.
func WithCodecAutoSelect() WriterOption {
	return topicwriterinternal.WithAutoCodec()
}

// WithWriterSetAutoSeqNo set messages SeqNo by SDK
// enabled by default
// if enabled - Message.SeqNo field must be zero
//
// # Experimental
//
// Notice: This API is EXPERIMENTAL and may be changed or removed in a later release.
func WithWriterSetAutoSeqNo(val bool) WriterOption {
	return topicwriterinternal.WithAutoSetSeqNo(val)
}

// WithWriterSetAutoCreatedAt set messages CreatedAt by SDK
// enabled by default
// if enabled - Message.CreatedAt field must be zero
//
// # Experimental
//
// Notice: This API is EXPERIMENTAL and may be changed or removed in a later release.
func WithWriterSetAutoCreatedAt(val bool) WriterOption {
	return topicwriterinternal.WithAutosetCreatedTime(val)
}
