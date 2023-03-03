package topicoptions

import (
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopiccommon"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicwriterinternal"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topictypes"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
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

// WithWriterCheckRetryErrorFunction can override default error retry policy
// use CheckErrorRetryDecisionDefault for use default behavior for the error
// callback func must be fast and deterministic: always result same result for same error - it can be called
// few times for every error
//
// # Experimental
//
// Notice: This API is EXPERIMENTAL and may be changed or removed in a later release.
func WithWriterCheckRetryErrorFunction(callback CheckErrorRetryFunction) WriterOption {
	return func(cfg *topicwriterinternal.WriterReconnectorConfig) {
		cfg.RetrySettings.CheckError = callback
	}
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

// WithWriterMaxQueueLen set max len of queue for wait ack
//
// # Experimental
//
// Notice: This API is EXPERIMENTAL and may be changed or removed in a later release.
func WithWriterMaxQueueLen(num int) WriterOption {
	return topicwriterinternal.WithMaxQueueLen(num)
}

// WithWriterMessageMaxBytesSize set max body size of one message in bytes
//
// # Experimental
//
// Notice: This API is EXPERIMENTAL and may be changed or removed in a later release.
func WithWriterMessageMaxBytesSize(size int) WriterOption {
	return func(cfg *topicwriterinternal.WriterReconnectorConfig) {
		cfg.MaxMessageSize = size
	}
}

// WithWriteSessionMeta set session metadata
//
// # Experimental
//
// Notice: This API is EXPERIMENTAL and may be changed or removed in a later release.
func WithWriteSessionMeta(meta map[string]string) WriterOption {
	return topicwriterinternal.WithSessionMeta(meta)
}

// WithProducerID set producer for write session
//
// # Experimental
//
// Notice: This API is EXPERIMENTAL and may be changed or removed in a later release.
func WithProducerID(producerID string) WriterOption {
	return topicwriterinternal.WithProducerID(producerID)
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

// WithWriterStartTimeout mean timeout for connect to writer stream and work some time without errors
//
// # Experimental
//
// Notice: This API is EXPERIMENTAL and may be changed or removed in a later release.
func WithWriterStartTimeout(timeout time.Duration) WriterOption {
	return topicwriterinternal.WithStartTimeout(timeout)
}

// WithWriterTrace
//
// # Experimental
//
// Notice: This API is EXPERIMENTAL and may be changed or removed in a later release.
func WithWriterTrace(tracer trace.Topic) WriterOption {
	return topicwriterinternal.WithTrace(tracer)
}

// WithWriterUpdateTokenInterval
//
// # Experimental
//
// Notice: This API is EXPERIMENTAL and may be changed or removed in a later release.
func WithWriterUpdateTokenInterval(interval time.Duration) WriterOption {
	return topicwriterinternal.WithTokenUpdateInterval(interval)
}
