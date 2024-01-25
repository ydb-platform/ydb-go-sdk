package topicoptions

import (
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopiccommon"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicwriterinternal"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topictypes"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

// WriterOption options for a topic writer
type WriterOption = topicwriterinternal.PublicWriterOption

// WriteSessionMetadata set key-value metadata for write session.
// The metadata will allow for messages of the session in topic reader.
type WriteSessionMetadata map[string]string

// CreateEncoderFunc for create message decoders
type CreateEncoderFunc = topicwriterinternal.PublicCreateEncoderFunc

// WithWriterAddEncoder add custom codec implementation to writer.
// It allows to set custom codecs implementations for custom and internal codecs.
func WithWriterAddEncoder(codec topictypes.Codec, f CreateEncoderFunc) WriterOption {
	return topicwriterinternal.WithAddEncoder(rawtopiccommon.Codec(codec), f)
}

// WithWriterCheckRetryErrorFunction can override default error retry policy
// use CheckErrorRetryDecisionDefault for use default behavior for the error
// callback func must be fast and deterministic: always result same result for same error - it can be called
// few times for every error
func WithWriterCheckRetryErrorFunction(callback CheckErrorRetryFunction) WriterOption {
	return func(cfg *topicwriterinternal.WriterReconnectorConfig) {
		cfg.RetrySettings.CheckError = callback
	}
}

// WithWriterCompressorCount set max count of goroutine for compress messages
// must be more zero
//
// panic if num <= 0
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

// WithWriterMessageMaxBytesSize set max body size of one message in bytes.
// Writer will return error in message will be more than the size.
func WithWriterMessageMaxBytesSize(size int) WriterOption {
	return func(cfg *topicwriterinternal.WriterReconnectorConfig) {
		cfg.MaxMessageSize = size
	}
}

// WithWriteSessionMeta
// Deprecated: (was experimental) will be removed soon.
// Use WithWriterSessionMeta instead
func WithWriteSessionMeta(meta map[string]string) WriterOption {
	return WithWriterSessionMeta(meta)
}

// WithWriterSessionMeta set writer's session metadata
func WithWriterSessionMeta(meta map[string]string) WriterOption {
	return topicwriterinternal.WithSessionMeta(meta)
}

// WithProducerID
// Deprecated: (was experimental) will be removed soon.
// Use WithWriterProducerID instead
func WithProducerID(producerID string) WriterOption {
	return WithWriterProducerID(producerID)
}

// WithWriterProducerID set producer for write session
func WithWriterProducerID(producerID string) WriterOption {
	return topicwriterinternal.WithProducerID(producerID)
}

// WithPartitionID
// Deprecated: (was experimental) will be removed soon
// Use WithWriterPartitionID instead
func WithPartitionID(partitionID int64) WriterOption {
	return WithWriterPartitionID(partitionID)
}

// WithWriterPartitionID set direct partition id on write session level
func WithWriterPartitionID(partitionID int64) WriterOption {
	return topicwriterinternal.WithPartitioning(topicwriterinternal.NewPartitioningWithPartitionID(partitionID))
}

// WithSyncWrite
// Deprecated: (was experimental) use WithWriterWaitServerAck instead
func WithSyncWrite(sync bool) WriterOption {
	return WithWriterWaitServerAck(sync)
}

// WithWriterWaitServerAck - when enabled every Write call wait ack from server for all messages from the call
// disabled by default. Make writer much slower, use only if you really need it.
func WithWriterWaitServerAck(wait bool) WriterOption {
	return topicwriterinternal.WithWaitAckOnWrite(wait)
}

type (
	// WithOnWriterConnectedInfo present information, received from server
	// Deprecated: (was experimental) will be removed soon
	WithOnWriterConnectedInfo = topicwriterinternal.PublicWithOnWriterConnectedInfo

	// OnWriterInitResponseCallback
	// Deprecated: (was experimental) will be removed soon.
	OnWriterInitResponseCallback = topicwriterinternal.PublicOnWriterInitResponseCallback
)

// WithOnWriterFirstConnected set callback f, which will called once - after first successfully init topic writer stream
// Deprecated: (was experimental) will be removed soon.
// Use Writer.WaitInit function instead
func WithOnWriterFirstConnected(f OnWriterInitResponseCallback) WriterOption {
	return func(cfg *topicwriterinternal.WriterReconnectorConfig) {
		cfg.OnWriterInitResponseCallback = f
	}
}

// WithCodec
// Deprecated: (was experimental) will be removed soon.
// Use WithWriterCodec instead
func WithCodec(codec topictypes.Codec) WriterOption {
	return WithWriterCodec(codec)
}

// WithWriterCodec disable codec auto select and force set codec for the write session
func WithWriterCodec(codec topictypes.Codec) WriterOption {
	return topicwriterinternal.WithCodec(rawtopiccommon.Codec(codec))
}

// WithCodecAutoSelect
// Deprecated: (was experimental) will be removed soon.
// Use WithWriterCodecAutoSelect instead.
func WithCodecAutoSelect() WriterOption {
	return topicwriterinternal.WithAutoCodec()
}

// WithWriterCodecAutoSelect - auto select best codec for messages stream
// enabled by default
// if option enabled - send a batch of messages for every allowed codec (for prevent delayed bad codec accident)
// then from time to time measure all codecs and select codec with the smallest result messages size
func WithWriterCodecAutoSelect() WriterOption {
	return topicwriterinternal.WithAutoCodec()
}

// WithWriterSetAutoSeqNo set messages SeqNo by SDK
// enabled by default
// if enabled - Message.SeqNo field must be zero
func WithWriterSetAutoSeqNo(val bool) WriterOption {
	return topicwriterinternal.WithAutoSetSeqNo(val)
}

// WithWriterSetAutoCreatedAt set messages CreatedAt by SDK
// enabled by default
// if enabled - Message.CreatedAt field must be zero
func WithWriterSetAutoCreatedAt(val bool) WriterOption {
	return topicwriterinternal.WithAutosetCreatedTime(val)
}

// WithWriterStartTimeout mean timeout for connect to writer stream and work some time without errors
func WithWriterStartTimeout(timeout time.Duration) WriterOption {
	return topicwriterinternal.WithStartTimeout(timeout)
}

// WithWriterTrace set tracer for the writer
func WithWriterTrace(t trace.Topic) WriterOption { //nolint:gocritic
	return topicwriterinternal.WithTrace(&t)
}

// WithWriterUpdateTokenInterval set time interval between send auth token to the server
func WithWriterUpdateTokenInterval(interval time.Duration) WriterOption {
	return topicwriterinternal.WithTokenUpdateInterval(interval)
}
