package topicoptions

import (
	"context"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopiccommon"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicmultiwriter"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicwritercommon"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicwriterinternal"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topictypes"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

// WriterOption options for a topic writer
type WriterOption = topicwritercommon.PublicWriterOption

// WriteSessionMetadata set key-value metadata for write session.
// The metadata will allow for messages of the session in topic reader.
type WriteSessionMetadata map[string]string

// CreateEncoderFunc for create message decoders
type CreateEncoderFunc = topicwriterinternal.PublicCreateEncoderFunc

// ResettableWriter is able to reset a nested writer between uses.
type ResetableWriter = topicwriterinternal.PublicResetableWriter

// WithWriterAddEncoder add custom codec implementation to writer.
// It allows to set custom codecs implementations for custom and internal codecs.
//
// If CreateEncoderFunc returns a writer implementing ResetableWriter, then the compression objects
// will be reused for this codec. This will reduce the load on the GC.
func WithWriterAddEncoder(codec topictypes.Codec, f CreateEncoderFunc) WriterOption {
	return func(
		writerCfg *topicwriterinternal.WriterReconnectorConfig,
		multiWriterCfg *topicmultiwriter.MultiWriterConfig,
	) {
		if writerCfg == nil {
			return
		}

		topicwriterinternal.WithAddEncoder(rawtopiccommon.Codec(codec), f)(writerCfg)
	}
}

// WithWriterCheckRetryErrorFunction can override default error retry policy
// use CheckErrorRetryDecisionDefault for use default behavior for the error
// callback func must be fast and deterministic: always result same result for same error - it can be called
// few times for every error
func WithWriterCheckRetryErrorFunction(callback CheckErrorRetryFunction) WriterOption {
	return func(
		writerCfg *topicwriterinternal.WriterReconnectorConfig,
		multiWriterCfg *topicmultiwriter.MultiWriterConfig,
	) {
		if writerCfg == nil {
			return
		}

		writerCfg.RetrySettings.CheckError = callback
	}
}

// WithWriterCompressorCount set max count of goroutine for compress messages
// must be more zero
//
// panic if num <= 0
func WithWriterCompressorCount(num int) WriterOption {
	return func(
		writerCfg *topicwriterinternal.WriterReconnectorConfig,
		multiWriterCfg *topicmultiwriter.MultiWriterConfig,
	) {
		if writerCfg == nil {
			return
		}

		topicwriterinternal.WithCompressorCount(num)(writerCfg)
	}
}

// WithWriterMaxQueueLen set max len of queue for wait ack
//
// Experimental: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#experimental
func WithWriterMaxQueueLen(num int) WriterOption {
	return func(
		writerCfg *topicwriterinternal.WriterReconnectorConfig,
		multiWriterCfg *topicmultiwriter.MultiWriterConfig,
	) {
		if writerCfg == nil {
			return
		}

		topicwriterinternal.WithMaxQueueLen(num)(writerCfg)
	}
}

// WithWriterMessageMaxBytesSize set max body size of one message in bytes.
// Writer will return error in message will be more than the size.
func WithWriterMessageMaxBytesSize(size int) WriterOption {
	return func(
		writerCfg *topicwriterinternal.WriterReconnectorConfig,
		multiWriterCfg *topicmultiwriter.MultiWriterConfig,
	) {
		if writerCfg == nil {
			return
		}

		writerCfg.MaxMessageSize = size
	}
}

// WithWriteSessionMeta
//
// Deprecated: was experimental and not actual now.
// Use WithWriterSessionMeta instead.
// Will be removed after Oct 2024.
// Read about versioning policy: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#deprecated
func WithWriteSessionMeta(meta map[string]string) WriterOption {
	return WithWriterSessionMeta(meta)
}

// WithWriterSessionMeta set writer's session metadata
func WithWriterSessionMeta(meta map[string]string) WriterOption {
	return func(
		writerCfg *topicwriterinternal.WriterReconnectorConfig,
		multiWriterCfg *topicmultiwriter.MultiWriterConfig,
	) {
		if writerCfg == nil {
			return
		}

		topicwriterinternal.WithSessionMeta(meta)(writerCfg)
	}
}

// WithProducerID
//
// Deprecated: was experimental and not actual now.
// Use WithWriterProducerID instead.
// Will be removed after Oct 2024.
// Read about versioning policy: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#deprecated
func WithProducerID(producerID string) WriterOption {
	return WithWriterProducerID(producerID)
}

// WithWriterProducerID set producer for write session
func WithWriterProducerID(producerID string) WriterOption {
	return func(
		writerCfg *topicwriterinternal.WriterReconnectorConfig,
		multiWriterCfg *topicmultiwriter.MultiWriterConfig,
	) {
		if writerCfg == nil {
			return
		}

		topicwriterinternal.WithProducerID(producerID)(writerCfg)
	}
}

// WithPartitionID
//
// Deprecated: was experimental and not actual now.
// Use WithWriterPartitionID instead.
// Will be removed after Oct 2024.
// Read about versioning policy: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#deprecated
func WithPartitionID(partitionID int64) WriterOption {
	return WithWriterPartitionID(partitionID)
}

// WithWriterPartitionID set direct partition id on write session level
func WithWriterPartitionID(partitionID int64) WriterOption {
	return func(
		writerCfg *topicwriterinternal.WriterReconnectorConfig,
		multiWriterCfg *topicmultiwriter.MultiWriterConfig,
	) {
		if writerCfg == nil {
			return
		}

		topicwriterinternal.WithPartitioning(topicwriterinternal.NewPartitioningWithPartitionID(partitionID))(writerCfg)
	}
}

// WithSyncWrite
//
// Deprecated: was experimental and not actual now.
// Use WithWriterWaitServerAck instead.
// Will be removed after Oct 2024.
// Read about versioning policy: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#deprecated
func WithSyncWrite(sync bool) WriterOption {
	return WithWriterWaitServerAck(sync)
}

// WithWriterWaitServerAck - when enabled every Write call wait ack from server for all messages from the call
// disabled by default. Make writer much slower, use only if you really need it.
func WithWriterWaitServerAck(wait bool) WriterOption {
	return func(
		writerCfg *topicwriterinternal.WriterReconnectorConfig,
		multiWriterCfg *topicmultiwriter.MultiWriterConfig,
	) {
		if writerCfg == nil {
			return
		}

		topicwriterinternal.WithWaitAckOnWrite(wait)(writerCfg)
	}
}

type (
	// WithOnWriterConnectedInfo present information, received from server
	//
	// Deprecated: was experimental and not actual now.
	// Will be removed after Oct 2024.
	// Read about versioning policy: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#deprecated
	WithOnWriterConnectedInfo = topicwriterinternal.PublicWithOnWriterConnectedInfo

	// OnWriterInitResponseCallback
	//
	// Deprecated: was experimental and not actual now.
	// Will be removed after Oct 2024.
	// Read about versioning policy: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#deprecated
	OnWriterInitResponseCallback = topicwriterinternal.PublicOnWriterInitResponseCallback
)

// WithOnWriterFirstConnected set callback f, which will called once - after first successfully init topic writer stream
//
// Deprecated: was experimental and not actual now.
// Use Writer.WaitInit function instead.
// Will be removed after Oct 2024.
// Read about versioning policy: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#deprecated
func WithOnWriterFirstConnected(f OnWriterInitResponseCallback) WriterOption {
	return func(
		writerCfg *topicwriterinternal.WriterReconnectorConfig,
		multiWriterCfg *topicmultiwriter.MultiWriterConfig,
	) {
		if writerCfg == nil {
			return
		}

		writerCfg.OnWriterInitResponseCallback = f
	}
}

// WithCodec
//
// Deprecated: was experimental and not actual now.
// Use WithWriterCodec instead.
// Will be removed after Oct 2024.
// Read about versioning policy: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#deprecated
func WithCodec(codec topictypes.Codec) WriterOption {
	return WithWriterCodec(codec)
}

// WithWriterCodec disable codec auto select and force set codec for the write session
func WithWriterCodec(codec topictypes.Codec) WriterOption {
	return func(
		writerCfg *topicwriterinternal.WriterReconnectorConfig,
		multiWriterCfg *topicmultiwriter.MultiWriterConfig,
	) {
		if writerCfg == nil {
			return
		}

		topicwriterinternal.WithCodec(rawtopiccommon.Codec(codec))(writerCfg)
	}
}

// WithCodecAutoSelect
//
// Deprecated: was experimental and not actual now.
// Use WithWriterCodecAutoSelect instead.
// Will be removed after Oct 2024.
// Read about versioning policy: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#deprecated
func WithCodecAutoSelect() WriterOption {
	return func(
		writerCfg *topicwriterinternal.WriterReconnectorConfig,
		multiWriterCfg *topicmultiwriter.MultiWriterConfig,
	) {
		if writerCfg == nil {
			return
		}

		topicwriterinternal.WithAutoCodec()(writerCfg)
	}
}

// WithWriterCodecAutoSelect - auto select best codec for messages stream
// enabled by default
// if option enabled - send a batch of messages for every allowed codec (for prevent delayed bad codec accident)
// then from time to time measure all codecs and select codec with the smallest result messages size
func WithWriterCodecAutoSelect() WriterOption {
	return func(
		writerCfg *topicwriterinternal.WriterReconnectorConfig,
		multiWriterCfg *topicmultiwriter.MultiWriterConfig,
	) {
		if writerCfg == nil {
			return
		}

		topicwriterinternal.WithAutoCodec()(writerCfg)
	}
}

// WithWriterSetAutoSeqNo set messages SeqNo by SDK
// enabled by default
// if enabled - Message.SeqNo field must be zero
func WithWriterSetAutoSeqNo(val bool) WriterOption {
	return func(
		writerCfg *topicwriterinternal.WriterReconnectorConfig,
		multiWriterCfg *topicmultiwriter.MultiWriterConfig,
	) {
		if writerCfg == nil {
			return
		}

		topicwriterinternal.WithAutoSetSeqNo(val)(writerCfg)
	}
}

// WithWriterSetAutoCreatedAt set messages CreatedAt by SDK
// enabled by default
// if enabled - Message.CreatedAt field must be zero
func WithWriterSetAutoCreatedAt(val bool) WriterOption {
	return func(
		writerCfg *topicwriterinternal.WriterReconnectorConfig,
		multiWriterCfg *topicmultiwriter.MultiWriterConfig,
	) {
		if writerCfg == nil {
			return
		}

		topicwriterinternal.WithAutosetCreatedTime(val)(writerCfg)
	}
}

// WithWriterStartTimeout mean timeout for connect to writer stream and work some time without errors
func WithWriterStartTimeout(timeout time.Duration) WriterOption {
	return func(
		writerCfg *topicwriterinternal.WriterReconnectorConfig,
		multiWriterCfg *topicmultiwriter.MultiWriterConfig,
	) {
		if writerCfg == nil {
			return
		}

		topicwriterinternal.WithStartTimeout(timeout)(writerCfg)
	}
}

// WithWriterTrace set tracer for the writer
func WithWriterTrace(t trace.Topic) WriterOption { //nolint:gocritic
	return func(
		writerCfg *topicwriterinternal.WriterReconnectorConfig,
		multiWriterCfg *topicmultiwriter.MultiWriterConfig,
	) {
		if writerCfg == nil {
			return
		}

		topicwriterinternal.WithTrace(&t)(writerCfg)
	}
}

// WithWriterUpdateTokenInterval set time interval between send auth token to the server
func WithWriterUpdateTokenInterval(interval time.Duration) WriterOption {
	return func(
		writerCfg *topicwriterinternal.WriterReconnectorConfig,
		multiWriterCfg *topicmultiwriter.MultiWriterConfig,
	) {
		if writerCfg == nil {
			return
		}

		topicwriterinternal.WithTokenUpdateInterval(interval)(writerCfg)
	}
}

// WithWriterLogContext allows providing a context.Context instance which will be used
// in log/topic events.
func WithWriterLogContext(ctx context.Context) WriterOption {
	return func(
		writerCfg *topicwriterinternal.WriterReconnectorConfig,
		multiWriterCfg *topicmultiwriter.MultiWriterConfig,
	) {
		if writerCfg == nil {
			return
		}

		writerCfg.LogContext = ctx
	}
}
