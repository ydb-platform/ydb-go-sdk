package topicoptions

import (
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopiccommon"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicreaderinternal"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topictypes"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

type ReadSelector = topicreaderinternal.PublicReadSelector

type ReadSelectors []ReadSelector

func ReadTopic(path string) ReadSelectors {
	return ReadSelectors{{Path: path}}
}

type ReaderOption = topicreaderinternal.PublicReaderOption

func WithReaderOperationTimeout(timeout time.Duration) ReaderOption {
	return func(cfg *topicreaderinternal.ReaderConfig) {
		config.SetOperationTimeout(&cfg.Common, timeout)
	}
}

func WithReaderOperationCancelAfter(cancelAfter time.Duration) ReaderOption {
	return func(cfg *topicreaderinternal.ReaderConfig) {
		config.SetOperationCancelAfter(&cfg.Common, cancelAfter)
	}
}

func WithCommonConfig(common config.Common) ReaderOption {
	return func(cfg *topicreaderinternal.ReaderConfig) {
		cfg.Common = common
	}
}

// WithCommitTimeLagTrigger set time lag from first commit message before send commit to server
// for accumulate many similar-time commits to one server request
// 0 mean no additional lag and send commit soon as possible
// Default value: 1 second
func WithCommitTimeLagTrigger(lag time.Duration) ReaderOption {
	return func(cfg *topicreaderinternal.ReaderConfig) {
		cfg.CommitterBatchTimeLag = lag
	}
}

// WithCommitCountTrigger set count trigger for send batch to server
// if count > 0 and sdk count of buffered commits >= count - send commit request to server
// 0 mean no count limit and use timer lag trigger only
func WithCommitCountTrigger(count int) ReaderOption {
	return func(cfg *topicreaderinternal.ReaderConfig) {
		cfg.CommitterBatchCounterTrigger = count
	}
}

func WithBatchReadMinCount(count int) ReaderOption {
	return func(cfg *topicreaderinternal.ReaderConfig) {
		cfg.DefaultBatchConfig.MinCount = count
	}
}

func WithBatchReadMaxCount(count int) ReaderOption {
	return func(cfg *topicreaderinternal.ReaderConfig) {
		cfg.DefaultBatchConfig.MaxCount = count
	}
}

func WithMessagesBufferSize(size int) ReaderOption {
	return func(cfg *topicreaderinternal.ReaderConfig) {
		cfg.BufferSizeProtoBytes = size
	}
}

type CreateDecoderFunc = topicreaderinternal.PublicCreateDecoderFunc

func WithAddDecoder(codec topictypes.Codec, decoderCreate CreateDecoderFunc) ReaderOption {
	return func(cfg *topicreaderinternal.ReaderConfig) {
		cfg.Decoders.AddDecoder(rawtopiccommon.Codec(codec), decoderCreate)
	}
}

type CommitMode = topicreaderinternal.CommitMode

const (
	// CommitModeAsync - commit return true if commit success add to internal send buffer (but not sent to server)
	// now it is grpc buffer, in feature it may be internal sdk buffer
	CommitModeAsync = topicreaderinternal.CommitModeAsync // default

	// CommitModeNone - reader will not be commit operation
	CommitModeNone = topicreaderinternal.CommitModeNone

	// CommitModeSync - commit return true when sdk receive ack of commit from server
	// The mode needs strong ordering client code for prevent deadlock.
	// Example:
	// Good:
	// CommitOffset(1)
	// CommitOffset(2)
	//
	// Deadlock:
	// CommitOffset(2) - server will wait commit offset 1 before send ack about offset 1 and 2 committed.
	// CommitOffset(1)
	CommitModeSync = topicreaderinternal.CommitModeSync
)

func WithCommitMode(mode CommitMode) ReaderOption {
	return func(cfg *topicreaderinternal.ReaderConfig) {
		cfg.CommitMode = mode
	}
}

type GetPartitionStartOffsetFunc = topicreaderinternal.PublicGetPartitionStartOffsetFunc
type GetPartitionStartOffsetRequest = topicreaderinternal.PublicGetPartitionStartOffsetRequest
type GetPartitionStartOffsetResponse = topicreaderinternal.PublicGetPartitionStartOffsetResponse

func WithGetPartitionStartOffset(f GetPartitionStartOffsetFunc) ReaderOption {
	return func(cfg *topicreaderinternal.ReaderConfig) {
		cfg.GetPartitionStartOffsetCallback = f
	}
}

func WithTracer(tracer trace.Topic) ReaderOption {
	return func(cfg *topicreaderinternal.ReaderConfig) {
		cfg.Tracer = cfg.Tracer.Compose(tracer)
	}
}
