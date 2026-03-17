package topicoptions

import (
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicmultiwriter"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicwriterinternal"
)

// MultiWriterOption configures a topic multiwriter.
// THIS IS EXPERIMENTAL SDK PART
//
// It is a thin alias for internal multiwriter options.
type (
	KeyHasher        = topicmultiwriter.KeyHasher
	PartitionChooser = topicmultiwriter.PartitionChooser
)

// WithProducerIDPrefix sets a prefix for producer IDs used by the internal producer.
func WithProducerIDPrefix(prefix string) WriterOption {
	return func(
		writerCfg *topicwriterinternal.WriterReconnectorConfig,
		multiWriterCfg *topicmultiwriter.MultiWriterConfig,
	) {
		if multiWriterCfg == nil {
			return
		}

		topicmultiwriter.WithProducerIDPrefix(prefix)(multiWriterCfg)
	}
}

// WithPartitioningKeyHasher sets a custom key hasher used before partition selection.
func WithPartitioningKeyHasher(hasher topicmultiwriter.KeyHasher) WriterOption {
	return func(
		writerCfg *topicwriterinternal.WriterReconnectorConfig,
		multiWriterCfg *topicmultiwriter.MultiWriterConfig,
	) {
		if multiWriterCfg == nil {
			return
		}

		topicmultiwriter.WithPartitioningKeyHasher(hasher)(multiWriterCfg)
	}
}

// WithPartitionChooserStrategy sets partition chooser strategy for the producer.
func WithHashPartitionChooser() WriterOption {
	return func(
		writerCfg *topicwriterinternal.WriterReconnectorConfig,
		multiWriterCfg *topicmultiwriter.MultiWriterConfig,
	) {
		if multiWriterCfg == nil {
			return
		}

		topicmultiwriter.WithHashPartitionChooser()(multiWriterCfg)
	}
}

// WithBoundPartitionChooser sets partition chooser strategy to bound-based.
func WithBoundPartitionChooser() WriterOption {
	return func(
		writerCfg *topicwriterinternal.WriterReconnectorConfig,
		multiWriterCfg *topicmultiwriter.MultiWriterConfig,
	) {
		if multiWriterCfg == nil {
			return
		}

		topicmultiwriter.WithBoundPartitionChooser()(multiWriterCfg)
	}
}

// WithCustomPartitionChooser sets a custom partition chooser.
func WithCustomPartitionChooser(customPartitionChooser topicmultiwriter.PartitionChooser) WriterOption {
	return func(
		writerCfg *topicwriterinternal.WriterReconnectorConfig,
		multiWriterCfg *topicmultiwriter.MultiWriterConfig,
	) {
		if multiWriterCfg == nil {
			return
		}

		topicmultiwriter.WithCustomPartitionChooser(customPartitionChooser)(multiWriterCfg)
	}
}

// WithWriterIdleTimeout sets timeout after which idle writers are closed.
func WithWriterIdleTimeout(timeout time.Duration) WriterOption {
	return func(
		writerCfg *topicwriterinternal.WriterReconnectorConfig,
		multiWriterCfg *topicmultiwriter.MultiWriterConfig,
	) {
		if multiWriterCfg == nil {
			return
		}

		topicmultiwriter.WithWriterIdleTimeout(timeout)(multiWriterCfg)
	}
}

// WithWriterPartitionByKey sets partition chooser strategy to key-based.
func WithWriterPartitionByKey() WriterOption {
	return func(
		writerCfg *topicwriterinternal.WriterReconnectorConfig,
		multiWriterCfg *topicmultiwriter.MultiWriterConfig,
	) {
		if multiWriterCfg == nil {
			return
		}

		topicmultiwriter.WithWriterPartitionByKey()(multiWriterCfg)
	}
}

// WithWriterPartitionByPartitionID sets partition chooser strategy to partition ID-based.
func WithWriterPartitionByPartitionID() WriterOption {
	return func(
		writerCfg *topicwriterinternal.WriterReconnectorConfig,
		multiWriterCfg *topicmultiwriter.MultiWriterConfig,
	) {
		if multiWriterCfg == nil {
			return
		}

		topicmultiwriter.WithWriterPartitionByPartitionID()(multiWriterCfg)
	}
}
