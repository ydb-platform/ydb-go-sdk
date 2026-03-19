package topicoptions

import (
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicmultiwriter"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicmultiwriter/partitionchooser"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicwriterinternal"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicpartitions"
)

// Experimental: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#experimental

type (
	KeyHasher = partitionchooser.KeyHasher

	BoundPartitionChooserOption = partitionchooser.BoundPartitionChooserOption
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

// WithPartitionChooserStrategy sets partition chooser strategy for the producer.
func WithKafkaHashPartitionChooser() WriterOption {
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

// WithBoundPartitionChooserPartitioningKeyHasher sets a custom key hasher used before partition selection by key.
func WithBoundPartitionChooserPartitioningKeyHasher(hasher KeyHasher) BoundPartitionChooserOption {
	return partitionchooser.WithKeyHasher(hasher)
}

// WithBoundPartitionChooser sets partition chooser strategy to bound-based.
func WithBoundPartitionChooser(options ...BoundPartitionChooserOption) WriterOption {
	return func(
		writerCfg *topicwriterinternal.WriterReconnectorConfig,
		multiWriterCfg *topicmultiwriter.MultiWriterConfig,
	) {
		if multiWriterCfg == nil {
			return
		}

		topicmultiwriter.WithBoundPartitionChooser(options...)(multiWriterCfg)
	}
}

// WithCustomPartitionChooser sets a custom partition chooser.
func WithCustomPartitionChooser(customPartitionChooser topicpartitions.PartitionChooser) WriterOption {
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
