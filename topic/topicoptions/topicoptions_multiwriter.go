package topicoptions

import (
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicmultiwriter"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicmultiwriter/partitionchooser"
)

// Experimental: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#experimental

type (
	KeyHasher = partitionchooser.KeyHasher

	BoundPartitionChooserOption = partitionchooser.BoundPartitionChooserOption
	MultiWriterOption           = topicmultiwriter.PublicMultiWriterOption
)

// WithProducerIDPrefix sets a prefix for producer IDs used by the internal producer.
func WithProducerIDPrefix(prefix string) MultiWriterOption {
	return func(cfg *topicmultiwriter.MultiWriterConfig) {
		topicmultiwriter.WithProducerIDPrefix(prefix)(cfg)
	}
}

// WithBoundPartitionChooserPartitioningKeyHasher sets a custom key hasher used before partition selection by key.
func WithBoundPartitionChooserPartitioningKeyHasher(hasher KeyHasher) BoundPartitionChooserOption {
	return partitionchooser.WithKeyHasher(hasher)
}

// WithWriterIdleTimeout sets timeout after which idle writers are closed.
func WithWriterIdleTimeout(timeout time.Duration) MultiWriterOption {
	return func(cfg *topicmultiwriter.MultiWriterConfig) {
		topicmultiwriter.WithWriterIdleTimeout(timeout)(cfg)
	}
}

// WithWriterPartitionByPartitionID sets partition chooser strategy to partition ID-based.
func WithWriterPartitionByPartitionID() MultiWriterOption {
	return func(cfg *topicmultiwriter.MultiWriterConfig) {
		topicmultiwriter.WithWriterPartitionByPartitionID()(cfg)
	}
}

// WithWriterPartitionByKey sets partition chooser strategy to key-based.
func WithWriterPartitionByKey(partitionChooser topicmultiwriter.PartitionChooser) MultiWriterOption {
	return func(cfg *topicmultiwriter.MultiWriterConfig) {
		topicmultiwriter.WithWriterPartitionByKey(partitionChooser)(cfg)
	}
}

// KafkaHashPartitionChooser returns Kafka-based partition chooser strategy.
func KafkaHashPartitionChooser() topicmultiwriter.PartitionChooser {
	return topicmultiwriter.KafkaHashPartitionChooser()
}

// BoundPartitionChooser returns bound-based partition chooser strategy.
func BoundPartitionChooser(options ...partitionchooser.BoundPartitionChooserOption) topicmultiwriter.PartitionChooser {
	return topicmultiwriter.BoundPartitionChooser(options...)
}
