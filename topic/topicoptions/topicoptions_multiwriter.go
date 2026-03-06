package topicoptions

import (
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicmultiwriter"
)

// MultiWriterOption configures a topic multiwriter.
//
// It is a thin alias for internal multiwriter options.
type (
	MultiWriterOption        = topicmultiwriter.PublicMultiWriterOption
	KeyHasher                = topicmultiwriter.KeyHasher
	PartitionChooserStrategy = topicmultiwriter.PartitionChooserStrategy
	PartitionChooser         = topicmultiwriter.PartitionChooser
)

const (
	PartitionChooserStrategyHash   PartitionChooserStrategy = topicmultiwriter.PartitionChooserStrategyHash
	PartitionChooserStrategyBound  PartitionChooserStrategy = topicmultiwriter.PartitionChooserStrategyBound
	PartitionChooserStrategyCustom PartitionChooserStrategy = topicmultiwriter.PartitionChooserStrategyCustom
)

// WithProducerIDPrefix sets a prefix for producer IDs used by the internal producer.
func WithProducerIDPrefix(prefix string) MultiWriterOption {
	return topicmultiwriter.WithProducerIDPrefix(prefix)
}

// WithPartitioningKeyHasher sets a custom key hasher used before partition selection.
func WithPartitioningKeyHasher(hasher topicmultiwriter.KeyHasher) MultiWriterOption {
	return topicmultiwriter.WithPartitioningKeyHasher(hasher)
}

// WithPartitionChooserStrategy sets partition chooser strategy for the producer.
func WithPartitionChooserStrategy(strategy topicmultiwriter.PartitionChooserStrategy) MultiWriterOption {
	return topicmultiwriter.WithPartitionChooserStrategy(strategy)
}

// WithCustomPartitionChooser sets a custom partition chooser.
func WithCustomPartitionChooser(customPartitionChooser topicmultiwriter.PartitionChooser) MultiWriterOption {
	return topicmultiwriter.WithCustomPartitionChooser(customPartitionChooser)
}

// WithWriterIdleTimeout sets timeout after which idle writers are closed.
func WithWriterIdleTimeout(timeout time.Duration) MultiWriterOption {
	return topicmultiwriter.WithWriterIdleTimeout(timeout)
}
