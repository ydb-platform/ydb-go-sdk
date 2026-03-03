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
	ChoosePartitionFunc      = topicmultiwriter.ChoosePartitionFunc
	PartitionChooserStrategy = topicmultiwriter.PartitionChooserStrategy
)

const (
	PartitionChooserStrategyHash  PartitionChooserStrategy = topicmultiwriter.PartitionChooserStrategyHash
	PartitionChooserStrategyBound PartitionChooserStrategy = topicmultiwriter.PartitionChooserStrategyBound
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

// WithCustomChoosePartitionFunc sets a custom partition selection function.
func WithCustomChoosePartitionFunc(customChoosePartitionFunc topicmultiwriter.ChoosePartitionFunc) MultiWriterOption {
	return topicmultiwriter.WithCustomChoosePartitionFunc(customChoosePartitionFunc)
}

// WithWriterIdleTimeout sets timeout after which idle writers are closed.
func WithWriterIdleTimeout(timeout time.Duration) MultiWriterOption {
	return topicmultiwriter.WithWriterIdleTimeout(timeout)
}
