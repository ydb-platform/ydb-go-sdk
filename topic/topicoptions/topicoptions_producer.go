package topicoptions

import (
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicmultiwriter"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicwriterinternal"
)

// ProducerOption configures a topic producer.
//
// It is a thin alias for internal producer options.
type MultiWriterOption = topicmultiwriter.PublicMultiWriterOption

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

// WithSubSessionIdleTimeout sets timeout after which idle sub-sessions are closed.
func WithSubSessionIdleTimeout(timeout time.Duration) MultiWriterOption {
	return topicmultiwriter.WithWriterIdleTimeout(timeout)
}

// WithBasicWriterOptions forwards basic writer options into underlying writer reconnectors.
func WithBasicWriterOptions(opts ...topicwriterinternal.PublicWriterOption) MultiWriterOption {
	return topicmultiwriter.WithBasicWriterOptions(opts...)
}
