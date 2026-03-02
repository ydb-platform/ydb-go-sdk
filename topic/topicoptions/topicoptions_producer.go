package topicoptions

import (
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicproducer"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicwriterinternal"
)

// ProducerOption configures a topic producer.
//
// It is a thin alias for internal producer options.
type ProducerOption = topicproducer.PublicProducerOption

// WithProducerIDPrefix sets a prefix for producer IDs used by the internal producer.
func WithProducerIDPrefix(prefix string) ProducerOption {
	return topicproducer.WithProducerIDPrefix(prefix)
}

// WithPartitioningKeyHasher sets a custom key hasher used before partition selection.
func WithPartitioningKeyHasher(hasher topicproducer.KeyHasher) ProducerOption {
	return topicproducer.WithPartitioningKeyHasher(hasher)
}

// WithPartitionChooserStrategy sets partition chooser strategy for the producer.
func WithPartitionChooserStrategy(strategy topicproducer.PartitionChooserStrategy) ProducerOption {
	return topicproducer.WithPartitionChooserStrategy(strategy)
}

// WithCustomChoosePartitionFunc sets a custom partition selection function.
func WithCustomChoosePartitionFunc(customChoosePartitionFunc topicproducer.ChoosePartitionFunc) ProducerOption {
	return topicproducer.WithCustomChoosePartitionFunc(customChoosePartitionFunc)
}

// WithSubSessionIdleTimeout sets timeout after which idle sub-sessions are closed.
func WithSubSessionIdleTimeout(timeout time.Duration) ProducerOption {
	return topicproducer.WithSubSessionIdleTimeout(timeout)
}

// WithBasicWriterOptions forwards basic writer options into underlying writer reconnectors.
func WithBasicWriterOptions(opts ...topicwriterinternal.PublicWriterOption) ProducerOption {
	return topicproducer.WithBasicWriterOptions(opts...)
}
