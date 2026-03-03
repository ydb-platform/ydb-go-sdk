package topicmultiwriter

import (
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicwriterinternal"
)

type PublicMultiWriterOption func(cfg *MultiWriterConfig)

func WithProducerIDPrefix(prefix string) PublicMultiWriterOption {
	return func(cfg *MultiWriterConfig) {
		cfg.ProducerIDPrefix = prefix
	}
}

func WithPartitioningKeyHasher(hasher KeyHasher) PublicMultiWriterOption {
	return func(cfg *MultiWriterConfig) {
		cfg.PartitioningKeyHasher = hasher
	}
}

func WithPartitionChooserStrategy(strategy PartitionChooserStrategy) PublicMultiWriterOption {
	return func(cfg *MultiWriterConfig) {
		cfg.PartitionChooserStrategy = strategy
	}
}

func WithCustomChoosePartitionFunc(customChoosePartitionFunc ChoosePartitionFunc) PublicMultiWriterOption {
	return func(cfg *MultiWriterConfig) {
		cfg.CustomChoosePartitionFunc = customChoosePartitionFunc
	}
}

func WithWriterIdleTimeout(timeout time.Duration) PublicMultiWriterOption {
	return func(cfg *MultiWriterConfig) {
		cfg.WriterIdleTimeout = timeout
	}
}

func withWritersFactory(writersFactory writersFactory) PublicMultiWriterOption {
	return func(cfg *MultiWriterConfig) {
		cfg.writersFactory = writersFactory
	}
}

func withBasicWriterOptions(opts ...topicwriterinternal.PublicWriterOption) PublicMultiWriterOption {
	return func(cfg *MultiWriterConfig) {
		for _, opt := range opts {
			opt(&cfg.WriterReconnectorConfig)
		}
	}
}
