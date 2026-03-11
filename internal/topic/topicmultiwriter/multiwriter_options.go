package topicmultiwriter

import (
	"time"
)

type PublicMultiWriterOption func(cfg *MultiWriterConfig)

func WithProducerIDPrefix(prefix string) PublicMultiWriterOption {
	return func(cfg *MultiWriterConfig) {
		cfg.ProducerIDPrefix = prefix
		cfg.Initialized = true
	}
}

func WithPartitioningKeyHasher(hasher KeyHasher) PublicMultiWriterOption {
	return func(cfg *MultiWriterConfig) {
		cfg.PartitioningKeyHasher = hasher
		cfg.Initialized = true
	}
}

func WithPartitionChooserStrategy(strategy PartitionChooserStrategy) PublicMultiWriterOption {
	return func(cfg *MultiWriterConfig) {
		cfg.PartitionChooserStrategy = strategy
		cfg.Initialized = true
	}
}

func WithCustomPartitionChooser(customPartitionChooser PartitionChooser) PublicMultiWriterOption {
	return func(cfg *MultiWriterConfig) {
		cfg.CustomPartitionChooser = customPartitionChooser
		cfg.Initialized = true
	}
}

func WithWriterIdleTimeout(timeout time.Duration) PublicMultiWriterOption {
	return func(cfg *MultiWriterConfig) {
		cfg.WriterIdleTimeout = timeout
		cfg.Initialized = true
	}
}

func withWritersFactory(writersFactory writersFactory) PublicMultiWriterOption {
	return func(cfg *MultiWriterConfig) {
		cfg.writersFactory = writersFactory
		cfg.Initialized = true
	}
}
