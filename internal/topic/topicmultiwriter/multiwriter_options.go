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

func WithCustomPartitionChooser(customPartitionChooser PartitionChooser) PublicMultiWriterOption {
	return func(cfg *MultiWriterConfig) {
		cfg.PartitionChooser = customPartitionChooser
		cfg.Initialized = true
	}
}

func WithHashPartitionChooser() PublicMultiWriterOption {
	return func(cfg *MultiWriterConfig) {
		cfg.PartitionChooser = newHashPartitionChooser()
		cfg.Initialized = true
	}
}

func WithBoundPartitionChooser() PublicMultiWriterOption {
	return func(cfg *MultiWriterConfig) {
		cfg.PartitionChooser = newBoundPartitionChooser()
		cfg.Initialized = true
	}
}

func WithWriterIdleTimeout(timeout time.Duration) PublicMultiWriterOption {
	return func(cfg *MultiWriterConfig) {
		cfg.WriterIdleTimeout = timeout
		cfg.Initialized = true
	}
}

func WithWriterPartitionByKey() PublicMultiWriterOption {
	return func(cfg *MultiWriterConfig) {
		cfg.Initialized = true
	}
}

func WithWriterPartitionByPartitionID() PublicMultiWriterOption {
	return func(cfg *MultiWriterConfig) {
		cfg.PartitionChooser = newByPartitionIDPartitionChooser()
		cfg.Initialized = true
	}
}

func withWritersFactory(writersFactory writersFactory) PublicMultiWriterOption {
	return func(cfg *MultiWriterConfig) {
		cfg.writersFactory = writersFactory
		cfg.Initialized = true
	}
}
