package topicmultiwriter

import (
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicmultiwriter/partitionchooser"
)

type PublicMultiWriterOption func(cfg *MultiWriterConfig)

func WithProducerIDPrefix(prefix string) PublicMultiWriterOption {
	return func(cfg *MultiWriterConfig) {
		cfg.ProducerIDPrefix = prefix
		cfg.Initialized = true
	}
}

func WithCustomPartitionChooser(customPartitionChooser partitionChooser) PublicMultiWriterOption {
	return func(cfg *MultiWriterConfig) {
		cfg.PartitionChooser = customPartitionChooser
		cfg.Initialized = true
	}
}

func WithHashPartitionChooser() PublicMultiWriterOption {
	return func(cfg *MultiWriterConfig) {
		cfg.PartitionChooser = partitionchooser.NewHashPartitionChooser()
		cfg.Initialized = true
	}
}

func WithBoundPartitionChooser(options ...partitionchooser.BoundPartitionChooserOption) PublicMultiWriterOption {
	return func(cfg *MultiWriterConfig) {
		cfg.PartitionChooser = partitionchooser.NewBoundPartitionChooser(options...)
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
		cfg.PartitionChooser = partitionchooser.NewByPartitionIDPartitionChooser()
		cfg.Initialized = true
	}
}

func withWritersFactory(writersFactory writersFactory) PublicMultiWriterOption {
	return func(cfg *MultiWriterConfig) {
		cfg.writersFactory = writersFactory
		cfg.Initialized = true
	}
}
