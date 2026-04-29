package topicmultiwriter

import (
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicmultiwriter/partitionchooser"
)

type PublicMultiWriterOption func(cfg *MultiWriterConfig)

type PublicPartitionChooserOption func(cfg *MultiWriterConfig)

func WithProducerIDPrefix(prefix string) PublicMultiWriterOption {
	return func(cfg *MultiWriterConfig) {
		cfg.ProducerIDPrefix = prefix
	}
}

func WithCustomPartitionChooser(customPartitionChooser PartitionChooser) PublicPartitionChooserOption {
	return func(cfg *MultiWriterConfig) {
		cfg.PartitionChooser = customPartitionChooser
	}
}

func WithWriterIdleTimeout(timeout time.Duration) PublicMultiWriterOption {
	return func(cfg *MultiWriterConfig) {
		cfg.WriterIdleTimeout = timeout
	}
}

func WithWriterPartitionByPartitionID() PublicMultiWriterOption {
	return func(cfg *MultiWriterConfig) {
		cfg.PartitionChooser = partitionchooser.NewByPartitionIDPartitionChooser()
	}
}

func KafkaHashPartitionChooser() PartitionChooser {
	return partitionchooser.NewHashPartitionChooser()
}

func BoundPartitionChooser(options ...partitionchooser.BoundPartitionChooserOption) PartitionChooser {
	return partitionchooser.NewBoundPartitionChooser(options...)
}

func WithWriterPartitionByKey(partitionChooser PartitionChooser) PublicPartitionChooserOption {
	return func(cfg *MultiWriterConfig) {
		cfg.PartitionChooser = partitionChooser
	}
}

func withWritersFactory(writersFactory writersFactory) PublicMultiWriterOption {
	return func(cfg *MultiWriterConfig) {
		cfg.writersFactory = writersFactory
	}
}
