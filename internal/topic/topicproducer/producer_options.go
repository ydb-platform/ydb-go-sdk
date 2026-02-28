package topicproducer

import (
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicproducer/stubs"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicwriterinternal"
)

type PublicProducerOption func(cfg *ProducerConfig)

func WithProducerTopicPath(path string) PublicProducerOption {
	return func(cfg *ProducerConfig) {
		cfg.TopicPath = path
	}
}

func WithProducerIDPrefix(prefix string) PublicProducerOption {
	return func(cfg *ProducerConfig) {
		cfg.ProducerIDPrefix = prefix
	}
}

func WithPartitioningKeyHasher(hasher KeyHasher) PublicProducerOption {
	return func(cfg *ProducerConfig) {
		cfg.PartitioningKeyHasher = hasher
	}
}

func WithPartitionChooserStrategy(strategy PartitionChooserStrategy) PublicProducerOption {
	return func(cfg *ProducerConfig) {
		cfg.PartitionChooserStrategy = strategy
	}
}

func WithCustomChoosePartitionFunc(customChoosePartitionFunc ChoosePartitionFunc) PublicProducerOption {
	return func(cfg *ProducerConfig) {
		cfg.CustomChoosePartitionFunc = customChoosePartitionFunc
	}
}

func WithSubSessionIdleTimeout(timeout time.Duration) PublicProducerOption {
	return func(cfg *ProducerConfig) {
		cfg.SubSessionIdleTimeout = timeout
	}
}

func WithBasicWriterOptions(opts ...topicwriterinternal.PublicWriterOption) PublicProducerOption {
	return func(cfg *ProducerConfig) {
		for _, opt := range opts {
			opt(&cfg.WriterReconnectorConfig)
		}
	}
}

func withTestMode() PublicProducerOption {
	return func(cfg *ProducerConfig) {
		cfg.testMode = true
	}
}

func withStubWriterType(stubWriterType stubs.StubWriterType) PublicProducerOption {
	return func(cfg *ProducerConfig) {
		cfg.stubWriterType = stubWriterType
	}
}
