package topicproducer

import (
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicwriterinternal"
)

type PartitionInfo struct {
	ID        int64
	FromBound string
	ToBound   string
	ParentID  int64
	Splitted  bool
}

type PartitionChooserStrategy uint8

const (
	PartitionChooserStrategyBound PartitionChooserStrategy = iota
	PartitionChooserStrategyHash
)

type KeyHasher func(key string) string

type ProducerConfig struct {
	topicwriterinternal.WriterReconnectorConfig

	SubSessionIdleTimeout    time.Duration
	PartitioningKeyHasher    KeyHasher
	PartitionChooserStrategy PartitionChooserStrategy
	ProducerIDPrefix         string
	TopicPath                string
}

type subWriterWrapper struct {
	subWriter
	inFlightCount int
}

type idleWriterInfo struct {
	partitionID int64
	deadline    time.Time
}
