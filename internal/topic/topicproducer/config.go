package topicproducer

import (
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicwriterinternal"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/tx"
)

type KeyHasher func(key string) string

type ChoosePartitionFunc func(msg Message) (int64, error)

type ProducerConfig struct {
	topicwriterinternal.WriterReconnectorConfig

	SubSessionIdleTimeout     time.Duration
	PartitioningKeyHasher     KeyHasher
	PartitionChooserStrategy  PartitionChooserStrategy
	ProducerIDPrefix          string
	CustomChoosePartitionFunc ChoosePartitionFunc
	Transaction               tx.Transaction

	writersFactory writersFactory
}
