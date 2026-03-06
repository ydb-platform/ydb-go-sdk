package topicmultiwriter

import (
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicwriterinternal"
)

type KeyHasher func(key string) string

type MultiWriterConfig struct {
	topicwriterinternal.WriterReconnectorConfig

	WriterIdleTimeout        time.Duration
	PartitioningKeyHasher    KeyHasher
	PartitionChooserStrategy PartitionChooserStrategy
	ProducerIDPrefix         string
	CustomPartitionChooser   PartitionChooser

	writersFactory writersFactory
}
