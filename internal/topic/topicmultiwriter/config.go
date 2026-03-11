package topicmultiwriter

import (
	"time"
)

type KeyHasher func(key string) string

type MultiWriterConfig struct {
	Initialized bool

	WriterIdleTimeout        time.Duration
	PartitioningKeyHasher    KeyHasher
	PartitionChooserStrategy PartitionChooserStrategy
	ProducerIDPrefix         string
	CustomPartitionChooser   PartitionChooser

	writersFactory writersFactory
}
