package topicmultiwriter

import (
	"time"
)

type KeyHasher func(key string) string

type MultiWriterConfig struct {
	Initialized bool

	WriterIdleTimeout     time.Duration
	PartitioningKeyHasher KeyHasher
	ProducerIDPrefix      string
	PartitionChooser      PartitionChooser

	writersFactory writersFactory
}
