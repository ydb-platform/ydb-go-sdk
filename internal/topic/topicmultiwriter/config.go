package topicmultiwriter

import (
	"time"
)

type MultiWriterConfig struct {
	Initialized bool

	WriterIdleTimeout time.Duration
	ProducerIDPrefix  string
	PartitionChooser  partitionChooser

	writersFactory writersFactory
}
