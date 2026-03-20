package topicmultiwriter

import (
	"time"
)

type MultiWriterConfig struct {
	WriterIdleTimeout time.Duration
	ProducerIDPrefix  string
	PartitionChooser  partitionChooser

	writersFactory writersFactory
}
