package topicmultiwriter

import (
	"time"
)

type MultiWriterConfig struct {
	WriterIdleTimeout time.Duration
	ProducerIDPrefix  string
	PartitionChooser  PartitionChooser
	DirectWrite       bool

	writersFactory writersFactory
}
