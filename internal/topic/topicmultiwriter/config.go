package topicmultiwriter

import (
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/partition"
)

type MultiWriterConfig struct {
	WriterIdleTimeout time.Duration
	ProducerIDPrefix  string
	PartitionChooser  partition.Chooser
	DirectWrite       bool

	writersFactory writersFactory
}
