package transactionalwriterbenchmark

import (
	"fmt"
	"os"
	"time"
)

type writerMode string

const (
	writerModeSingle writerMode = "single"
	writerModeMany   writerMode = "many"
)

type routingMode string

const (
	routingModeKey        routingMode = "key"
	routingModeBoundedKey routingMode = "bounded-key"
)

type config struct {
	DSN                    string
	TopicPath              string
	TablePath              string
	RunID                  string
	ProducerIDPrefix       string
	Mode                   writerMode
	Routing                routingMode
	AutoSeqNo              bool
	QueryRetries           bool
	Duration               time.Duration
	TransactionTimeout     time.Duration
	Concurrency            int
	MessagesPerTx          int
	MessageSize            int
	LatencySampleEvery     int
	PreparePartitions      int64
	AutoSplit              bool
	AutoSplitWriteSpeed    int64
	AutoSplitBurstBytes    int64
	AutoSplitUpUtilization int
	AutoSplitStabilization time.Duration
	SkipTableWrite         bool
}

func defaultRunID() string {
	return fmt.Sprintf(
		"go-%s-%d",
		time.Now().UTC().Format("20060102T150405.000000000Z"),
		os.Getpid(),
	)
}
