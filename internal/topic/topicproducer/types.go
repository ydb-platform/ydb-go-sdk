package topicproducer

import (
	"time"
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

type subWriterWrapper struct {
	subWriter
	inFlightCount int
}

type idleWriterInfo struct {
	partitionID int64
	deadline    time.Time
}
