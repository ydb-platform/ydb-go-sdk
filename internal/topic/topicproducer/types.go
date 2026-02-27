package topicproducer

import (
	"time"
)

type PartitionInfo struct {
	ID        int64
	FromBound []byte
	ToBound   []byte
	ParentID  *int64
	Children  []int64
	Locked    bool
}

type partitionShortInfo struct {
	ID        int64
	FromBound string
	ToBound   string
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
