package topicproducer

import (
	"context"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topictypes"
)

type TopicDescriber func(ctx context.Context, path string) (topictypes.TopicDescription, error)

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

type ack struct {
	partitionID int64
	seqNo       int64
}

type PartitionChooserStrategy uint8

const (
	PartitionChooserStrategyBound PartitionChooserStrategy = iota
	PartitionChooserStrategyHash
)

type writerWrapper struct {
	writer

	initDone bool
}

type idleWriterInfo struct {
	partitionID int64
	deadline    time.Time
}
