package topicwriter

import "github.com/ydb-platform/ydb-go-sdk/v3/topic/topictypes"

type WithOnWriterConnectedInfo struct {
	LastSeqNo   int64
	SessionID   string
	PartitionID int64
	Codecs      []topictypes.Codec
}

type OnWriterInitResponseCallback func(info WithOnWriterConnectedInfo) error
