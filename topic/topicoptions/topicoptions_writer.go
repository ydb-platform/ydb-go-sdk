package topicoptions

import (
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicwriterinternal"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topictypes"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicwriter"
)

type WriterOption func(options *topicwriterinternal.WriteOptions)

type WriteSessionMetadata map[string]string

func WithWriteSessionMeta(meta map[string]string) WriterOption {
	panic("not implemented")
}

func WithMessageGroupID(groupID string) WriterOption {
	panic("not implemented")
}

func WithSyncWrite(sync bool) WriterOption {
	panic("not implemented")
}

func WithPartitionID(partitionID string) WriterOption {
	panic("not implemented")
}

func WithGetLastSeqNo(get bool) WriterOption {
	panic("not implemented")
}

func WithWriterPartitioning(partitioning topicwriter.Partitioning) WriterOption {
	panic("not implemented")
}

func WithOnWriterConnected(f topicwriter.OnWriterInitResponseCallback) WriterOption {
	panic("not implemented")
}

func WithCodec(codec topictypes.Codec) WriterOption {
	panic("not implemented")
}

func WithCodecAutoSelect(autoSelect bool, codecs ...topictypes.Codec) WriterOption {
	panic("not implemented")
}

func WithWriterSetAutoSeqNo(val bool) WriterOption {
	panic("not implemented")
}

func WithWriterSetAutoCreatedAt(val bool) WriterOption {
	panic("not implemented")
}
