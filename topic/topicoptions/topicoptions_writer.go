package topicoptions

import (
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicwriterinternal"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topictypes"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicwriter"
)

type WriterOption = topicwriterinternal.PublicWriterOption

type WriteSessionMetadata map[string]string

// WithWriteSessionMeta
//
// # Experimental
//
// Notice: This API is EXPERIMENTAL and may be changed or removed in a later release.
func WithWriteSessionMeta(meta map[string]string) WriterOption {
	panic("not implemented")
}

// WithMessageGroupID
//
// # Experimental
//
// Notice: This API is EXPERIMENTAL and may be changed or removed in a later release.
func WithMessageGroupID(groupID string) WriterOption {
	panic("not implemented")
}

// WithSyncWrite
//
// # Experimental
//
// Notice: This API is EXPERIMENTAL and may be changed or removed in a later release.
func WithSyncWrite(sync bool) WriterOption {
	return topicwriterinternal.WithSyncWrite(sync)
}

// WithPartitionID
//
// # Experimental
//
// Notice: This API is EXPERIMENTAL and may be changed or removed in a later release.
func WithPartitionID(partitionID string) WriterOption {
	panic("not implemented")
}

// WithGetLastSeqNo
//
// # Experimental
//
// Notice: This API is EXPERIMENTAL and may be changed or removed in a later release.
func WithGetLastSeqNo(get bool) WriterOption {
	panic("not implemented")
}

// WithWriterPartitioning
//
// # Experimental
//
// Notice: This API is EXPERIMENTAL and may be changed or removed in a later release.
func WithWriterPartitioning(partitioning topicwriter.Partitioning) WriterOption {
	return topicwriterinternal.WithPartitioning(partitioning)
}

// WithOnWriterConnected
//
// # Experimental
//
// Notice: This API is EXPERIMENTAL and may be changed or removed in a later release.
func WithOnWriterConnected(f topicwriter.OnWriterInitResponseCallback) WriterOption {
	panic("not implemented")
}

// WithCodec
//
// # Experimental
//
// Notice: This API is EXPERIMENTAL and may be changed or removed in a later release.
func WithCodec(codec topictypes.Codec) WriterOption {
	panic("not implemented")
}

// WithCodecAutoSelect
//
// # Experimental
//
// Notice: This API is EXPERIMENTAL and may be changed or removed in a later release.
func WithCodecAutoSelect(autoSelect bool, codecs ...topictypes.Codec) WriterOption {
	panic("not implemented")
}

// WithWriterSetAutoSeqNo
//
// # Experimental
//
// Notice: This API is EXPERIMENTAL and may be changed or removed in a later release.
func WithWriterSetAutoSeqNo(val bool) WriterOption {
	panic("not implemented")
}

// WithWriterSetAutoCreatedAt
//
// # Experimental
//
// Notice: This API is EXPERIMENTAL and may be changed or removed in a later release.
func WithWriterSetAutoCreatedAt(val bool) WriterOption {
	panic("not implemented")
}
