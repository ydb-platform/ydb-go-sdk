package topicreaderinternal

import (
	"context"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopicreader"
)

// PublicGetPartitionStartOffsetResponse
//
// # Experimental
//
// Notice: This API is EXPERIMENTAL and may be changed or removed in a later release.
type PublicGetPartitionStartOffsetResponse struct {
	startOffset     rawtopicreader.Offset
	startOffsetUsed bool
}

func (r *PublicGetPartitionStartOffsetResponse) StartFrom(offset int64) {
	r.startOffset.FromInt64(offset)
	r.startOffsetUsed = true
}

// PublicGetPartitionStartOffsetRequest
//
// # Experimental
//
// Notice: This API is EXPERIMENTAL and may be changed or removed in a later release.
type PublicGetPartitionStartOffsetRequest struct {
	Topic       string
	PartitionID int64
}

// PublicGetPartitionStartOffsetFunc
//
// # Experimental
//
// Notice: This API is EXPERIMENTAL and may be changed or removed in a later release.
type PublicGetPartitionStartOffsetFunc func(
	ctx context.Context,
	req PublicGetPartitionStartOffsetRequest,
) (res PublicGetPartitionStartOffsetResponse, err error)
