package topicreaderinternal

import (
	"context"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopiccommon"
)

// PublicGetPartitionStartOffsetResponse allow to set start offset for read messages for the partition
type PublicGetPartitionStartOffsetResponse struct {
	startOffset     rawtopiccommon.Offset
	startOffsetUsed bool
}

// StartFrom set start offset for read the partition
func (r *PublicGetPartitionStartOffsetResponse) StartFrom(offset int64) {
	r.startOffset.FromInt64(offset)
	r.startOffsetUsed = true
}

// PublicGetPartitionStartOffsetRequest info about partition
type PublicGetPartitionStartOffsetRequest struct {
	Topic       string
	PartitionID int64

	// ExampleOnly
	PartitionSessionID int64
}

// PublicGetPartitionStartOffsetFunc callback function for optional manage read progress store at own side
type PublicGetPartitionStartOffsetFunc func(
	ctx context.Context,
	req PublicGetPartitionStartOffsetRequest,
) (res PublicGetPartitionStartOffsetResponse, err error)

// PublicStopPartitionSessionRequest describes the partition session the server
// is going to stop on the reader. It is passed to the user callback registered
// via topicoptions.WithReaderOnStopPartitionSession.
type PublicStopPartitionSessionRequest struct {
	// Topic is the path of the topic of the partition session.
	Topic string

	// PartitionID is the ID of the topic partition.
	PartitionID int64

	// PartitionSessionID is the server-assigned ID of the partition session.
	PartitionSessionID int64

	// CommittedOffset is the committed offset reported by the server in the
	// StopPartitionSessionRequest message.
	CommittedOffset int64

	// Graceful is true when the server asks for a graceful stop and waits for
	// the SDK to acknowledge it; false means the session has been torn down
	// on the server side already and the user must stop using it immediately.
	Graceful bool
}

// PublicOnStopPartitionSessionFunc is the type of the user callback registered
// via topicoptions.WithReaderOnStopPartitionSession.
type PublicOnStopPartitionSessionFunc func(req PublicStopPartitionSessionRequest)
