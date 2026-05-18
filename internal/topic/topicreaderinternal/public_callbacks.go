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
//
// For graceful stops the user can flush local state and perform final commit
// attempts before the SDK sends StopPartitionSessionResponse to the server.
//
// When Graceful is false, the partition session is no longer valid on the
// client for operations tied to that session: do not commit messages or
// otherwise rely on this session—the server has already revoked it.
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
	// the SDK to acknowledge it. When false, the session has already been torn
	// down on the server side; you must not commit messages or perform other
	// session-scoped work for this partition session.
	Graceful bool
}

// PublicOnStopPartitionSessionResult is the callback return value for
// WithReaderOnStopPartitionSession. It is reserved for future feedback from the
// user handler to the SDK.
type PublicOnStopPartitionSessionResult struct{}

// PublicOnStopPartitionSessionFunc is the type of the user callback registered
// via topicoptions.WithReaderOnStopPartitionSession.
type PublicOnStopPartitionSessionFunc func(req PublicStopPartitionSessionRequest) PublicOnStopPartitionSessionResult
