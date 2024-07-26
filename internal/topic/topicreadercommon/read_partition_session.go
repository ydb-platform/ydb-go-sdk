package topicreadercommon

import (
	"context"
	"sync/atomic"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopicreader"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xcontext"
)

type PartitionSession struct {
	Topic       string
	PartitionID int64

	ReaderID     int64
	connectionID string

	ctx                      context.Context //nolint:containedctx
	ctxCancel                context.CancelFunc
	StreamPartitionSessionID rawtopicreader.PartitionSessionID
	ClientPartitionSessionID int64

	lastReceivedOffsetEndVal atomic.Int64
	committedOffsetVal       atomic.Int64
}

func NewPartitionSession(
	partitionContext context.Context,
	topic string,
	partitionID int64,
	readerID int64,
	connectionID string,
	partitionSessionID rawtopicreader.PartitionSessionID,
	clientPartitionSessionID int64,
	committedOffset rawtopicreader.Offset,
) *PartitionSession {
	partitionContext, cancel := xcontext.WithCancel(partitionContext)

	res := &PartitionSession{
		Topic:                    topic,
		PartitionID:              partitionID,
		ReaderID:                 readerID,
		connectionID:             connectionID,
		ctx:                      partitionContext,
		ctxCancel:                cancel,
		StreamPartitionSessionID: partitionSessionID,
		ClientPartitionSessionID: clientPartitionSessionID,
	}
	res.committedOffsetVal.Store(committedOffset.ToInt64())
	res.lastReceivedOffsetEndVal.Store(committedOffset.ToInt64() - 1)

	return res
}

func (s *PartitionSession) Context() context.Context {
	return s.ctx
}

func (s *PartitionSession) SetContext(ctx context.Context) {
	s.ctx, s.ctxCancel = xcontext.WithCancel(ctx)
}

func (s *PartitionSession) Close() {
	s.ctxCancel()
}

func (s *PartitionSession) CommittedOffset() rawtopicreader.Offset {
	v := s.committedOffsetVal.Load()

	var res rawtopicreader.Offset
	res.FromInt64(v)

	return res
}

func (s *PartitionSession) SetCommittedOffset(v rawtopicreader.Offset) {
	s.committedOffsetVal.Store(v.ToInt64())
}

func (s *PartitionSession) LastReceivedMessageOffset() rawtopicreader.Offset {
	v := s.lastReceivedOffsetEndVal.Load()

	var res rawtopicreader.Offset
	res.FromInt64(v)

	return res
}

func (s *PartitionSession) SetLastReceivedMessageOffset(v rawtopicreader.Offset) {
	s.lastReceivedOffsetEndVal.Store(v.ToInt64())
}

func (s *PartitionSession) ToPublic() PublicPartitionSession {
	return PublicPartitionSession{
		PartitionSessionID: s.ClientPartitionSessionID,
		TopicPath:          s.Topic,
		PartitionID:        s.PartitionID,
	}
}

// PublicPartitionSession contains information about partition session for the event
//
// Experimental: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#experimental
type PublicPartitionSession struct {
	// PartitionSessionID is unique session ID per listener object
	PartitionSessionID int64

	// TopicPath contains path for the topic
	TopicPath string

	// PartitionID contains partition id. It can be repeated for one reader if the partition will stop/start few times.
	PartitionID int64
}
