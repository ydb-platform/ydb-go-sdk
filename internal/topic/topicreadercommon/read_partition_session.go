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

	ctx                context.Context //nolint:containedctx
	ctxCancel          context.CancelFunc
	PartitionSessionID rawtopicreader.PartitionSessionID

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
	committedOffset rawtopicreader.Offset,
) *PartitionSession {
	partitionContext, cancel := xcontext.WithCancel(partitionContext)

	res := &PartitionSession{
		Topic:              topic,
		PartitionID:        partitionID,
		ReaderID:           readerID,
		connectionID:       connectionID,
		ctx:                partitionContext,
		ctxCancel:          cancel,
		PartitionSessionID: partitionSessionID,
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
