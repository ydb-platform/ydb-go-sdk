package topicreaderinternal

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopicreader"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xcontext"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
)

type partitionSession struct {
	Topic       string
	PartitionID int64

	ctx                context.Context
	ctxCancel          xcontext.CancelErrFunc
	partitionSessionID rawtopicreader.PartitionSessionID

	lastReceivedOffsetEndVal int64
	committedOffsetVal       int64
}

func newPartitionSession(
	partitionContext context.Context,
	topic string,
	partitionID int64,
	partitionSessionID rawtopicreader.PartitionSessionID,
	committedOffset rawtopicreader.Offset,
) *partitionSession {
	partitionContext, cancel := xcontext.WithErrCancel(partitionContext)

	return &partitionSession{
		Topic:                    topic,
		PartitionID:              partitionID,
		ctx:                      partitionContext,
		ctxCancel:                cancel,
		partitionSessionID:       partitionSessionID,
		committedOffsetVal:       committedOffset.ToInt64(),
		lastReceivedOffsetEndVal: committedOffset.ToInt64() - 1,
	}
}

func (s *partitionSession) Context() context.Context {
	return s.ctx
}

func (s *partitionSession) close(err error) {
	s.ctxCancel(err)
}

func (s *partitionSession) committedOffset() rawtopicreader.Offset {
	v := atomic.LoadInt64(&s.committedOffsetVal)

	var res rawtopicreader.Offset
	res.FromInt64(v)
	return res
}

func (s *partitionSession) setCommittedOffset(v rawtopicreader.Offset) {
	atomic.StoreInt64(&s.committedOffsetVal, v.ToInt64())
}

func (s *partitionSession) lastReceivedMessageOffset() rawtopicreader.Offset {
	v := atomic.LoadInt64(&s.lastReceivedOffsetEndVal)

	var res rawtopicreader.Offset
	res.FromInt64(v)
	return res
}

func (s *partitionSession) setLastReceivedMessageOffset(v rawtopicreader.Offset) {
	atomic.StoreInt64(&s.lastReceivedOffsetEndVal, v.ToInt64())
}

type partitionSessionStorage struct {
	m        sync.RWMutex
	sessions map[partitionSessionID]*partitionSession
}

func (c *partitionSessionStorage) init() {
	c.sessions = make(map[partitionSessionID]*partitionSession)
}

func (c *partitionSessionStorage) Add(session *partitionSession) error {
	c.m.Lock()
	defer c.m.Unlock()

	if _, ok := c.sessions[session.partitionSessionID]; ok {
		return xerrors.WithStackTrace(fmt.Errorf("session id already existed: %v", session.partitionSessionID))
	}
	c.sessions[session.partitionSessionID] = session
	return nil
}

func (c *partitionSessionStorage) Get(id partitionSessionID) (*partitionSession, error) {
	c.m.RLock()
	defer c.m.RUnlock()

	partition := c.sessions[id]
	if partition == nil {
		return nil, xerrors.WithStackTrace(fmt.Errorf("ydb: read undefined partition with id: %v", id))
	}

	return partition, nil
}

func (c *partitionSessionStorage) Remove(id partitionSessionID) (*partitionSession, error) {
	c.m.Lock()
	defer c.m.Unlock()

	if data, ok := c.sessions[id]; ok {
		delete(c.sessions, id)
		return data, nil
	}

	return nil, xerrors.WithStackTrace(fmt.Errorf("ydb: delete undefined partition session with id: %v", id))
}
