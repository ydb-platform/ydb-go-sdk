package topicreaderinternal

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopicreader"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xcontext"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
)

const (
	compactionIntervalTime    = time.Hour
	compactionIntervalRemoves = 10000
)

type partitionSession struct {
	Topic       string
	PartitionID int64

	readerID     int64
	connectionID string

	ctx                context.Context
	ctxCancel          context.CancelFunc
	partitionSessionID rawtopicreader.PartitionSessionID

	lastReceivedOffsetEndVal int64
	committedOffsetVal       int64
}

func newPartitionSession(
	partitionContext context.Context,
	topic string,
	partitionID int64,
	readerID int64,
	connectionID string,
	partitionSessionID rawtopicreader.PartitionSessionID,
	committedOffset rawtopicreader.Offset,
) *partitionSession {
	partitionContext, cancel := xcontext.WithCancel(partitionContext)

	return &partitionSession{
		Topic:                    topic,
		PartitionID:              partitionID,
		readerID:                 readerID,
		connectionID:             connectionID,
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

func (s *partitionSession) Close() {
	s.ctxCancel()
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
	m sync.RWMutex

	sessions map[partitionSessionID]*sessionInfo

	removeIndex              int
	lastCompactedTime        time.Time
	lastCompactedRemoveIndex int
}

func (c *partitionSessionStorage) init() {
	c.sessions = make(map[partitionSessionID]*sessionInfo)
	c.lastCompactedTime = time.Now()
}

func (c *partitionSessionStorage) Add(session *partitionSession) error {
	c.m.Lock()
	defer c.m.Unlock()

	if _, ok := c.sessions[session.partitionSessionID]; ok {
		return xerrors.WithStackTrace(fmt.Errorf("session id already existed: %v", session.partitionSessionID))
	}
	c.sessions[session.partitionSessionID] = &sessionInfo{Session: session}
	return nil
}

func (c *partitionSessionStorage) Get(id partitionSessionID) (*partitionSession, error) {
	c.m.RLock()
	defer c.m.RUnlock()

	partitionInfo, has := c.sessions[id]
	if !has || partitionInfo.Session == nil {
		return nil, xerrors.WithStackTrace(fmt.Errorf("ydb: read undefined partition session with id: %v", id))
	}

	return partitionInfo.Session, nil
}

func (c *partitionSessionStorage) Remove(id partitionSessionID) (*partitionSession, error) {
	now := time.Now()
	c.m.Lock()
	defer c.m.Unlock()

	c.removeIndex++
	if partitionInfo, ok := c.sessions[id]; ok {
		partitionInfo.RemoveTime = now
		return partitionInfo.Session, nil
	}

	c.compactionNeedLock(now)

	return nil, xerrors.WithStackTrace(fmt.Errorf("ydb: delete undefined partition session with id: %v", id))
}

func (c *partitionSessionStorage) compactionNeedLock(now time.Time) {
	if !c.isNeedCompactionNeedLock(now) {
		return
	}
	c.doCompactionNeedLock(now)
}

func (c *partitionSessionStorage) isNeedCompactionNeedLock(now time.Time) bool {
	return c.removeIndex-c.lastCompactedRemoveIndex < compactionIntervalRemoves &&
		now.Sub(c.lastCompactedTime) < compactionIntervalTime
}

func (c *partitionSessionStorage) doCompactionNeedLock(now time.Time) {
	newSessions := make(map[partitionSessionID]*sessionInfo, len(c.sessions))
	for sessionID, info := range c.sessions {
		if info.IsGarbage(c.removeIndex, now) {
			continue
		}
		newSessions[sessionID] = info
	}
	c.sessions = newSessions
}

type sessionInfo struct {
	RemoveTime   time.Time
	RemovedIndex int
	Session      *partitionSession
}

func (si *sessionInfo) IsGarbage(removeIndexNow int, timeNow time.Time) bool {
	return removeIndexNow-si.RemovedIndex >= compactionIntervalRemoves ||
		timeNow.Sub(si.RemoveTime) >= compactionIntervalTime
}
