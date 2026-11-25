package topicreadercommon

import (
	"fmt"
	"sync"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopicreader"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
)

const (
	compactionIntervalTime    = time.Hour
	compactionIntervalRemoves = 10000
)

type PartitionSessionStorage struct {
	m sync.RWMutex

	sessions map[rawtopicreader.PartitionSessionID]*sessionInfo
	// active excludes closed and removed sessions, which can still be looked
	// up by the protocol.
	active map[rawtopicreader.PartitionSessionID]*PartitionSession

	removeIndex              int
	lastCompactedTime        time.Time
	lastCompactedRemoveIndex int
}

func (c *PartitionSessionStorage) initNeedLock() {
	if c.sessions == nil {
		c.sessions = make(map[rawtopicreader.PartitionSessionID]*sessionInfo)
		c.active = make(map[rawtopicreader.PartitionSessionID]*PartitionSession)
		c.lastCompactedTime = time.Now()
	}
}

func (c *PartitionSessionStorage) Add(session *PartitionSession) error {
	c.m.Lock()
	defer c.m.Unlock()

	c.initNeedLock()

	if _, ok := c.sessions[session.StreamPartitionSessionID]; ok {
		return xerrors.WithStackTrace(fmt.Errorf("session id already existed: %v", session.StreamPartitionSessionID))
	}
	c.sessions[session.StreamPartitionSessionID] = &sessionInfo{Session: session}
	session.storage.Store(c)
	if session.retired.Load() {
		session.storage.CompareAndSwap(c, nil)
	} else {
		c.active[session.StreamPartitionSessionID] = session
	}

	return nil
}

func (c *PartitionSessionStorage) Get(id rawtopicreader.PartitionSessionID) (*PartitionSession, error) {
	c.m.RLock()
	defer c.m.RUnlock()

	// A lookup in a nil map is safe; initializing here would write under RLock.
	partitionInfo, has := c.sessions[id]
	if !has || partitionInfo.Session == nil {
		return nil, xerrors.WithStackTrace(fmt.Errorf("ydb: read undefined partition session with id: %v", id))
	}

	return partitionInfo.Session, nil
}

func (c *PartitionSessionStorage) GetAll() []*PartitionSession {
	c.m.Lock()
	defer c.m.Unlock()

	res := make([]*PartitionSession, 0, len(c.sessions))
	for _, s := range c.sessions {
		res = append(res, s.Session)
	}

	return res
}

// ActiveCountsByTopic visits the active index, not the retained protocol history.
func (c *PartitionSessionStorage) ActiveCountsByTopic() map[string]int64 {
	c.m.RLock()
	defer c.m.RUnlock()

	counts := make(map[string]int64)
	for _, session := range c.active {
		if !session.retired.Load() {
			counts[session.Topic]++
		}
	}

	return counts
}

func (c *PartitionSessionStorage) Remove(id rawtopicreader.PartitionSessionID) (*PartitionSession, error) {
	now := time.Now()
	c.m.Lock()
	defer c.m.Unlock()

	c.initNeedLock()

	c.removeIndex++
	if partitionInfo, ok := c.sessions[id]; ok {
		partitionInfo.RemoveTime = now
		delete(c.active, id)
		partitionInfo.Session.storage.CompareAndSwap(c, nil)

		return partitionInfo.Session, nil
	}

	c.compactionNeedLock(now)

	return nil, xerrors.WithStackTrace(fmt.Errorf("ydb: delete undefined partition session with id: %v", id))
}

func (c *PartitionSessionStorage) dropActive(session *PartitionSession) {
	c.m.Lock()
	defer c.m.Unlock()

	id := session.StreamPartitionSessionID
	if c.active[id] == session {
		delete(c.active, id)
	}
}

func (c *PartitionSessionStorage) compactionNeedLock(now time.Time) {
	if !c.isNeedCompactionNeedLock(now) {
		return
	}
	c.doCompactionNeedLock(now)
}

func (c *PartitionSessionStorage) isNeedCompactionNeedLock(now time.Time) bool {
	return c.removeIndex-c.lastCompactedRemoveIndex < compactionIntervalRemoves &&
		now.Sub(c.lastCompactedTime) < compactionIntervalTime
}

func (c *PartitionSessionStorage) doCompactionNeedLock(now time.Time) {
	newSessions := make(map[rawtopicreader.PartitionSessionID]*sessionInfo, len(c.sessions))
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
	Session      *PartitionSession
}

func (si *sessionInfo) IsGarbage(removeIndexNow int, timeNow time.Time) bool {
	return removeIndexNow-si.RemovedIndex >= compactionIntervalRemoves ||
		timeNow.Sub(si.RemoveTime) >= compactionIntervalTime
}
