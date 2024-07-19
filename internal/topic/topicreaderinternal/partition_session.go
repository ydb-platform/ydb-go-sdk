package topicreaderinternal

import (
	"fmt"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicreadercommon"
	"sync"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
)

const (
	compactionIntervalTime    = time.Hour
	compactionIntervalRemoves = 10000
)

type PartitionSessionStorage struct {
	m sync.RWMutex

	sessions map[partitionSessionID]*sessionInfo

	removeIndex              int
	lastCompactedTime        time.Time
	lastCompactedRemoveIndex int
}

func (c *PartitionSessionStorage) init() {
	c.sessions = make(map[partitionSessionID]*sessionInfo)
	c.lastCompactedTime = time.Now()
}

func (c *PartitionSessionStorage) Add(session *topicreadercommon.PartitionSession) error {
	c.m.Lock()
	defer c.m.Unlock()

	if _, ok := c.sessions[session.PartitionSessionID]; ok {
		return xerrors.WithStackTrace(fmt.Errorf("session id already existed: %v", session.PartitionSessionID))
	}
	c.sessions[session.PartitionSessionID] = &sessionInfo{Session: session}

	return nil
}

func (c *PartitionSessionStorage) Get(id partitionSessionID) (*topicreadercommon.PartitionSession, error) {
	c.m.RLock()
	defer c.m.RUnlock()

	partitionInfo, has := c.sessions[id]
	if !has || partitionInfo.Session == nil {
		return nil, xerrors.WithStackTrace(fmt.Errorf("ydb: read undefined partition session with id: %v", id))
	}

	return partitionInfo.Session, nil
}

func (c *PartitionSessionStorage) Remove(id partitionSessionID) (*topicreadercommon.PartitionSession, error) {
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
	Session      *topicreadercommon.PartitionSession
}

func (si *sessionInfo) IsGarbage(removeIndexNow int, timeNow time.Time) bool {
	return removeIndexNow-si.RemovedIndex >= compactionIntervalRemoves ||
		timeNow.Sub(si.RemoveTime) >= compactionIntervalTime
}
