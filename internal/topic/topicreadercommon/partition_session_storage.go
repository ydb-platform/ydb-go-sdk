package topicreadercommon

import (
	"fmt"
	"maps"
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
	active         *activeSessions
	activeReleased bool

	removeIndex              int
	lastCompactedTime        time.Time
	lastCompactedRemoveIndex int
}

func (c *PartitionSessionStorage) initNeedLock() {
	if c.sessions == nil {
		c.sessions = make(map[rawtopicreader.PartitionSessionID]*sessionInfo)
		c.lastCompactedTime = time.Now()
	}
}

// Add records a protocol session. Metered streams opt into active tracking on assignment.
func (c *PartitionSessionStorage) Add(session *PartitionSession, trackActive ...bool) error {
	c.m.Lock()
	defer c.m.Unlock()

	c.initNeedLock()

	if _, ok := c.sessions[session.StreamPartitionSessionID]; ok {
		return xerrors.WithStackTrace(fmt.Errorf("session id already existed: %v", session.StreamPartitionSessionID))
	}
	c.sessions[session.StreamPartitionSessionID] = &sessionInfo{Session: session}
	if c.active == nil && !c.activeReleased && len(trackActive) != 0 && trackActive[0] {
		c.active = &activeSessions{
			sessions: make(map[rawtopicreader.PartitionSessionID]*PartitionSession),
			counts:   make(map[string]int64),
		}
	}
	if c.active != nil {
		c.active.add(session)
	}

	return nil
}

// ReleaseActiveIndex stops active tracking without changing protocol lookups.
func (c *PartitionSessionStorage) ReleaseActiveIndex() {
	c.m.Lock()
	defer c.m.Unlock()

	c.activeReleased = true
	if c.active != nil {
		c.active.release()
		c.active = nil
	}
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

func (c *PartitionSessionStorage) ActiveCountsByTopic() map[string]int64 {
	active := c.activeIndex()
	if active == nil {
		return make(map[string]int64)
	}

	return active.snapshot()
}

func (c *PartitionSessionStorage) activeIndex() *activeSessions {
	c.m.RLock()
	defer c.m.RUnlock()

	return c.active
}

func (c *PartitionSessionStorage) Remove(id rawtopicreader.PartitionSessionID) (*PartitionSession, error) {
	now := time.Now()
	c.m.Lock()
	defer c.m.Unlock()

	c.initNeedLock()

	c.removeIndex++
	if partitionInfo, ok := c.sessions[id]; ok {
		partitionInfo.RemoveTime = now
		if c.active != nil {
			c.active.drop(partitionInfo.Session)
		}

		return partitionInfo.Session, nil
	}

	c.compactionNeedLock(now)

	return nil, xerrors.WithStackTrace(fmt.Errorf("ydb: delete undefined partition session with id: %v", id))
}

// activeSessions is separate from the storage: a session points only here, so a
// retained message does not keep the removal history of its stream alive.
type activeSessions struct {
	m        sync.Mutex
	sessions map[rawtopicreader.PartitionSessionID]*PartitionSession
	counts   map[string]int64
}

func (a *activeSessions) snapshot() map[string]int64 {
	a.m.Lock()
	defer a.m.Unlock()

	counts := make(map[string]int64, len(a.counts))
	maps.Copy(counts, a.counts)

	return counts
}

func (a *activeSessions) release() {
	a.m.Lock()
	defer a.m.Unlock()

	a.sessions = nil
	a.counts = nil
}

func (a *activeSessions) add(session *PartitionSession) {
	a.m.Lock()
	defer a.m.Unlock()

	session.active.Store(a)
	if session.retired.Load() {
		session.active.CompareAndSwap(a, nil)
	} else {
		a.sessions[session.StreamPartitionSessionID] = session
		a.counts[session.Topic]++
	}
}

func (a *activeSessions) drop(session *PartitionSession) {
	a.m.Lock()
	defer a.m.Unlock()

	id := session.StreamPartitionSessionID
	if a.sessions[id] == session {
		delete(a.sessions, id)
		topic := session.Topic
		if a.counts[topic] == 1 {
			delete(a.counts, topic)
		} else {
			a.counts[topic]--
		}
	}
	session.active.CompareAndSwap(a, nil)
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
