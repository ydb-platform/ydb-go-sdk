package topicmultiwriter

import (
	"context"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/empty"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xlist"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xsync"
)

// idleWriterManager tracks idle writers and closes them after a timeout.
type idleWriterManager struct {
	ctx              context.Context //nolint:containedctx
	idleWriters      xlist.List[idleWriterInfo]
	idleWritersIndex map[int64]*xlist.Element[idleWriterInfo]
	idleWritersMap   map[int64]*writerWrapper
	mu               xsync.Mutex
	timeout          time.Duration
	wakeupChan       empty.Chan
}

func newIdleWriterManager(
	ctx context.Context,
	idleTimeout time.Duration,
) *idleWriterManager {
	return &idleWriterManager{
		ctx:              ctx,
		idleWriters:      xlist.New[idleWriterInfo](),
		idleWritersIndex: make(map[int64]*xlist.Element[idleWriterInfo]),
		idleWritersMap:   make(map[int64]*writerWrapper),
		wakeupChan:       make(empty.Chan, 1),
		timeout:          idleTimeout,
	}
}

func (m *idleWriterManager) addWriter(partitionID int64, writer *writerWrapper) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.idleWritersMap[partitionID] = writer
	wasEmpty := m.idleWriters.Len() == 0
	element := m.idleWriters.PushBack(idleWriterInfo{
		partitionID: partitionID,
		deadline:    time.Now().Add(m.timeout),
	})
	m.idleWritersIndex[partitionID] = element

	if wasEmpty {
		m.wakeup()
	}
}

func (m *idleWriterManager) getWriterIfExists(partitionID int64) (*writerWrapper, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	writer, ok := m.idleWritersMap[partitionID]
	if !ok {
		return nil, false
	}

	m.removeNeedLock(partitionID)

	return writer, true
}

func (m *idleWriterManager) removeNeedLock(partitionID int64) {
	element, ok := m.idleWritersIndex[partitionID]
	if !ok {
		return
	}

	wasHead := element.Prev() == nil
	m.idleWriters.Remove(element)
	delete(m.idleWritersIndex, partitionID)
	delete(m.idleWritersMap, partitionID)

	if wasHead {
		m.wakeup()
	}
}

func (m *idleWriterManager) wakeup() {
	select {
	case m.wakeupChan <- empty.Struct{}:
	default:
	}
}

func (m *idleWriterManager) run() {
	for {
		nextTimeout := value.InfiniteDuration
		m.mu.WithLock(func() {
			if m.idleWriters.Len() > 0 {
				nextTimeout = time.Until(m.idleWriters.Front().Value.deadline)
			}
		})

		timer := time.NewTimer(nextTimeout)
		select {
		case <-m.ctx.Done():
			timer.Stop()

			return
		case <-m.wakeupChan:
		case <-timer.C:
		}
		timer.Stop()

		m.mu.WithLock(func() {
			element := m.idleWriters.Front()
			if element == nil {
				return
			}
			if !element.Value.deadline.After(time.Now()) {
				m.removeNeedLock(element.Value.partitionID)
			}
		})
	}
}

func (m *idleWriterManager) getWritersCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()

	return len(m.idleWritersMap)
}
