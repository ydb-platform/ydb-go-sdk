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

	writer := m.removeNeedLock(partitionID)
	if writer == nil {
		return nil, false
	}

	return writer, true
}

func (m *idleWriterManager) removeNeedLock(partitionID int64) *writerWrapper {
	element, ok := m.idleWritersIndex[partitionID]
	if !ok {
		return nil
	}

	writer := m.idleWritersMap[partitionID]
	wasHead := element.Prev() == nil
	m.idleWriters.Remove(element)
	delete(m.idleWritersIndex, partitionID)
	delete(m.idleWritersMap, partitionID)

	if wasHead {
		m.wakeup()
	}

	return writer
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

		var expiredWriter *writerWrapper
		m.mu.WithLock(func() {
			element := m.idleWriters.Front()
			if element == nil {
				return
			}
			if !element.Value.deadline.After(time.Now()) {
				expiredWriter = m.removeNeedLock(element.Value.partitionID)
			}
		})
		if expiredWriter != nil {
			_ = expiredWriter.Close(m.ctx)
		}
	}
}

func (m *idleWriterManager) close(ctx context.Context) error {
	var writersToClose []writer
	m.mu.WithLock(func() {
		writersToClose = make([]writer, 0, len(m.idleWritersMap))
		for _, writer := range m.idleWritersMap {
			writersToClose = append(writersToClose, writer)
		}

		m.idleWriters.Clear()
		m.idleWritersIndex = make(map[int64]*xlist.Element[idleWriterInfo])
		m.idleWritersMap = make(map[int64]*writerWrapper)
	})

	for _, writer := range writersToClose {
		if err := writer.Close(ctx); err != nil {
			return err
		}
	}

	return nil
}

func (m *idleWriterManager) getWritersCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()

	return len(m.idleWritersMap)
}
