package topicmultiwriter

import (
	"context"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/empty"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xlist"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xsync"
)

type idleWritersSupervisor struct {
	ctx              context.Context //nolint:containedctx
	idleWriters      xlist.List[idleWriterInfo]
	idleWritersIndex map[int64]*xlist.Element[idleWriterInfo]
	mu               xsync.Mutex
	timeout          time.Duration
	wakeupChan       empty.Chan
	worker           *worker
}

func newIdleWritersSupervisor(
	ctx context.Context,
	worker *worker,
	idleTimeout time.Duration,
) *idleWritersSupervisor {
	return &idleWritersSupervisor{
		ctx:              ctx,
		idleWriters:      xlist.New[idleWriterInfo](),
		idleWritersIndex: make(map[int64]*xlist.Element[idleWriterInfo]),
		wakeupChan:       make(empty.Chan, 1),
		worker:           worker,
		timeout:          idleTimeout,
	}
}

func (s *idleWritersSupervisor) add(partitionID int64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	wasEmpty := s.idleWriters.Len() == 0
	element := s.idleWriters.PushBack(idleWriterInfo{
		partitionID: partitionID,
		deadline:    time.Now().Add(s.timeout),
	})
	s.idleWritersIndex[partitionID] = element

	if wasEmpty {
		s.wakeup()
	}
}

func (s *idleWritersSupervisor) remove(partitionID int64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	element, ok := s.idleWritersIndex[partitionID]
	if !ok {
		return
	}

	wasHead := element.Prev() == nil
	s.idleWriters.Remove(element)
	delete(s.idleWritersIndex, partitionID)

	if wasHead {
		s.wakeup()
	}
}

func (s *idleWritersSupervisor) wakeup() {
	select {
	case s.wakeupChan <- empty.Struct{}:
	default:
	}
}

func (s *idleWritersSupervisor) run() {
	for {
		nextTimeout := infiniteTimeout
		s.mu.WithLock(func() {
			if s.idleWriters.Len() > 0 {
				nextTimeout = time.Until(s.idleWriters.Front().Value.deadline)
			}
		})

		select {
		case <-s.ctx.Done():
			return
		case <-s.wakeupChan:
		case <-time.After(nextTimeout):
		}

		var partitionID *int64
		s.mu.WithLock(func() {
			element := s.idleWriters.Front()
			if element == nil {
				return
			}
			if !element.Value.deadline.After(time.Now()) {
				partitionID = &element.Value.partitionID
				s.idleWriters.Remove(element)
			}
		})

		if partitionID == nil {
			continue
		}

		s.worker.removeWriter(*partitionID)
	}
}
