package partition

import (
	"context"
	"sync"
)

type topologyEvent struct {
	partitionID int64
	err         error
}

type subscription struct {
	ctx    context.Context //nolint:containedctx // Subscription lifetime context.
	router *Router
	events []topologyEvent
	wake   chan struct{}
	mu     sync.Mutex
}

func newSubscription(ctx context.Context, router *Router) *subscription {
	return &subscription{
		ctx:    ctx,
		router: router,
		wake:   make(chan struct{}, 1),
	}
}

func (s *subscription) wait() (topologyEvent, error) {
	for {
		if event, ok := s.next(); ok {
			return event, nil
		}
		select {
		case <-s.ctx.Done():
			return topologyEvent{}, s.ctx.Err()
		case <-s.wake:
		}
	}
}

func (s *subscription) notifyReplacements(previous, current *Partitions) {
	for _, partition := range previous.all {
		if partition.IsActive() && replacementPublished(current, partition.ID()) {
			s.push(topologyEvent{partitionID: partition.ID()})
		}
	}
}

func (s *subscription) push(event topologyEvent) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.events = append(s.events, event)
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

func (s *subscription) next() (event topologyEvent, ok bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.events) == 0 {
		return topologyEvent{}, false
	}
	event = s.events[0]
	s.events = s.events[1:]

	return event, true
}
