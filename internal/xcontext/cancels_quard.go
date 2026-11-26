package xcontext

import (
	"context"
	"sync"
)

type (
	CancelsGuard struct {
		mu      sync.Mutex
		cancels map[*context.CancelFunc]struct{}
		drained chan struct{}
	}
)

func NewCancelsGuard() *CancelsGuard {
	return &CancelsGuard{
		cancels: make(map[*context.CancelFunc]struct{}),
	}
}

// WithCancel registers a context until its cancel function is called.
// It returns nil values if the guard has been sealed.
func (g *CancelsGuard) WithCancel(ctx context.Context) (context.Context, context.CancelFunc) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.drained != nil {
		return nil, nil
	}
	ctx, cancel := WithCancel(ctx)
	g.cancels[&cancel] = struct{}{}

	return ctx, func() {
		cancel()
		g.mu.Lock()
		defer g.mu.Unlock()
		if _, ok := g.cancels[&cancel]; ok {
			delete(g.cancels, &cancel)
			if g.drained != nil && len(g.cancels) == 0 {
				close(g.drained)
			}
		}
	}
}

// Seal rejects new registrations and signals when the existing ones finish.
func (g *CancelsGuard) Seal() <-chan struct{} {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.drained == nil {
		g.drained = make(chan struct{})
		if len(g.cancels) == 0 {
			close(g.drained)
		}
	}

	return g.drained
}

func (g *CancelsGuard) Cancel() {
	g.mu.Lock()
	defer g.mu.Unlock()
	for cancel := range g.cancels {
		(*cancel)()
	}
	if g.drained != nil && len(g.cancels) > 0 {
		close(g.drained)
	}
	clear(g.cancels)
}
