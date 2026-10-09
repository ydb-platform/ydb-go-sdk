package topicwriterinternal

import (
	"context"
	"slices"
	"sync"
)

// ReconnectorPool owns writer reconnectors across transactional writers of one Topic client.
type ReconnectorPool struct {
	mu     sync.Mutex
	idle   map[string][]*WriterReconnector
	closed bool
	create func(WriterReconnectorConfig) (*WriterReconnector, error)
}

func NewReconnectorPool() *ReconnectorPool {
	return &ReconnectorPool{create: NewWriterReconnector}
}

func (p *ReconnectorPool) Get(cfg WriterReconnectorConfig) (*WriterReconnector, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return nil, ErrPublicWriterClosed
	}

	writers := p.idle[cfg.topic]
	for i, w := range slices.Backward(writers) {
		select {
		case <-w.background.Done():
			writers = append(writers[:i], writers[i+1:]...)

			continue
		default:
		}
		if !w.cfg.PoolCompatible(cfg) {
			continue
		}
		p.idle[cfg.topic] = append(writers[:i], writers[i+1:]...)

		return w, nil
	}
	if p.idle != nil {
		p.idle[cfg.topic] = writers
	}

	return p.create(cfg)
}

func (p *ReconnectorPool) Put(w *WriterReconnector, reusable bool) {
	reusable = reusable && w.cfg.CanPool()
	if reusable {
		select {
		case <-w.background.Done():
			reusable = false
		default:
		}
	}

	p.mu.Lock()
	if reusable && !p.closed {
		if p.idle == nil {
			p.idle = make(map[string][]*WriterReconnector)
		}
		p.idle[w.cfg.topic] = append(p.idle[w.cfg.topic], w)
		p.mu.Unlock()

		return
	}
	p.mu.Unlock()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_ = w.Close(ctx)
}

func (p *ReconnectorPool) Close(ctx context.Context) error {
	p.mu.Lock()
	p.closed = true
	idle := p.idle
	p.idle = nil
	p.mu.Unlock()

	for _, writers := range idle {
		for _, w := range writers {
			_ = w.Close(ctx)
		}
	}

	return nil
}
