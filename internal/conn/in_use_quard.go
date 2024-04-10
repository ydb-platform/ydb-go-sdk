package conn

import "sync"

type inUseGuard struct {
	usages  sync.WaitGroup
	mu      sync.Mutex
	stopped bool
}

func (g *inUseGuard) TryLock() (locked bool, unlock func()) {
	g.mu.Lock()
	defer g.mu.Unlock()

	if g.stopped {
		return false, nil
	}

	g.usages.Add(1)

	return true, sync.OnceFunc(func() {
		g.usages.Done()
	})
}

func (g *inUseGuard) Stop() {
	g.mu.Lock()
	defer g.mu.Unlock()

	g.stopped = true

	g.usages.Wait()

	g.stopped = true
}
