package topicreadercommon

import "sync"

// pendingCommitTrackers owns only the trackers that still have work on a
// stream. Partition retirement alone cannot drop them: final ACKs may arrive.
type pendingCommitTrackers struct {
	mu     sync.Mutex
	live   map[*commitMessageTracker]struct{}
	closed bool
}

func (p *pendingCommitTrackers) add(t *commitMessageTracker) bool {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return false
	}
	if p.live == nil {
		p.live = make(map[*commitMessageTracker]struct{})
	}
	p.live[t] = struct{}{}

	return true
}

func (p *pendingCommitTrackers) remove(t *commitMessageTracker) {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.live, t)
}

func (p *pendingCommitTrackers) close() {
	// Queue and Acknowledge take the tracker mutex before the set mutex.
	// Detach the set before touching any individual tracker.
	for t := range p.detach() {
		t.discard()
	}
}

func (p *pendingCommitTrackers) detach() map[*commitMessageTracker]struct{} {
	p.mu.Lock()
	defer p.mu.Unlock()

	live := p.live
	p.closed = true
	p.live = nil

	return live
}
