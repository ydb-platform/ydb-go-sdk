package topicreadercommon

import "sync"

// pendingCommitTrackers owns only the trackers that still have work on a
// stream. Partition retirement alone cannot drop them: final ACKs may arrive.
type pendingCommitTrackers struct {
	mu     sync.Mutex
	head   *commitMessageTracker
	closed bool
}

func (p *pendingCommitTrackers) add(t *commitMessageTracker) bool {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return false
	}
	if p.head != nil {
		p.head.prev = t
	}
	t.next = p.head
	t.linked = true
	p.head = t

	return true
}

func (p *pendingCommitTrackers) remove(t *commitMessageTracker) {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Once closed, only close can access the detached links.
	if p.closed || !t.linked {
		return
	}
	if t.prev == nil {
		p.head = t.next
	} else {
		t.prev.next = t.next
	}
	if t.next != nil {
		t.next.prev = t.prev
	}
	t.prev, t.next, t.linked = nil, nil, false
}

func (p *pendingCommitTrackers) close() {
	// Queue and Acknowledge take the tracker mutex before the set mutex.
	// Detach the set before touching any individual tracker.
	for t := p.detach(); t != nil; {
		next := t.next
		t.prev, t.next, t.linked = nil, nil, false
		t.discard()
		t = next
	}
}

func (p *pendingCommitTrackers) detach() *commitMessageTracker {
	p.mu.Lock()
	defer p.mu.Unlock()

	head := p.head
	p.closed = true
	p.head = nil

	return head
}
