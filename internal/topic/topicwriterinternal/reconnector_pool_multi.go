package topicwriterinternal

import (
	"context"
	"slices"
	"sync"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicwritercommon"
)

// multiReconnectorCallbacks routes events to the multiwriter holding the lease.
// The reconnector keeps these functions across transactions; the destinations change.
type multiReconnectorCallbacks struct {
	mu         sync.RWMutex
	onAck      func(int64)
	checkError topic.PublicCheckErrorRetryFunction
}

func (c *multiReconnectorCallbacks) bind(
	onAck func(int64),
	checkError topic.PublicCheckErrorRetryFunction,
) {
	c.mu.Lock()
	c.onAck = onAck
	c.checkError = checkError
	c.mu.Unlock()
}

func (c *multiReconnectorCallbacks) ack(seqNo int64) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.onAck != nil {
		c.onAck(seqNo)
	}
}

func (c *multiReconnectorCallbacks) retry(args topic.PublicCheckErrorRetryArgs) topic.PublicCheckRetryResult {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.checkError != nil {
		return c.checkError(args)
	}

	return topic.PublicRetryDecisionDefault
}

type multiReconnectorEntry struct {
	writer    *WriterReconnector
	cfg       WriterReconnectorConfig
	callbacks *multiReconnectorCallbacks
}

type multiPoolKey struct {
	topic      string
	producerID string
}

// MultiReconnectorLease is a transaction's exclusive use of a client-owned
// per-partition reconnector. Close flushes without closing the stream.
type MultiReconnectorLease struct {
	entry      *multiReconnectorEntry
	pool       *ReconnectorPool
	mu         sync.RWMutex
	closed     bool
	closeErr   error
	initial    InitialInfo
	hasInitial bool
}

func (p *ReconnectorPool) GetMulti(cfg WriterReconnectorConfig) (*MultiReconnectorLease, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return nil, ErrPublicWriterClosed
	}

	key := multiPoolKey{topic: cfg.topic, producerID: cfg.producerID}
	entries := p.multiIdle[key]
	for i, entry := range slices.Backward(entries) {
		select {
		case <-entry.writer.background.Done():
			entries = append(entries[:i], entries[i+1:]...)

			continue
		default:
		}
		if !entry.cfg.poolCompatibleMulti(cfg) {
			continue
		}
		p.multiIdle[key] = append(entries[:i], entries[i+1:]...)
		entry.callbacks.bind(cfg.OnAckReceivedCallback, cfg.RetrySettings.CheckError)

		return newMultiReconnectorLease(p, entry), nil
	}
	if p.multiIdle != nil {
		p.multiIdle[key] = entries
	}

	callbacks := &multiReconnectorCallbacks{}
	callbacks.bind(cfg.OnAckReceivedCallback, cfg.RetrySettings.CheckError)
	wiringCfg := cfg
	wiringCfg.OnAckReceivedCallback = callbacks.ack
	wiringCfg.RetrySettings.CheckError = callbacks.retry
	writer, err := p.create(wiringCfg)
	if err != nil {
		return nil, err
	}

	return newMultiReconnectorLease(p, &multiReconnectorEntry{
		writer: writer, cfg: cfg, callbacks: callbacks,
	}), nil
}

func newMultiReconnectorLease(p *ReconnectorPool, entry *multiReconnectorEntry) *MultiReconnectorLease {
	lease := &MultiReconnectorLease{entry: entry, pool: p}
	w := entry.writer
	w.m.RLock()
	lease.hasInitial = w.initDone
	lease.initial = w.initInfo
	w.m.RUnlock()
	if lease.hasInitial {
		w.queue.m.RLock()
		lease.initial.LastSeqNum = max(lease.initial.LastSeqNum, w.queue.lastSeqNo)
		w.queue.m.RUnlock()
	}

	return lease
}

func (l *MultiReconnectorLease) WaitInitInfo(ctx context.Context) (InitialInfo, error) {
	info, err := l.entry.writer.WaitInitInfo(ctx)
	if err != nil {
		return InitialInfo{}, err
	}
	if l.hasInitial {
		return l.initial, nil
	}

	return info, nil
}

func (l *MultiReconnectorLease) WriteInternal(
	ctx context.Context,
	messages []topicwritercommon.MessageWithDataContent,
) error {
	l.mu.RLock()
	defer l.mu.RUnlock()
	if l.closed {
		return ErrPublicWriterClosed
	}

	return l.entry.writer.WriteInternal(ctx, messages)
}

func (l *MultiReconnectorLease) Close(ctx context.Context) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if !l.closed {
		l.closed = true
		l.closeErr = l.entry.writer.Flush(ctx)
	}

	return l.closeErr
}

func (l *MultiReconnectorLease) Release(reusable bool) {
	l.mu.Lock()
	reusable = reusable && l.closed && l.closeErr == nil
	l.closed = true
	l.mu.Unlock()
	l.pool.putMulti(l.entry, reusable)
}

func (p *ReconnectorPool) putMulti(entry *multiReconnectorEntry, reusable bool) {
	if reusable {
		select {
		case <-entry.writer.background.Done():
			reusable = false
		default:
		}
	}
	p.mu.Lock()
	entry.callbacks.bind(nil, nil)
	if reusable && !p.closed {
		if p.multiIdle == nil {
			p.multiIdle = make(map[multiPoolKey][]*multiReconnectorEntry)
		}
		key := multiPoolKey{topic: entry.cfg.topic, producerID: entry.cfg.producerID}
		p.multiIdle[key] = append(p.multiIdle[key], entry)
		p.mu.Unlock()

		return
	}
	p.mu.Unlock()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_ = entry.writer.Close(ctx)
}
