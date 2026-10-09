package topicmultiwriter

import (
	"context"
	"sync"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicwriterinternal"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/tx"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

type pooledWritersFactory struct {
	pool       *topicwriterinternal.ReconnectorPool
	mu         sync.Mutex
	committing bool
	leases     []*pooledPartitionWriter
}

type pooledPartitionWriter struct {
	*topicwriterinternal.MultiReconnectorLease

	factory  *pooledWritersFactory
	mu       sync.Mutex
	reusable bool
}

func (f *pooledWritersFactory) Create(cfg topicwriterinternal.WriterReconnectorConfig) (writer, error) {
	if !cfg.CanPoolMulti() {
		return newBaseWritersFactory().Create(cfg)
	}
	lease, err := f.pool.GetMulti(cfg)
	if err != nil {
		return nil, err
	}
	w := &pooledPartitionWriter{MultiReconnectorLease: lease, factory: f}
	f.mu.Lock()
	f.leases = append(f.leases, w)
	f.mu.Unlock()

	return w, nil
}

func (w *pooledPartitionWriter) Close(ctx context.Context) error {
	err := w.MultiReconnectorLease.Close(ctx)
	w.factory.mu.Lock()
	committing := w.factory.committing
	w.factory.mu.Unlock()
	w.mu.Lock()
	if err == nil && committing {
		w.reusable = true
	}
	w.mu.Unlock()

	return err
}

func (f *pooledWritersFactory) beginCommit() {
	f.mu.Lock()
	f.committing = true
	f.mu.Unlock()
}

func (f *pooledWritersFactory) completed(err error) {
	f.mu.Lock()
	leases := f.leases
	f.leases = nil
	f.mu.Unlock()
	for _, lease := range leases {
		lease.mu.Lock()
		reusable := lease.reusable
		lease.mu.Unlock()
		lease.Release(err == nil && reusable)
	}
}

// NewPooledTransactionalMultiWriter leases per-partition reconnectors from a
// Topic client pool while keeping the multiwriter itself transaction-scoped.
func NewPooledTransactionalMultiWriter(
	topicDescriber TopicDescriber,
	writerCfg *topicwriterinternal.WriterReconnectorConfig,
	multiWriterCfg *MultiWriterConfig,
	transaction tx.Transaction,
	tracer *trace.Topic,
	pool *topicwriterinternal.ReconnectorPool,
) (*MultiWriterWithTransaction, error) {
	factory := &pooledWritersFactory{pool: pool}
	multiWriterCfg.writersFactory = factory
	multiWriter, err := NewMultiWriter(topicDescriber, writerCfg, multiWriterCfg)
	if err != nil {
		return nil, err
	}
	wrapped := NewTopicMultiWriterTransaction(multiWriter, transaction, tracer)
	wrapped.pooledFactory = factory

	return wrapped, nil
}
