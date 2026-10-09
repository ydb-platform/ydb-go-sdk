package topicwriterinternal

import (
	"context"
	"fmt"
	"sync"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/gtrace"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/tx"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

type WriterWithTransaction struct {
	streamWriter *WriterReconnector
	tx           tx.Transaction
	tracer       *trace.Topic
	pool         *ReconnectorPool
	pooledMu     sync.RWMutex
	closed       bool
	closeErr     error
	initialInfo  InitialInfo
	hasSnapshot  bool
}

func NewPooledTopicWriterTransaction(
	w *WriterReconnector,
	tx tx.Transaction,
	tracer *trace.Topic,
	pool *ReconnectorPool,
) *WriterWithTransaction {
	res := NewTopicWriterTransaction(w, tx, tracer)
	res.pool = pool
	w.m.RLock()
	res.hasSnapshot = w.initDone
	res.initialInfo = w.initInfo
	w.m.RUnlock()
	if res.hasSnapshot {
		w.queue.m.RLock()
		if w.queue.lastSeqNo > res.initialInfo.LastSeqNum {
			res.initialInfo.LastSeqNum = w.queue.lastSeqNo
		}
		w.queue.m.RUnlock()
	}

	return res
}

func NewTopicWriterTransaction(w *WriterReconnector, tx tx.Transaction, tracer *trace.Topic) *WriterWithTransaction {
	res := &WriterWithTransaction{
		streamWriter: w,
		tx:           tx,
	}
	if tracer == nil {
		res.tracer = &trace.Topic{}
	} else {
		res.tracer = tracer
	}

	tx.OnBeforeCommit(res.onBeforeCommitTransaction)
	tx.OnCompleted(res.onTransactionCompleted)

	return res
}

func (w *WriterWithTransaction) onBeforeCommitTransaction(ctx context.Context) (err error) {
	traceCtx := ctx
	onDone := gtrace.TopicOnWriterBeforeCommitTransaction(
		w.tracer,
		&traceCtx,
		w.tx.SessionID(),
		w.streamWriter.GetSessionID(),
		w.tx.ID(),
	)

	defer func() {
		onDone(err, w.streamWriter.GetSessionID())
		ctx = traceCtx
	}()

	// wait message flushing
	return w.Close(ctx)
}

func (w *WriterWithTransaction) WaitInit(ctx context.Context) error {
	return w.streamWriter.WaitInit(ctx)
}

func (w *WriterWithTransaction) WaitInitInfo(ctx context.Context) (InitialInfo, error) {
	info, err := w.streamWriter.WaitInitInfo(ctx)
	if err != nil {
		return InitialInfo{}, err
	}
	if w.pool != nil && w.hasSnapshot {
		return w.initialInfo, nil
	}

	return info, nil
}

func (w *WriterWithTransaction) Write(ctx context.Context, messages []PublicMessage) error {
	if w.pool != nil {
		w.pooledMu.RLock()
		defer w.pooledMu.RUnlock()
		if w.closed {
			return ErrPublicWriterClosed
		}
	}

	if err := w.tx.UnLazy(ctx); err != nil {
		return fmt.Errorf("ydb: failed to materialize transaction: %w", err)
	}

	for i := range messages {
		messages[i].Tx = w.tx
	}

	return w.streamWriter.Write(ctx, messages)
}

func (w *WriterWithTransaction) Close(ctx context.Context) error {
	if w.pool != nil {
		w.pooledMu.Lock()
		defer w.pooledMu.Unlock()
		if !w.closed {
			w.closed = true
			w.closeErr = w.streamWriter.Flush(ctx)
		}

		return w.closeErr
	}

	return w.streamWriter.Close(ctx)
}

func (w *WriterWithTransaction) onTransactionCompleted(err error) {
	// after transaction finished by any reason - the writer closed without flush
	noNeedFlushCtx, cancel := context.WithCancel(context.Background())
	cancel()

	if w.pool != nil {
		_ = w.Close(noNeedFlushCtx)
		w.pool.Put(w.streamWriter, err == nil && w.closeErr == nil)

		return
	}

	_ = w.Close(noNeedFlushCtx)
}
