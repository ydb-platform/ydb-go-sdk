package topicwriterinternal

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/tx"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func TestReconnectorPool_ReusesOnlyCompletedTransactions(t *testing.T) {
	pool := newStoppedReconnectorPool()
	cfg := NewWriterReconnectorConfig(WithTopic("topic"))

	first, err := pool.Get(cfg)
	require.NoError(t, err)
	other, err := pool.Get(cfg)
	require.NoError(t, err)
	require.NotSame(t, first, other, "simultaneous transactions need exclusive reconnectors")

	transaction := &poolTestTransaction{}
	writer := NewPooledTopicWriterTransaction(first, transaction, nil, pool)
	require.NoError(t, transaction.beforeCommit(context.Background()))
	require.ErrorIs(t, writer.Write(context.Background(), nil), ErrPublicWriterClosed)
	require.NotSame(t, first, mustGetReconnector(t, pool, cfg), "commit has not completed")
	transaction.complete(nil)

	reused := mustGetReconnector(t, pool, cfg)
	require.Same(t, first, reused)
	require.False(t, reused.queue.closed)
}

func TestReconnectorPool_DiscardsRolledBackTransaction(t *testing.T) {
	pool := newStoppedReconnectorPool()
	cfg := NewWriterReconnectorConfig(WithTopic("topic"))
	first := mustGetReconnector(t, pool, cfg)
	transaction := &poolTestTransaction{}
	NewPooledTopicWriterTransaction(first, transaction, nil, pool)
	transaction.complete(errors.New("rollback"))

	require.NotSame(t, first, mustGetReconnector(t, pool, cfg))
}

func TestReconnectorPool_DiscardsFailedCommit(t *testing.T) {
	pool := newStoppedReconnectorPool()
	cfg := NewWriterReconnectorConfig(WithTopic("topic"))
	first := mustGetReconnector(t, pool, cfg)
	transaction := &poolTestTransaction{}
	NewPooledTopicWriterTransaction(first, transaction, nil, pool)
	require.NoError(t, transaction.beforeCommit(context.Background()))
	transaction.complete(errors.New("commit failed"))

	require.NotSame(t, first, mustGetReconnector(t, pool, cfg))
}

func TestReconnectorPool_ClosesIdleReconnectorWithClient(t *testing.T) {
	pool := newStoppedReconnectorPool()
	cfg := NewWriterReconnectorConfig(WithTopic("topic"))
	w := mustGetReconnector(t, pool, cfg)
	pool.Put(w, true)
	require.NoError(t, pool.Close(context.Background()))
	require.True(t, w.queue.closed)
	_, err := pool.Get(cfg)
	require.Error(t, err)
}

func TestReconnectorPool_WaitInitInfoReflectsPreviousCommittedWrites(t *testing.T) {
	pool := newStoppedReconnectorPool()
	cfg := NewWriterReconnectorConfig(WithTopic("topic"))
	w := mustGetReconnector(t, pool, cfg)
	w.initDone = true
	w.initInfo = InitialInfo{LastSeqNum: 2}
	close(w.initDoneCh)
	w.queue.lastSeqNo = 5
	pool.Put(w, true)

	reused := mustGetReconnector(t, pool, cfg)
	transaction := &poolTestTransaction{}
	writer := NewPooledTopicWriterTransaction(reused, transaction, nil, pool)
	info, err := writer.WaitInitInfo(context.Background())
	require.NoError(t, err)
	require.Equal(t, int64(5), info.LastSeqNum)
}

func TestReconnectorPool_DoesNotReuseDifferentProducer(t *testing.T) {
	pool := newStoppedReconnectorPool()
	firstCfg := NewWriterReconnectorConfig(WithTopic("topic"), WithProducerID("first"))
	secondCfg := NewWriterReconnectorConfig(WithTopic("topic"), WithProducerID("second"))
	first := mustGetReconnector(t, pool, firstCfg)
	pool.Put(first, true)

	second := mustGetReconnector(t, pool, secondCfg)
	require.NotSame(t, first, second)
	require.Equal(t, "second", second.cfg.ProducerID())
}

func TestReconnectorPool_ReusesMatchingWriterOptions(t *testing.T) {
	pool := newStoppedReconnectorPool()
	firstCfg := NewWriterReconnectorConfig(WithTopic("topic"), WithProducerID("producer"), WithDirectWrite(false))
	secondCfg := NewWriterReconnectorConfig(WithTopic("topic"), WithProducerID("producer"), WithDirectWrite(false))
	first := mustGetReconnector(t, pool, firstCfg)
	pool.Put(first, true)

	require.Same(t, first, mustGetReconnector(t, pool, secondCfg))
}

func TestReconnectorPool_DoesNotReuseDifferentWriterOptions(t *testing.T) {
	pool := newStoppedReconnectorPool()
	firstCfg := NewWriterReconnectorConfig(WithTopic("topic"), WithProducerID("producer"))
	secondCfg := NewWriterReconnectorConfig(WithTopic("topic"), WithProducerID("producer"), WithMaxQueueLen(2))
	first := mustGetReconnector(t, pool, firstCfg)
	pool.Put(first, true)

	require.NotSame(t, first, mustGetReconnector(t, pool, secondCfg))
	require.Same(t, first, mustGetReconnector(t, pool, firstCfg), "incompatible idle writer remains in the pool")
}

func TestReconnectorPool_DoesNotPoolWriterSpecificTrace(t *testing.T) {
	baseline := WithTrace(&trace.Topic{})
	tracedCfg := NewWriterReconnectorConfig(WithTopic("topic"), baseline, WithPoolBaseline(), WithTrace(&trace.Topic{}))
	require.False(t, tracedCfg.CanPool())

	plainCfg := NewWriterReconnectorConfig(WithTopic("topic"), baseline, WithPoolBaseline())
	require.True(t, plainCfg.CanPool())
}

func TestReconnectorPool_DoesNotPoolWriterSpecificCallback(t *testing.T) {
	cfg := NewWriterReconnectorConfig(WithTopic("topic"), func(cfg *WriterReconnectorConfig) {
		cfg.OnAckReceivedCallback = func(int64) {}
	})
	require.False(t, cfg.CanPool())
}

func TestReconnectorPool_ReusesMultiwriterReconnectorAcrossLeases(t *testing.T) {
	pool := newStoppedReconnectorPool()
	cfg := NewWriterReconnectorConfig(WithTopic("topic"), WithProducerID("producer"))
	cfg.MultiMode = true
	cfg.MultiWriterConfig = &struct{}{}
	cfg.OnAckReceivedCallback = func(int64) {}
	first, err := pool.GetMulti(cfg)
	require.NoError(t, err)
	require.NoError(t, first.Close(context.Background()))
	first.Release(true)

	second, err := pool.GetMulti(cfg)
	require.NoError(t, err)
	require.Same(t, first.entry.writer, second.entry.writer)
}

func TestReconnectorPool_MultiwriterCallbacksFollowCurrentLease(t *testing.T) {
	pool := newStoppedReconnectorPool()
	cfg := NewWriterReconnectorConfig(WithTopic("topic"), WithProducerID("producer"))
	cfg.MultiMode = true
	cfg.MultiWriterConfig = &struct{}{}
	firstAcks := 0
	cfg.OnAckReceivedCallback = func(int64) { firstAcks++ }
	cfg.RetrySettings.CheckError = func(topic.PublicCheckErrorRetryArgs) topic.PublicCheckRetryResult {
		return topic.PublicRetryDecisionStop
	}
	first, err := pool.GetMulti(cfg)
	require.NoError(t, err)
	first.entry.callbacks.ack(1)
	require.Equal(t, topic.PublicRetryDecisionStop,
		first.entry.callbacks.retry(topic.NewCheckRetryArgs(errors.New("retry"))))
	require.NoError(t, first.Close(context.Background()))
	first.Release(true)

	secondAcks := 0
	cfg.OnAckReceivedCallback = func(int64) { secondAcks++ }
	cfg.RetrySettings.CheckError = func(topic.PublicCheckErrorRetryArgs) topic.PublicCheckRetryResult {
		return topic.PublicRetryDecisionRetry
	}
	second, err := pool.GetMulti(cfg)
	require.NoError(t, err)
	second.entry.callbacks.ack(2)
	require.Equal(t, 1, firstAcks)
	require.Equal(t, 1, secondAcks)
	require.Equal(t, topic.PublicRetryDecisionRetry,
		second.entry.callbacks.retry(topic.NewCheckRetryArgs(errors.New("retry"))))
}

func TestReconnectorPool_MultiwriterFailedTransactionDiscardsLease(t *testing.T) {
	pool := newStoppedReconnectorPool()
	cfg := NewWriterReconnectorConfig(WithTopic("topic"), WithProducerID("producer"))
	cfg.MultiMode = true
	cfg.MultiWriterConfig = &struct{}{}
	first, err := pool.GetMulti(cfg)
	require.NoError(t, err)
	require.NoError(t, first.Close(context.Background()))
	first.Release(false)

	second, err := pool.GetMulti(cfg)
	require.NoError(t, err)
	require.NotSame(t, first.entry.writer, second.entry.writer)
}

func TestReconnectorPool_MultiwriterDifferentProducerKeepsSeparateSession(t *testing.T) {
	pool := newStoppedReconnectorPool()
	cfg := NewWriterReconnectorConfig(WithTopic("topic"), WithProducerID("first"))
	cfg.MultiMode = true
	cfg.MultiWriterConfig = &struct{}{}
	first, err := pool.GetMulti(cfg)
	require.NoError(t, err)
	require.NoError(t, first.Close(context.Background()))
	first.Release(true)

	WithProducerID("second")(&cfg)
	second, err := pool.GetMulti(cfg)
	require.NoError(t, err)
	require.NotSame(t, first.entry.writer, second.entry.writer)
}

func TestReconnectorPool_ClientCloseClosesIdleMultiwriterReconnector(t *testing.T) {
	pool := newStoppedReconnectorPool()
	cfg := NewWriterReconnectorConfig(WithTopic("topic"), WithProducerID("producer"))
	cfg.MultiMode = true
	cfg.MultiWriterConfig = &struct{}{}
	lease, err := pool.GetMulti(cfg)
	require.NoError(t, err)
	require.NoError(t, lease.Close(context.Background()))
	lease.Release(true)

	require.NoError(t, pool.Close(context.Background()))
	require.True(t, lease.entry.writer.queue.closed)
}

func newStoppedReconnectorPool() *ReconnectorPool {
	return &ReconnectorPool{create: func(cfg WriterReconnectorConfig) (*WriterReconnector, error) {
		return newWriterReconnectorStopped(cfg), nil
	}}
}

func mustGetReconnector(t *testing.T, pool *ReconnectorPool, cfg WriterReconnectorConfig) *WriterReconnector {
	t.Helper()
	w, err := pool.Get(cfg)
	require.NoError(t, err)

	return w
}

type poolTestTransaction struct {
	tx.LazyID

	before []tx.OnTransactionBeforeCommit
	done   []tx.OnTransactionCompletedFunc
}

func (t *poolTestTransaction) UnLazy(context.Context) error { return nil }
func (t *poolTestTransaction) SessionID() string            { return "session" }
func (t *poolTestTransaction) NodeID() uint32               { return 1 }

func (t *poolTestTransaction) Rollback(context.Context) error {
	t.complete(errors.New("rollback"))

	return nil
}

func (t *poolTestTransaction) OnBeforeCommit(f tx.OnTransactionBeforeCommit) {
	t.before = append(t.before, f)
}

func (t *poolTestTransaction) OnCompleted(f tx.OnTransactionCompletedFunc) {
	t.done = append(t.done, f)
}

func (t *poolTestTransaction) beforeCommit(ctx context.Context) error {
	for _, f := range t.before {
		if err := f(ctx); err != nil {
			return err
		}
	}

	return nil
}

func (t *poolTestTransaction) complete(err error) {
	for _, f := range t.done {
		f(err)
	}
}
