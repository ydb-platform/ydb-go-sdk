package topicmultiwriter

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/background"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicwritercommon"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicwriterinternal"
	"github.com/ydb-platform/ydb-go-sdk/v3/pkg/xtest"
)

var errCreate = errors.New("create error")

// poolTestWriter is a writer implementation that tracks Close calls for tests.
type poolTestWriter struct {
	closed atomic.Bool
}

func (w *poolTestWriter) Close(_ context.Context) error {
	w.closed.Store(true)

	return nil
}

func (w *poolTestWriter) WaitInitInfo(_ context.Context) (topicwriterinternal.InitialInfo, error) {
	return topicwriterinternal.InitialInfo{}, nil
}

func (w *poolTestWriter) WriteInternal(_ context.Context, _ []topicwritercommon.MessageWithDataContent) error {
	return nil
}

func (w *poolTestWriter) GetBufferedMessages() []topicwritercommon.MessageWithDataContent {
	return nil
}

// poolMockFactory records Create calls and returns configurable writers or error.
type poolMockFactory struct {
	createCalls  int
	partitionIDs []int64
	producerIDs  []string
	returnError  bool
	writers      []*poolTestWriter
}

func (f *poolMockFactory) Create(cfg topicwriterinternal.WriterReconnectorConfig) (writer, error) {
	f.createCalls++
	if partID, ok := cfg.PartitionID(); ok {
		f.partitionIDs = append(f.partitionIDs, partID)
	}
	f.producerIDs = append(f.producerIDs, cfg.ProducerID())

	if f.returnError {
		return nil, errCreate
	}

	w := &poolTestWriter{}
	f.writers = append(f.writers, w)

	return w, nil
}

func newPoolForTest(t *testing.T, factory *poolMockFactory) (*partitionWriterPool, context.CancelFunc) {
	t.Helper()

	ctx, cancel := context.WithCancel(xtest.Context(t))
	bg := background.NewWorker(ctx, "pool-test")

	writerCfg := &topicwriterinternal.WriterReconnectorConfig{}
	cfg := &MultiWriterConfig{
		ProducerIDPrefix:  "test-prefix",
		WriterIdleTimeout: defaultWriterIdleTimeout,
	}
	cfg.writersFactory = factory

	pool := newPartitionWriterPool(
		ctx,
		cfg,
		writerCfg,
		bg,
		func(partitionID, seqNo int64) {},
		func(partitionID int64) {},
		func() {},
		func(err error) {},
	)

	return pool, cancel
}

func TestPartitionWriterPool_GetCreatesWriterAndReturnsSameOnSecondGet(t *testing.T) {
	t.Parallel()

	factory := &poolMockFactory{}
	pool, cancel := newPoolForTest(t, factory)
	defer cancel()

	w1, err := pool.get(1, true, false)
	require.NoError(t, err)
	require.NotNil(t, w1)
	require.Equal(t, 1, factory.createCalls)
	require.Equal(t, []int64{1}, factory.partitionIDs)
	require.Equal(t, []string{"test-prefix-1"}, factory.producerIDs)

	w2, err := pool.get(1, true, false)
	require.NoError(t, err)
	require.Same(t, w1, w2)
	require.Equal(t, 1, factory.createCalls)
}

func TestPartitionWriterPool_GetReturnsFromIdleAfterEvict(t *testing.T) {
	t.Parallel()

	factory := &poolMockFactory{}
	pool, cancel := newPoolForTest(t, factory)
	defer cancel()

	w1, err := pool.get(1, true, false)
	require.NoError(t, err)
	require.NotNil(t, w1)
	require.Equal(t, 1, factory.createCalls)

	pool.evict(1)

	w2, err := pool.get(1, true, false)
	require.NoError(t, err)
	require.Same(t, w1, w2)
	require.Equal(t, 1, factory.createCalls)
}

func TestPartitionWriterPool_CloseAllClosesAllWriters(t *testing.T) {
	t.Parallel()

	factory := &poolMockFactory{}
	pool, cancel := newPoolForTest(t, factory)
	defer cancel()

	_, err := pool.get(1, true, false)
	require.NoError(t, err)
	_, err = pool.get(2, true, false)
	require.NoError(t, err)
	require.Len(t, factory.writers, 2)

	err = pool.close(xtest.Context(t))
	require.NoError(t, err)

	require.True(t, factory.writers[0].closed.Load())
	require.True(t, factory.writers[1].closed.Load())
}

func TestPartitionWriterPool_GetProducerIDFormat(t *testing.T) {
	t.Parallel()

	factory := &poolMockFactory{}
	pool, cancel := newPoolForTest(t, factory)
	defer cancel()

	_, err := pool.get(5, true, false)
	require.NoError(t, err)
	require.Equal(t, []string{"test-prefix-5"}, factory.producerIDs)
	require.Equal(t, []int64{5}, factory.partitionIDs)

	cancel()
}

func TestPartitionWriterPool_GetReturnsErrorWhenCreateFails(t *testing.T) {
	t.Parallel()

	factory := &poolMockFactory{returnError: true}
	pool, cancel := newPoolForTest(t, factory)
	defer cancel()

	w, err := pool.get(1, true, false)
	require.ErrorIs(t, err, errCreate)
	require.Nil(t, w)
	require.Equal(t, 1, factory.createCalls)
}
