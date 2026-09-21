package topicmultiwriter

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/background"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/partition"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicmultiwriter/stubs"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicwritercommon"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicwriterinternal"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xsync"
	"github.com/ydb-platform/ydb-go-sdk/v3/pkg/xtest"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topictypes"
)

func TestPartitionWriterPool_ReadSeqNo(t *testing.T) {
	t.Parallel()

	overloaded := xerrors.Operation(xerrors.WithStatusCode(Ydb.StatusIds_OVERLOADED))
	for _, tc := range []struct {
		name        string
		active      bool
		directWrite bool
		createError bool
		initErr     error
		cancel      bool
		wantErr     error
	}{
		{name: "active", active: true},
		{name: "active direct write", active: true, directWrite: true},
		{name: "inactive"},
		{name: "create error", active: true, createError: true, wantErr: errCreate},
		{name: "init error", active: true, initErr: overloaded, wantErr: overloaded},
		{name: "canceled", active: true, cancel: true, wantErr: context.Canceled},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			factory := &poolMockFactory{
				initInfo:    topicwriterinternal.InitialInfo{LastSeqNum: 42},
				initErr:     tc.initErr,
				returnError: tc.createError,
			}
			pool, cancel := newPoolForTest(t, factory)
			defer cancel()
			pool.cfg.DirectWrite = tc.directWrite
			ctx, cancelRead := context.WithCancel(xtest.Context(t))
			defer cancelRead()
			if tc.cancel {
				cancelRead()
			}

			seqNo, err := pool.readSeqNo(ctx, 7, tc.active)
			require.ErrorIs(t, err, tc.wantErr)
			if tc.wantErr == nil {
				require.Equal(t, int64(42), seqNo)
			} else {
				require.Zero(t, seqNo)
			}
			require.Zero(t, pool.getWritersCount(), "temporary sessions must not enter the working pool")
			if !tc.createError {
				require.Len(t, factory.writers, 1)
				require.True(t, factory.writers[0].closed.Load(), "temporary sessions must close on every return path")
			}
			require.Equal(t, []string{"test-prefix-7"}, factory.producerIDs)
			partitionID, pinned := factory.lastCfg.PartitionID()
			require.Equal(t, tc.active, pinned)
			if tc.active {
				require.Equal(t, int64(7), partitionID)
			}
			// SeqNo session errors return to the caller without consulting Source.
			require.Equal(t, topic.PublicRetryDecisionStop,
				factory.lastCfg.RetrySettings.CheckError(topic.PublicCheckErrorRetryArgs{Error: overloaded}))
			require.Nil(t, pool.writerCfg.RetrySettings.CheckError, "the working writer config must not be modified")
		})
	}
}

func TestPartitionWriterPool_ForceEvictClosesWorkingSession(t *testing.T) {
	t.Parallel()

	for _, idle := range []bool{false, true} {
		t.Run(map[bool]string{false: "busy", true: "idle"}[idle], func(t *testing.T) {
			t.Parallel()

			factory := &poolMockFactory{}
			pool, cancel := newPoolForTest(t, factory)
			defer cancel()
			_, err := pool.get(7)
			require.NoError(t, err)
			if idle {
				pool.evict(7)
			}

			pool.forceEvict(7)
			require.True(t, factory.writers[0].closed.Load())
			require.Zero(t, pool.getWritersCount())
		})
	}
}

var errCreate = errors.New("create error")

// poolTestWriter is a writer implementation that tracks Close calls for tests.
type poolTestWriter struct {
	initInfo    topicwriterinternal.InitialInfo
	initErr     error
	closed      atomic.Bool
	writeCalled atomic.Int64
}

func (w *poolTestWriter) Close(_ context.Context) error {
	w.closed.Store(true)

	return nil
}

func (w *poolTestWriter) WaitInitInfo(ctx context.Context) (topicwriterinternal.InitialInfo, error) {
	if err := ctx.Err(); err != nil {
		return topicwriterinternal.InitialInfo{}, err
	}

	return w.initInfo, w.initErr
}

func (w *poolTestWriter) WriteInternal(_ context.Context, _ []topicwritercommon.MessageWithDataContent) error {
	w.writeCalled.Add(1)

	return nil
}

// poolMockFactory records Create calls and returns configurable writers or error.
type poolMockFactory struct {
	initInfo     topicwriterinternal.InitialInfo
	initErr      error
	createCalls  int
	partitionIDs []int64
	producerIDs  []string
	returnError  bool
	writers      []*poolTestWriter
	lastCfg      topicwriterinternal.WriterReconnectorConfig
}

func (f *poolMockFactory) Create(cfg topicwriterinternal.WriterReconnectorConfig) (writer, error) {
	f.createCalls++
	f.lastCfg = cfg
	if partID, ok := cfg.PartitionID(); ok {
		f.partitionIDs = append(f.partitionIDs, partID)
	}
	f.producerIDs = append(f.producerIDs, cfg.ProducerID())

	if f.returnError {
		return nil, errCreate
	}

	w := &poolTestWriter{initInfo: f.initInfo, initErr: f.initErr}
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
		nil, // These pool lifecycle tests do not deliver session errors to Source.
		func() {},
		func(err error) {},
	)

	return pool, cancel
}

func TestSenderStepReturnsNonOverloadedWriterInitError(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(xtest.Context(t))
	defer cancel()

	initErr := errors.New("writer init failed")
	testWriter := &poolTestWriter{}
	wrapper := &writerWrapper{writer: testWriter}
	wrapper.setInitErr(initErr)
	wrapper.initDone.Store(true)

	mu := &xsync.Mutex{}
	writerCfg := &topicwriterinternal.WriterReconnectorConfig{}
	topicwriterinternal.WithMaxQueueLen(10)(writerCfg)
	buf := newInflightBuffer(ctx, mu, writerCfg, func() error { return nil })
	partitions := map[int64]*PartitionInfo{1: {}}

	mu.WithLock(func() {
		buf.pushNeedLock(message{
			MessageWithDataContent: topicwritercommon.MessageWithDataContent{
				PublicMessage: topicwritercommon.PublicMessage{
					SeqNo:       1,
					PartitionID: 1,
				},
			},
		})
	})

	s := newSender(
		ctx,
		partitions,
		mu,
		buf,
		&partitionWriterPool{writers: map[int64]*writerWrapper{1: wrapper}},
		newPartitionSourceForTest(t),
		func(err error) {},
	)

	err := s.step()
	require.ErrorIs(t, err, initErr)
	require.Equal(t, int64(0), testWriter.writeCalled.Load())
}

func TestPartitionWriterPool_CreateDirectWriterSeedsDirectWritePartition(t *testing.T) {
	t.Parallel()

	factory := &poolMockFactory{}
	pool, cancel := newPoolForTest(t, factory)
	defer cancel()

	pool.cfg.DirectWrite = true

	_, err := pool.get(7)
	require.NoError(t, err)
	require.Equal(t, []int64{7}, factory.partitionIDs)
}

func TestPartitionWriterPool_GetCreatesWriterAndReturnsSameOnSecondGet(t *testing.T) {
	t.Parallel()

	factory := &poolMockFactory{}
	pool, cancel := newPoolForTest(t, factory)
	defer cancel()

	w1, err := pool.get(1)
	require.NoError(t, err)
	require.NotNil(t, w1)
	require.Equal(t, 1, factory.createCalls)
	require.Equal(t, []int64{1}, factory.partitionIDs)
	require.Equal(t, []string{"test-prefix-1"}, factory.producerIDs)

	w2, err := pool.get(1)
	require.NoError(t, err)
	require.Same(t, w1, w2)
	require.Equal(t, 1, factory.createCalls)
}

func TestPartitionWriterPool_GetReturnsFromIdleAfterEvict(t *testing.T) {
	t.Parallel()

	factory := &poolMockFactory{}
	pool, cancel := newPoolForTest(t, factory)
	defer cancel()

	w1, err := pool.get(1)
	require.NoError(t, err)
	require.NotNil(t, w1)
	require.Equal(t, 1, factory.createCalls)

	pool.evict(1)

	w2, err := pool.get(1)
	require.NoError(t, err)
	require.Same(t, w1, w2)
	require.Equal(t, 1, factory.createCalls)
}

func TestPartitionWriterPool_CloseAllClosesAllWriters(t *testing.T) {
	t.Parallel()

	factory := &poolMockFactory{}
	pool, cancel := newPoolForTest(t, factory)
	defer cancel()

	_, err := pool.get(1)
	require.NoError(t, err)
	_, err = pool.get(2)
	require.NoError(t, err)
	require.Len(t, factory.writers, 2)

	err = pool.close(xtest.Context(t))
	require.NoError(t, err)

	require.True(t, factory.writers[0].closed.Load())
	require.True(t, factory.writers[1].closed.Load())
}

func TestPartitionWriterPool_CloseAllClosesIdleWriters(t *testing.T) {
	t.Parallel()

	factory := &poolMockFactory{}
	pool, cancel := newPoolForTest(t, factory)
	defer cancel()

	_, err := pool.get(1)
	require.NoError(t, err)
	require.Len(t, factory.writers, 1)

	pool.evict(1)
	require.Equal(t, 1, pool.idle.getWritersCount())

	err = pool.close(xtest.Context(t))
	require.NoError(t, err)
	require.True(t, factory.writers[0].closed.Load())
	require.Equal(t, 0, pool.idle.getWritersCount())
}

func TestPartitionWriterPool_GetProducerIDFormat(t *testing.T) {
	t.Parallel()

	factory := &poolMockFactory{}
	pool, cancel := newPoolForTest(t, factory)
	defer cancel()

	_, err := pool.get(5)
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

	w, err := pool.get(1)
	require.ErrorIs(t, err, errCreate)
	require.Nil(t, w)
	require.Equal(t, 1, factory.createCalls)
}

func newPartitionSourceForTest(t *testing.T) *partition.Source {
	t.Helper()

	source := partition.NewSources(func(ctx context.Context, path string) (topictypes.TopicDescription, error) {
		return stubs.DefaultStubTopicDescription(t), nil
	}).Get("test/topic")
	_, err := source.NewRouter(xtest.Context(t), nil)
	require.NoError(t, err)

	return source
}
