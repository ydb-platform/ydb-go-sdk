package topicreaderinternal

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/background"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/empty"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicreadercommon"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xcontext"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xtest"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

var _ batchedStreamReader = &readerReconnector{} // check interface implementation

func TestTopicReaderReconnectorReadMessageBatch(t *testing.T) {
	xtest.TestManyTimesWithName(t, "Ok", func(t testing.TB) {
		mc := gomock.NewController(t)
		defer mc.Finish()

		baseReader := NewMockbatchedStreamReader(mc)

		opts := ReadMessageBatchOptions{batcherGetOptions: batcherGetOptions{MaxCount: 10}}
		batch := &topicreadercommon.PublicBatch{
			Messages: []*topicreadercommon.PublicMessage{{WrittenAt: time.Date(2022, 0o6, 15, 17, 56, 0, 0, time.UTC)}},
		}
		baseReader.EXPECT().ReadMessageBatch(gomock.Any(), opts).Return(batch, nil)

		reader := &readerReconnector{
			streamVal:           baseReader,
			streamContextCancel: func(cause error) {},
			tracer:              &trace.Topic{},
		}
		reader.initChannelsAndClock()
		res, err := reader.ReadMessageBatch(context.Background(), opts)
		require.NoError(t, err)
		require.Equal(t, batch, res)
	})

	xtest.TestManyTimesWithName(t, "WithConnect", func(t testing.TB) {
		mc := gomock.NewController(t)

		baseReader := NewMockbatchedStreamReader(mc)
		opts := ReadMessageBatchOptions{batcherGetOptions: batcherGetOptions{MaxCount: 10}}
		batch := &topicreadercommon.PublicBatch{
			Messages: []*topicreadercommon.PublicMessage{{WrittenAt: time.Date(2022, 0o6, 15, 17, 56, 0, 0, time.UTC)}},
		}
		baseReader.EXPECT().ReadMessageBatch(gomock.Any(), opts).Return(batch, nil)

		connectCalled := 0
		reader := &readerReconnector{
			readerConnect: func(ctx context.Context) (batchedStreamReader, error) {
				connectCalled++
				if connectCalled > 1 {
					return nil, errors.New("unexpected call test connect function")
				}

				return baseReader, nil
			},
			retrySettings: topic.RetrySettings{
				StartTimeout: topic.DefaultStartTimeout,
			},
			streamErr: errUnconnected,
			tracer:    &trace.Topic{},
		}
		reader.initChannelsAndClock()
		reader.background.Start("test-reconnectionLoop", reader.reconnectionLoop)

		res, err := reader.ReadMessageBatch(context.Background(), opts)
		require.NoError(t, err)
		require.Equal(t, batch, res)
		mc.Finish()
	})

	xtest.TestManyTimesWithName(t, "WithReConnect", func(t testing.TB) {
		mc := gomock.NewController(t)
		defer mc.Finish()

		opts := ReadMessageBatchOptions{batcherGetOptions: batcherGetOptions{MaxCount: 10}}

		baseReader1 := NewMockbatchedStreamReader(mc)
		baseReader1.EXPECT().ReadMessageBatch(gomock.Any(), opts).MinTimes(1).
			Return(nil, xerrors.Retryable(errors.New("test1")))
		baseReader1.EXPECT().CloseWithError(gomock.Any(), gomock.Any()).Return(nil)

		baseReader2 := NewMockbatchedStreamReader(mc)
		baseReader2.EXPECT().ReadMessageBatch(gomock.Any(), opts).MinTimes(1).
			Return(nil, xerrors.Retryable(errors.New("test2")))
		baseReader2.EXPECT().CloseWithError(gomock.Any(), gomock.Any()).Return(nil)

		baseReader3 := NewMockbatchedStreamReader(mc)
		batch := &topicreadercommon.PublicBatch{
			Messages: []*topicreadercommon.PublicMessage{{WrittenAt: time.Date(2022, 0o6, 15, 17, 56, 0, 0, time.UTC)}},
		}
		baseReader3.EXPECT().ReadMessageBatch(gomock.Any(), opts).Return(batch, nil)

		readers := []batchedStreamReader{
			baseReader1, baseReader2, baseReader3,
		}
		connectCalled := 0
		reader := &readerReconnector{
			readerConnect: func(ctx context.Context) (batchedStreamReader, error) {
				connectCalled++

				return readers[connectCalled-1], nil
			},
			retrySettings: topic.RetrySettings{
				StartTimeout: topic.DefaultStartTimeout,
			},
			streamErr: errUnconnected,
			tracer:    &trace.Topic{},
		}
		reader.initChannelsAndClock()
		reader.background.Start("test-reconnectionLoop", reader.reconnectionLoop)

		res, err := reader.ReadMessageBatch(context.Background(), opts)
		require.NoError(t, err)
		require.Equal(t, batch, res)
	})

	xtest.TestManyTimesWithName(t, "StartWithCancelledContext", func(t testing.TB) {
		cancelledCtx, cancelledCtxCancel := xcontext.WithCancel(context.Background())
		cancelledCtxCancel()

		for i := 0; i < 100; i++ {
			reconnector := &readerReconnector{tracer: &trace.Topic{}}
			reconnector.initChannelsAndClock()

			_, err := reconnector.ReadMessageBatch(cancelledCtx, ReadMessageBatchOptions{})
			require.ErrorIs(t, err, context.Canceled)
		}
	})

	xtest.TestManyTimesWithName(t, "OnClose", func(t testing.TB) {
		reconnector := &readerReconnector{
			tracer:    &trace.Topic{},
			streamErr: errUnconnected,
		}
		reconnector.initChannelsAndClock()
		testErr := errors.New("test'")

		go func() {
			_ = reconnector.CloseWithError(context.Background(), testErr)
		}()

		_, err := reconnector.ReadMessageBatch(context.Background(), ReadMessageBatchOptions{})
		require.ErrorIs(t, err, testErr)
	})
}

func TestTopicReaderReconnectorCommit(t *testing.T) {
	type k struct{}
	ctx := context.WithValue(context.Background(), k{}, "v")
	expectedCommitRange := topicreadercommon.CommitRange{CommitOffsetStart: 1, CommitOffsetEnd: 2}
	testErr := errors.New("test")
	testErr2 := errors.New("test2")
	t.Run("AllOk", func(t *testing.T) {
		mc := gomock.NewController(t)
		defer mc.Finish()

		stream := NewMockbatchedStreamReader(mc)
		stream.EXPECT().Commit(
			gomock.Any(),
			gomock.Any(),
		).DoAndReturn(func(ctx context.Context, offset topicreadercommon.CommitRange) error {
			require.Equal(t, "v", ctx.Value(k{}))
			require.Equal(t, expectedCommitRange, offset)

			return nil
		})

		reconnector := &readerReconnector{
			streamVal:           stream,
			streamContextCancel: func(cause error) {},
			tracer:              &trace.Topic{},
		}
		reconnector.initChannelsAndClock()
		require.NoError(t, reconnector.Commit(ctx, expectedCommitRange))
	})
	t.Run("StreamOkCommitErr", func(t *testing.T) {
		mc := gomock.NewController(t)
		stream := NewMockbatchedStreamReader(mc)
		stream.EXPECT().Commit(
			gomock.Any(),
			gomock.Any(),
		).DoAndReturn(func(ctx context.Context, offset topicreadercommon.CommitRange) error {
			require.Equal(t, "v", ctx.Value(k{}))
			require.Equal(t, expectedCommitRange, offset)

			return testErr
		})

		reconnector := &readerReconnector{
			streamVal:           stream,
			streamContextCancel: func(cause error) {},
			tracer:              &trace.Topic{},
		}
		reconnector.initChannelsAndClock()
		require.ErrorIs(t, reconnector.Commit(ctx, expectedCommitRange), testErr)
	})
	t.Run("StreamErr", func(t *testing.T) {
		reconnector := &readerReconnector{streamErr: testErr, tracer: &trace.Topic{}}
		reconnector.initChannelsAndClock()
		require.ErrorIs(t, reconnector.Commit(ctx, expectedCommitRange), testErr)
	})
	t.Run("CloseErr", func(t *testing.T) {
		reconnector := &readerReconnector{tracer: &trace.Topic{}}
		_ = reconnector.background.Close(ctx, testErr)
		reconnector.initChannelsAndClock()
		require.ErrorIs(t, reconnector.Commit(ctx, expectedCommitRange), testErr)
	})
	t.Run("StreamAndCloseErr", func(t *testing.T) {
		reconnector := &readerReconnector{streamErr: testErr2, tracer: &trace.Topic{}}
		_ = reconnector.background.Close(ctx, testErr)
		reconnector.initChannelsAndClock()
		require.ErrorIs(t, reconnector.Commit(ctx, expectedCommitRange), testErr)
	})
}

func TestTopicReaderReconnectorConnectionLoop(t *testing.T) {
	t.Run("Reconnect", func(t *testing.T) {
		ctx := xtest.Context(t)
		mc := gomock.NewController(t)
		defer mc.Finish()

		newStream1 := NewMockbatchedStreamReader(mc)
		newStream1.EXPECT().CloseWithError(gomock.Any(), gomock.Any()).MinTimes(1)
		newStream2 := NewMockbatchedStreamReader(mc)

		newStream2.EXPECT().CloseWithError(gomock.Any(), gomock.Any()).MinTimes(1)

		reconnector := &readerReconnector{
			background: *background.NewWorker(ctx, "test-worker, "+t.Name()),
			retrySettings: topic.RetrySettings{
				StartTimeout: value.InfiniteDuration,
			},
			tracer:         &trace.Topic{},
			connectTimeout: value.InfiniteDuration,
		}
		reconnector.initChannelsAndClock()

		stream1Ready := make(empty.Chan)
		stream2Ready := make(empty.Chan)
		reconnector.readerConnect = readerConnectFuncMock([]readerConnectFuncAnswer{
			{
				callback: func(ctx context.Context) (batchedStreamReader, error) {
					close(stream1Ready)

					return newStream1, nil
				},
			},
			{
				err: xerrors.Retryable(errors.New("test reconnect error")),
			},
			{
				callback: func(ctx context.Context) (batchedStreamReader, error) {
					close(stream2Ready)

					return newStream2, nil
				},
			},
			{
				callback: func(ctx context.Context) (batchedStreamReader, error) {
					t.Fatal()

					return nil, errors.New("unexpected call")
				},
			},
		}...)

		reconnector.start()

		<-stream1Ready

		// skip bad (old) stream
		reconnector.reconnectFromBadStream <- newReconnectRequest(NewMockbatchedStreamReader(mc), nil)

		reconnector.reconnectFromBadStream <- newReconnectRequest(newStream1, nil)

		<-stream2Ready

		// wait apply stream2 connection
		xtest.SpinWaitCondition(t, &reconnector.m, func() bool {
			return reconnector.streamVal == newStream2
		})

		require.NoError(t, reconnector.CloseWithError(ctx, errReaderClosed))
	})

	t.Run("StartWithCancelledContext", func(t *testing.T) {
		ctx, cancel := xcontext.WithCancel(context.Background())
		cancel()
		reconnector := &readerReconnector{
			tracer: &trace.Topic{},
		}
		reconnector.initChannelsAndClock()
		reconnector.reconnectionLoop(ctx) // must return
	})
}

func TestTopicReaderReconnectorStart(t *testing.T) {
	mc := gomock.NewController(t)
	defer mc.Finish()

	ctx := context.Background()

	reconnector := &readerReconnector{
		tracer: &trace.Topic{},
	}
	reconnector.initChannelsAndClock()

	stream := NewMockbatchedStreamReader(mc)
	stream.EXPECT().CloseWithError(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, err error) error {
		require.Error(t, err)

		return nil
	})

	connectionRequested := make(empty.Chan)
	reconnector.readerConnect = readerConnectFuncMock([]readerConnectFuncAnswer{
		{callback: func(ctx context.Context) (batchedStreamReader, error) {
			close(connectionRequested)

			return stream, nil
		}},
		{callback: func(ctx context.Context) (batchedStreamReader, error) {
			t.Error()

			return nil, errors.New("unexpected call")
		}},
	}...)

	reconnector.start()

	<-connectionRequested
	_ = reconnector.CloseWithError(ctx, nil)
}

func TestTopicReaderReconnectorWaitInit(t *testing.T) {
	t.Run("OK", func(t *testing.T) {
		mc := gomock.NewController(t)
		defer mc.Finish()

		reconnector := &readerReconnector{
			tracer: &trace.Topic{},
		}
		reconnector.initChannelsAndClock()

		stream := NewMockbatchedStreamReader(mc)

		reconnector.readerConnect = readerConnectFuncMock(readerConnectFuncAnswer{
			callback: func(ctx context.Context) (batchedStreamReader, error) {
				return stream, nil
			},
		})

		reconnector.start()

		err := reconnector.WaitInit(context.Background())
		require.NoError(t, err)

		// one more run is needed to check idempotency
		err = reconnector.WaitInit(context.Background())
		require.NoError(t, err)
	})

	t.Run("contextDeadlineInProgress", func(t *testing.T) {
		xtest.TestManyTimes(t, func(t testing.TB) {
			mc := gomock.NewController(t)
			defer mc.Finish()

			reconnector := &readerReconnector{
				tracer: &trace.Topic{},
			}
			reconnector.initChannelsAndClock()

			stream := NewMockbatchedStreamReader(mc)

			waitInitReturned := make(empty.Chan)
			ctx, cancel := context.WithCancel(context.Background())
			reconnector.readerConnect = readerConnectFuncMock(readerConnectFuncAnswer{
				callback: func(ctx context.Context) (batchedStreamReader, error) {
					cancel()
					<-waitInitReturned

					return stream, nil
				},
			})
			reconnector.start()

			err := reconnector.WaitInit(ctx)
			close(waitInitReturned)
			require.ErrorIs(t, err, ctx.Err())
		})
	})

	t.Run("contextDeadlineBeforeStart", func(t *testing.T) {
		mc := gomock.NewController(t)
		defer mc.Finish()

		reconnector := &readerReconnector{
			tracer: &trace.Topic{},
		}
		reconnector.initDoneCh = make(empty.Chan, 1)
		reconnector.initChannelsAndClock()

		stream := NewMockbatchedStreamReader(mc)

		reconnector.readerConnect = readerConnectFuncMock(readerConnectFuncAnswer{
			callback: func(ctx context.Context) (batchedStreamReader, error) {
				return stream, nil
			},
		})

		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		err := reconnector.WaitInit(ctx)

		require.ErrorIs(t, err, ctx.Err())
	})

	t.Run("UnretriableError", func(t *testing.T) {
		reconnector := &readerReconnector{
			tracer: &trace.Topic{},
		}
		reconnector.initChannelsAndClock()

		testErr := errors.New("test error")
		ctx := context.Background()
		reconnector.readerConnect = readerConnectFuncMock(readerConnectFuncAnswer{
			callback: func(ctx context.Context) (batchedStreamReader, error) {
				return nil, testErr
			},
		})
		reconnector.start()

		err := reconnector.WaitInit(ctx)
		require.ErrorIs(t, err, testErr)
	})
}

func TestTopicReaderReconnectorFireReconnectOnRetryableError(t *testing.T) {
	t.Run("Ok", func(t *testing.T) {
		mc := gomock.NewController(t)
		reconnector := &readerReconnector{
			tracer: &trace.Topic{},
		}

		stream := NewMockbatchedStreamReader(mc)
		reconnector.initChannelsAndClock()

		reconnector.fireReconnectOnRetryableError(stream, nil)
		select {
		case <-reconnector.reconnectFromBadStream:
			t.Fatal()
		default:
			// OK
		}

		reconnector.fireReconnectOnRetryableError(stream, xerrors.Wrap(errors.New("test")))
		select {
		case <-reconnector.reconnectFromBadStream:
			t.Fatal()
		default:
			// OK
		}

		testErr := errors.New("test")
		reconnector.fireReconnectOnRetryableError(stream, xerrors.Retryable(testErr))
		res := <-reconnector.reconnectFromBadStream
		require.Equal(t, stream, res.oldReader)
		require.ErrorIs(t, res.reason, testErr)
	})

	t.Run("SkipWriteOnFullChannel", func(t *testing.T) {
		mc := gomock.NewController(t)
		defer mc.Finish()

		reconnector := &readerReconnector{
			tracer: &trace.Topic{},
		}
		stream := NewMockbatchedStreamReader(mc)
		reconnector.initChannelsAndClock()

	fillChannel:
		for {
			select {
			case reconnector.reconnectFromBadStream <- newReconnectRequest(nil, nil):
				// repeat
			default:
				break fillChannel
			}
		}

		// write skipped
		reconnector.fireReconnectOnRetryableError(stream, xerrors.Retryable(errors.New("test")))
		res := <-reconnector.reconnectFromBadStream
		require.Nil(t, res.oldReader)
	})
}

func TestTopicReaderReconnectorReconnectWithError(t *testing.T) {
	ctx := context.Background()
	testErr := errors.New("test")
	reconnector := &readerReconnector{
		connectTimeout: value.InfiniteDuration,
		readerConnect: func(ctx context.Context) (batchedStreamReader, error) {
			return nil, testErr
		},
		streamErr: errors.New("start-error"),
		tracer:    &trace.Topic{},
	}
	reconnector.initChannelsAndClock()
	err := reconnector.reconnect(ctx, errors.New("test-reconnect"), nil)
	require.ErrorIs(t, err, testErr)
	require.ErrorIs(t, reconnector.streamErr, testErr)
}

type readerConnectFuncAnswer struct {
	callback readerConnectFunc
	stream   batchedStreamReader
	err      error
}

func readerConnectFuncMock(answers ...readerConnectFuncAnswer) readerConnectFunc {
	return func(ctx context.Context) (batchedStreamReader, error) {
		res := answers[0]
		if len(answers) > 1 {
			answers = answers[1:]
		}

		if res.callback == nil {
			return res.stream, res.err
		}

		return res.callback(ctx)
	}
}
