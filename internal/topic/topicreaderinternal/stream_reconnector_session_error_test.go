package topicreaderinternal

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"go.uber.org/mock/gomock"
	grpcCodes "google.golang.org/grpc/codes"
	grpcStatus "google.golang.org/grpc/status"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/background"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicreadercommon"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func TestReaderReconnectorSessionErrorReportsInitialTerminalFailure(t *testing.T) {
	events := make(chan trace.TopicReaderSessionErrorInfo, 4)
	connectErr := xerrors.Operation(xerrors.WithStatusCode(Ydb.StatusIds_UNAUTHORIZED))
	reconnector := &readerReconnector{
		readerConnect: func(context.Context) (batchedStreamReader, error) {
			return nil, connectErr
		},
		connectTimeout: time.Hour,
		tracer:         sessionErrorTestTracer(events),
		readerInfo:     sessionErrorTestReaderInfo(),
	}
	reconnector.initChannelsAndClock()
	reconnector.start()

	waitCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	require.ErrorIs(t, reconnector.WaitInit(waitCtx), connectErr)

	select {
	case event := <-events:
		require.Equal(t, "stop", event.RetryDecision)
		require.Equal(t, "UNAUTHORIZED", event.StatusCode)
		require.Equal(t, "ydb_error", event.ErrorType)
	case <-time.After(time.Second):
		t.Fatal("initial terminal session error event was not emitted")
	}

	select {
	case event := <-events:
		t.Fatalf("unexpected duplicate initial session error event: %+v", event)
	case <-time.After(20 * time.Millisecond):
	}
}

func TestReaderReconnectorSessionErrorReportsFatalReadOnce(t *testing.T) {
	events := make(chan trace.TopicReaderSessionErrorInfo, 4)
	fatalErr := errors.New("fatal read failure")
	reconnector := &readerReconnector{
		streamErr:  nil,
		tracer:     sessionErrorTestTracer(events),
		readerInfo: sessionErrorTestReaderInfo(),
	}
	reconnector.initChannelsAndClock()

	read := func(context.Context, batchedStreamReader) (*topicreadercommon.PublicBatch, error) {
		return nil, fatalErr
	}
	for range 2 {
		_, err := reconnector.readWithReconnections(context.Background(), read)
		require.ErrorIs(t, err, fatalErr)
	}

	select {
	case event := <-events:
		require.Equal(t, "stop", event.RetryDecision)
		require.Equal(t, "unknown", event.StatusCode)
		require.Equal(t, "unknown", event.ErrorType)
	case <-time.After(time.Second):
		t.Fatal("fatal read session error event was not emitted")
	}

	select {
	case event := <-events:
		t.Fatalf("unexpected duplicate fatal read session error event: %+v", event)
	default:
	}
}

func TestReaderReconnectorSessionErrorReportsAdmittedRetries(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	events := make(chan trace.TopicReaderSessionErrorInfo, 4)
	stream := NewMockbatchedStreamReader(ctrl)
	stream.EXPECT().CloseWithError(gomock.Any(), gomock.Any()).Return(nil)
	reconnector := &readerReconnector{
		streamVal: stream,
		readerConnect: func(context.Context) (batchedStreamReader, error) {
			return stream, nil
		},
		connectTimeout: time.Hour,
		streamErr:      nil,
		tracer:         sessionErrorTestTracer(events),
		readerInfo:     sessionErrorTestReaderInfo(),
	}
	reconnector.initChannelsAndClock()
	retryErr := xerrors.Retryable(grpcStatus.Error(grpcCodes.Unavailable, "connection lost"))

	// A request for a stream that has already been replaced is stale and does
	// not count as a retry decision.
	reconnectErr := reconnector.reconnect(context.Background(), retryErr, nil, true)
	require.ErrorIs(t, reconnectErr, errReconnectRequestOutdated)
	select {
	case event := <-events:
		t.Fatalf("unexpected stale retry event: %+v", event)
	default:
	}

	// The fresh request reaches the connection attempt, so it is a real retry
	// decision and is reported once.
	reconnectErr = reconnector.reconnect(context.Background(), retryErr, stream, true)
	require.NoError(t, reconnectErr)
	select {
	case event := <-events:
		require.Equal(t, "retry", event.RetryDecision)
		require.Equal(t, "Unavailable", event.StatusCode)
		require.Equal(t, "transport_error", event.ErrorType)
	case <-time.After(time.Second):
		t.Fatal("retry session error event was not emitted")
	}
}

func TestReaderReconnectorSessionErrorDoesNotPropagateSuppressedReconnectReason(t *testing.T) {
	t.Run("terminal connector failure still stops", func(t *testing.T) {
		connectErr := xerrors.Operation(xerrors.WithStatusCode(Ydb.StatusIds_UNAUTHORIZED))
		events := make(chan trace.TopicReaderSessionErrorInfo, 2)
		reconnector := &readerReconnector{
			background: *background.NewWorker(context.Background(), "session-error-test"),
			readerConnect: func(context.Context) (batchedStreamReader, error) {
				return nil, connectErr
			},
			connectTimeout: time.Second,
			tracer:         sessionErrorTestTracer(events),
			readerInfo:     sessionErrorTestReaderInfo(),
		}
		reconnector.initChannelsAndClock()

		err := reconnector.reconnect(context.Background(), context.DeadlineExceeded, nil, false)
		require.ErrorIs(t, err, connectErr)
		select {
		case event := <-events:
			require.Equal(t, "stop", event.RetryDecision)
			require.Equal(t, "UNAUTHORIZED", event.StatusCode)
			require.Equal(t, "ydb_error", event.ErrorType)
		case <-time.After(time.Second):
			t.Fatal("terminal connector failure was not reported")
		}
	})

	t.Run("retry connector failure reports when admitted", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		retryErr := xerrors.Retryable(grpcStatus.Error(grpcCodes.Unavailable, "connection lost"))
		events := make(chan trace.TopicReaderSessionErrorInfo, 2)
		stream := NewMockbatchedStreamReader(ctrl)
		stream.EXPECT().CloseWithError(gomock.Any(), gomock.Any()).Return(nil)
		connectCalls := 0
		reconnector := &readerReconnector{
			background: *background.NewWorker(context.Background(), "session-error-test"),
			readerConnect: func(context.Context) (batchedStreamReader, error) {
				connectCalls++
				if connectCalls == 1 {
					return nil, retryErr
				}

				return stream, nil
			},
			connectTimeout: time.Second,
			tracer:         sessionErrorTestTracer(events),
			readerInfo:     sessionErrorTestReaderInfo(),
		}
		reconnector.initChannelsAndClock()

		err := reconnector.reconnect(context.Background(), context.DeadlineExceeded, nil, false)
		require.ErrorIs(t, err, retryErr)
		var request reconnectRequest
		select {
		case request = <-reconnector.reconnectFromBadStream:
		case <-time.After(time.Second):
			t.Fatal("retry connector failure did not enqueue a reconnect request")
		}
		require.True(t, request.reportSessionError)
		require.ErrorIs(t, request.reason, retryErr)

		require.NoError(t, reconnector.reconnect(
			context.Background(),
			request.reason,
			request.oldReader,
			request.reportSessionError,
		))
		select {
		case event := <-events:
			require.Equal(t, "retry", event.RetryDecision)
			require.Equal(t, "Unavailable", event.StatusCode)
			require.Equal(t, "transport_error", event.ErrorType)
		case <-time.After(time.Second):
			t.Fatal("admitted retry was not reported")
		}

		closeCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		require.NoError(t, reconnector.CloseWithError(closeCtx, errors.New("test finished")))
	})
}

func TestReaderReconnectorSessionErrorSkipsCancellationAndClose(t *testing.T) {
	events := make(chan trace.TopicReaderSessionErrorInfo, 4)
	reconnector := &readerReconnector{
		tracer:     sessionErrorTestTracer(events),
		readerInfo: sessionErrorTestReaderInfo(),
	}
	reconnector.initChannelsAndClock()
	retryErr := xerrors.Retryable(grpcStatus.Error(grpcCodes.Unavailable, "connection lost"))
	cancelledCtx, cancel := context.WithCancel(context.Background())
	cancel()

	// Only the session-error event is suppressed for caller cancellation.
	reconnector.traceSessionRetry(cancelledCtx, retryErr)
	reconnector.traceSessionStop(context.Background(), errReaderClosed)

	select {
	case event := <-events:
		t.Fatalf("unexpected termination session error event: %+v", event)
	default:
	}
}

func TestReaderReconnectorSessionErrorKeepsDeadlineFailures(t *testing.T) {
	events := make(chan trace.TopicReaderSessionErrorInfo, 4)
	reconnector := &readerReconnector{
		tracer:     sessionErrorTestTracer(events),
		readerInfo: sessionErrorTestReaderInfo(),
	}

	reconnector.traceSessionStop(
		context.Background(),
		grpcStatus.Error(grpcCodes.DeadlineExceeded, "connect timeout"),
	)

	select {
	case event := <-events:
		require.Equal(t, "DeadlineExceeded", event.StatusCode)
		require.Equal(t, "transport_error", event.ErrorType)
	case <-time.After(time.Second):
		t.Fatal("deadline session error event was not emitted")
	}
}

func TestReaderReconnectorSessionErrorSkipsCallerDeadline(t *testing.T) {
	events := make(chan trace.TopicReaderSessionErrorInfo, 4)
	reconnector := &readerReconnector{
		tracer:     sessionErrorTestTracer(events),
		readerInfo: sessionErrorTestReaderInfo(),
	}
	reconnector.initChannelsAndClock()
	deadlineCtx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	_, err := reconnector.readWithReconnections(
		deadlineCtx,
		func(ctx context.Context, _ batchedStreamReader) (*topicreadercommon.PublicBatch, error) {
			<-ctx.Done()

			return nil, ctx.Err()
		},
	)
	require.ErrorIs(t, err, context.DeadlineExceeded)

	select {
	case event := <-events:
		t.Fatalf("unexpected caller deadline session error event: %+v", event)
	default:
	}

	_, err = reconnector.readWithReconnections(
		context.Background(),
		func(context.Context, batchedStreamReader) (*topicreadercommon.PublicBatch, error) {
			return nil, errors.New("real session failure")
		},
	)
	require.Error(t, err)

	select {
	case event := <-events:
		require.Equal(t, "stop", event.RetryDecision)
		require.Equal(t, "unknown", event.StatusCode)
	case <-time.After(time.Second):
		t.Fatal("real session failure was not reported after caller deadline")
	}
}

func sessionErrorTestTracer(events chan<- trace.TopicReaderSessionErrorInfo) *trace.Topic {
	return &trace.Topic{
		OnReaderSessionError: func(info trace.TopicReaderSessionErrorInfo) {
			select {
			case events <- info:
			default:
			}
		},
	}
}

func sessionErrorTestReaderInfo() topicreadercommon.ReaderInfo {
	return topicreadercommon.ReaderInfo{
		Endpoint:   "endpoint",
		Database:   "/database",
		Consumer:   "consumer",
		ReaderName: "reader",
	}
}
