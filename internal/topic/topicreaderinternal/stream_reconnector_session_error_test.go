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
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic"
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

func TestReaderReconnectorCloseReportsStoredRawStreamError(t *testing.T) {
	tests := []struct {
		name             string
		streamErr        error
		customCheckError bool
		wantEvent        bool
	}{
		{
			name:      "terminal receive error",
			streamErr: grpcStatus.Error(grpcCodes.PermissionDenied, "permission denied"),
			wantEvent: true,
		},
		{
			name:      "retryable receive error",
			streamErr: xerrors.Retryable(grpcStatus.Error(grpcCodes.Unavailable, "connection lost")),
		},
		{
			name:      "graceful cancellation",
			streamErr: context.Canceled,
		},
		{
			name:      "graceful deadline",
			streamErr: context.DeadlineExceeded,
		},
		{
			name:             "custom retry policy",
			streamErr:        grpcStatus.Error(grpcCodes.PermissionDenied, "permission denied"),
			customCheckError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			events := make(chan trace.TopicReaderSessionErrorInfo, 4)
			reentrantClose := make(chan error, 1)
			e := newTopicReaderTestEnv(t)
			var reader *Reader
			e.reader.cfg.Trace = &trace.Topic{
				OnReaderSessionError: func(info trace.TopicReaderSessionErrorInfo) {
					events <- info
					if tt.wantEvent {
						reentrantClose <- reader.Close(context.Background())
					}
				},
			}
			e.reader.cfg.ReaderInfo = sessionErrorTestReaderInfo()
			reader = newMetricsReader(&e)
			reconnector := reader.reader.(*readerReconnector)
			var checkErrorCalls int
			if tt.customCheckError {
				reconnector.retrySettings.CheckError = func(topic.PublicCheckErrorRetryArgs) topic.PublicCheckRetryResult {
					checkErrorCalls++

					return topic.PublicRetryDecisionStop
				}
			}
			e.Start()

			e.messagesFromServerToClient <- testStreamResult{err: tt.streamErr}

			select {
			case <-e.partitionSession.Context().Done():
			case <-time.After(time.Second):
				t.Fatal("partition session was not closed after raw stream failure")
			}
			require.ErrorIs(t, e.reader.streamError(), tt.streamErr)
			select {
			case event := <-events:
				t.Fatalf("unexpected session error before Read or Close: %+v", event)
			default:
			}

			closeCtx, cancel := context.WithCancel(context.Background())
			cancel()
			closeDone := make(chan error, 1)
			go func() {
				closeDone <- reader.Close(closeCtx)
			}()
			select {
			case <-closeDone:
			case <-time.After(time.Second):
				t.Fatal("reader Close deadlocked while reporting stored raw stream error")
			}

			if !tt.wantEvent {
				require.Zero(t, checkErrorCalls)
				select {
				case event := <-events:
					t.Fatalf("unexpected stored raw stream error event: %+v", event)
				default:
				}

				return
			}

			select {
			case event := <-events:
				require.Equal(t, "stop", event.RetryDecision)
				require.ErrorIs(t, event.Error, tt.streamErr)
				require.Equal(t, "PermissionDenied", event.StatusCode)
				require.Equal(t, "transport_error", event.ErrorType)
				require.Equal(t, "endpoint", event.Endpoint)
				require.Equal(t, "/database", event.Database)
				require.Equal(t, "consumer", event.Consumer)
				require.Equal(t, "reader", event.ReaderName)
			case <-time.After(time.Second):
				t.Fatal("stored raw stream session error was not emitted by Close")
			}
			select {
			case err := <-reentrantClose:
				require.NoError(t, err)
			case <-time.After(time.Second):
				t.Fatal("session error callback could not close the reader")
			}

			_ = reader.Close(context.Background())
			select {
			case event := <-events:
				t.Fatalf("unexpected duplicate stored raw stream error event: %+v", event)
			default:
			}
		})
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
