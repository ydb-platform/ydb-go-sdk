package topicreaderinternal

import (
	"context"
	"errors"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	grpcCodes "google.golang.org/grpc/codes"
	grpcStatus "google.golang.org/grpc/status"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/background"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func TestReaderReconnectorLoopStopsAfterAdmittedRetry(t *testing.T) {
	for _, reportSessionError := range []bool{true, false} {
		t.Run("report_session_error="+strconv.FormatBool(reportSessionError), func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			events := make(chan trace.TopicReaderSessionErrorInfo, 2)
			var connectAttempts atomic.Int32
			retryErr := xerrors.Retryable(grpcStatus.Error(grpcCodes.Unavailable, "retry budget exhausted"))
			stream := NewMockbatchedStreamReader(ctrl)
			stream.EXPECT().CloseWithError(gomock.Any(), gomock.Any()).Return(nil)

			loopCtx, cancelLoop := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancelLoop()
			fakeClock := clockwork.NewFakeClockAt(time.Now())

			reconnector := &readerReconnector{
				background: *background.NewWorker(loopCtx, "reconnector-loop-stop-test"),
				clock:      fakeClock,
				retrySettings: topic.RetrySettings{
					StartTimeout: time.Minute,
				},
				readerConnect: func(context.Context) (batchedStreamReader, error) {
					if connectAttempts.Add(1) == 1 {
						return stream, nil
					}

					return nil, errors.New("unexpected second reconnect attempt")
				},
				tracer:         sessionErrorTestTracer(events),
				readerInfo:     sessionErrorTestReaderInfo(),
				connectTimeout: 24 * time.Hour,
			}
			reconnector.initChannelsAndClock()
			t.Cleanup(func() {
				cleanupCtx, cancelCleanup := context.WithTimeout(context.Background(), time.Second)
				defer cancelCleanup()
				_ = reconnector.CloseWithError(cleanupCtx, errors.New("test cleanup"))
			})

			// Keep the initial stream error nil so startup does not enqueue an
			// unrelated retry request before the request under test is admitted.
			loopStarted := make(chan struct{})
			loopDone := make(chan struct{})
			reconnector.background.Start("reconnector-loop-stop-test", func(ctx context.Context) {
				close(loopStarted)
				defer close(loopDone)
				reconnector.reconnectionLoop(ctx)
			})
			select {
			case <-loopStarted:
			case <-loopCtx.Done():
				t.Fatal("reconnection loop did not start")
			}
			reconnector.reconnectFromBadStream <- newReconnectRequest(nil)
			waitCtx, cancelWait := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancelWait()
			require.NoError(t, reconnector.WaitInit(waitCtx))
			require.Equal(t, int32(1), connectAttempts.Load())

			fakeClock.Advance(time.Hour)
			admissionCtx := context.Background()
			if !reportSessionError {
				cancelledCtx, cancel := context.WithCancel(admissionCtx)
				cancel()
				admissionCtx = cancelledCtx
			}
			reconnector.fireReconnectOnRetryableErrorWithContext(admissionCtx, stream, retryErr)

			select {
			case <-loopDone:
			case <-loopCtx.Done():
				t.Fatal("reconnection loop did not stop after retry timeout")
			}
			require.NoError(t, loopCtx.Err())
			require.Equal(t, int32(1), connectAttempts.Load(), "reconnection continued after retry decision requested stop")
			require.Empty(t, reconnector.reconnectFromBadStream)
			require.ErrorIs(t, reconnector.background.CloseReason(), retryErr)

			if reportSessionError {
				select {
				case event := <-events:
					require.Equal(t, "stop", event.RetryDecision)
					require.ErrorIs(t, event.Error, retryErr)
					require.Equal(t, "Unavailable", event.StatusCode)
					require.Equal(t, "transport_error", event.ErrorType)
					require.Equal(t, "endpoint", event.Endpoint)
					require.Equal(t, "/database", event.Database)
					require.Equal(t, "consumer", event.Consumer)
					require.Equal(t, readerNamePointer("reader"), event.ReaderName)
				default:
					t.Fatal("retry stop session error was not emitted")
				}
				select {
				case event := <-events:
					t.Fatalf("unexpected duplicate retry stop session error event: %+v", event)
				default:
				}
			} else {
				select {
				case event := <-events:
					t.Fatalf("suppressed retry stop session error event: %+v", event)
				default:
				}
			}
		})
	}
}
