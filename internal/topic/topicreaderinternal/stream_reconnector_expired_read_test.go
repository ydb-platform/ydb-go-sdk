package topicreaderinternal

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/backoff"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicreadercommon"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func TestExpiredReadCountsAdmittedStreamRetry(t *testing.T) {
	ctx, stop := context.WithTimeout(context.Background(), 5*time.Second)
	defer stop()
	stream := NewMockbatchedStreamReader(gomock.NewController(t))
	stream.EXPECT().CloseWithError(gomock.Any(), gomock.Any()).Return(nil).Times(2)
	connections := make(chan struct{}, 2)
	events := make(chan trace.TopicReaderSessionErrorInfo, 4)
	r := newReaderReconnector(ctx, 1, func(context.Context) (batchedStreamReader, error) {
		connections <- struct{}{}

		return stream, nil
	}, time.Second, topic.RetrySettings{StartTimeout: time.Minute},
		sessionErrorTestTracer(events), sessionErrorTestReaderInfo())
	require.NoError(t, r.WaitInit(ctx))
	<-connections
	caller, cancel := context.WithCancel(ctx)
	defer cancel()
	retryErr := xerrors.Retryable(status.Error(codes.Unavailable, "stream disconnected"),
		xerrors.WithBackoff(backoff.TypeFast))
	read := func(context.Context, batchedStreamReader) (*topicreadercommon.PublicBatch, error) {
		cancel()

		return nil, retryErr
	}
	_, err := r.readWithReconnections(caller, read)
	require.ErrorIs(t, err, context.Canceled)
	select {
	case <-connections:
	case <-ctx.Done():
		t.Fatal("replacement stream was not opened")
	}
	require.NoError(t, r.CloseWithError(ctx, errReaderClosed))
	require.Len(t, events, 1)
	event := <-events
	require.Equal(t, "retry", event.RetryDecision)
	require.ErrorIs(t, event.Error, retryErr)
}
