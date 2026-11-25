package topicoptions_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicreadercommon"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicreaderinternal"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicoptions"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicreader"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func TestReaderCloseFromTerminalSessionError(t *testing.T) {
	var reader *topicreader.Reader
	var stops atomic.Int32
	ready := make(chan struct{})
	callbackReturned := make(chan error, 1)
	connectErr := status.Error(codes.PermissionDenied, "access denied")
	internalReader, err := topicreaderinternal.NewReader(nil,
		func(context.Context, int64, *trace.Topic) (topicreadercommon.RawTopicReaderStream, error) {
			<-ready

			return nil, connectErr
		}, "consumer", []topicreadercommon.PublicReadSelector{{Path: "/topic"}},
		topicoptions.WithReaderTrace(trace.Topic{
			OnReaderSessionError: func(info trace.TopicReaderSessionErrorInfo) {
				if info.RetryDecision == "stop" {
					stops.Add(1)
				}
				callbackReturned <- reader.Close(context.Background())
			},
		}),
	)
	require.NoError(t, err)
	reader = topicreader.NewReader(internalReader)
	close(ready)

	select {
	case err := <-callbackReturned:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("session-error callback did not return from Reader.Close")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.ErrorIs(t, reader.WaitInit(ctx), connectErr)
	require.NoError(t, reader.Close(ctx))
	require.EqualValues(t, 1, stops.Load())
}

func TestReaderCloseFromRetrySessionError(t *testing.T) {
	var reader *topicreader.Reader
	ready := make(chan struct{})
	callbackStarted := make(chan struct{}, 1)
	callbackReturned := make(chan error, 1)
	connectErr := xerrors.Transport(status.Error(codes.Unavailable, "connection unavailable"))
	internalReader, err := topicreaderinternal.NewReader(nil,
		func(context.Context, int64, *trace.Topic) (topicreadercommon.RawTopicReaderStream, error) {
			<-ready

			return nil, connectErr
		}, "consumer", []topicreadercommon.PublicReadSelector{{Path: "/topic"}},
		topicoptions.WithReaderStartTimeout(time.Minute),
		topicoptions.WithReaderTrace(trace.Topic{
			OnReaderSessionError: func(info trace.TopicReaderSessionErrorInfo) {
				if info.RetryDecision == "retry" {
					callbackStarted <- struct{}{}
					callbackReturned <- reader.Close(context.Background())
				}
			},
		}),
	)
	require.NoError(t, err)
	reader = topicreader.NewReader(internalReader)
	close(ready)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	select {
	case <-callbackStarted:
	case <-ctx.Done():
		t.Fatal("retry session-error callback did not start")
	}
	select {
	case err := <-callbackReturned:
		require.NoError(t, err)
	case <-ctx.Done():
		t.Fatal("retry session-error callback did not return from Reader.Close")
	}
	require.ErrorContains(t, reader.WaitInit(ctx), "ydb: reader closed")
	require.NoError(t, reader.Close(ctx))
}
