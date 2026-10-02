package topicoptions

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	grpcCodes "google.golang.org/grpc/codes"
	grpcStatus "google.golang.org/grpc/status"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopicreader"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topiclistenerinternal"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicreadercommon"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicreaderinternal"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func TestReaderNameDefaultsToDefault(t *testing.T) {
	tests := []struct {
		name    string
		options []string
		want    string
	}{
		{name: "omitted", want: "default"},
		{name: "empty", options: []string{""}, want: "default"},
		{name: "custom", options: []string{"reader-custom"}, want: "reader-custom"},
		{name: "explicit default", options: []string{"default"}, want: "default"},
	}
	readerIDs := make(map[int64]struct{}, len(tests))
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			reader, name := newNameTestReader(t, test.options...)
			defer closeNameTestReader(t, &reader)

			require.Equal(t, test.want, name)
			readerID := reader.ID()
			require.NotContains(t, readerIDs, readerID)
			readerIDs[readerID] = struct{}{}
		})
	}
}

func TestListenerNameDefaultsToDefault(t *testing.T) {
	tests := []struct {
		name    string
		options []string
		want    string
	}{
		{name: "omitted", want: "default"},
		{name: "empty", options: []string{""}, want: "default"},
		{name: "custom", options: []string{"listener-custom"}, want: "listener-custom"},
		{name: "explicit default", options: []string{"default"}, want: "default"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			reconnector, name := newNameTestListener(t, test.options...)
			defer closeNameTestListener(t, reconnector)

			require.Equal(t, test.want, name)
		})
	}
}

func newNameTestReader(t *testing.T, names ...string) (topicreaderinternal.Reader, string) {
	t.Helper()

	var readerName string
	readerTrace := trace.Topic{
		OnReaderSessionError: func(info trace.TopicReaderSessionErrorInfo) {
			readerName = info.ReaderName
		},
	}
	opts := []ReaderOption{
		WithReaderTrace(readerTrace),
		WithReaderCheckRetryErrorFunction(func(CheckErrorRetryArgs) CheckErrorRetryResult {
			return CheckErrorRetryDecisionStop
		}),
	}
	if len(names) != 0 {
		opts = append(opts, WithReaderName(names[0]))
	}
	reader, err := topicreaderinternal.NewReader(
		nil,
		func(context.Context, int64, *trace.Topic) (topicreadercommon.RawTopicReaderStream, error) {
			return nil, grpcStatus.Error(grpcCodes.PermissionDenied, "reader name test")
		},
		"consumer",
		[]topicreadercommon.PublicReadSelector{{Path: "/topic"}},
		opts...,
	)
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	initErr := reader.WaitInit(ctx)
	require.Error(t, initErr)
	require.Equal(t, grpcCodes.PermissionDenied, grpcStatus.Code(initErr))
	require.NotEmpty(t, readerName)

	return reader, readerName
}

func newNameTestListener(
	t *testing.T,
	names ...string,
) (*topiclistenerinternal.TopicListenerReconnector, string) {
	t.Helper()

	cfg := topiclistenerinternal.NewStreamListenerConfig()
	cfg.Consumer = "consumer"
	cfg.Selectors = []*topicreadercommon.PublicReadSelector{{Path: "/topic"}}
	if len(names) != 0 {
		WithListenerName(names[0])(&cfg)
	}
	reconnector, err := topiclistenerinternal.NewTopicListenerReconnector(
		nameTestTopicClient{}, &cfg, nil,
	)
	require.NoError(t, err)
	readerName := cfg.ReaderName
	require.NotEmpty(t, readerName)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	require.ErrorIs(t, reconnector.WaitInit(ctx), context.Canceled)

	return reconnector, readerName
}

func closeNameTestReader(t *testing.T, reader *topicreaderinternal.Reader) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	require.NoError(t, reader.Close(ctx))
}

func closeNameTestListener(t *testing.T, reconnector *topiclistenerinternal.TopicListenerReconnector) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	require.NoError(t, reconnector.Close(ctx, context.Canceled))
}

type nameTestTopicClient struct{}

func (nameTestTopicClient) StreamRead(
	context.Context,
	int64,
	*trace.Topic,
) (rawtopicreader.StreamReader, error) {
	return rawtopicreader.StreamReader{}, context.Canceled
}
