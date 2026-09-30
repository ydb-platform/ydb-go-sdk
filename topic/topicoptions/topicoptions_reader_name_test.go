package topicoptions

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

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
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			reader, name := newNameTestReader(t, test.options...)
			defer closeNameTestReader(t, &reader)

			require.Equal(t, test.want, name)
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

func TestDefaultNamesAreSharedAcrossReadersAndListeners(t *testing.T) {
	firstReader, firstReaderName := newNameTestReader(t)
	secondReader, secondReaderName := newNameTestReader(t, "")
	defer closeNameTestReader(t, &firstReader)
	defer closeNameTestReader(t, &secondReader)

	firstListener, firstListenerName := newNameTestListener(t)
	defer closeNameTestListener(t, firstListener)

	secondListener, secondListenerName := newNameTestListener(t)
	defer closeNameTestListener(t, secondListener)

	require.Equal(t, []string{"default", "default", "default", "default"}, []string{
		firstReaderName,
		secondReaderName,
		firstListenerName,
		secondListenerName,
	})
	require.NotEqual(t, firstReader.ID(), secondReader.ID())
}

func newNameTestReader(t *testing.T, names ...string) (topicreaderinternal.Reader, string) {
	t.Helper()

	var readerName string
	readerTrace := trace.Topic{
		OnReaderMetricsSource: func(
			info trace.TopicReaderMetricsSourceStartInfo,
		) func(trace.TopicReaderMetricsSourceDoneInfo) {
			readerName = info.ReaderName

			return func(trace.TopicReaderMetricsSourceDoneInfo) {}
		},
	}
	opts := []ReaderOption{WithReaderTrace(readerTrace)}
	if len(names) != 0 {
		opts = append(opts, WithReaderName(names[0]))
	}
	reader, err := topicreaderinternal.NewReader(
		nil,
		func(context.Context, int64, *trace.Topic) (topicreadercommon.RawTopicReaderStream, error) {
			return nil, context.Canceled
		},
		"consumer",
		[]topicreadercommon.PublicReadSelector{{Path: "/topic"}},
		opts...,
	)
	require.NoError(t, err)
	require.NotEmpty(t, readerName)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	require.ErrorIs(t, reader.WaitInit(ctx), context.Canceled)

	return reader, readerName
}

func newNameTestListener(
	t *testing.T,
	names ...string,
) (*topiclistenerinternal.TopicListenerReconnector, string) {
	t.Helper()

	var readerName string
	cfg := topiclistenerinternal.NewStreamListenerConfig()
	cfg.Consumer = "consumer"
	cfg.Selectors = []*topicreadercommon.PublicReadSelector{{Path: "/topic"}}
	if len(names) != 0 {
		WithListenerName(names[0])(&cfg)
	}
	cfg.Tracer = &trace.Topic{
		OnReaderMetricsSource: func(
			info trace.TopicReaderMetricsSourceStartInfo,
		) func(trace.TopicReaderMetricsSourceDoneInfo) {
			readerName = info.ReaderName

			return func(trace.TopicReaderMetricsSourceDoneInfo) {}
		},
	}
	reconnector, err := topiclistenerinternal.NewTopicListenerReconnector(
		nameTestTopicClient{}, &cfg, nil,
	)
	require.NoError(t, err)
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
