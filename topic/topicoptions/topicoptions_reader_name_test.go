package topicoptions

import (
	"context"
	"reflect"
	"regexp"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopiccommon"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopicreader"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawydb"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topiclistenerinternal"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicreadercommon"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicreaderinternal"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func TestWithReaderNameSetsCustomAndEmptyValues(t *testing.T) {
	cfg := topicreaderinternal.ReaderConfig{}

	WithReaderName("reader-custom")(&cfg)
	require.Equal(t, "reader-custom", cfg.ReaderInfo.ReaderName)

	WithReaderName("")(&cfg)
	require.Empty(t, cfg.ReaderInfo.ReaderName)
}

func TestWithReaderNameIsFinalizedByReaderConstructor(t *testing.T) {
	t.Run("custom name persists", func(t *testing.T) {
		reader := newNameTestReader(t, "reader-custom")
		defer closeNameTestReader(t, &reader)

		require.Equal(t, "reader-custom", internalReaderName(reader))
	})

	t.Run("empty name gets one generated name", func(t *testing.T) {
		reader := newNameTestReader(t, "")
		defer closeNameTestReader(t, &reader)

		name := internalReaderName(reader)
		require.Regexp(t, regexp.MustCompile(`^reader-[0-9]+$`), name)
		require.Equal(t, name, internalReaderName(reader))
	})
}

func TestWithListenerNameSetsCustomAndEmptyValues(t *testing.T) {
	cfg := topiclistenerinternal.NewStreamListenerConfig()

	WithListenerName("listener-custom")(&cfg)
	require.Equal(t, "listener-custom", cfg.ReaderName)

	WithListenerName("")(&cfg)
	require.Empty(t, cfg.ReaderName)
}

func TestWithListenerNameIsFinalizedByListenerConstructor(t *testing.T) {
	for _, test := range []struct {
		name string
		want *regexp.Regexp
	}{
		{name: "listener-custom", want: regexp.MustCompile(`^listener-custom$`)},
		{name: "", want: regexp.MustCompile(`^reader-[0-9]+$`)},
	} {
		t.Run(test.name, func(t *testing.T) {
			cfg := topiclistenerinternal.NewStreamListenerConfig()
			cfg.Consumer = "consumer"
			cfg.Selectors = []*topicreadercommon.PublicReadSelector{{Path: "/topic"}}
			WithListenerName(test.name)(&cfg)

			reconnector, err := topiclistenerinternal.NewTopicListenerReconnector(
				nameTestTopicClient{}, &cfg, nameTestEventHandler{},
			)
			require.NoError(t, err)
			defer closeNameTestListener(t, reconnector)

			require.Regexp(t, test.want, cfg.ReaderName)
		})
	}
}

func TestDefaultNamesAreUniqueAcrossReadersAndListeners(t *testing.T) {
	firstReader := newNameTestReader(t, "")
	secondReader := newNameTestReader(t, "")
	defer closeNameTestReader(t, &firstReader)
	defer closeNameTestReader(t, &secondReader)

	firstListenerConfig := topiclistenerinternal.NewStreamListenerConfig()
	firstListenerConfig.Consumer = "consumer"
	firstListenerConfig.Selectors = []*topicreadercommon.PublicReadSelector{{Path: "/topic"}}
	firstListener, err := topiclistenerinternal.NewTopicListenerReconnector(
		nameTestTopicClient{}, &firstListenerConfig, nameTestEventHandler{},
	)
	require.NoError(t, err)
	defer closeNameTestListener(t, firstListener)

	secondListenerConfig := topiclistenerinternal.NewStreamListenerConfig()
	secondListenerConfig.Consumer = "consumer"
	secondListenerConfig.Selectors = []*topicreadercommon.PublicReadSelector{{Path: "/topic"}}
	secondListener, err := topiclistenerinternal.NewTopicListenerReconnector(
		nameTestTopicClient{}, &secondListenerConfig, nameTestEventHandler{},
	)
	require.NoError(t, err)
	defer closeNameTestListener(t, secondListener)

	names := map[string]struct{}{
		internalReaderName(firstReader):  {},
		internalReaderName(secondReader): {},
		firstListenerConfig.ReaderName:   {},
		secondListenerConfig.ReaderName:  {},
	}
	require.Len(t, names, 4)
}

func newNameTestReader(t *testing.T, name string) topicreaderinternal.Reader {
	t.Helper()

	connectorStarted := make(chan struct{})
	stream := &nameTestRawStream{closed: make(chan struct{})}
	reader, err := topicreaderinternal.NewReader(
		nil,
		func(context.Context, int64, *trace.Topic) (topicreadercommon.RawTopicReaderStream, error) {
			close(connectorStarted)

			return stream, nil
		},
		"consumer",
		[]topicreadercommon.PublicReadSelector{{Path: "/topic"}},
		WithReaderName(name),
	)
	require.NoError(t, err)
	select {
	case <-connectorStarted:
	case <-time.After(time.Second):
		t.Fatal("reader connector was not called")
	}
	require.Eventually(t, func() bool {
		return reader.ReadSessionID() == "name-test-session"
	}, time.Second, time.Millisecond)

	return reader
}

func internalReaderName(reader topicreaderinternal.Reader) string {
	value := reflect.ValueOf(reader)

	return value.FieldByName("readerInfo").FieldByName("ReaderName").String()
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

type nameTestRawStream struct {
	closed    chan struct{}
	closeOnce sync.Once
	initSent  bool
}

func (s *nameTestRawStream) Send(rawtopicreader.ClientMessage) error {
	return nil
}

func (s *nameTestRawStream) Recv() (rawtopicreader.ServerMessage, error) {
	if !s.initSent {
		s.initSent = true

		return &rawtopicreader.InitResponse{
			ServerMessageMetadata: rawtopiccommon.ServerMessageMetadata{Status: rawydb.StatusSuccess},
			SessionID:             "name-test-session",
		}, nil
	}

	<-s.closed

	return nil, context.Canceled
}

func (s *nameTestRawStream) CloseSend() error {
	s.closeOnce.Do(func() {
		close(s.closed)
	})

	return nil
}

func (nameTestTopicClient) StreamRead(
	context.Context,
	int64,
	*trace.Topic,
) (rawtopicreader.StreamReader, error) {
	return rawtopicreader.StreamReader{}, context.Canceled
}

type nameTestEventHandler struct{}

func (nameTestEventHandler) OnStartPartitionSessionRequest(
	context.Context,
	*topiclistenerinternal.PublicEventStartPartitionSession,
) error {
	return nil
}

func (nameTestEventHandler) OnReadMessages(context.Context, *topiclistenerinternal.PublicReadMessages) error {
	return nil
}

func (nameTestEventHandler) OnStopPartitionSessionRequest(
	context.Context,
	*topiclistenerinternal.PublicEventStopPartitionSession,
) error {
	return nil
}
