package topicoptions

import (
	"context"
	"fmt"
	"reflect"
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

func TestReaderNameIsOptional(t *testing.T) {
	for _, names := range [][]string{nil, {""}, {"reader-custom"}} {
		t.Run(fmt.Sprint(names), func(t *testing.T) {
			reader := newNameTestReader(t, names...)
			defer closeNameTestReader(t, &reader)
			name := internalReaderName(reader)
			if len(names) == 0 {
				require.Nil(t, name)
			} else {
				require.NotNil(t, name)
				require.Equal(t, names[0], *name)
			}
		})
	}
}

func TestListenerNameIsOptional(t *testing.T) {
	for _, names := range [][]string{nil, {""}, {"listener-custom"}} {
		t.Run(fmt.Sprint(names), func(t *testing.T) {
			cfg := topiclistenerinternal.NewStreamListenerConfig()
			cfg.Consumer = "consumer"
			cfg.Selectors = []*topicreadercommon.PublicReadSelector{{Path: "/topic"}}
			if len(names) != 0 {
				WithListenerName(names[0])(&cfg)
			}
			reconnector, err := topiclistenerinternal.NewTopicListenerReconnector(
				nameTestTopicClient{}, &cfg, nameTestEventHandler{},
			)
			require.NoError(t, err)
			defer closeNameTestListener(t, reconnector)
			if len(names) == 0 {
				require.Nil(t, cfg.ReaderName)
			} else {
				require.NotNil(t, cfg.ReaderName)
				require.Equal(t, names[0], *cfg.ReaderName)
			}
		})
	}
}

func newNameTestReader(t *testing.T, names ...string) topicreaderinternal.Reader {
	t.Helper()

	var opts []ReaderOption
	if len(names) != 0 {
		opts = append(opts, WithReaderName(names[0]))
	}
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
		opts...,
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

func internalReaderName(reader topicreaderinternal.Reader) *string {
	value := reflect.ValueOf(reader).FieldByName("readerInfo").FieldByName("ReaderName")
	if value.IsNil() {
		return nil
	}
	name := value.Elem().String()

	return &name
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
