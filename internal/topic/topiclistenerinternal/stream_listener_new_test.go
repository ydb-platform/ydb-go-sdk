package topiclistenerinternal

import (
	"context"
	"errors"
	"io"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rekby/fixenv"
	"github.com/rekby/fixenv/sf"
	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Topic"
	"go.uber.org/mock/gomock"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopicreader"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicreadercommon"
	xtest "github.com/ydb-platform/ydb-go-sdk/v3/pkg/xtest"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

type testInitGrpcStream struct {
	sessionID        string
	initSent         bool
	recvBlock        chan struct{}
	readRequestBytes atomic.Int64
}

func (s *testInitGrpcStream) Send(message *Ydb_Topic.StreamReadMessage_FromClient) error {
	if readReq := message.GetReadRequest(); readReq != nil {
		s.readRequestBytes.Store(readReq.GetBytesSize())
	}

	return nil
}

func (s *testInitGrpcStream) Recv() (*Ydb_Topic.StreamReadMessage_FromServer, error) {
	if !s.initSent {
		s.initSent = true

		return &Ydb_Topic.StreamReadMessage_FromServer{
			Status: Ydb.StatusIds_SUCCESS,
			ServerMessage: &Ydb_Topic.StreamReadMessage_FromServer_InitResponse{
				InitResponse: &Ydb_Topic.StreamReadMessage_InitResponse{
					SessionId: s.sessionID,
				},
			},
		}, nil
	}

	<-s.recvBlock

	return nil, io.EOF
}

func (s *testInitGrpcStream) CloseSend() error {
	return nil
}

type testTopicClient struct {
	stream rawtopicreader.StreamReader
}

func (c *testTopicClient) StreamRead(
	_ context.Context,
	_ int64,
	_ *trace.Topic,
) (rawtopicreader.StreamReader, error) {
	return c.stream, nil
}

func TestNewStreamListener_SeedsInitialReadRequest(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx := xtest.Context(t)
	const bufferSize = 42

	grpcStream := &testInitGrpcStream{
		sessionID: "test-session-id",
		recvBlock: make(chan struct{}),
	}
	client := &testTopicClient{
		stream: rawtopicreader.StreamReader{
			Stream: grpcStream,
			Tracer: &trace.Topic{},
		},
	}

	handler := NewMockEventHandler(ctrl)
	cfg := NewStreamListenerConfig()
	cfg.Consumer = "test-consumer"
	cfg.Selectors = []*topicreadercommon.PublicReadSelector{{Path: "test-topic"}}
	cfg.BufferSize = bufferSize

	listener, err := newStreamListener(ctx, client, handler, &cfg, &atomic.Int64{})
	require.NoError(t, err)
	require.NotNil(t, listener)
	defer func() {
		close(grpcStream.recvBlock)
		require.NoError(t, listener.Close(ctx, nil))
	}()

	require.Eventually(t, func() bool {
		return grpcStream.readRequestBytes.Load() == bufferSize
	}, time.Second, 10*time.Millisecond)
}

func TestStreamListener_SeedInitialBufferCreditSkipsWhenStopped(t *testing.T) {
	e := fixenv.New(t)
	ctx := sf.Context(e)
	listener := StreamListener(e)

	listener.background.Start("stream listener send loop", listener.sendMessagesLoop)
	require.NoError(t, listener.background.Close(ctx, errors.New("shutdown")))

	require.NotPanics(t, func() {
		listener.seedInitialBufferCredit(42)
	})
}
