package topiclistenerinternal

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Topic"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/background"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopicreader"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicreadercommon"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	xtest "github.com/ydb-platform/ydb-go-sdk/v3/pkg/xtest"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func TestStreamListenerCloseWaitsForTraceDoneHook(t *testing.T) {
	listener := &streamListener{
		background: *background.NewWorker(context.Background(), "test stream listener"),
		tracer:     &trace.Topic{},
		listenerID: "test-listener-id",
	}
	listener.initVars(&atomic.Int64{})
	listener.background.Context()

	traceGate := newListenerCloseTraceGate()
	listener.tracer.OnListenerClose = traceGate.hook
	t.Cleanup(func() {
		traceGate.release()
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), time.Second)
		defer cleanupCancel()
		_ = listener.Close(cleanupCtx, ErrUserCloseTopic)
	})

	closeCtx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	closeResult := make(chan error, 1)
	go func() {
		closeResult <- listener.Close(closeCtx, ErrUserCloseTopic)
	}()

	select {
	case <-traceGate.started:
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for listener close trace start hook")
	}
	select {
	case <-traceGate.callbackStarted:
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for listener close trace done callback")
	}

	select {
	case err := <-closeResult:
		require.ErrorIs(t, err, context.DeadlineExceeded)
	case <-time.After(time.Second):
		t.Fatal("listener Close did not honor its deadline while trace done hook was blocked")
	}
	select {
	case <-traceGate.done:
		t.Fatal("listener Close trace done hook finished before it was released")
	default:
	}

	traceGate.release()
	select {
	case <-traceGate.done:
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for listener close trace done hook")
	}
	require.NoError(t, listener.Close(context.Background(), ErrUserCloseTopic))
}

func TestTopicListenerReconnectorWaitsForTraceDoneBeforeReconnect(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	first := newReconnectorTestStream("session-1")
	second := newReconnectorTestStream("session-2")
	client := newReconnectorTestClient(
		reconnectorTestConnectResult{stream: first},
		reconnectorTestConnectResult{stream: second},
	)
	listener, _ := newTestTopicListenerReconnector(t, client)

	require.NoError(t, listener.WaitInit(ctx))
	waitReconnectorCall(ctx, t, client, 0)

	listener.m.Lock()
	streamListener := listener.streamListener
	listener.m.Unlock()
	require.NotNil(t, streamListener)

	traceGate := newListenerCloseTraceGate()
	streamListener.tracer.OnListenerClose = traceGate.hook
	t.Cleanup(traceGate.release)

	first.recvErr <- xerrors.Transport(status.Error(codes.Unavailable, "stream failed"))
	select {
	case <-traceGate.callbackStarted:
	case <-ctx.Done():
		t.Fatal("timeout waiting for listener close trace done callback")
	}

	select {
	case call := <-client.calls:
		t.Fatalf("reconnected with attempt %d before listener close trace done hook", call)
	case <-time.After(time.Second):
	}
	closeCtx, closeCancel := context.WithTimeout(ctx, 100*time.Millisecond)
	closeResult := make(chan error, 1)
	go func() {
		closeResult <- listener.Close(closeCtx, ErrUserCloseTopic)
	}()
	select {
	case err := <-closeResult:
		require.ErrorIs(t, err, context.DeadlineExceeded)
	case <-ctx.Done():
		t.Fatal("listener Close did not honor its deadline while trace done hook was blocked")
	}
	closeCancel()

	waitCtx, waitCancel := context.WithTimeout(ctx, 100*time.Millisecond)
	require.ErrorIs(t, listener.WaitStop(waitCtx), context.DeadlineExceeded)
	waitCancel()

	traceGate.release()
	select {
	case <-traceGate.done:
	case <-ctx.Done():
		t.Fatal("timeout waiting for listener close trace done hook")
	}
	require.NoError(t, listener.WaitStop(ctx))
}

func TestNewStreamListenerWaitsForInitCleanupAndPreservesTransportError(t *testing.T) {
	initErr := status.Error(codes.Unavailable, "init stream transport failure")
	stream := &listenerInitFailureGRPCStream{recvErr: initErr}
	client := &listenerInitFailureClient{first: stream}
	traceGate := newListenerCloseStartTraceGate()

	cfg := NewStreamListenerConfig()
	cfg.Consumer = "test-consumer"
	cfg.Selectors = []*topicreadercommon.PublicReadSelector{{Path: "test-topic"}}
	cfg.Tracer.OnListenerClose = traceGate.hook

	result := make(chan streamListenerInitResult, 1)
	returned := make(chan struct{})
	t.Cleanup(func() {
		traceGate.release()
		select {
		case <-returned:
		case <-time.After(time.Second):
		}
	})
	go func() {
		listener, err := newStreamListener(
			context.Background(),
			client,
			newReconnectorTestHandler(),
			&cfg,
			&atomic.Int64{},
		)
		close(returned)
		result <- streamListenerInitResult{listener: listener, err: err}
	}()

	select {
	case <-traceGate.started:
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for failed listener init cleanup trace start hook")
	}
	select {
	case <-returned:
		t.Fatal("newStreamListener returned before failed init cleanup completed")
	case <-time.After(100 * time.Millisecond):
	}
	traceGate.release()
	var initResult streamListenerInitResult
	select {
	case initResult = <-result:
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for failed listener init cleanup")
	}
	require.Nil(t, initResult.listener)
	require.ErrorIs(t, initResult.err, initErr)

	select {
	case <-traceGate.done:
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for failed listener close trace done hook")
	}
	select {
	case <-stream.ctx.Done():
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for failed stream cancellation")
	}
	require.ErrorIs(t, context.Cause(stream.ctx), initErr)
}

func TestTopicListenerReconnectorWaitsForFailedInitCleanup(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	initErr := status.Error(codes.Unavailable, "init stream transport failure")
	first := &listenerInitFailureGRPCStream{recvErr: initErr}
	second := newReconnectorTestStream("session-2")
	client := &listenerInitFailureClient{
		first:       first,
		replacement: second,
		calls:       make(chan int, 4),
	}

	cfg := NewStreamListenerConfig()
	cfg.Consumer = "test-consumer"
	cfg.Selectors = []*topicreadercommon.PublicReadSelector{{Path: "test-topic"}}
	traceGate := newListenerCloseStartTraceGate()
	cfg.Tracer.OnListenerClose = traceGate.hook

	listener := newTopicListenerReconnector(
		client,
		&cfg,
		newReconnectorTestHandler(),
		xtest.FastClock(t),
	)
	t.Cleanup(func() {
		traceGate.release()
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), time.Second)
		defer cleanupCancel()
		_ = listener.Close(cleanupCtx, ErrUserCloseTopic)
		_ = listener.WaitStop(cleanupCtx)
	})

	waitListenerInitClientCall(ctx, t, client, 0)
	select {
	case <-traceGate.started:
	case <-ctx.Done():
		t.Fatal("timeout waiting for failed listener init cleanup trace start hook")
	}

	select {
	case call := <-client.calls:
		t.Fatalf("replacement connection attempt %d started before failed init cleanup completed", call)
	case <-time.After(100 * time.Millisecond):
	}
	closeCtx, closeCancel := context.WithTimeout(ctx, 100*time.Millisecond)
	closeResult := make(chan error, 1)
	go func() {
		closeResult <- listener.Close(closeCtx, ErrUserCloseTopic)
	}()
	select {
	case err := <-closeResult:
		require.ErrorIs(t, err, context.DeadlineExceeded)
	case <-ctx.Done():
		t.Fatal("listener Close did not honor its deadline while failed init cleanup was blocked")
	}
	closeCancel()

	waitCtx, waitCancel := context.WithTimeout(ctx, 100*time.Millisecond)
	require.ErrorIs(t, listener.WaitStop(waitCtx), context.DeadlineExceeded)
	waitCancel()

	traceGate.release()
	select {
	case <-traceGate.done:
	case <-ctx.Done():
		t.Fatal("timeout waiting for failed listener close trace done hook")
	}
	require.NoError(t, listener.WaitStop(ctx))
}

type listenerCloseTraceGate struct {
	started         chan struct{}
	callbackStarted chan struct{}
	done            chan struct{}
	releaseChan     chan struct{}
	startOnce       sync.Once
	callbackOnce    sync.Once
	doneOnce        sync.Once
	releaseOnce     sync.Once
}

type listenerCloseStartTraceGate struct {
	started     chan struct{}
	done        chan struct{}
	releaseChan chan struct{}
	startOnce   sync.Once
	doneOnce    sync.Once
	releaseOnce sync.Once
}

func newListenerCloseStartTraceGate() *listenerCloseStartTraceGate {
	return &listenerCloseStartTraceGate{
		started:     make(chan struct{}),
		done:        make(chan struct{}),
		releaseChan: make(chan struct{}),
	}
}

func (g *listenerCloseStartTraceGate) hook(
	trace.TopicListenerCloseStartInfo,
) func(trace.TopicListenerCloseDoneInfo) {
	g.startOnce.Do(func() { close(g.started) })
	<-g.releaseChan

	return func(trace.TopicListenerCloseDoneInfo) {
		g.doneOnce.Do(func() { close(g.done) })
	}
}

func (g *listenerCloseStartTraceGate) release() {
	g.releaseOnce.Do(func() { close(g.releaseChan) })
}

func newListenerCloseTraceGate() *listenerCloseTraceGate {
	return &listenerCloseTraceGate{
		started:         make(chan struct{}),
		callbackStarted: make(chan struct{}),
		done:            make(chan struct{}),
		releaseChan:     make(chan struct{}),
	}
}

func (g *listenerCloseTraceGate) hook(trace.TopicListenerCloseStartInfo) func(trace.TopicListenerCloseDoneInfo) {
	g.startOnce.Do(func() { close(g.started) })

	return func(trace.TopicListenerCloseDoneInfo) {
		g.callbackOnce.Do(func() { close(g.callbackStarted) })
		<-g.releaseChan
		g.doneOnce.Do(func() { close(g.done) })
	}
}

func (g *listenerCloseTraceGate) release() {
	g.releaseOnce.Do(func() { close(g.releaseChan) })
}

type streamListenerInitResult struct {
	listener *streamListener
	err      error
}

type listenerInitFailureGRPCStream struct {
	ctx     context.Context //nolint:containedctx
	recvErr error
}

func (s *listenerInitFailureGRPCStream) Send(*Ydb_Topic.StreamReadMessage_FromClient) error {
	return nil
}

func (s *listenerInitFailureGRPCStream) Recv() (*Ydb_Topic.StreamReadMessage_FromServer, error) {
	return nil, s.recvErr
}

func (s *listenerInitFailureGRPCStream) CloseSend() error {
	return nil
}

type listenerInitFailureClient struct {
	first       *listenerInitFailureGRPCStream
	replacement *reconnectorTestStream
	calls       chan int

	m     sync.Mutex
	count int
}

func (c *listenerInitFailureClient) StreamRead(
	ctx context.Context,
	_ int64,
	_ *trace.Topic,
) (rawtopicreader.StreamReader, error) {
	c.m.Lock()
	call := c.count
	c.count++
	c.m.Unlock()
	if c.calls != nil {
		c.calls <- call
	}

	if call == 0 {
		c.first.ctx = ctx

		return rawtopicreader.StreamReader{
			Stream: c.first,
			Tracer: &trace.Topic{},
		}, nil
	}
	if c.replacement == nil {
		<-ctx.Done()

		return rawtopicreader.StreamReader{}, ctx.Err()
	}
	c.replacement.ctx = ctx

	return rawtopicreader.StreamReader{
		Stream: c.replacement,
		Tracer: &trace.Topic{},
	}, nil
}

func waitListenerInitClientCall(
	ctx context.Context,
	t *testing.T,
	client *listenerInitFailureClient,
	expected int,
) {
	t.Helper()

	select {
	case call := <-client.calls:
		require.Equal(t, expected, call)
	case <-ctx.Done():
		t.Fatal("timeout waiting for listener connection attempt")
	}
}
