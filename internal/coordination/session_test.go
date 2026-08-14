package coordination

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Coordination"
	"go.uber.org/mock/gomock"

	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func TestReceiveLoopUpdatesLastGoodResponseTimeBeforeSessionStarted(t *testing.T) {
	const testTimeout = 5 * time.Second

	ctrl := gomock.NewController(t)
	client := NewMockCoordinationService_SessionClient(ctrl)
	testCtx, cancelTest := context.WithTimeout(context.Background(), testTimeout)
	defer cancelTest()
	streamCtx, cancelStream := context.WithCancel(context.Background())
	defer cancelStream()
	startedResponse := &Ydb_Coordination.SessionResponse_SessionStarted{
		SessionId: 42,
	}
	recvReturned := make(chan struct{})

	firstRecv := client.EXPECT().Recv().DoAndReturn(func() (*Ydb_Coordination.SessionResponse, error) {
		close(recvReturned)

		return &Ydb_Coordination.SessionResponse{
			Response: &Ydb_Coordination.SessionResponse_SessionStarted_{
				SessionStarted: startedResponse,
			},
		}, nil
	})
	secondRecv := client.EXPECT().Recv().DoAndReturn(func() (*Ydb_Coordination.SessionResponse, error) {
		<-streamCtx.Done()

		return nil, streamCtx.Err()
	})
	gomock.InOrder(firstRecv.Call, secondRecv.Call)

	s := &session{
		trace: &trace.Coordination{},
	}
	sessionStarted := make(chan *Ydb_Coordination.SessionResponse_SessionStarted, 1)
	sessionStopped := make(chan *Ydb_Coordination.SessionResponse_SessionStopped, 1)
	wg := sync.WaitGroup{}
	wg.Add(1)
	s.mutex.Lock()
	receiveLoopDone := make(chan struct{})
	go func() {
		defer close(receiveLoopDone)
		s.receiveLoop(&wg, client, cancelStream, sessionStarted, sessionStopped)
	}()

	select {
	case <-recvReturned:
	case <-receiveLoopDone:
		s.mutex.Unlock()
		t.Fatal("receive loop exited before the first Recv returned")
	case <-testCtx.Done():
		s.mutex.Unlock()
		t.Fatal("timed out waiting for the first Recv")
	}
	var start *Ydb_Coordination.SessionResponse_SessionStarted
	orderCheckTimer := time.NewTimer(100 * time.Millisecond) //nolint:mnd
	select {
	case start = <-sessionStarted:
	case <-orderCheckTimer.C:
	case <-receiveLoopDone:
		orderCheckTimer.Stop()
		s.mutex.Unlock()
		t.Fatal("receive loop exited before publishing session started")
	case <-testCtx.Done():
		orderCheckTimer.Stop()
		s.mutex.Unlock()
		t.Fatal("timed out checking session started publication order")
	}
	orderCheckTimer.Stop()
	publishedBeforeTimestamp := start != nil
	s.mutex.Unlock()
	if start == nil {
		select {
		case start = <-sessionStarted:
		case <-receiveLoopDone:
			t.Fatal("receive loop exited before publishing session started")
		case <-testCtx.Done():
			t.Fatal("timed out waiting for session started")
		}
	}

	cancelStream()
	select {
	case <-receiveLoopDone:
	case <-testCtx.Done():
		t.Fatal("timed out waiting for receive loop to stop")
	}
	require.False(t, publishedBeforeTimestamp, "session started was published before the keep-alive timestamp update")
	require.Same(t, startedResponse, start)
	require.False(t, s.getLastGoodResponseTime().IsZero())
}

func TestNewProtectionKey(t *testing.T) {
	key1 := newProtectionKey()
	require.NotNil(t, key1)
	require.Len(t, key1, 8)

	key2 := newProtectionKey()
	require.NotNil(t, key2)
	require.Len(t, key2, 8)

	// Protection keys should be different (with very high probability)
	require.NotEqual(t, key1, key2)
}

func TestNewReqID(t *testing.T) {
	id1 := newReqID()
	id2 := newReqID()

	// IDs should be different (with very high probability)
	require.NotEqual(t, id1, id2)
}
