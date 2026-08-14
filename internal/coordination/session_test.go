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
	ctrl := gomock.NewController(t)
	client := NewMockCoordinationService_SessionClient(ctrl)
	streamCtx, cancelStream := context.WithCancel(context.Background())
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
	go s.receiveLoop(&wg, client, cancelStream, sessionStarted, sessionStopped)

	<-recvReturned
	var start *Ydb_Coordination.SessionResponse_SessionStarted
	select {
	case start = <-sessionStarted:
	case <-time.After(100 * time.Millisecond): //nolint:mnd
	}
	publishedBeforeTimestamp := start != nil
	s.mutex.Unlock()
	if start == nil {
		start = <-sessionStarted
	}

	cancelStream()
	wg.Wait()
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
