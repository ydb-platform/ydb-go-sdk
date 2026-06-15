package topicwritetest

import (
	"context"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Topic"
	"google.golang.org/grpc"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/endpoint"
)

// StreamWriteSession records how a single StreamWrite stream was opened on the client.
type StreamWriteSession struct {
	NodeID          uint32
	DisableFallback bool
	InitRequest     *Ydb_Topic.StreamWriteMessage_InitRequest
}

// StreamWriteRecorder records StreamWrite client streams via a gRPC interceptor.
type StreamWriteRecorder struct {
	mu       sync.Mutex
	sessions []StreamWriteSession
}

// NewStreamWriteRecorder returns an empty StreamWrite session recorder.
func NewStreamWriteRecorder() *StreamWriteRecorder {
	return &StreamWriteRecorder{}
}

// Interceptor returns a gRPC stream client interceptor that records StreamWrite sessions.
func (r *StreamWriteRecorder) Interceptor() grpc.StreamClientInterceptor {
	return func(
		ctx context.Context,
		desc *grpc.StreamDesc,
		cc *grpc.ClientConn,
		method string,
		streamer grpc.Streamer,
		opts ...grpc.CallOption,
	) (grpc.ClientStream, error) {
		if !strings.Contains(method, "StreamWrite") {
			return streamer(ctx, desc, cc, method, opts...)
		}

		session := StreamWriteSession{
			DisableFallback: endpoint.ContextDisableFallback(ctx),
		}
		if nodeID, ok := endpoint.ContextNodeID(ctx); ok {
			session.NodeID = nodeID
		}

		r.mu.Lock()
		r.sessions = append(r.sessions, session)
		sessionIndex := len(r.sessions) - 1
		r.mu.Unlock()

		stream, err := streamer(ctx, desc, cc, method, opts...)
		if err != nil {
			return nil, err
		}

		return &streamWriteInitRecorder{
			ClientStream: stream,
			recordInitReq: func(req *Ydb_Topic.StreamWriteMessage_InitRequest) {
				r.mu.Lock()
				defer r.mu.Unlock()
				r.sessions[sessionIndex].InitRequest = req
			},
		}, nil
	}
}

// SessionsCount returns the number of recorded StreamWrite sessions.
func (r *StreamWriteRecorder) SessionsCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()

	return len(r.sessions)
}

// Session returns the recorded session at index.
func (r *StreamWriteRecorder) Session(index int) StreamWriteSession {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.sessions[index]
}

// LastSession returns the most recently recorded StreamWrite session.
func (r *StreamWriteRecorder) LastSession() StreamWriteSession {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.sessions[len(r.sessions)-1]
}

// RequireRoutedToNode asserts that StreamWrite was opened in direct-write mode
// (disableFallback) targeting the given node ID.
func (r *StreamWriteRecorder) RequireRoutedToNode(t testing.TB, expectedNode uint32) {
	t.Helper()

	r.mu.Lock()
	defer r.mu.Unlock()

	for _, session := range r.sessions {
		if session.DisableFallback && session.NodeID == expectedNode {
			return
		}
	}
	require.Failf(t, "direct-write StreamWrite routing not found",
		"expected disableFallback=true nodeID=%d, sessions=%v", expectedNode, r.sessions)
}

// RequireInitGeneration asserts that a direct StreamWrite init request carried
// partition_with_generation matching the expected partition and generation.
func (r *StreamWriteRecorder) RequireInitGeneration(
	t testing.TB,
	partitionID int64,
	expectedGeneration int64,
) {
	t.Helper()

	r.mu.Lock()
	defer r.mu.Unlock()

	for _, session := range r.sessions {
		if !session.DisableFallback {
			continue
		}

		RequireDirectInit(t, session.InitRequest, partitionID, expectedGeneration)

		return
	}

	require.Fail(t, "direct-write StreamWrite init with generation not found")
}

// RequireInitPartitionWithoutGeneration asserts that StreamWrite init used a plain
// partition_id (proxy path) and did not send partition_with_generation.
func (r *StreamWriteRecorder) RequireInitPartitionWithoutGeneration(
	t testing.TB,
	partitionID int64,
) {
	t.Helper()

	r.mu.Lock()
	defer r.mu.Unlock()

	require.NotEmpty(t, r.sessions, "expected at least one StreamWrite session")

	for i, session := range r.sessions {
		initReq := session.InitRequest
		require.NotNilf(t, initReq, "StreamWrite session %d has no recorded init request", i)
		require.Nilf(
			t,
			initReq.GetPartitionWithGeneration(),
			"StreamWrite session %d must not send partition_with_generation",
			i,
		)
		require.Equal(t, partitionID, initReq.GetPartitionId())

		return
	}
}

// RequireDirectInit asserts partition_with_generation on a single init request.
func RequireDirectInit(
	t testing.TB,
	initReq *Ydb_Topic.StreamWriteMessage_InitRequest,
	partitionID int64,
	generation int64,
) {
	t.Helper()

	require.NotNil(t, initReq)
	pwg := initReq.GetPartitionWithGeneration()
	require.NotNilf(t, pwg, "init request must use partition_with_generation")
	require.Equal(t, partitionID, pwg.GetPartitionId())
	require.Equal(t, generation, pwg.GetGeneration())
}

type streamWriteInitRecorder struct {
	grpc.ClientStream

	recordInitReq func(req *Ydb_Topic.StreamWriteMessage_InitRequest)
	recorded      bool
}

func (s *streamWriteInitRecorder) SendMsg(m any) error {
	if !s.recorded {
		if msg, ok := m.(*Ydb_Topic.StreamWriteMessage_FromClient); ok {
			if initReq := msg.GetInitRequest(); initReq != nil {
				s.recordInitReq(initReq)
				s.recorded = true
			}
		}
	}

	return s.ClientStream.SendMsg(m)
}
