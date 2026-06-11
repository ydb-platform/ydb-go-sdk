//go:build integration
// +build integration

package integration

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Topic"
	"google.golang.org/grpc"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/endpoint"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicoptions"
)

// directWriteStreamChecker records how StreamWrite streams are opened on the client.
// It is used when the pinned partition is known up front (WithWriterPartitionID).
// On a single-node cluster matching nodeID to DescribeTopic is tautological; the
// check still verifies that the SDK enables disableFallback and passes the resolved node ID.
// Multi-writer and probe-and-rebind flows are covered by unit tests and grpc mocks.
type directWriteStreamChecker struct {
	mu       sync.Mutex
	sessions []directWriteStreamSession
}

type directWriteStreamSession struct {
	nodeID          uint32
	disableFallback bool
	initRequest     *Ydb_Topic.StreamWriteMessage_InitRequest
}

func newDirectWriteStreamChecker() *directWriteStreamChecker {
	return &directWriteStreamChecker{}
}

// Driver opens a dedicated cached driver instance that includes the StreamWrite interceptor.
// Do not pass the option to [scopeT.Driver]: fixenv caches drivers by name only, so opts
// on the default driver are ignored after the first connect.
// Each subtest must pass a unique driverName: the interceptor is bound to this checker
// instance and a reused driver name would record sessions into another checker.
func (c *directWriteStreamChecker) Driver(scope *scopeT, driverName string) *ydb.Driver {
	return scope.driverNamed(driverName, ydb.With(config.WithGrpcOptions(
		grpc.WithChainStreamInterceptor(c.streamInterceptor),
	)))
}

func (c *directWriteStreamChecker) streamInterceptor(
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

	session := directWriteStreamSession{
		disableFallback: endpoint.ContextDisableFallback(ctx),
	}
	if nodeID, ok := endpoint.ContextNodeID(ctx); ok {
		session.nodeID = nodeID
	}

	c.mu.Lock()
	c.sessions = append(c.sessions, session)
	sessionIndex := len(c.sessions) - 1
	c.mu.Unlock()

	stream, err := streamer(ctx, desc, cc, method, opts...)
	if err != nil {
		return nil, err
	}

	return &directWriteInitRecorder{
		ClientStream: stream,
		recordInitReq: func(req *Ydb_Topic.StreamWriteMessage_InitRequest) {
			c.mu.Lock()
			defer c.mu.Unlock()
			c.sessions[sessionIndex].initRequest = req
		},
	}, nil
}

// RequireRoutedToNode asserts that StreamWrite was opened in direct-write mode
// (disableFallback) targeting the given node ID.
func (c *directWriteStreamChecker) RequireRoutedToNode(t testing.TB, expectedNode uint32) {
	t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, session := range c.sessions {
		if session.disableFallback && session.nodeID == expectedNode {
			return
		}
	}
	require.Failf(t, "direct-write StreamWrite routing not found",
		"expected disableFallback=true nodeID=%d, sessions=%v", expectedNode, c.sessions)
}

// RequireInitGeneration asserts that a direct StreamWrite init request carried
// partition_with_generation matching DescribeTopic for the pinned partition.
func (c *directWriteStreamChecker) RequireInitGeneration(
	t testing.TB,
	partitionID int64,
	expectedGeneration int64,
) {
	t.Helper()

	c.mu.Lock()
	defer c.mu.Unlock()

	for _, session := range c.sessions {
		if !session.disableFallback {
			continue
		}

		initReq := session.initRequest
		require.NotNilf(t, initReq, "direct StreamWrite session has no recorded init request")

		pwg := initReq.GetPartitionWithGeneration()
		require.NotNilf(t, pwg, "init request must use partition_with_generation")
		require.Equal(t, partitionID, pwg.GetPartitionId())
		require.Equal(t, expectedGeneration, pwg.GetGeneration())

		return
	}

	require.Fail(t, "direct-write StreamWrite init with generation not found")
}

// RequireInitPartitionWithoutGeneration asserts that StreamWrite init used a plain
// partition_id (proxy path) and did not send partition_with_generation.
func (c *directWriteStreamChecker) RequireInitPartitionWithoutGeneration(
	t testing.TB,
	partitionID int64,
) {
	t.Helper()

	c.mu.Lock()
	defer c.mu.Unlock()

	require.NotEmpty(t, c.sessions, "expected at least one StreamWrite session")

	for i, session := range c.sessions {
		initReq := session.initRequest
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

type directWriteInitRecorder struct {
	grpc.ClientStream

	recordInitReq func(req *Ydb_Topic.StreamWriteMessage_InitRequest)
	recorded      bool
}

func (s *directWriteInitRecorder) SendMsg(m any) error {
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

func topicPartitionLocation(
	ctx context.Context,
	topicClient topic.Client,
	topicPath string,
	partitionID int64,
) (nodeID uint32, generation int64, err error) {
	desc, err := topicClient.Describe(ctx, topicPath, topicoptions.IncludeLocation())
	if err != nil {
		return 0, 0, err
	}
	for i := range desc.Partitions {
		p := &desc.Partitions[i]
		if p.PartitionID == partitionID {
			if p.Location.NodeID == 0 {
				return 0, 0, fmt.Errorf(
					"partition %d has empty location (IncludeLocation missing on server?)",
					partitionID,
				)
			}

			return p.Location.NodeID, p.Location.Generation, nil
		}
	}

	return 0, 0, fmt.Errorf("partition %d not found in topic %q", partitionID, topicPath)
}
