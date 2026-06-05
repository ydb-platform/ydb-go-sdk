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
	mu    sync.Mutex
	calls []directWriteStreamCall
}

type directWriteStreamCall struct {
	NodeID          uint32
	DisableFallback bool
}

func newDirectWriteStreamChecker() *directWriteStreamChecker {
	return &directWriteStreamChecker{}
}

func (c *directWriteStreamChecker) DriverOption() ydb.Option {
	return ydb.With(config.WithGrpcOptions(
		grpc.WithChainStreamInterceptor(c.streamInterceptor),
	))
}

// Driver opens a dedicated cached driver instance that includes the StreamWrite interceptor.
// Do not pass the option to [scopeT.Driver]: fixenv caches drivers by name only, so opts
// on the default driver are ignored after the first connect.
func (c *directWriteStreamChecker) Driver(scope *scopeT) *ydb.Driver {
	return scope.driverNamed("direct-write-routing", c.DriverOption())
}

func (c *directWriteStreamChecker) streamInterceptor(
	ctx context.Context,
	desc *grpc.StreamDesc,
	cc *grpc.ClientConn,
	method string,
	streamer grpc.Streamer,
	opts ...grpc.CallOption,
) (grpc.ClientStream, error) {
	if strings.Contains(method, "StreamWrite") {
		nodeID, hasNode := endpoint.ContextNodeID(ctx)
		if !hasNode {
			nodeID = 0
		}
		c.mu.Lock()
		c.calls = append(c.calls, directWriteStreamCall{
			NodeID:          nodeID,
			DisableFallback: endpoint.ContextDisableFallback(ctx),
		})
		c.mu.Unlock()
	}

	return streamer(ctx, desc, cc, method, opts...)
}

// RequireRoutedToNode asserts that StreamWrite was opened in direct-write mode
// (disableFallback) targeting the given node ID.
func (c *directWriteStreamChecker) RequireRoutedToNode(t testing.TB, expectedNode uint32) {
	t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, call := range c.calls {
		if call.DisableFallback && call.NodeID == expectedNode {
			return
		}
	}
	require.Failf(t, "direct-write StreamWrite routing not found",
		"expected disableFallback=true nodeID=%d, calls=%v", expectedNode, c.calls)
}

func topicPartitionHostNodeID(
	ctx context.Context,
	topicClient topic.Client,
	topicPath string,
	partitionID int64,
) (uint32, error) {
	desc, err := topicClient.Describe(ctx, topicPath, topicoptions.IncludeLocation())
	if err != nil {
		return 0, err
	}
	for i := range desc.Partitions {
		p := &desc.Partitions[i]
		if p.PartitionID == partitionID {
			if p.Location.NodeID == 0 {
				return 0, fmt.Errorf("partition %d has empty location (IncludeLocation missing on server?)", partitionID)
			}
			return p.Location.NodeID, nil
		}
	}

	return 0, fmt.Errorf("partition %d not found in topic %q", partitionID, topicPath)
}
