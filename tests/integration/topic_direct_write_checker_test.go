//go:build integration
// +build integration

package integration

import (
	"context"
	"fmt"
	"testing"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicwritetest"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicoptions"
	"google.golang.org/grpc"
)

// directWriteStreamChecker records how StreamWrite streams are opened on the client.
// It is used when the pinned partition is known up front (WithWriterPartitionID).
// On a single-node cluster matching nodeID to DescribeTopic is tautological; the
// check still verifies that the SDK enables disableFallback and passes the resolved node ID.
// Multi-writer and probe-and-rebind flows are covered by unit tests and grpc mocks.
type directWriteStreamChecker struct {
	*topicwritetest.StreamWriteRecorder
}

func newDirectWriteStreamChecker() *directWriteStreamChecker {
	return &directWriteStreamChecker{
		StreamWriteRecorder: topicwritetest.NewStreamWriteRecorder(),
	}
}

// Driver opens a dedicated cached driver instance that includes the StreamWrite interceptor.
// Do not pass the option to [scopeT.Driver]: fixenv caches drivers by name only, so opts
// on the default driver are ignored after the first connect.
// Each subtest must pass a unique driverName: the interceptor is bound to this checker
// instance and a reused driver name would record sessions into another checker.
func (c *directWriteStreamChecker) Driver(scope *scopeT, driverName string) *ydb.Driver {
	return scope.driverNamed(driverName, ydb.With(config.WithGrpcOptions(
		grpc.WithChainStreamInterceptor(c.Interceptor()),
	)))
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
