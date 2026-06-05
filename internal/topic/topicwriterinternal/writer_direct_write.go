package topicwriterinternal

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/endpoint"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopicwriter"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
)

var (
	errDirectWritePartitionNotFound = xerrors.Wrap(
		errors.New("ydb: direct write: target partition not found in topic description"),
	)
	errDirectWriteRequiresPartitionOrProducer = xerrors.Wrap(
		errors.New("ydb: direct write: requires WithWriterPartitionID or WithProducerID"),
	)
)

// directWrite holds partition resolution and proxy→direct rebind state for a
// topic writer. resolved is shared by pointer so connect closures and the
// reconnector see the same value even if WriterReconnectorConfig is copied.
type directWrite struct {
	enabled bool

	resolved     *atomic.Int64
	pinnedByUser bool
	original     rawtopicwriter.Partitioning
	partitioning *rawtopicwriter.Partitioning
}

func (dw *directWrite) finishInit(partitioning *rawtopicwriter.Partitioning) {
	dw.original = *partitioning
	dw.partitioning = partitioning
	dw.resolved = &atomic.Int64{}

	if dw.enabled && partitioning.Type == rawtopicwriter.PartitioningPartitionID {
		dw.resolved.Store(partitioning.PartitionID)
		dw.pinnedByUser = true

		return
	}

	dw.resolved.Store(-1)
}

func (dw *directWrite) validateForWriter(
	partitioning rawtopicwriter.Partitioning,
	producerID string,
) error {
	if !dw.enabled {
		return nil
	}

	if partitioning.Type == rawtopicwriter.PartitioningPartitionID {
		return nil
	}

	if producerID != "" {
		return nil
	}

	return xerrors.WithStackTrace(errDirectWriteRequiresPartitionOrProducer)
}

// bindConnectContext resolves the node for a known partition and binds the
// gRPC call via endpoint.WithNodeID(..., endpoint.WithDisableFallback()). When the partition is still unknown (-1)
// the context is returned unchanged (proxy connect).
func (dw *directWrite) bindConnectContext(
	ctx context.Context,
	rawClient *rawtopic.Client,
	topicPath string,
) (context.Context, error) {
	if !dw.enabled {
		return ctx, nil
	}

	partitionID := dw.resolved.Load()
	if partitionID < 0 {
		return ctx, nil
	}

	return resolvePartitionNode(ctx, rawClient, topicPath, partitionID)
}

type directWriteLocker interface {
	WithLock(callback func())
}

// dropLearnedPartitionIfNeeded clears a server-learned partition after a
// session failure so the next connect re-discovers via the proxy.
func (dw *directWrite) dropLearnedPartitionIfNeeded(reason error, locker directWriteLocker) {
	if reason == nil || !dw.enabled || dw.pinnedByUser || dw.resolved.Load() < 0 {
		return
	}

	dw.resolved.Store(-1)
	locker.WithLock(func() {
		*dw.partitioning = dw.original
	})
}

func (dw *directWrite) awaitingPartition() bool {
	return dw.enabled && dw.resolved.Load() < 0
}

// pinPartitionFromInit records the partition from InitResponse for the rebound
// connect. Must be called with the writer mutex held.
func (dw *directWrite) pinPartitionFromInit(partitionID int64) {
	dw.resolved.Store(partitionID)
	*dw.partitioning = rawtopicwriter.NewPartitioningPartitionID(partitionID)
}

func (dw *directWrite) closeStreamAfterRebind(streamCtx context.Context, writer *SingleStreamWriter) {
	_ = writer.close(streamCtx, nil)
}

// resolvePartitionNode looks up which node currently hosts the given partition
// of the topic and returns a context bound to that node with disableFallback enabled.
func resolvePartitionNode(
	ctx context.Context,
	rawClient *rawtopic.Client,
	topicPath string,
	partitionID int64,
) (context.Context, error) {
	res, err := rawClient.DescribeTopic(ctx, rawtopic.DescribeTopicRequest{
		Path:            topicPath,
		IncludeLocation: true,
	})
	if err != nil {
		return nil, xerrors.WithStackTrace(fmt.Errorf("ydb: direct write: describe topic failed: %w", err))
	}

	for i := range res.Partitions {
		p := &res.Partitions[i]
		if p.PartitionID == partitionID {
			nodeID := uint32(p.PartitionLocation.NodeID)

			return endpoint.WithNodeID(ctx, nodeID, endpoint.WithDisableFallback()), nil
		}
	}

	return nil, xerrors.WithStackTrace(fmt.Errorf(
		"%w: topic=%q partition_id=%d",
		errDirectWritePartitionNotFound, topicPath, partitionID,
	))
}
