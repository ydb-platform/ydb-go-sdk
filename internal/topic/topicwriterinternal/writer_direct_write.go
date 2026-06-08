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

// directWrite holds partition resolution and proxy→direct rebind state for a topic writer.
type directWrite struct {
	// enabled turns on direct StreamWrite to the node that hosts the target partition.
	enabled bool

	// resolved holds the partition used for node lookup and connect routing.
	// -1 means unknown (connect via proxy until InitResponse); >= 0 is a concrete partition ID.
	// Shared by pointer so connect closures and the reconnector see the same value
	// even if WriterReconnectorConfig is copied.
	resolved *atomic.Int64

	// pinnedByUser is true when the caller set an explicit partition ID via WithPartitioning.
	// A pinned partition is never cleared after a session failure.
	pinnedByUser bool

	// original is defaultPartitioning before a server-learned partition is applied.
	// Restored when a learned partition must be dropped after a failed direct session.
	original rawtopicwriter.Partitioning

	// partitioning points at cfg.defaultPartitioning; updated when the partition is pinned or learned.
	partitioning *rawtopicwriter.Partitioning

	// retryResetPending is set after a successful proxy probe; the next connect must not count as a failed retry.
	retryResetPending bool
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

func (dw *directWrite) validate(partitioning rawtopicwriter.Partitioning, producerID string) error {
	if !dw.enabled ||
		partitioning.Type == rawtopicwriter.PartitioningPartitionID ||
		producerID != "" {
		return nil
	}

	return xerrors.WithStackTrace(errDirectWriteRequiresPartitionOrProducer)
}

// handleProbeInit records partition from a proxy InitResponse. Must be called with the writer mutex held.
// It returns true when the connection loop must rebind to the partition host node.
func (dw *directWrite) handleProbeInit(writer *SingleStreamWriter, setLastSeqNo func(int64)) bool {
	if !dw.awaitingPartition() {
		return false
	}

	dw.pinPartitionFromInit(writer.PartitionID)
	if writer.LastSeqNumRequested {
		setLastSeqNo(writer.ReceivedLastSeqNum)
	}

	return true
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

func (dw *directWrite) completeProbe(streamCtx context.Context, writer *SingleStreamWriter) {
	_ = writer.close(streamCtx, nil)
	dw.retryResetPending = true
}

func (dw *directWrite) consumeRetryReset() bool {
	if !dw.retryResetPending {
		return false
	}
	dw.retryResetPending = false

	return true
}

// dropLearnedPartitionIfNeeded clears a server-learned partition after a
// session failure so the next connect re-discovers via the proxy.
func (dw *directWrite) dropLearnedPartitionIfNeeded(reason error, withLock func(func())) {
	if reason == nil || !dw.enabled || dw.pinnedByUser || dw.resolved.Load() < 0 {
		return
	}

	dw.resolved.Store(-1)
	withLock(func() {
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
