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

// resolvedPartition holds direct-write partition resolution state shared across
// WriterReconnectorConfig copies via pointer.
type resolvedPartition struct {
	// partitionID is the target partition for direct write.
	// -1 means unknown (connect via proxy until InitResponse).
	partitionID atomic.Int64
	// generation is from the latest DescribeTopic when partitionID >= 0.
	// -1 means not yet resolved.
	generation atomic.Int64
}

func newResolvedPartition() *resolvedPartition {
	rp := &resolvedPartition{}
	rp.clear()

	return rp
}

func (rp *resolvedPartition) clear() {
	rp.partitionID.Store(-1)
	rp.generation.Store(-1)
}

func (rp *resolvedPartition) setPartitionID(partitionID int64) {
	rp.partitionID.Store(partitionID)
	rp.generation.Store(-1)
}

func (rp *resolvedPartition) partitionIDValue() int64 {
	return rp.partitionID.Load()
}

func (rp *resolvedPartition) setLocation(generation int64) {
	rp.generation.Store(generation)
}

func (rp *resolvedPartition) generationValue() int64 {
	return rp.generation.Load()
}

func (rp *resolvedPartition) unknown() bool {
	return rp.partitionID.Load() < 0
}

func (rp *resolvedPartition) initPartitioning() (rawtopicwriter.Partitioning, bool) {
	partitionID := rp.partitionID.Load()
	generation := rp.generation.Load()
	if partitionID < 0 || generation < 0 {
		return rawtopicwriter.Partitioning{}, false
	}

	return rawtopicwriter.NewPartitioningPartitionWithGeneration(partitionID, generation), true
}

// directWrite holds partition resolution and proxy→direct rebind state for a topic writer.
type directWrite struct {
	// enabled turns on direct StreamWrite to the node that hosts the target partition.
	enabled bool

	// resolved holds partition ID and generation used for node lookup and InitRequest.
	// Shared by pointer so connect closures and the reconnector see the same value
	// even if WriterReconnectorConfig is copied.
	resolved *resolvedPartition

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
	dw.resolved = newResolvedPartition()

	if dw.enabled && partitioning.Type == rawtopicwriter.PartitioningPartitionID {
		dw.resolved.setPartitionID(partitioning.PartitionID)
		dw.pinnedByUser = true

		return
	}
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

	partitionID := dw.resolved.partitionIDValue()
	if partitionID < 0 {
		return ctx, nil
	}

	streamCtx, location, err := resolvePartitionNode(ctx, rawClient, topicPath, partitionID)
	if err != nil {
		return nil, err
	}

	dw.resolved.setLocation(location.Generation)

	return streamCtx, nil
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
	if reason == nil || !dw.enabled || dw.pinnedByUser || dw.resolved.unknown() {
		return
	}

	dw.resolved.clear()
	withLock(func() {
		*dw.partitioning = dw.original
	})
}

func (dw *directWrite) awaitingPartition() bool {
	return dw.enabled && dw.resolved.unknown()
}

// pinPartitionFromInit records the partition from InitResponse for the rebound
// connect. Must be called with the writer mutex held.
func (dw *directWrite) pinPartitionFromInit(partitionID int64) {
	dw.resolved.setPartitionID(partitionID)
	*dw.partitioning = rawtopicwriter.NewPartitioningPartitionID(partitionID)
}

// applyResolvedToPartitioning updates init partitioning with partition ID and generation
// resolved by the latest DescribeTopic during direct connect.
func (dw *directWrite) applyResolvedToPartitioning(partitioning *rawtopicwriter.Partitioning) {
	if !dw.enabled {
		return
	}

	if resolved, ok := dw.resolved.initPartitioning(); ok {
		*partitioning = resolved
	}
}

// resolvePartitionNode looks up which node currently hosts the given partition
// of the topic and returns a context bound to that node with disableFallback enabled.
func resolvePartitionNode(
	ctx context.Context,
	rawClient *rawtopic.Client,
	topicPath string,
	partitionID int64,
) (context.Context, rawtopic.PartitionLocation, error) {
	res, err := rawClient.DescribeTopic(ctx, rawtopic.DescribeTopicRequest{
		Path:            topicPath,
		IncludeLocation: true,
	})
	if err != nil {
		return nil, rawtopic.PartitionLocation{}, xerrors.WithStackTrace(
			fmt.Errorf("ydb: direct write: describe topic failed: %w", err),
		)
	}

	location, ok := res.LocationOf(partitionID)
	if !ok {
		return nil, rawtopic.PartitionLocation{}, xerrors.WithStackTrace(fmt.Errorf(
			"%w: topic=%q partition_id=%d",
			errDirectWritePartitionNotFound, topicPath, partitionID,
		))
	}

	return endpoint.WithNodeID(
		ctx,
		location.NodeIDUint32(),
		endpoint.WithDisableFallback(),
	), location, nil
}
