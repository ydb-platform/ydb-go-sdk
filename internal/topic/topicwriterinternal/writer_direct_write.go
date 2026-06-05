package topicwriterinternal

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/jonboulle/clockwork"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/endpoint"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopicwriter"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xcontext"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
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

func newWriterConnectFunc(cfg *WriterReconnectorConfig) ConnectFunc {
	return func(ctx context.Context, tracer *trace.Topic) (RawTopicWriterStream, error) {
		mergedCtx := xcontext.MergeContexts(ctx, cfg.LogContext)
		resolvedCtx, err := cfg.directWrite.bindConnectContext(mergedCtx, cfg.rawTopicClient, cfg.topic)
		if err != nil {
			return nil, err
		}

		return cfg.rawTopicClient.StreamWrite(resolvedCtx, tracer)
	}
}

func (dw *directWrite) bumpConnectionAttempt(
	clock clockwork.Clock,
	connectTimeout time.Duration,
	reconnectReason error,
	withLock func(func()),
	prevAttemptTime time.Time,
	attempt int,
	startOfRetries time.Time,
) (newAttempt int, newStartOfRetries, now time.Time) {
	dw.dropLearnedPartitionIfNeeded(reconnectReason, withLock)

	now = time.Now()
	if dw.consumeRetryReset() ||
		startOfRetries.IsZero() ||
		topic.CheckResetReconnectionCounters(prevAttemptTime, now, connectTimeout) {
		return 0, clock.Now(), now
	}

	return attempt + 1, startOfRetries, now
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
