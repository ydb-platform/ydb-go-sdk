package topicwriterinternal

import (
	"context"
	"errors"
	"fmt"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopicwriter"
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

// resolvedPartition holds direct-write partition resolution state.
// Access is owned by the writer connection loop and methods that require its mutex.
type resolvedPartition struct {
	// partitionID is the target partition for direct write.
	// -1 means unknown until resolved by a proxy probe.
	partitionID int64
	// generation is from the latest DescribeTopic when partitionID >= 0.
	// -1 means not yet resolved.
	generation int64
}

func newResolvedPartition() *resolvedPartition {
	rp := &resolvedPartition{}
	rp.clear()

	return rp
}

func (rp *resolvedPartition) clear() {
	rp.partitionID = -1
	rp.generation = -1
}

func (rp *resolvedPartition) setPartitionID(partitionID int64) {
	rp.partitionID = partitionID
	rp.generation = -1
}

func (rp *resolvedPartition) partitionIDValue() int64 {
	return rp.partitionID
}

func (rp *resolvedPartition) setLocation(generation int64) {
	rp.generation = generation
}

func (rp *resolvedPartition) unknown() bool {
	return rp.partitionID < 0
}

func (rp *resolvedPartition) initPartitioning() (rawtopicwriter.Partitioning, bool) {
	if rp.partitionID < 0 || rp.generation < 0 {
		return rawtopicwriter.Partitioning{}, false
	}

	return rawtopicwriter.NewPartitioningPartitionWithGeneration(rp.partitionID, rp.generation), true
}

// directWrite holds partition resolution state for a topic writer.
type directWrite struct {
	// enabled turns on direct StreamWrite to the node that hosts the target partition.
	enabled bool

	// resolved holds partition ID and generation used for node lookup and InitRequest.
	resolved *resolvedPartition

	// pinnedByUser is true when the caller set an explicit partition ID via WithPartitioning.
	// A user-pinned partition is never cleared after a session failure.
	pinnedByUser bool

	// original is defaultPartitioning before a proxy probe updated partitioning.
	// Restored when auto-resolved partition state is reset after a failed direct session.
	original rawtopicwriter.Partitioning

	// partitioning points at cfg.defaultPartitioning; updated when pinned by user or set from InitResponse.
	partitioning *rawtopicwriter.Partitioning
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

	dw.pinnedByUser = false
}

func (dw *directWrite) validate(partitioning rawtopicwriter.Partitioning, producerID string) error {
	if !dw.enabled ||
		partitioning.Type == rawtopicwriter.PartitioningPartitionID ||
		producerID != "" {
		return nil
	}

	return xerrors.WithStackTrace(errDirectWriteRequiresPartitionOrProducer)
}

// resetAutoResolvedPartitionOnFailure clears an auto-resolved partition after a failed
// session and rolls back defaultPartitioning changed by a proxy probe.
// User-pinned partitions are left unchanged.
// Must be called with the writer mutex held.
func (dw *directWrite) resetAutoResolvedPartitionOnFailure(reason error) {
	if reason == nil || !dw.enabled || dw.pinnedByUser || dw.resolved.unknown() {
		return
	}

	dw.resolved.clear()
	dw.restoreOriginalPartitioning()
}

// restoreOriginalPartitioning rolls back defaultPartitioning changed by a proxy probe.
// Must be called with the writer mutex held.
func (dw *directWrite) restoreOriginalPartitioning() {
	*dw.partitioning = dw.original
}

// recordProbedPartition stores partition ID discovered by a synchronous proxy probe.
// Must be called with the writer mutex held.
func (dw *directWrite) recordProbedPartition(partitionID int64) {
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

// lookupPartitionLocation looks up which node currently hosts the given partition.
func lookupPartitionLocation(
	ctx context.Context,
	rawClient *rawtopic.Client,
	topicPath string,
	partitionID int64,
) (rawtopic.PartitionLocation, error) {
	res, err := rawClient.DescribeTopic(ctx, rawtopic.DescribeTopicRequest{
		Path:            topicPath,
		IncludeLocation: true,
	})
	if err != nil {
		return rawtopic.PartitionLocation{}, xerrors.WithStackTrace(
			fmt.Errorf("ydb: direct write: describe topic failed: %w", err),
		)
	}

	location, ok := res.LocationOf(partitionID)
	if !ok {
		return rawtopic.PartitionLocation{}, xerrors.WithStackTrace(fmt.Errorf(
			"%w: topic=%q partition_id=%d",
			errDirectWritePartitionNotFound, topicPath, partitionID,
		))
	}

	return location, nil
}

// probeWriterPartition opens a short-lived proxy StreamWrite session, sends InitRequest,
// reads InitResponse, and returns the assigned partition ID.
func probeWriterPartition(
	ctx context.Context,
	rawClient *rawtopic.Client,
	tracer *trace.Topic,
	req rawtopicwriter.InitRequest,
) (partitionID int64, lastSeqNo int64, err error) {
	stream, err := rawClient.StreamWrite(ctx, tracer)
	if err != nil {
		return 0, 0, err
	}
	defer func() {
		_ = stream.CloseSend()
	}()

	if err = stream.Send(&req); err != nil {
		return 0, 0, err
	}

	recvMessage, err := stream.Recv()
	if err != nil {
		return 0, 0, err
	}

	result, ok := recvMessage.(*rawtopicwriter.InitResult)
	if !ok {
		return 0, 0, xerrors.WithStackTrace(fmt.Errorf(
			"ydb: direct write: probe init response has unexpected type: %T",
			recvMessage,
		))
	}

	return result.PartitionID, result.LastSeqNo, nil
}
