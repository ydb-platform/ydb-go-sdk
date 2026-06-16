package topicwriterinternal

import (
	"context"
	"errors"
	"fmt"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopicwriter"
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

// directWrite holds direct-write settings for a topic writer.
type directWrite struct {
	// enabled turns on direct StreamWrite to the node that hosts the target partition.
	enabled bool

	// initPartitioning is set for the current connect attempt after DescribeTopic.
	initPartitioning rawtopicwriter.Partitioning
}

func (dw *directWrite) validate(partitioning rawtopicwriter.Partitioning, producerID string) error {
	if !dw.enabled ||
		partitioning.Type == rawtopicwriter.PartitioningPartitionID ||
		producerID != "" {
		return nil
	}

	return xerrors.WithStackTrace(errDirectWriteRequiresPartitionOrProducer)
}

func (dw *directWrite) clearConnectState() {
	dw.initPartitioning = rawtopicwriter.Partitioning{}
}

func (dw *directWrite) connectInitPartitioning(
	defaultPartitioning rawtopicwriter.Partitioning,
) rawtopicwriter.Partitioning {
	if dw.initPartitioning.Type == rawtopicwriter.PartitioningPartitionWithGeneration {
		return dw.initPartitioning
	}

	return defaultPartitioning
}

func pinnedPartitionID(partitioning rawtopicwriter.Partitioning) (int64, bool) {
	if partitioning.Type == rawtopicwriter.PartitioningPartitionID {
		return partitioning.PartitionID, true
	}

	return 0, false
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

func (w *WriterReconnector) resolveDirectWritePartition(ctx context.Context) (int64, error) {
	if partitionID, ok := pinnedPartitionID(w.cfg.defaultPartitioning); ok {
		return partitionID, nil
	}

	getLastSeqNo := w.needReceiveLastSeqNo()
	partitionID, lastSeqNo, err := probeWriterPartition(
		xcontext.MergeContexts(ctx, w.cfg.LogContext),
		w.cfg.rawTopicClient,
		w.cfg.Tracer,
		rawtopicwriter.InitRequest{
			Path:             w.cfg.topic,
			ProducerID:       w.cfg.producerID,
			WriteSessionMeta: w.cfg.writerMeta,
			Partitioning:     w.cfg.defaultPartitioning,
			GetLastSeqNo:     getLastSeqNo,
		},
	)
	if err != nil {
		return 0, err
	}

	if getLastSeqNo {
		w.m.WithLock(func() {
			w.lastSeqNo = lastSeqNo
		})
	}

	return partitionID, nil
}

func (w *WriterReconnector) resolveDirectWriteHost(
	ctx context.Context,
	partitionID int64,
) (uint32, error) {
	location, err := lookupPartitionLocation(
		xcontext.MergeContexts(ctx, w.cfg.LogContext),
		w.cfg.rawTopicClient,
		w.cfg.topic,
		partitionID,
	)
	if err != nil {
		return 0, err
	}

	w.cfg.directWrite.initPartitioning = rawtopicwriter.NewPartitioningPartitionWithGeneration(
		partitionID,
		location.Generation,
	)

	return location.NodeIDUint32(), nil
}
