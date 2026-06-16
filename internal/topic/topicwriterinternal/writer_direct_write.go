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

func validateDirectWrite(
	enabled bool,
	partitioning rawtopicwriter.Partitioning,
	producerID string,
) error {
	if !enabled ||
		partitioning.Type == rawtopicwriter.PartitioningPartitionID ||
		producerID != "" {
		return nil
	}

	return xerrors.WithStackTrace(errDirectWriteRequiresPartitionOrProducer)
}

// lookupPartitionLocation looks up which node currently hosts the given partition.
func (w *WriterReconnector) lookupPartitionLocation(
	ctx context.Context,
	partitionID int64,
) (rawtopic.PartitionLocation, error) {
	res, err := w.cfg.rawTopicClient.DescribeTopic(ctx, rawtopic.DescribeTopicRequest{
		Path:            w.cfg.topic,
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
			errDirectWritePartitionNotFound, w.cfg.topic, partitionID,
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
	if partitionID, ok := w.cfg.PartitionID(); ok &&
		w.cfg.defaultPartitioning.Type == rawtopicwriter.PartitioningPartitionID {
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
