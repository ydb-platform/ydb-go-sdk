package topicwriterinternal

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/Ydb_Topic_V1"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Operations"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Scheme"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Topic"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/anypb"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/endpoint"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopicwriter"
)

// TestDirectWriteResolvedPartitionSeed covers how the shared resolved-partition
// atomic is initialized by NewWriterReconnectorConfig. The connect path uses
// this value to decide whether to bind to a node (>= 0) or fall back to the
// proxy (-1) until the server tells us the partition.
func TestDirectWriteResolvedPartitionSeed(t *testing.T) {
	t.Run("UnknownWhenNoPartitionPinned", func(t *testing.T) {
		cfg := NewWriterReconnectorConfig(
			WithTopic("test-topic"),
			WithProducerID("p1"),
			WithDirectWrite(true),
		)
		require.NoError(t, cfg.validate())
		require.True(t, cfg.directWrite.resolved.unknown())
	})

	t.Run("UnknownWithMessageGroupID", func(t *testing.T) {
		cfg := NewWriterReconnectorConfig(
			WithTopic("test-topic"),
			WithProducerID("p1"),
			WithPartitioning(NewPartitioningWithMessageGroupID("p1")),
			WithDirectWrite(true),
		)
		require.NoError(t, cfg.validate())
		require.True(t, cfg.directWrite.resolved.unknown())
	})

	t.Run("SeededFromStaticPartitionID", func(t *testing.T) {
		cfg := NewWriterReconnectorConfig(
			WithTopic("test-topic"),
			WithPartitioning(NewPartitioningWithPartitionID(7)),
			WithDirectWrite(true),
		)
		require.NoError(t, cfg.validate())
		require.EqualValues(t, 7, cfg.directWrite.resolved.partitionIDValue())
	})

	t.Run("UnknownWhenDirectWriteDisabled", func(t *testing.T) {
		cfg := NewWriterReconnectorConfig(
			WithTopic("test-topic"),
			WithPartitioning(NewPartitioningWithPartitionID(7)),
			WithDirectWrite(false),
		)
		require.NoError(t, cfg.validate())
		require.True(t, cfg.directWrite.resolved.unknown())
	})
}

func TestDirectWriteValidate(t *testing.T) {
	t.Run("OkWithAutoProducer", func(t *testing.T) {
		cfg := NewWriterReconnectorConfig(
			WithTopic("test-topic"),
			WithDirectWrite(true),
		)
		require.NotEmpty(t, cfg.producerID)
		require.NoError(t, cfg.validate())
	})

	t.Run("OkWithPinnedPartition", func(t *testing.T) {
		cfg := NewWriterReconnectorConfig(
			WithTopic("test-topic"),
			WithPartitioning(NewPartitioningWithPartitionID(3)),
			WithDirectWrite(true),
		)
		require.NoError(t, cfg.validate())
	})

	t.Run("OkWithExplicitProducer", func(t *testing.T) {
		cfg := NewWriterReconnectorConfig(
			WithTopic("test-topic"),
			WithProducerID("producer-1"),
			WithDirectWrite(true),
		)
		require.NoError(t, cfg.validate())
	})
}

func TestResolvePartitionNode(t *testing.T) {
	const (
		topicPath   = "test-topic"
		partitionID = int64(7)
		nodeID      = int32(42)
		generation  = int64(3)
	)

	t.Run("HappyPath", func(t *testing.T) {
		stub := &topicServiceClientStub{
			describeTopic: func(in *Ydb_Topic.DescribeTopicRequest) (*Ydb_Topic.DescribeTopicResponse, error) {
				require.Equal(t, topicPath, in.GetPath())
				require.True(t, in.GetIncludeLocation())

				return describeTopicResponse(t, []*Ydb_Topic.DescribeTopicResult_PartitionInfo{
					{PartitionId: 0, PartitionLocation: &Ydb_Topic.PartitionLocation{NodeId: 100}},
					{
						PartitionId: partitionID,
						PartitionLocation: &Ydb_Topic.PartitionLocation{
							NodeId:     nodeID,
							Generation: generation,
						},
					},
					{PartitionId: 9, PartitionLocation: &Ydb_Topic.PartitionLocation{NodeId: 200}},
				}), nil
			},
		}
		rawClient := rawtopic.NewClient(stub)

		ctx, location, err := resolvePartitionNode(context.Background(), &rawClient, topicPath, partitionID)
		require.NoError(t, err)
		require.Equal(t, generation, location.Generation)

		got, ok := endpoint.ContextNodeID(ctx)
		require.True(t, ok)
		require.Equal(t, uint32(nodeID), got)
		require.True(t, endpoint.ContextDisableFallback(ctx))
	})

	t.Run("PartitionNotFound", func(t *testing.T) {
		stub := &topicServiceClientStub{
			describeTopic: func(_ *Ydb_Topic.DescribeTopicRequest) (*Ydb_Topic.DescribeTopicResponse, error) {
				return describeTopicResponse(t, []*Ydb_Topic.DescribeTopicResult_PartitionInfo{
					{PartitionId: 0, PartitionLocation: &Ydb_Topic.PartitionLocation{NodeId: 100}},
				}), nil
			},
		}
		rawClient := rawtopic.NewClient(stub)

		_, _, err := resolvePartitionNode(context.Background(), &rawClient, topicPath, partitionID)
		require.ErrorIs(t, err, errDirectWritePartitionNotFound)
	})

	t.Run("DescribeError", func(t *testing.T) {
		describeErr := errors.New("transport boom")
		stub := &topicServiceClientStub{
			describeTopic: func(_ *Ydb_Topic.DescribeTopicRequest) (*Ydb_Topic.DescribeTopicResponse, error) {
				return nil, describeErr
			},
		}
		rawClient := rawtopic.NewClient(stub)

		_, _, err := resolvePartitionNode(context.Background(), &rawClient, topicPath, partitionID)
		require.Error(t, err)
		require.ErrorIs(t, err, describeErr)
	})

	t.Run("OperationStatusError", func(t *testing.T) {
		// Server returned a logical error (e.g. SCHEME_ERROR for missing topic).
		// resolvePartitionNode must propagate the error so the writer's reconnect
		// loop (topic.RetryDecision) classifies it as non-retryable and stops.
		stub := &topicServiceClientStub{
			describeTopic: func(_ *Ydb_Topic.DescribeTopicRequest) (*Ydb_Topic.DescribeTopicResponse, error) {
				return &Ydb_Topic.DescribeTopicResponse{
					Operation: &Ydb_Operations.Operation{
						Ready:  true,
						Status: Ydb.StatusIds_SCHEME_ERROR,
					},
				}, nil
			},
		}
		rawClient := rawtopic.NewClient(stub)

		_, _, err := resolvePartitionNode(context.Background(), &rawClient, topicPath, partitionID)
		require.Error(t, err)
		require.Contains(t, err.Error(), "SCHEME_ERROR")
	})
}

func TestDirectWriteBindConnectContextResolvesLocation(t *testing.T) {
	const (
		topicPath   = "test-topic"
		partitionID = int64(7)
		generation  = int64(11)
	)

	stub := &topicServiceClientStub{
		describeTopic: func(_ *Ydb_Topic.DescribeTopicRequest) (*Ydb_Topic.DescribeTopicResponse, error) {
			return describeTopicResponse(t, []*Ydb_Topic.DescribeTopicResult_PartitionInfo{
				{
					PartitionId: partitionID,
					PartitionLocation: &Ydb_Topic.PartitionLocation{
						NodeId:     42,
						Generation: generation,
					},
				},
			}), nil
		},
	}
	rawClient := rawtopic.NewClient(stub)

	cfg := NewWriterReconnectorConfig(
		WithTopic(topicPath),
		WithPartitioning(NewPartitioningWithPartitionID(partitionID)),
		WithDirectWrite(true),
	)
	cfg.rawTopicClient = &rawClient

	_, err := cfg.directWrite.bindConnectContext(context.Background(), &rawClient, topicPath)
	require.NoError(t, err)
	require.Equal(t, generation, cfg.directWrite.resolved.generationValue())

	partitioning := rawtopicwriter.NewPartitioningPartitionID(partitionID)
	cfg.directWrite.applyResolvedToPartitioning(&partitioning)
	require.Equal(
		t,
		rawtopicwriter.NewPartitioningPartitionWithGeneration(partitionID, generation),
		partitioning,
	)
}

func describeTopicResponse(
	t *testing.T,
	partitions []*Ydb_Topic.DescribeTopicResult_PartitionInfo,
) *Ydb_Topic.DescribeTopicResponse {
	t.Helper()

	result := &Ydb_Topic.DescribeTopicResult{
		Self: &Ydb_Scheme.Entry{
			Name: "test-topic",
			Type: Ydb_Scheme.Entry_TOPIC,
		},
		PartitioningSettings: &Ydb_Topic.PartitioningSettings{
			MinActivePartitions:      int64(len(partitions)),
			AutoPartitioningSettings: &Ydb_Topic.AutoPartitioningSettings{},
		},
		Partitions: partitions,
	}
	resp := &Ydb_Topic.DescribeTopicResponse{
		Operation: &Ydb_Operations.Operation{
			Ready:  true,
			Status: Ydb.StatusIds_SUCCESS,
			Result: &anypb.Any{},
		},
	}
	require.NoError(t, resp.GetOperation().GetResult().MarshalFrom(result))

	return resp
}

// topicServiceClientStub is a minimal hand-rolled stub of Ydb_Topic_V1.TopicServiceClient.
// It only implements DescribeTopic; other methods panic to make accidental use obvious.
type topicServiceClientStub struct {
	describeTopic func(in *Ydb_Topic.DescribeTopicRequest) (*Ydb_Topic.DescribeTopicResponse, error)
}

var _ Ydb_Topic_V1.TopicServiceClient = (*topicServiceClientStub)(nil)

func (s *topicServiceClientStub) StreamWrite(
	_ context.Context, _ ...grpc.CallOption,
) (Ydb_Topic_V1.TopicService_StreamWriteClient, error) {
	panic("StreamWrite not stubbed")
}

func (s *topicServiceClientStub) StreamRead(
	_ context.Context, _ ...grpc.CallOption,
) (Ydb_Topic_V1.TopicService_StreamReadClient, error) {
	panic("StreamRead not stubbed")
}

func (s *topicServiceClientStub) CommitOffset(
	_ context.Context, _ *Ydb_Topic.CommitOffsetRequest, _ ...grpc.CallOption,
) (*Ydb_Topic.CommitOffsetResponse, error) {
	panic("CommitOffset not stubbed")
}

func (s *topicServiceClientStub) UpdateOffsetsInTransaction(
	_ context.Context, _ *Ydb_Topic.UpdateOffsetsInTransactionRequest, _ ...grpc.CallOption,
) (*Ydb_Topic.UpdateOffsetsInTransactionResponse, error) {
	panic("UpdateOffsetsInTransaction not stubbed")
}

func (s *topicServiceClientStub) CreateTopic(
	_ context.Context, _ *Ydb_Topic.CreateTopicRequest, _ ...grpc.CallOption,
) (*Ydb_Topic.CreateTopicResponse, error) {
	panic("CreateTopic not stubbed")
}

func (s *topicServiceClientStub) DescribeTopic(
	_ context.Context, in *Ydb_Topic.DescribeTopicRequest, _ ...grpc.CallOption,
) (*Ydb_Topic.DescribeTopicResponse, error) {
	return s.describeTopic(in)
}

func (s *topicServiceClientStub) DescribeConsumer(
	_ context.Context, _ *Ydb_Topic.DescribeConsumerRequest, _ ...grpc.CallOption,
) (*Ydb_Topic.DescribeConsumerResponse, error) {
	panic("DescribeConsumer not stubbed")
}

func (s *topicServiceClientStub) AlterTopic(
	_ context.Context, _ *Ydb_Topic.AlterTopicRequest, _ ...grpc.CallOption,
) (*Ydb_Topic.AlterTopicResponse, error) {
	panic("AlterTopic not stubbed")
}

func (s *topicServiceClientStub) DropTopic(
	_ context.Context, _ *Ydb_Topic.DropTopicRequest, _ ...grpc.CallOption,
) (*Ydb_Topic.DropTopicResponse, error) {
	panic("DropTopic not stubbed")
}
