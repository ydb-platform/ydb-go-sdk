package topicwriterinternal

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/Ydb_Topic_V1"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Operations"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Topic"
	"google.golang.org/grpc"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/endpoint"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopicwriter"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicwritetest"
)

func TestDirectWriteConfig(t *testing.T) {
	t.Run("RequiresProducerOrPinnedPartition", func(t *testing.T) {
		cfg := WriterReconnectorConfig{
			directWrite: directWrite{enabled: true},
			WritersCommonConfig: WritersCommonConfig{
				topic:               "test-topic",
				defaultPartitioning: rawtopicwriter.Partitioning{Type: rawtopicwriter.PartitioningUndefined},
				producerID:          "",
			},
		}

		err := cfg.validate()
		require.Error(t, err)
		require.Contains(t, err.Error(), errDirectWriteRequiresPartitionOrProducer.Error())
	})

	tests := []struct {
		name            string
		opts            []PublicWriterOption
		wantPartitionID int64
	}{
		{
			name: "ProducerOnlyWaitsForServerPartition",
			opts: []PublicWriterOption{
				WithTopic("test-topic"),
				WithProducerID("producer-1"),
				WithDirectWrite(true),
			},
			wantPartitionID: -1,
		},
		{
			name: "PinnedPartitionReadyForDescribe",
			opts: []PublicWriterOption{
				WithTopic("test-topic"),
				WithPartitioning(NewPartitioningWithPartitionID(7)),
				WithDirectWrite(true),
			},
			wantPartitionID: 7,
		},
		{
			name: "DirectWriteDisabledIgnoresPinnedPartition",
			opts: []PublicWriterOption{
				WithTopic("test-topic"),
				WithPartitioning(NewPartitioningWithPartitionID(7)),
				WithDirectWrite(false),
			},
			wantPartitionID: -1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := NewWriterReconnectorConfig(tt.opts...)
			require.NoError(t, cfg.validate())
			if tt.wantPartitionID < 0 {
				require.True(t, cfg.directWrite.resolved.unknown())
			} else {
				require.EqualValues(t, tt.wantPartitionID, cfg.directWrite.resolved.partitionIDValue())
			}
		})
	}
}

func TestResolvePartitionNode(t *testing.T) {
	const (
		topicPath   = "test-topic"
		partitionID = int64(7)
	)

	tests := []struct {
		name           string
		partitions     []*Ydb_Topic.DescribeTopicResult_PartitionInfo
		describeTopic  func(in *Ydb_Topic.DescribeTopicRequest) (*Ydb_Topic.DescribeTopicResponse, error)
		wantNodeID     uint32
		wantGeneration int64
		wantErr        error
		wantErrSubstr  string
	}{
		{
			name: "ReturnsHostNodeAndGenerationFromDescribe",
			partitions: []*Ydb_Topic.DescribeTopicResult_PartitionInfo{
				{PartitionId: 0, PartitionLocation: &Ydb_Topic.PartitionLocation{NodeId: 100}},
				{
					PartitionId: partitionID,
					PartitionLocation: &Ydb_Topic.PartitionLocation{
						NodeId:     42,
						Generation: 3,
					},
				},
			},
			wantNodeID:     42,
			wantGeneration: 3,
		},
		{
			name: "PartitionSplitOrRemoved",
			partitions: []*Ydb_Topic.DescribeTopicResult_PartitionInfo{
				{PartitionId: 0, PartitionLocation: &Ydb_Topic.PartitionLocation{NodeId: 100}},
			},
			wantErr: errDirectWritePartitionNotFound,
		},
		{
			name: "DescribeTransportError",
			describeTopic: func(_ *Ydb_Topic.DescribeTopicRequest) (*Ydb_Topic.DescribeTopicResponse, error) {
				return nil, errors.New("transport boom")
			},
			wantErrSubstr: "transport boom",
		},
		{
			name: "DescribeSchemeErrorStopsWriter",
			describeTopic: func(_ *Ydb_Topic.DescribeTopicRequest) (*Ydb_Topic.DescribeTopicResponse, error) {
				return &Ydb_Topic.DescribeTopicResponse{
					Operation: &Ydb_Operations.Operation{
						Ready:  true,
						Status: Ydb.StatusIds_SCHEME_ERROR,
					},
				}, nil
			},
			wantErrSubstr: "SCHEME_ERROR",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stub := &topicServiceClientStub{
				describeTopic: tt.describeTopic,
			}
			if stub.describeTopic == nil {
				partitions := tt.partitions
				stub.describeTopic = func(in *Ydb_Topic.DescribeTopicRequest) (*Ydb_Topic.DescribeTopicResponse, error) {
					require.Equal(t, topicPath, in.GetPath())
					require.True(t, in.GetIncludeLocation())

					resp, err := topicwritetest.DescribeTopicResponse(topicPath, partitions)
					require.NoError(t, err)

					return resp, nil
				}
			}

			rawClient := rawtopic.NewClient(stub)

			ctx, location, err := resolvePartitionNode(context.Background(), &rawClient, topicPath, partitionID)
			if tt.wantErr != nil || tt.wantErrSubstr != "" {
				require.Error(t, err)
				if tt.wantErr != nil {
					require.ErrorIs(t, err, tt.wantErr)
				}
				if tt.wantErrSubstr != "" {
					require.Contains(t, err.Error(), tt.wantErrSubstr)
				}

				return
			}

			require.NoError(t, err)
			require.Equal(t, tt.wantGeneration, location.Generation)

			gotNodeID, ok := endpoint.ContextNodeID(ctx)
			require.True(t, ok)
			require.Equal(t, tt.wantNodeID, gotNodeID)
			require.True(t, endpoint.ContextDisableFallback(ctx))
		})
	}
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
