package topicwriterinternal

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Operations"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Topic"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/endpoint"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopicwriter"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicwritetest"
)

func TestDirectWriteConfig(t *testing.T) {
	t.Run("RequiresProducerOrPinnedPartition", func(t *testing.T) {
		cfg := WriterReconnectorConfig{
			directWriteEnabled: true,
			WritersCommonConfig: WritersCommonConfig{
				topic:        "test-topic",
				partitioning: rawtopicwriter.Partitioning{Type: rawtopicwriter.PartitioningUndefined},
				producerID:   "",
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
		wantPinned      bool
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
			wantPinned:      true,
		},
		{
			name: "DirectWriteDisabledKeepsPinnedPartitionInConfig",
			opts: []PublicWriterOption{
				WithTopic("test-topic"),
				WithPartitioning(NewPartitioningWithPartitionID(7)),
				WithDirectWrite(false),
			},
			wantPartitionID: 7,
			wantPinned:      true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := NewWriterReconnectorConfig(tt.opts...)
			require.NoError(t, cfg.validate())

			partitionID, ok := cfg.PartitionID()
			if tt.wantPinned {
				require.True(t, ok)
				require.EqualValues(t, tt.wantPartitionID, partitionID)
			} else {
				require.False(t, ok)
			}
		})
	}
}

func TestWithPartitioningSetsPinnedPartitionID(t *testing.T) {
	cfg := NewWriterReconnectorConfig(
		WithTopic("test"),
		WithDirectWrite(true),
	)

	partitionID, ok := cfg.PartitionID()
	require.False(t, ok)
	require.Zero(t, partitionID)

	WithPartitioning(NewPartitioningWithPartitionID(7))(&cfg)

	partitionID, ok = cfg.PartitionID()
	require.True(t, ok)
	require.EqualValues(t, 7, partitionID)
}

func TestLookupPartitionLocation(t *testing.T) {
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
			describe := tt.describeTopic
			if describe == nil {
				partitions := tt.partitions
				describe = func(in *Ydb_Topic.DescribeTopicRequest) (*Ydb_Topic.DescribeTopicResponse, error) {
					require.Equal(t, topicPath, in.GetPath())
					require.True(t, in.GetIncludeLocation())

					resp, err := topicwritetest.DescribeTopicResponse(topicPath, partitions)
					require.NoError(t, err)

					return resp, nil
				}
			}

			rawClient := rawtopic.NewClient(topicwritetest.TopicServiceClientDescribeOnly(describe))
			writer := &WriterReconnector{
				cfg: WriterReconnectorConfig{
					WritersCommonConfig: WritersCommonConfig{
						topic:          topicPath,
						rawTopicClient: &rawClient,
					},
				},
			}

			location, err := writer.lookupPartitionLocation(context.Background(), partitionID)
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

			ctx := endpoint.WithNodeID(
				context.Background(),
				location.NodeIDUint32(),
				endpoint.WithFallback(false),
			)
			gotNodeID, ok := endpoint.ContextNodeID(ctx)
			require.True(t, ok)
			require.Equal(t, tt.wantNodeID, gotNodeID)
			require.False(t, endpoint.ContextFallback(ctx))
		})
	}
}
