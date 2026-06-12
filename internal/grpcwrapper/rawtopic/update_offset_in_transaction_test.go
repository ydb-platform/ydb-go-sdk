package rawtopic

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Operations"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Topic"
	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawoptional"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopiccommon"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawydb"
)

func TestUpdateOffsetsInTransactionRequestToProto(t *testing.T) {
	res := (&UpdateOffsetsInTransactionRequest{
		OperationParams: rawydb.OperationParams{
			OperationMode: rawydb.OperationParamsModeSync,
			OperationTimeout: rawoptional.Duration{
				Value:    time.Second,
				HasValue: true,
			},
			CancelAfter: rawoptional.Duration{
				Value:    time.Minute,
				HasValue: true,
			},
		},
		Tx: rawtopiccommon.TransactionIdentity{
			ID:      "tx-id",
			Session: "session-id",
		},
		Topics: []UpdateOffsetsInTransactionRequest_TopicOffsets{
			{
				Path: "test-topic",
				Partitions: []UpdateOffsetsInTransactionRequest_PartitionOffsets{
					{
						PartitionID: 1,
						PartitionOffsets: []rawtopiccommon.OffsetRange{
							{
								Start: 1,
								End:   2,
							},
							{
								Start: 10,
								End:   20,
							},
						},
					},
					{
						PartitionID: 5,
						PartitionOffsets: []rawtopiccommon.OffsetRange{
							{
								Start: 15,
								End:   25,
							},
							{
								Start: 35,
								End:   55,
							},
						},
					},
				},
			},
			{
				Path: "test-topic-2",
				Partitions: []UpdateOffsetsInTransactionRequest_PartitionOffsets{
					{
						PartitionID: 56,
						PartitionOffsets: []rawtopiccommon.OffsetRange{
							{
								Start: 11,
								End:   52,
							},
						},
					},
				},
			},
		},
		Consumer: "test-consumer",
	}).ToProto()

	expected := Ydb_Topic.UpdateOffsetsInTransactionRequest_builder{
		OperationParams: Ydb_Operations.OperationParams_builder{
			OperationMode:    Ydb_Operations.OperationParams_SYNC,
			OperationTimeout: durationpb.New(time.Second),
			CancelAfter:      durationpb.New(time.Minute),
		}.Build(),
		Tx: Ydb_Topic.TransactionIdentity_builder{
			Id:      "tx-id",
			Session: "session-id",
		}.Build(),
		Topics: []*Ydb_Topic.UpdateOffsetsInTransactionRequest_TopicOffsets{
			Ydb_Topic.UpdateOffsetsInTransactionRequest_TopicOffsets_builder{
				Path: "test-topic",
				Partitions: []*Ydb_Topic.UpdateOffsetsInTransactionRequest_TopicOffsets_PartitionOffsets{
					Ydb_Topic.UpdateOffsetsInTransactionRequest_TopicOffsets_PartitionOffsets_builder{
						PartitionId: 1,
						PartitionOffsets: []*Ydb_Topic.OffsetsRange{
							Ydb_Topic.OffsetsRange_builder{
								Start: 1,
								End:   2,
							}.Build(),
							Ydb_Topic.OffsetsRange_builder{
								Start: 10,
								End:   20,
							}.Build(),
						},
					}.Build(),
					Ydb_Topic.UpdateOffsetsInTransactionRequest_TopicOffsets_PartitionOffsets_builder{
						PartitionId: 5,
						PartitionOffsets: []*Ydb_Topic.OffsetsRange{
							Ydb_Topic.OffsetsRange_builder{
								Start: 15,
								End:   25,
							}.Build(),
							Ydb_Topic.OffsetsRange_builder{
								Start: 35,
								End:   55,
							}.Build(),
						},
					}.Build(),
				},
			}.Build(),
			Ydb_Topic.UpdateOffsetsInTransactionRequest_TopicOffsets_builder{
				Path: "test-topic-2",
				Partitions: []*Ydb_Topic.UpdateOffsetsInTransactionRequest_TopicOffsets_PartitionOffsets{
					Ydb_Topic.UpdateOffsetsInTransactionRequest_TopicOffsets_PartitionOffsets_builder{
						PartitionId: 56,
						PartitionOffsets: []*Ydb_Topic.OffsetsRange{
							Ydb_Topic.OffsetsRange_builder{
								Start: 11,
								End:   52,
							}.Build(),
						},
					}.Build(),
				},
			}.Build(),
		},
		Consumer: "test-consumer",
	}.Build()

	require.Equal(t, expected, res)
}
