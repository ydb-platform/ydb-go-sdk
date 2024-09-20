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

	expected := &Ydb_Topic.UpdateOffsetsInTransactionRequest{
		OperationParams: &Ydb_Operations.OperationParams{
			OperationMode:    Ydb_Operations.OperationParams_SYNC,
			OperationTimeout: durationpb.New(time.Second),
			CancelAfter:      durationpb.New(time.Minute),
		},
		Tx: &Ydb_Topic.TransactionIdentity{
			Id:      "tx-id",
			Session: "session-id",
		},
		Topics: []*Ydb_Topic.UpdateOffsetsInTransactionRequest_TopicOffsets{
			{
				Path: "test-topic",
				Partitions: []*Ydb_Topic.UpdateOffsetsInTransactionRequest_TopicOffsets_PartitionOffsets{
					{
						PartitionId: 1,
						PartitionOffsets: []*Ydb_Topic.OffsetsRange{
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
						PartitionId: 5,
						PartitionOffsets: []*Ydb_Topic.OffsetsRange{
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
				Partitions: []*Ydb_Topic.UpdateOffsetsInTransactionRequest_TopicOffsets_PartitionOffsets{
					{
						PartitionId: 56,
						PartitionOffsets: []*Ydb_Topic.OffsetsRange{
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
	}

	require.Equal(t, expected, res)
}
