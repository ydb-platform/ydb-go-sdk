package topicwritetest

import (
	"fmt"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Operations"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Scheme"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Topic"
	"google.golang.org/protobuf/types/known/anypb"
)

// DescribeTopicResponse builds a successful DescribeTopic gRPC response for tests.
func DescribeTopicResponse(
	topicPath string,
	partitions []*Ydb_Topic.DescribeTopicResult_PartitionInfo,
) (*Ydb_Topic.DescribeTopicResponse, error) {
	result := &Ydb_Topic.DescribeTopicResult{
		Self: &Ydb_Scheme.Entry{
			Name: topicPath,
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
	if err := resp.GetOperation().GetResult().MarshalFrom(result); err != nil {
		return nil, fmt.Errorf("marshal describe topic result: %w", err)
	}

	return resp, nil
}
