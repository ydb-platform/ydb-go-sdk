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
	result := Ydb_Topic.DescribeTopicResult_builder{
		Self: Ydb_Scheme.Entry_builder{
			Name: topicPath,
			Type: Ydb_Scheme.Entry_TOPIC,
		}.Build(),
		PartitioningSettings: Ydb_Topic.PartitioningSettings_builder{
			MinActivePartitions:      int64(len(partitions)),
			AutoPartitioningSettings: Ydb_Topic.AutoPartitioningSettings_builder{}.Build(),
		}.Build(),
		Partitions: partitions,
	}.Build()
	resp := Ydb_Topic.DescribeTopicResponse_builder{
		Operation: Ydb_Operations.Operation_builder{
			Ready:  true,
			Status: Ydb.StatusIds_SUCCESS,
			Result: &anypb.Any{},
		}.Build(),
	}.Build()
	if err := resp.GetOperation().GetResult().MarshalFrom(result); err != nil {
		return nil, fmt.Errorf("marshal describe topic result: %w", err)
	}

	return resp, nil
}
