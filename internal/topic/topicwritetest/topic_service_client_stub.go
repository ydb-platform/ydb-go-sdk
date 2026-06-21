package topicwritetest

import (
	"context"

	"github.com/ydb-platform/ydb-go-genproto/Ydb_Topic_V1"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Topic"
	"google.golang.org/grpc"
)

// TopicServiceClientDescribeOnly returns a TopicServiceClient stub that only
// implements DescribeTopic; other RPCs panic on accidental use.
func TopicServiceClientDescribeOnly(
	describe func(in *Ydb_Topic.DescribeTopicRequest) (*Ydb_Topic.DescribeTopicResponse, error),
) Ydb_Topic_V1.TopicServiceClient {
	return &topicServiceClientStub{describeTopic: describe}
}

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
