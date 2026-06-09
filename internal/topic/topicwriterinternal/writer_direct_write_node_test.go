package topicwriterinternal_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/rekby/fixenv"
	"github.com/rekby/fixenv/sf"
	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/Ydb_Topic_V1"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Operations"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Scheme"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Topic"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/anypb"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/endpoint"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicmock"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicoptions"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicwriter"
)

// TestDirectWriteStreamInitUsesPartitionHostNodeID checks that a pinned partition
// with direct write routes StreamWrite to the node returned by DescribeTopic, not
// to another endpoint from discovery.
func TestDirectWriteStreamInitUsesPartitionHostNodeID(t *testing.T) {
	e := fixenv.New(t)

	const (
		partitionID = int64(7)
		hostNodeID  = int32(2)
		generation  = int64(5)
	)

	mock := newDirectWriteHostNodeTopicService(t, hostNodeID, generation)
	connString := topicmock.GrpcMockTopicConnStringWithNodeIDs(e, mock, []uint32{1, 2})

	var streamWriteNodeID atomic.Uint32
	streamNodeInterceptor := func(
		ctx context.Context,
		desc *grpc.StreamDesc,
		cc *grpc.ClientConn,
		method string,
		streamer grpc.Streamer,
		opts ...grpc.CallOption,
	) (grpc.ClientStream, error) {
		if strings.Contains(method, "StreamWrite") {
			if nodeID, ok := endpoint.ContextNodeID(ctx); ok {
				streamWriteNodeID.Store(nodeID)
			}
		}

		return streamer(ctx, desc, cc, method, opts...)
	}

	db, err := ydb.Open(sf.Context(e), connString,
		ydb.With(config.WithGrpcOptions(
			grpc.WithChainStreamInterceptor(streamNodeInterceptor),
		)),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close(sf.Context(e)) })

	writer, err := db.Topic().StartWriter("test",
		topicoptions.WithWriterPartitionID(partitionID),
		topicoptions.WithWriterDirectWrite(true),
		topicoptions.WithWriterWaitServerAck(true),
	)
	require.NoError(t, err)

	err = writer.Write(sf.Context(e), topicwriter.Message{
		Data: strings.NewReader("payload"),
	})
	require.NoError(t, err)

	require.Equal(t, uint32(hostNodeID), streamWriteNodeID.Load(),
		"StreamWrite must be opened with disable-fallback routing to the partition host node")
}

type directWriteHostNodeTopicService struct {
	Ydb_Topic_V1.UnimplementedTopicServiceServer

	t          testing.TB
	hostNodeID int32
	generation int64
}

func newDirectWriteHostNodeTopicService(
	t testing.TB,
	hostNodeID int32,
	generation int64,
) *directWriteHostNodeTopicService {
	return &directWriteHostNodeTopicService{
		t:          t,
		hostNodeID: hostNodeID,
		generation: generation,
	}
}

func (s *directWriteHostNodeTopicService) DescribeTopic(
	_ context.Context,
	in *Ydb_Topic.DescribeTopicRequest,
) (*Ydb_Topic.DescribeTopicResponse, error) {
	if !in.GetIncludeLocation() {
		return nil, errors.New("IncludeLocation required")
	}

	return describeTopicResponseForDirectWriteNode(s.t, []*Ydb_Topic.DescribeTopicResult_PartitionInfo{
		{PartitionId: 0, PartitionLocation: &Ydb_Topic.PartitionLocation{NodeId: 1}},
		{
			PartitionId: 7,
			PartitionLocation: &Ydb_Topic.PartitionLocation{
				NodeId:     s.hostNodeID,
				Generation: s.generation,
			},
		},
	}), nil
}

func (s *directWriteHostNodeTopicService) StreamWrite(server Ydb_Topic_V1.TopicService_StreamWriteServer) error {
	initMsg, err := server.Recv()
	if err != nil {
		return fmt.Errorf("failed read init message: %w", err)
	}
	initReq := initMsg.GetInitRequest()
	if initReq == nil {
		return errors.New("first message must be init message")
	}
	partitionWithGeneration := initReq.GetPartitionWithGeneration()
	if partitionWithGeneration == nil {
		return errors.New("init request must include partition with generation")
	}
	if partitionWithGeneration.GetPartitionId() != 7 {
		return fmt.Errorf("unexpected partition id: %d", partitionWithGeneration.GetPartitionId())
	}
	if partitionWithGeneration.GetGeneration() != s.generation {
		return fmt.Errorf("unexpected generation: %d", partitionWithGeneration.GetGeneration())
	}

	err = server.Send(&Ydb_Topic.StreamWriteMessage_FromServer{
		Status: Ydb.StatusIds_SUCCESS,
		ServerMessage: &Ydb_Topic.StreamWriteMessage_FromServer_InitResponse{
			InitResponse: &Ydb_Topic.StreamWriteMessage_InitResponse{
				LastSeqNo:       0,
				SessionId:       "test",
				PartitionId:     7,
				SupportedCodecs: nil,
			},
		},
	})
	if err != nil {
		return fmt.Errorf("failed to send init response: %w", err)
	}

	messagesMsg, err := server.Recv()
	if err != nil {
		return errors.New("failed to read messages block")
	}

	writeReq, ok := messagesMsg.GetClientMessage().(*Ydb_Topic.StreamWriteMessage_FromClient_WriteRequest)
	if !ok || len(writeReq.WriteRequest.GetMessages()) == 0 {
		return errors.New("received zero messages block")
	}

	return server.Send(&Ydb_Topic.StreamWriteMessage_FromServer{
		Status: Ydb.StatusIds_SUCCESS,
		ServerMessage: &Ydb_Topic.StreamWriteMessage_FromServer_WriteResponse{
			WriteResponse: &Ydb_Topic.StreamWriteMessage_WriteResponse{
				Acks: []*Ydb_Topic.StreamWriteMessage_WriteResponse_WriteAck{
					{
						SeqNo: 1,
						MessageWriteStatus: &Ydb_Topic.StreamWriteMessage_WriteResponse_WriteAck_Written_{
							Written: &Ydb_Topic.StreamWriteMessage_WriteResponse_WriteAck_Written{
								Offset: 1,
							},
						},
					},
				},
				PartitionId:     7,
				WriteStatistics: &Ydb_Topic.StreamWriteMessage_WriteResponse_WriteStatistics{},
			},
		},
	})
}

func describeTopicResponseForDirectWriteNode(
	t testing.TB,
	partitions []*Ydb_Topic.DescribeTopicResult_PartitionInfo,
) *Ydb_Topic.DescribeTopicResponse {
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
