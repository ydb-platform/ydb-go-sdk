package topicwritetest

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"

	"github.com/ydb-platform/ydb-go-genproto/Ydb_Topic_V1"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Issue"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Topic"
)

// DirectWriteCluster is a gRPC TopicService mock for direct-write scenario tests.
type DirectWriteCluster struct {
	Ydb_Topic_V1.UnimplementedTopicServiceServer

	TopicPath          string
	ProbePartitionID   int64
	mu                 sync.Mutex
	partitions         map[int64]partitionLocation
	rejectedGeneration int64
	afterDescribeHook  func(call int)
	describeCount      atomic.Int32
	streamCount        atomic.Int32
}

type partitionLocation struct {
	nodeID     int32
	generation int64
}

// NewDirectWriteCluster seeds one partition location for direct-write tests.
func NewDirectWriteCluster(
	topicPath string,
	partitionID int64,
	nodeID int32,
	generation int64,
) *DirectWriteCluster {
	cluster := &DirectWriteCluster{
		TopicPath:        topicPath,
		ProbePartitionID: partitionID,
		partitions:       make(map[int64]partitionLocation),
	}
	cluster.SetPartitionLocation(partitionID, nodeID, generation)

	return cluster
}

// SetPartitionLocation updates DescribeTopic metadata for a partition.
func (c *DirectWriteCluster) SetPartitionLocation(partitionID int64, nodeID int32, generation int64) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.partitions[partitionID] = partitionLocation{
		nodeID:     nodeID,
		generation: generation,
	}
}

// RejectInitGeneration makes StreamWrite fail when init uses the given generation.
func (c *DirectWriteCluster) RejectInitGeneration(generation int64) {
	c.rejectedGeneration = generation
}

// AfterDescribe registers a hook invoked after each DescribeTopic call.
func (c *DirectWriteCluster) AfterDescribe(hook func(call int)) {
	c.afterDescribeHook = hook
}

// DescribeCalls returns how many DescribeTopic calls were served.
func (c *DirectWriteCluster) DescribeCalls() int {
	return int(c.describeCount.Load())
}

func (c *DirectWriteCluster) DescribeTopic(
	_ context.Context,
	in *Ydb_Topic.DescribeTopicRequest,
) (*Ydb_Topic.DescribeTopicResponse, error) {
	if !in.GetIncludeLocation() {
		return nil, errors.New("IncludeLocation required")
	}

	call := int(c.describeCount.Add(1))
	if c.afterDescribeHook != nil {
		c.afterDescribeHook(call)
	}

	c.mu.Lock()
	partitions := make([]*Ydb_Topic.DescribeTopicResult_PartitionInfo, 0, len(c.partitions))
	for partitionID, location := range c.partitions {
		partitions = append(partitions, &Ydb_Topic.DescribeTopicResult_PartitionInfo{
			PartitionId: partitionID,
			PartitionLocation: &Ydb_Topic.PartitionLocation{
				NodeId:     location.nodeID,
				Generation: location.generation,
			},
		})
	}
	c.mu.Unlock()

	resp, _ := DescribeTopicResponse(c.TopicPath, partitions)

	return resp, nil
}

func (c *DirectWriteCluster) StreamWrite(server Ydb_Topic_V1.TopicService_StreamWriteServer) error {
	c.streamCount.Add(1)

	initMsg, err := server.Recv()
	if err != nil {
		return fmt.Errorf("read init message: %w", err)
	}

	initReq := initMsg.GetInitRequest()
	if initReq == nil {
		return errors.New("first message must be init request")
	}

	if pwg := initReq.GetPartitionWithGeneration(); pwg != nil {
		if pwg.GetGeneration() == c.rejectedGeneration {
			return server.Send(&Ydb_Topic.StreamWriteMessage_FromServer{
				Status: Ydb.StatusIds_ABORTED,
				Issues: []*Ydb_Issue.IssueMessage{
					{Message: "partition generation mismatch"},
				},
			})
		}

		return c.serveDirectSession(server, pwg.GetPartitionId())
	}

	return c.serveProbeSession(server)
}

func (c *DirectWriteCluster) serveProbeSession(server Ydb_Topic_V1.TopicService_StreamWriteServer) error {
	if err := c.sendInitResponse(server, c.ProbePartitionID); err != nil {
		return err
	}

	// Probe stream is closed by the SDK right after InitResponse.
	_, err := server.Recv()
	if errors.Is(err, io.EOF) {
		return nil
	}

	return err
}

func (c *DirectWriteCluster) serveDirectSession(
	server Ydb_Topic_V1.TopicService_StreamWriteServer,
	partitionID int64,
) error {
	if err := c.sendInitResponse(server, partitionID); err != nil {
		return err
	}

	messagesMsg, err := server.Recv()
	if err != nil {
		return fmt.Errorf("read write request: %w", err)
	}

	writeReq, ok := messagesMsg.GetClientMessage().(*Ydb_Topic.StreamWriteMessage_FromClient_WriteRequest)
	if !ok || len(writeReq.WriteRequest.GetMessages()) == 0 {
		return errors.New("expected non-empty write request")
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
				PartitionId:     partitionID,
				WriteStatistics: &Ydb_Topic.StreamWriteMessage_WriteResponse_WriteStatistics{},
			},
		},
	})
}

func (c *DirectWriteCluster) sendInitResponse(
	server Ydb_Topic_V1.TopicService_StreamWriteServer,
	partitionID int64,
) error {
	return server.Send(&Ydb_Topic.StreamWriteMessage_FromServer{
		Status: Ydb.StatusIds_SUCCESS,
		ServerMessage: &Ydb_Topic.StreamWriteMessage_FromServer_InitResponse{
			InitResponse: &Ydb_Topic.StreamWriteMessage_InitResponse{
				LastSeqNo:       0,
				SessionId:       "test-session",
				PartitionId:     partitionID,
				SupportedCodecs: nil,
			},
		},
	})
}
