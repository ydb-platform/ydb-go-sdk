package topicwriterinternal_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/rekby/fixenv"
	"github.com/rekby/fixenv/sf"
	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/Ydb_Topic_V1"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Issue"
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

const (
	testTopicPath   = "test-topic"
	testPartitionID = int64(7)
	testProxyNodeID = uint32(1)
	testHostNodeID  = int32(2)
	testMovedNodeID = int32(3)
	testInitialGen  = int64(5)
	testMovedGen    = int64(6)
)

// TestDirectWritePinnedPartitionConnectsToDescribeHost sends the very first
// StreamWrite to the node returned by DescribeTopic and includes partition
// generation in InitRequest.
func TestDirectWritePinnedPartitionConnectsToDescribeHost(t *testing.T) {
	cluster := newDirectWriteCluster(testPartitionID, testHostNodeID, testInitialGen)
	recorder := newStreamWriteRecorder()

	writer, ctx := startDirectWriteWriter(t, cluster, recorder,
		topicoptions.WithWriterPartitionID(testPartitionID),
	)

	err := writer.Write(ctx, topicwriter.Message{Data: strings.NewReader("payload")})
	require.NoError(t, err)

	require.Equal(t, 1, cluster.describeCalls())
	require.Equal(t, 1, recorder.sessionsCount())
	require.Equal(t, uint32(testHostNodeID), recorder.lastSession().nodeID)
	require.True(t, recorder.lastSession().disableFallback)
	requireDirectInit(t, recorder.lastSession().initRequest, testPartitionID, testInitialGen)
}

// TestDirectWriteProducerProbeRebind discovers the partition through the proxy,
// then reconnects directly to the partition host with generation from Describe.
func TestDirectWriteProducerProbeRebind(t *testing.T) {
	cluster := newDirectWriteCluster(testPartitionID, testHostNodeID, testInitialGen)
	recorder := newStreamWriteRecorder()

	writer, ctx := startDirectWriteWriter(t, cluster, recorder,
		topicoptions.WithWriterProducerID("producer-1"),
	)

	err := writer.Write(ctx, topicwriter.Message{Data: strings.NewReader("payload")})
	require.NoError(t, err)

	require.Equal(t, 2, recorder.sessionsCount())
	require.Equal(t, 1, cluster.describeCalls())

	probe := recorder.session(0)
	require.False(t, probe.disableFallback, "first connect must go through proxy")
	require.Nil(t, probe.initRequest.GetPartitionWithGeneration())
	require.NotEmpty(t, probe.initRequest.GetMessageGroupId())

	direct := recorder.session(1)
	require.True(t, direct.disableFallback)
	require.Equal(t, uint32(testHostNodeID), direct.nodeID)
	requireDirectInit(t, direct.initRequest, testPartitionID, testInitialGen)
}

// TestDirectWriteStaleGenerationReconnect models a partition move between Describe
// and Init: the first generation is rejected, Describe is called again, and the
// writer reconnects to the new node with the fresh generation.
func TestDirectWriteStaleGenerationReconnect(t *testing.T) {
	cluster := newDirectWriteCluster(testPartitionID, testHostNodeID, testInitialGen)
	cluster.rejectInitGeneration(testInitialGen)
	cluster.afterDescribe(func(call int) {
		if call >= 2 {
			cluster.setPartitionLocation(testPartitionID, testMovedNodeID, testMovedGen)
		}
	})
	recorder := newStreamWriteRecorder()

	writer, ctx := startDirectWriteWriter(t, cluster, recorder,
		topicoptions.WithWriterPartitionID(testPartitionID),
	)

	err := writer.Write(ctx, topicwriter.Message{Data: strings.NewReader("payload")})
	require.NoError(t, err)

	require.Equal(t, 2, cluster.describeCalls())
	require.Equal(t, 2, recorder.sessionsCount())

	stale := recorder.session(0)
	requireDirectInit(t, stale.initRequest, testPartitionID, testInitialGen)

	fresh := recorder.session(1)
	require.Equal(t, uint32(testMovedNodeID), fresh.nodeID)
	requireDirectInit(t, fresh.initRequest, testPartitionID, testMovedGen)
}

// TestDirectWriteMissingPartitionFailsWrite covers a pinned partition that no
// longer exists in topic metadata after split/merge.
func TestDirectWriteMissingPartitionFailsWrite(t *testing.T) {
	cluster := newDirectWriteCluster(testPartitionID, testHostNodeID, testInitialGen)
	recorder := newStreamWriteRecorder()

	writer, ctx := startDirectWriteWriter(t, cluster, recorder,
		topicoptions.WithWriterPartitionID(99),
	)

	err := writer.Write(ctx, topicwriter.Message{Data: strings.NewReader("payload")})
	require.Error(t, err)
	require.Contains(t, err.Error(), "target partition not found")
	require.Zero(t, recorder.sessionsCount())
}

type directWriteCluster struct {
	Ydb_Topic_V1.UnimplementedTopicServiceServer

	mu sync.Mutex

	partitions map[int64]partitionLocation

	rejectedGeneration int64
	afterDescribeHook  func(call int)

	describeCount atomic.Int32
	streamCount   atomic.Int32
}

type partitionLocation struct {
	nodeID     int32
	generation int64
}

func newDirectWriteCluster(partitionID int64, nodeID int32, generation int64) *directWriteCluster {
	cluster := &directWriteCluster{
		partitions: make(map[int64]partitionLocation),
	}
	cluster.setPartitionLocation(partitionID, nodeID, generation)

	return cluster
}

func (c *directWriteCluster) setPartitionLocation(partitionID int64, nodeID int32, generation int64) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.partitions[partitionID] = partitionLocation{
		nodeID:     nodeID,
		generation: generation,
	}
}

func (c *directWriteCluster) rejectInitGeneration(generation int64) {
	c.rejectedGeneration = generation
}

func (c *directWriteCluster) afterDescribe(hook func(call int)) {
	c.afterDescribeHook = hook
}

func (c *directWriteCluster) describeCalls() int {
	return int(c.describeCount.Load())
}

func (c *directWriteCluster) DescribeTopic(
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

	return describeTopicResult(partitions), nil
}

func (c *directWriteCluster) StreamWrite(server Ydb_Topic_V1.TopicService_StreamWriteServer) error {
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

func (c *directWriteCluster) serveProbeSession(server Ydb_Topic_V1.TopicService_StreamWriteServer) error {
	if err := c.sendInitResponse(server, testPartitionID); err != nil {
		return err
	}

	// Probe stream is closed by the SDK right after InitResponse.
	_, err := server.Recv()
	if errors.Is(err, io.EOF) {
		return nil
	}

	return err
}

func (c *directWriteCluster) serveDirectSession(
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

func (c *directWriteCluster) sendInitResponse(
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

type streamWriteSession struct {
	nodeID          uint32
	disableFallback bool
	initRequest     *Ydb_Topic.StreamWriteMessage_InitRequest
}

type streamWriteRecorder struct {
	mu       sync.Mutex
	sessions []streamWriteSession
}

func newStreamWriteRecorder() *streamWriteRecorder {
	return &streamWriteRecorder{}
}

func (r *streamWriteRecorder) interceptor() grpc.StreamClientInterceptor {
	return func(
		ctx context.Context,
		desc *grpc.StreamDesc,
		cc *grpc.ClientConn,
		method string,
		streamer grpc.Streamer,
		opts ...grpc.CallOption,
	) (grpc.ClientStream, error) {
		if strings.Contains(method, "StreamWrite") {
			session := streamWriteSession{
				disableFallback: endpoint.ContextDisableFallback(ctx),
			}
			if nodeID, ok := endpoint.ContextNodeID(ctx); ok {
				session.nodeID = nodeID
			}

			r.mu.Lock()
			r.sessions = append(r.sessions, session)
			sessionIndex := len(r.sessions) - 1
			r.mu.Unlock()

			stream, err := streamer(ctx, desc, cc, method, opts...)
			if err != nil {
				return nil, err
			}

			return &streamWriteInitRecorder{
				ClientStream: stream,
				recordInitReq: func(req *Ydb_Topic.StreamWriteMessage_InitRequest) {
					r.mu.Lock()
					defer r.mu.Unlock()
					r.sessions[sessionIndex].initRequest = req
				},
			}, nil
		}

		return streamer(ctx, desc, cc, method, opts...)
	}
}

func (r *streamWriteRecorder) sessionsCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()

	return len(r.sessions)
}

func (r *streamWriteRecorder) session(index int) streamWriteSession {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.sessions[index]
}

func (r *streamWriteRecorder) lastSession() streamWriteSession {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.sessions[len(r.sessions)-1]
}

type streamWriteInitRecorder struct {
	grpc.ClientStream

	recordInitReq func(req *Ydb_Topic.StreamWriteMessage_InitRequest)
	recorded      bool
}

func (s *streamWriteInitRecorder) SendMsg(m any) error {
	if !s.recorded {
		if msg, ok := m.(*Ydb_Topic.StreamWriteMessage_FromClient); ok {
			if initReq := msg.GetInitRequest(); initReq != nil {
				s.recordInitReq(initReq)
				s.recorded = true
			}
		}
	}

	return s.ClientStream.SendMsg(m)
}

func startDirectWriteWriter(
	t *testing.T,
	cluster *directWriteCluster,
	recorder *streamWriteRecorder,
	opts ...topicoptions.WriterOption,
) (*topicwriter.Writer, context.Context) {
	t.Helper()

	e := fixenv.New(t)
	connString := topicmock.GrpcMockTopicConnStringWithNodeIDs(
		e,
		cluster,
		[]uint32{testProxyNodeID, uint32(testHostNodeID), uint32(testMovedNodeID)},
	)

	db, err := ydb.Open(sf.Context(e), connString,
		ydb.With(config.WithGrpcOptions(
			grpc.WithChainStreamInterceptor(recorder.interceptor()),
		)),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close(sf.Context(e)) })

	writerOpts := append([]topicoptions.WriterOption{
		topicoptions.WithWriterDirectWrite(true),
		topicoptions.WithWriterWaitServerAck(true),
	}, opts...)

	writer, err := db.Topic().StartWriter(testTopicPath, writerOpts...)
	require.NoError(t, err)

	return writer, sf.Context(e)
}

func requireDirectInit(
	t *testing.T,
	initReq *Ydb_Topic.StreamWriteMessage_InitRequest,
	partitionID int64,
	generation int64,
) {
	t.Helper()

	require.NotNil(t, initReq)
	pwg := initReq.GetPartitionWithGeneration()
	require.NotNilf(t, pwg, "init request must use partition_with_generation")
	require.Equal(t, partitionID, pwg.GetPartitionId())
	require.Equal(t, generation, pwg.GetGeneration())
}

func describeTopicResult(
	partitions []*Ydb_Topic.DescribeTopicResult_PartitionInfo,
) *Ydb_Topic.DescribeTopicResponse {
	result := &Ydb_Topic.DescribeTopicResult{
		Self: &Ydb_Scheme.Entry{
			Name: testTopicPath,
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
	_ = resp.GetOperation().GetResult().MarshalFrom(result)

	return resp
}
