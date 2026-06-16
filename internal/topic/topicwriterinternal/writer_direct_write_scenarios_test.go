package topicwriterinternal_test

import (
	"context"
	"strings"
	"testing"

	"github.com/rekby/fixenv"
	"github.com/rekby/fixenv/sf"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicmock"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicwritetest"
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

var testClusterNodeIDs = []uint32{testProxyNodeID, uint32(testHostNodeID), uint32(testMovedNodeID)}

// TestDirectWritePinnedPartitionConnectsToDescribeHost sends the very first
// StreamWrite to the node returned by DescribeTopic and includes partition
// generation in InitRequest.
func TestDirectWritePinnedPartitionConnectsToDescribeHost(t *testing.T) {
	cluster := topicwritetest.NewDirectWriteCluster(testTopicPath, testPartitionID, testHostNodeID, testInitialGen)
	recorder := topicwritetest.NewStreamWriteRecorder()

	writer, ctx := startDirectWriteWriter(t, cluster, recorder, testClusterNodeIDs,
		topicoptions.WithWriterPartitionID(testPartitionID),
	)

	err := writer.Write(ctx, topicwriter.Message{Data: strings.NewReader("payload")})
	require.NoError(t, err)

	require.Equal(t, 1, cluster.DescribeCalls())
	require.Equal(t, 1, recorder.SessionsCount())
	last := recorder.LastSession()
	require.Equal(t, uint32(testHostNodeID), last.NodeID)
	require.True(t, last.DisableFallback)
	topicwritetest.RequireDirectInit(t, last.InitRequest, testPartitionID, testInitialGen)
}

// TestDirectWriteProducerProbeRebind discovers the partition through a synchronous
// proxy probe, then connects directly to the partition host with generation from Describe.
func TestDirectWriteProducerProbeRebind(t *testing.T) {
	cluster := topicwritetest.NewDirectWriteCluster(testTopicPath, testPartitionID, testHostNodeID, testInitialGen)
	recorder := topicwritetest.NewStreamWriteRecorder()

	writer, ctx := startDirectWriteWriter(t, cluster, recorder, testClusterNodeIDs,
		topicoptions.WithWriterProducerID("producer-1"),
	)

	err := writer.Write(ctx, topicwriter.Message{Data: strings.NewReader("payload")})
	require.NoError(t, err)

	require.Equal(t, 2, recorder.SessionsCount())
	require.Equal(t, 1, cluster.DescribeCalls())

	probe := recorder.Session(0)
	require.False(t, probe.DisableFallback, "first connect must go through proxy")
	require.Nil(t, probe.InitRequest.GetPartitionWithGeneration())
	require.NotEmpty(t, probe.InitRequest.GetMessageGroupId())

	direct := recorder.Session(1)
	require.True(t, direct.DisableFallback)
	require.Equal(t, uint32(testHostNodeID), direct.NodeID)
	topicwritetest.RequireDirectInit(t, direct.InitRequest, testPartitionID, testInitialGen)
}

// TestDirectWriteStaleGenerationReconnect models a partition move between Describe
// and Init: the first generation is rejected, Describe is called again, and the
// writer reconnects to the new node with the fresh generation.
func TestDirectWriteStaleGenerationReconnect(t *testing.T) {
	cluster := topicwritetest.NewDirectWriteCluster(testTopicPath, testPartitionID, testHostNodeID, testInitialGen)
	cluster.RejectInitGeneration(testInitialGen)
	cluster.AfterDescribe(func(call int) {
		if call >= 2 {
			cluster.SetPartitionLocation(testPartitionID, testMovedNodeID, testMovedGen)
		}
	})
	recorder := topicwritetest.NewStreamWriteRecorder()

	writer, ctx := startDirectWriteWriter(t, cluster, recorder, testClusterNodeIDs,
		topicoptions.WithWriterPartitionID(testPartitionID),
	)

	err := writer.Write(ctx, topicwriter.Message{Data: strings.NewReader("payload")})
	require.NoError(t, err)

	require.Equal(t, 2, cluster.DescribeCalls())
	require.Equal(t, 2, recorder.SessionsCount())

	stale := recorder.Session(0)
	topicwritetest.RequireDirectInit(t, stale.InitRequest, testPartitionID, testInitialGen)

	fresh := recorder.Session(1)
	require.Equal(t, uint32(testMovedNodeID), fresh.NodeID)
	topicwritetest.RequireDirectInit(t, fresh.InitRequest, testPartitionID, testMovedGen)
}

// TestDirectWriteMissingPartitionFailsWrite covers a pinned partition that no
// longer exists in topic metadata after split/merge.
func TestDirectWriteMissingPartitionFailsWrite(t *testing.T) {
	cluster := topicwritetest.NewDirectWriteCluster(testTopicPath, testPartitionID, testHostNodeID, testInitialGen)
	recorder := topicwritetest.NewStreamWriteRecorder()

	writer, ctx := startDirectWriteWriter(t, cluster, recorder, testClusterNodeIDs,
		topicoptions.WithWriterPartitionID(99),
	)

	err := writer.Write(ctx, topicwriter.Message{Data: strings.NewReader("payload")})
	require.Error(t, err)
	require.Contains(t, err.Error(), "target partition not found")
	require.Zero(t, recorder.SessionsCount())
}

func startDirectWriteWriter(
	t *testing.T,
	cluster *topicwritetest.DirectWriteCluster,
	recorder *topicwritetest.StreamWriteRecorder,
	nodeIDs []uint32,
	opts ...topicoptions.WriterOption,
) (*topicwriter.Writer, context.Context) {
	t.Helper()

	e := fixenv.New(t)
	connString := topicmock.GrpcMockTopicConnStringWithNodeIDs(e, cluster, nodeIDs)

	db, err := ydb.Open(sf.Context(e), connString,
		ydb.With(config.WithGrpcOptions(
			grpc.WithChainStreamInterceptor(recorder.Interceptor()),
		)),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close(sf.Context(e)) })

	writerOpts := append([]topicoptions.WriterOption{
		topicoptions.WithWriterDirectWrite(true),
		topicoptions.WithWriterWaitServerAck(true),
	}, opts...)

	writer, err := db.Topic().StartWriter(cluster.TopicPath, writerOpts...)
	require.NoError(t, err)

	return writer, sf.Context(e)
}
