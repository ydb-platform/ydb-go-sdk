package topicreaderinternal

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/gtrace"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/topicreadercommon"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func TestReaderReceiptAggregationWithoutObserver(t *testing.T) {
	e := newTopicReaderTestEnv(t)
	e.reader.cfg.Trace = gtrace.ComposeReader(&trace.Topic{}, &trace.Topic{})
	response := readerMetricResponseWithOffsets(&e, 16, 1)
	const topicCount = 16
	sessions := make([]*topicreadercommon.PartitionSession, 0, topicCount)
	for i := range topicCount {
		data := response.PartitionData[0]
		data.PartitionSessionID++
		data.PartitionSessionID += partitionSessionID(i)
		session := topicreadercommon.NewPartitionSession(
			e.ctx, "/test", int64(i), e.reader.readerID, e.reader.readConnectionID,
			data.PartitionSessionID, int64(i+1), 0,
		)
		require.NoError(t, e.reader.sessionController.Add(session))
		sessions = append(sessions, session)
		response.PartitionData = append(response.PartitionData, data)
	}

	allocations := func() float64 {
		return testing.AllocsPerRun(20, func() {
			e.reader.batcher = newBatcher()
			if err := e.reader.onReadResponse(response); err != nil {
				t.Fatal(err)
			}
		})
	}
	singleTopic := allocations()
	for i, session := range sessions {
		session.Topic = "/test-" + strconv.Itoa(i)
	}
	require.Equal(t, singleTopic, allocations(), "distinct topics must not add receipt aggregation allocations")
	require.Nil(t, e.reader.cfg.Trace.OnReaderMessagesReceived)
}
