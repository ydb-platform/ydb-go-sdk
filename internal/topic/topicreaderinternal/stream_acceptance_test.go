package topicreaderinternal

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func TestPullAcceptanceIncludesOnlySuccessfullyPushedPrefix(t *testing.T) {
	e := newTopicReaderTestEnv(t)
	response := readerMetricResponseWithOffsets(&e, 50, 1, 2)
	for _, offset := range []int64{3, 4} {
		batch := readerMetricResponseWithOffsets(&e, 50, offset).PartitionData[0].Batches[0]
		response.PartitionData[0].Batches = append(response.PartitionData[0].Batches, batch)
	}
	var reservations int
	var accepted []trace.TopicReaderMessagesReceivedInfo
	stop := errors.New("reject third push")
	e.reader.cfg.Trace = &trace.Topic{
		OnReaderLocalBufferChanged: func(info trace.TopicReaderLocalBufferChangedInfo) {
			if info.MessagesDelta > 0 {
				reservations++
				if reservations == 3 {
					require.NoError(t, e.reader.batcher.Close(stop))
				}
			}
		},
		OnReaderMessagesReceived: func(info trace.TopicReaderMessagesReceivedInfo) { accepted = append(accepted, info) },
	}
	require.ErrorIs(t, e.reader.onReadResponse(response), stop)
	require.Len(t, accepted, 1)
	require.Equal(t, "/test", accepted[0].Topic)
	require.Equal(t, 3, accepted[0].MessagesCount)
}
