package topicreadercommon

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopicreader"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func TestCommitMessageTrackerInterleavedSubmissions(t *testing.T) {
	tracker := NewCommitMessageTracker(0)
	require.EqualValues(t, 2, tracker.Queue(1022, 1024))
	require.EqualValues(t, 1, tracker.Queue(16, 17))
	require.EqualValues(t, 8, tracker.Queue(8, 16))
	require.EqualValues(t, 8, tracker.Acknowledge(16))
	require.EqualValues(t, 1, tracker.Queue(16, 17))
	require.Zero(t, tracker.Acknowledge(15))
	require.Zero(t, tracker.Acknowledge(16))
	require.EqualValues(t, 2, tracker.Acknowledge(512))
	require.EqualValues(t, 3, tracker.Queue(512, 515))
	require.EqualValues(t, 5, tracker.Acknowledge(1024))
}

func TestCommitAtWatermarkDoesNotAwaitAnotherMetricAck(t *testing.T) {
	for _, mode := range []string{"async", "confirm", "wait"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			var queued int
			tracer := &trace.Topic{OnReaderCommitQueued: func(info trace.TopicReaderCommitQueuedInfo) {
				queued += info.MessagesCount
			}}
			session := newCommitMetricsTestSession(t, tracer)
			r := CommitRange{PartitionSession: session, CommitOffsetStart: 0, CommitOffsetEnd: 5}
			TraceCommitQueued(ctx, r)
			require.Equal(t, 5, RegisterCommitAcknowledged(session, 5))
			session.SetCommittedOffsetForward(5)
			var sent []rawtopicreader.ClientMessage
			c := NewCommitterStopped(&trace.Topic{}, ctx, CommitModeAsync, func(msg rawtopicreader.ClientMessage) error {
				sent = append(sent, msg)

				return nil
			})
			t.Cleanup(func() { require.NoError(t, c.Close(ctx, errors.New("test complete"))) })
			switch mode {
			case "async":
				require.NoError(t, c.Commit(ctx, r))
			case "confirm":
				c.NewCommitRequest(r).Confirm()
			case "wait":
				require.NoError(t, c.NewCommitRequest(r).Wait(ctx))
			}
			require.NoError(t, c.Flush())
			if mode == "wait" {
				require.Empty(t, sent)
			} else {
				require.Len(t, sent, 1)
				require.Equal(t, int64(5), sent[0].(*rawtopicreader.CommitOffsetRequest).CommitOffsets[0].Offsets[0].End.ToInt64())
			}
			// No new server response is required for an already covered range.
			require.Equal(t, 5, queued)
			require.Zero(t, RegisterCommitAcknowledged(session, 6))
		})
	}
}
