package topicreadercommon

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopiccommon"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopicreader"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func TestStreamCommitAcknowledgementCountsOrdinaryRangeAtTransactionWatermark(t *testing.T) {
	for _, ack := range []rawtopiccommon.Offset{11, 12} {
		t.Run(fmt.Sprint(ack), func(t *testing.T) {
			tracker := newCommitMessageTracker(10)
			require.EqualValues(t, 1, tracker.Queue(10, 11))

			// Transaction success advances the watermark without acknowledging ordinary ranges.
			tracker.session.SetCommittedOffsetForward(12)
			count := tracker.Acknowledge(ack)
			t.Logf("transaction watermark=12 stream ACK=%d acknowledged=%d", ack, count)
			require.EqualValues(t, 1, count)
			require.EqualValues(t, 12, tracker.session.CommittedOffset())
			require.Zero(t, tracker.Acknowledge(ack))
		})
	}
}

func TestCommitMessageTrackerInterleavedSubmissions(t *testing.T) {
	tracker := newCommitMessageTracker(0)
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

func TestCommitMessageTrackerScatteredSubmissions(t *testing.T) {
	const (
		ranges = 1000
		step   = 8
	)
	tracker := newCommitMessageTracker(0)
	for i := range ranges {
		// 7919 is coprime with the range count, so every range is submitted once, far from its neighbors.
		end := rawtopiccommon.Offset(i*7919%ranges + 1)
		require.EqualValues(t, 1, tracker.Queue(end-1, end))
	}
	for end := rawtopiccommon.Offset(step); end <= ranges; end += step {
		require.EqualValues(t, step, tracker.Acknowledge(end), "acknowledged offset %d", end)
	}
	require.Empty(t, tracker.pending)
}

func TestCommitMessageTrackerReleasesStorageOfClosedSession(t *testing.T) {
	const ranges = 1000
	names := map[bool]string{true: "close then acknowledge", false: "acknowledge then close"}
	for _, closeFirst := range []bool{true, false} {
		t.Run(names[closeFirst], func(t *testing.T) {
			session := newCommitMetricsTestSession(t, &trace.Topic{
				OnReaderCommitQueued: func(trace.TopicReaderCommitQueuedInfo) {},
			})
			for i := range rawtopiccommon.Offset(ranges) {
				require.EqualValues(t, 1, RegisterCommitQueued(CommitRange{
					PartitionSession: session, CommitOffsetStart: i, CommitOffsetEnd: i + 1,
				}))
			}
			if closeFirst {
				session.Close()
			}
			// The final acknowledgement still counts after close.
			require.EqualValues(t, ranges, RegisterCommitAcknowledged(session, ranges))
			if !closeFirst {
				session.Close()
			}
			require.Nil(t, session.commitMetrics.tracker.pending)
		})
	}
}

func TestCommitMessageTrackerWideSpans(t *testing.T) {
	var queued []trace.TopicReaderCommitQueuedInfo
	session := newCommitMetricsTestSession(t, &trace.Topic{
		OnReaderCommitQueued: func(info trace.TopicReaderCommitQueuedInfo) {
			queued = append(queued, info)
		},
	})
	const width int64 = 1 << 32
	r := CommitRange{PartitionSession: session, CommitOffsetEnd: 1 << 32}
	TraceCommitQueued(session.Context(), r)
	c := NewCommitterStopped(&trace.Topic{}, session.Context(), CommitModeAsync, nil)
	t.Cleanup(func() { require.NoError(t, c.Close(context.Background(), errors.New("test complete"))) })
	c.NewCommitRequest(r).Confirm()
	require.Len(t, queued, 2)
	require.Equal(t, width, queued[0].MessagesCount)
	require.Equal(t, width, queued[1].MessagesCount)
	require.Equal(t, 2*width, RegisterCommitAcknowledged(session, r.CommitOffsetEnd))
}

func TestCommitAtWatermarkDoesNotAwaitAnotherMetricAck(t *testing.T) {
	for _, mode := range []string{"async", "confirm", "wait"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			var queued int64
			tracer := &trace.Topic{OnReaderCommitQueued: func(info trace.TopicReaderCommitQueuedInfo) {
				queued += info.MessagesCount
			}}
			session := newCommitMetricsTestSession(t, tracer)
			r := CommitRange{PartitionSession: session, CommitOffsetStart: 0, CommitOffsetEnd: 5}
			TraceCommitQueued(ctx, r)
			require.Equal(t, int64(5), RegisterCommitAcknowledged(session, 5))
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
			require.Equal(t, int64(5), queued)
			require.Zero(t, RegisterCommitAcknowledged(session, 6))
		})
	}
}
