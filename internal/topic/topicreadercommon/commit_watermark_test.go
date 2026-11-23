package topicreadercommon

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopicreader"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

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
