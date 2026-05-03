//go:build go1.25

package topicwriterinternal

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopicwriter"
)

// TestWriterReconnector_Write_ErrOnQueueFull uses testing/synctest for deterministic,
// fast tests (no xtest.TestManyTimes hot loop). Use t.Context so contexts and timers
// are created in the same synctest bubble as the test.
//
// This file is built only on Go 1.25+ (see go1.25 build tag) where testing/synctest exists.
func TestWriterReconnector_Write_ErrOnQueueFull(t *testing.T) {
	t.Run("ReturnsErrWhenQueueFull", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			const maxQueueLen = int64(2)
			w := newWriterReconnectorStopped(NewWriterReconnectorConfig(
				WithAutoSetSeqNo(false),
				WithMaxQueueLen(int(maxQueueLen)),
				WithErrOnQueueFull(true),
			))
			w.firstConnectionHandled.Store(true)

			err := w.Write(t.Context(), newTestMessages(1, 2, 3)) //nolint:mnd // 3 > maxQueueLen(2) soft first batch
			require.NoError(t, err)

			err = w.Write(t.Context(), newTestMessages(4))
			require.ErrorIs(t, err, ErrPublicQueueIsFull)
			require.NotErrorIs(t, err, ErrPublicMessagesPutToInternalQueueBeforeError)

			ackErr := w.queue.AcksReceived([]rawtopicwriter.WriteAck{
				{SeqNo: 1},
				{SeqNo: 2},
				{SeqNo: 3},
			})
			require.NoError(t, ackErr)

			err = w.Write(t.Context(), newTestMessages(4, 5))
			require.NoError(t, err)
		})
	})

	t.Run("DoesNotBlockOnCtx", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			w := newWriterReconnectorStopped(NewWriterReconnectorConfig(
				WithAutoSetSeqNo(false),
				WithMaxQueueLen(1), //nolint:mnd
				WithErrOnQueueFull(true),
			))
			w.firstConnectionHandled.Store(true)

			err := w.Write(t.Context(), newTestMessages(1))
			require.NoError(t, err)

			err = w.Write(t.Context(), newTestMessages(2))
			require.ErrorIs(t, err, ErrPublicQueueIsFull)
		})
	})

	t.Run("DefaultBehaviorStillBlocks", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			w := newWriterReconnectorStopped(NewWriterReconnectorConfig(
				WithAutoSetSeqNo(false),
				WithMaxQueueLen(1), //nolint:mnd
			))
			w.firstConnectionHandled.Store(true)

			err := w.Write(t.Context(), newTestMessages(1))
			require.NoError(t, err)

			const timeout = 50 * time.Millisecond //nolint:mnd
			ctxTimeout, cancel := context.WithTimeout(t.Context(), timeout)
			defer cancel()
			err = w.Write(ctxTimeout, newTestMessages(2))
			require.ErrorIs(t, err, context.DeadlineExceeded)
			require.NotErrorIs(t, err, ErrPublicQueueIsFull)
		})
	})
}
