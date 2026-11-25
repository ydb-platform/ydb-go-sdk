//go:build go1.25

package query

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Query"
)

func TestPrefetchDeliversResponseAfterTransportCompletion(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		transport, finish := context.WithCancel(t.Context())
		terminal := errors.New("terminal query error")
		var next int64
		inner := &testExecuteQueryStream{ctx: transport, recv: func() (*Ydb_Query.ExecuteQueryResponsePart, error) {
			next++
			switch next {
			case 1:
				return partWithIndex(1), nil
			case 2:
				// Read-ahead has observed the terminal error while returning the final part.
				finish()

				return partWithIndex(2), nil
			default:
				return nil, terminal
			}
		}}
		stream := wrapExecuteQueryStreamWithAsyncPrefetch(t.Context(), inner, 1)
		synctest.Wait() // The first part fills the queue before the consumer starts.
		for _, want := range []int64{1, 2} {
			part, err := stream.Recv()
			require.NoError(t, err)
			require.Equal(t, want, part.GetResultSetIndex())
		}
		_, err := stream.Recv()
		require.ErrorIs(t, err, terminal)
	})
}

func TestCancelPrefetchWithFullQueue(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		next := int64(0)
		inner := &testExecuteQueryStream{recv: func() (*Ydb_Query.ExecuteQueryResponsePart, error) {
			next++

			return partWithIndex(next), nil
		}}
		stream := wrapExecuteQueryStreamWithAsyncPrefetch(ctx, inner, 1)
		synctest.Wait()
		cancel()
		synctest.Wait()
		part, err := stream.Recv()
		require.NoError(t, err)
		require.EqualValues(t, 1, part.GetResultSetIndex())
		_, err = stream.Recv()
		require.ErrorIs(t, err, context.Canceled)
		require.EqualValues(t, 2, next, "cancellation must unblock the producer while the queue is full")
	})
}
