package query

import (
	"context"
	"errors"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/Ydb_Query_V1"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Query"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// testExecuteQueryStream is a minimal [Ydb_Query_V1.QueryService_ExecuteQueryClient] for tests.
type testExecuteQueryStream struct {
	ctx context.Context //nolint:containedctx

	recv func() (*Ydb_Query.ExecuteQueryResponsePart, error)
}

func (s *testExecuteQueryStream) Recv() (*Ydb_Query.ExecuteQueryResponsePart, error) {
	return s.recv()
}

func (s *testExecuteQueryStream) Header() (metadata.MD, error) {
	return nil, nil //nolint:nilnil
}

func (s *testExecuteQueryStream) Trailer() metadata.MD {
	return nil
}

func (s *testExecuteQueryStream) CloseSend() error {
	return nil
}

func (s *testExecuteQueryStream) Context() context.Context {
	if s.ctx != nil {
		return s.ctx
	}

	return context.Background()
}

func (s *testExecuteQueryStream) SendMsg(any) error {
	return status.Error(codes.Unimplemented, "SendMsg")
}

func (s *testExecuteQueryStream) RecvMsg(any) error {
	return status.Error(codes.Unimplemented, "RecvMsg")
}

func partWithIndex(idx int64) *Ydb_Query.ExecuteQueryResponsePart {
	return &Ydb_Query.ExecuteQueryResponsePart{
		ResultSetIndex: idx,
	}
}

func TestWrapExecuteQueryStreamWithPrefetchOrderAndEOF(t *testing.T) {
	t.Parallel()

	var seq int
	inner := &testExecuteQueryStream{
		recv: func() (*Ydb_Query.ExecuteQueryResponsePart, error) {
			seq++
			switch seq {
			case 1:
				return partWithIndex(0), nil
			case 2:
				return partWithIndex(0), nil
			case 3:
				return nil, io.EOF
			default:
				// Stacked synchronous middleware may read one message past stream end.
				return nil, io.EOF
			}
		},
	}

	wrapped := wrapExecuteQueryStreamWithPrefetch(inner, 2)
	require.Implements(t, (*Ydb_Query_V1.QueryService_ExecuteQueryClient)(nil), wrapped)

	p1, err := wrapped.Recv()
	require.NoError(t, err)
	require.Equal(t, int64(0), p1.GetResultSetIndex())

	p2, err := wrapped.Recv()
	require.NoError(t, err)
	require.Equal(t, int64(0), p2.GetResultSetIndex())

	_, err = wrapped.Recv()
	require.ErrorIs(t, err, io.EOF)

	_, err = wrapped.Recv()
	require.ErrorIs(t, err, io.EOF)
}

func TestWrapExecuteQueryStreamWithPrefetchPropagatesError(t *testing.T) {
	t.Parallel()

	testErr := errors.New("stream read error")

	var seq int
	inner := &testExecuteQueryStream{
		recv: func() (*Ydb_Query.ExecuteQueryResponsePart, error) {
			seq++
			if seq == 1 {
				return partWithIndex(0), nil
			}

			return nil, testErr
		},
	}

	wrapped := wrapExecuteQueryStreamWithPrefetch(inner, 2)

	_, err := wrapped.Recv()
	require.NoError(t, err)

	_, err = wrapped.Recv()
	require.ErrorIs(t, err, testErr)
}

func TestWrapExecuteQueryStreamWithPrefetchZeroPassthrough(t *testing.T) {
	t.Parallel()

	inner := &testExecuteQueryStream{
		recv: func() (*Ydb_Query.ExecuteQueryResponsePart, error) {
			return nil, io.EOF
		},
	}
	same := wrapExecuteQueryStreamWithPrefetch(inner, 0)
	require.Equal(t, inner, same)
}

func benchmarkRecvDrainLoop(b *testing.B, wrap func(Ydb_Query_V1.QueryService_ExecuteQueryClient) Ydb_Query_V1.QueryService_ExecuteQueryClient) {
	const (
		parts       = 8
		netDelay    = 200 * time.Microsecond
		workBetween = 300 * time.Microsecond
	)

	b.ReportAllocs()
	for b.Loop() {
		var n int
		inner := &testExecuteQueryStream{
			recv: func() (*Ydb_Query.ExecuteQueryResponsePart, error) {
				time.Sleep(netDelay)
				n++
				if n > parts {
					return nil, io.EOF
				}

				return partWithIndex(int64(n - 1)), nil
			},
		}
		s := wrap(inner)
		for {
			_, err := s.Recv()
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				b.Fatal(err)
			}
			time.Sleep(workBetween)
		}
	}
}

type executeQueryOneSlotAheadMiddleware struct {
	Ydb_Query_V1.QueryService_ExecuteQueryClient

	ahead    executeQueryPartRecv
	hasAhead bool
}

func (m *executeQueryOneSlotAheadMiddleware) Recv() (*Ydb_Query.ExecuteQueryResponsePart, error) {
	var item executeQueryPartRecv
	if m.hasAhead {
		item = m.ahead
		m.hasAhead = false
	} else {
		part, err := m.QueryService_ExecuteQueryClient.Recv()
		item = executeQueryPartRecv{part: part, err: err}
	}

	if item.err == nil {
		part2, err2 := m.QueryService_ExecuteQueryClient.Recv()
		m.ahead = executeQueryPartRecv{part: part2, err: err2}
		m.hasAhead = true
	}

	return item.part, item.err
}

func (m *executeQueryOneSlotAheadMiddleware) RecvMsg(msg any) error {
	part, err := m.Recv()
	if err != nil {
		return err
	}
	dst, ok := msg.(*Ydb_Query.ExecuteQueryResponsePart)
	if !ok {
		return xerrors.WithStackTrace(fmt.Errorf(
			"%T is not '*Ydb_Query.ExecuteQueryResponsePart'", msg,
		))
	}
	proto.Reset(dst)
	if part != nil {
		proto.Merge(dst, part)
	}

	return nil
}

// bufferNextPartMiddleware returns a stream that always keeps one extra part read
// from inner (when the last delivered part had no error). Stacking this middleware
// prefetch times yields a synchronous read-ahead depth of prefetch.
func bufferNextPartMiddleware(inner Ydb_Query_V1.QueryService_ExecuteQueryClient) Ydb_Query_V1.QueryService_ExecuteQueryClient {
	return &executeQueryOneSlotAheadMiddleware{
		QueryService_ExecuteQueryClient: inner,
	}
}

func wrapExecuteQueryStreamWithPrefetch(
	stream Ydb_Query_V1.QueryService_ExecuteQueryClient,
	prefetch int,
) Ydb_Query_V1.QueryService_ExecuteQueryClient {
	if prefetch <= 0 {
		return stream
	}
	for range prefetch {
		stream = bufferNextPartMiddleware(stream)
	}

	return stream
}

// BenchmarkExecuteQueryStreamPrefetchCompare runs the same workload: baseline
// (no prefetch), synchronous stacked one-slot middleware, and async channel prefetch.
//
// BenchmarkExecuteQueryStreamPrefetchCompare/baseline_no_prefetch-12         	     244	   4918889 ns/op	     944 B/op	      11 allocs/op
// BenchmarkExecuteQueryStreamPrefetchCompare/sync_mw_depth_1-12              	     237	   4983030 ns/op	     992 B/op	      12 allocs/op
// BenchmarkExecuteQueryStreamPrefetchCompare/sync_mw_depth_2-12              	     229	   5234604 ns/op	    1040 B/op	      13 allocs/op
// BenchmarkExecuteQueryStreamPrefetchCompare/sync_mw_depth_4-12              	     236	   5079231 ns/op	    1136 B/op	      15 allocs/op
// BenchmarkExecuteQueryStreamPrefetchCompare/sync_mw_depth_8-12              	     242	   4927544 ns/op	    1328 B/op	      19 allocs/op
// BenchmarkExecuteQueryStreamPrefetchCompare/async_chan_depth_1-12           	     428	   2785861 ns/op	    1234 B/op	      16 allocs/op
// BenchmarkExecuteQueryStreamPrefetchCompare/async_chan_depth_2-12           	     426	   2817006 ns/op	    1241 B/op	      16 allocs/op
// BenchmarkExecuteQueryStreamPrefetchCompare/async_chan_depth_4-12           	     423	   2821469 ns/op	    1291 B/op	      16 allocs/op
// BenchmarkExecuteQueryStreamPrefetchCompare/async_chan_depth_8-12           	     424	   2823053 ns/op	    1393 B/op	      16 allocs/op
func BenchmarkExecuteQueryStreamPrefetchCompare(b *testing.B) {
	b.Run("baseline_no_prefetch", func(b *testing.B) {
		benchmarkRecvDrainLoop(b, func(inner Ydb_Query_V1.QueryService_ExecuteQueryClient) Ydb_Query_V1.QueryService_ExecuteQueryClient {
			return inner
		})
	})

	prefetchParts := []int{1, 2, 4, 8}

	for _, depth := range prefetchParts {
		b.Run(fmt.Sprintf("sync_mw_depth_%d", depth), func(b *testing.B) {
			d := depth
			benchmarkRecvDrainLoop(b, func(inner Ydb_Query_V1.QueryService_ExecuteQueryClient) Ydb_Query_V1.QueryService_ExecuteQueryClient {
				return wrapExecuteQueryStreamWithPrefetch(inner, d)
			})
		})
	}
	for _, depth := range prefetchParts {
		b.Run(fmt.Sprintf("async_chan_depth_%d", depth), func(b *testing.B) {
			d := depth
			benchmarkRecvDrainLoop(b, func(inner Ydb_Query_V1.QueryService_ExecuteQueryClient) Ydb_Query_V1.QueryService_ExecuteQueryClient {
				return wrapExecuteQueryStreamWithAsyncPrefetch(inner, d)
			})
		})
	}

}
