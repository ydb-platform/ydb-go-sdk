package query

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/Ydb_Query_V1"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Query"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
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
				return nil, errors.New("unexpected extra inner Recv")
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

func TestPrefetchOverlapsWorkBetweenRecv(t *testing.T) {
	t.Parallel()

	const parts = 3

	started := make([]chan struct{}, parts+1) // last slot is the EOF recv
	allow := make([]chan struct{}, parts+1)
	for i := range started {
		started[i] = make(chan struct{})
		allow[i] = make(chan struct{})
	}

	var innerRecv int
	inner := &testExecuteQueryStream{
		recv: func() (*Ydb_Query.ExecuteQueryResponsePart, error) {
			idx := innerRecv
			innerRecv++
			close(started[idx])
			<-allow[idx]

			if idx >= parts {
				return nil, io.EOF
			}

			return partWithIndex(int64(idx)), nil
		},
	}

	wrapped := wrapExecuteQueryStreamWithPrefetch(inner, 2)

	// Let the first inner recv complete so the first outer Recv gets part 0.
	<-started[0]
	close(allow[0])

	part, err := wrapped.Recv()
	require.NoError(t, err)
	require.Equal(t, partWithIndex(0), part)

	// The prefetcher should fetch the next part while the caller is doing work between Recv calls.
	<-started[1]
	close(allow[1])

	type recvResult struct {
		part *Ydb_Query.ExecuteQueryResponsePart
		err  error
	}
	done := make(chan recvResult, 1)
	go func() {
		part, err := wrapped.Recv()
		done <- recvResult{part: part, err: err}
	}()

	select {
	case got := <-done:
		require.NoError(t, got.err)
		require.Equal(t, partWithIndex(1), got.part)
	case <-time.After(time.Second):
		t.Fatal("second Recv blocked even though the next part had already been prefetched")
	}
}

// BenchmarkExecuteQueryRecvWithPrefetch/prefetch_off-12      247	4840979 ns/op	 944 B/op	 11 allocs/op
// BenchmarkExecuteQueryRecvWithPrefetch/prefetch_1-12        430	2792482 ns/op	 1254 B/op	 16 allocs/op
// BenchmarkExecuteQueryRecvWithPrefetch/prefetch_2-12        426	2813529 ns/op	 1266 B/op	 16 allocs/op
// BenchmarkExecuteQueryRecvWithPrefetch/prefetch_4-12        424	2816163 ns/op	 1314 B/op	 16 allocs/op
// BenchmarkExecuteQueryRecvWithPrefetch/prefetch_8-12        426	2836546 ns/op	 1417 B/op	 16 allocs/op
func BenchmarkExecuteQueryRecvWithPrefetch(b *testing.B) {
	const (
		parts       = 8
		netDelay    = 200 * time.Microsecond
		workBetween = 300 * time.Microsecond
	)

	bench := func(b *testing.B, prefetch int) {
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
			var s Ydb_Query_V1.QueryService_ExecuteQueryClient = inner
			if prefetch > 0 {
				s = wrapExecuteQueryStreamWithPrefetch(inner, prefetch)
			}
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

	b.Run("prefetch_off", func(b *testing.B) {
		bench(b, 0)
	})
	b.Run("prefetch_1", func(b *testing.B) {
		bench(b, 1)
	})
	b.Run("prefetch_2", func(b *testing.B) {
		bench(b, 2)
	})
	b.Run("prefetch_4", func(b *testing.B) {
		bench(b, 4)
	})
	b.Run("prefetch_8", func(b *testing.B) {
		bench(b, 8)
	})
}
