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
	ctx context.Context

	recv func() (*Ydb_Query.ExecuteQueryResponsePart, error)
}

func (s *testExecuteQueryStream) Recv() (*Ydb_Query.ExecuteQueryResponsePart, error) {
	return s.recv()
}

func (s *testExecuteQueryStream) Header() (metadata.MD, error) {
	return nil, nil
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

	const (
		parts       = 6
		netDelay    = 2 * time.Millisecond
		workBetween = 3 * time.Millisecond
	)

	var innerRecv, outerRecv int
	inner := &testExecuteQueryStream{
		recv: func() (*Ydb_Query.ExecuteQueryResponsePart, error) {
			time.Sleep(netDelay)
			innerRecv++
			if innerRecv > parts {
				return nil, io.EOF
			}

			return partWithIndex(int64(innerRecv - 1)), nil
		},
	}

	wrapped := wrapExecuteQueryStreamWithPrefetch(inner, 2)
	start := time.Now()
	for {
		_, err := wrapped.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		require.NoError(t, err)
		outerRecv++
		time.Sleep(workBetween)
	}
	elapsed := time.Since(start)

	// Without overlap, each outer iteration would wait netDelay inside Recv after workBetween,
	// lower bound ~ parts*(netDelay+workBetween). Prefetch hides part of netDelay behind workBetween.
	noPrefetchFloor := time.Duration(parts) * (netDelay + workBetween)
	require.Less(t, elapsed, noPrefetchFloor*92/100,
		"expected prefetch to finish measurably faster than fully sequential Recv+work schedule")
}

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
