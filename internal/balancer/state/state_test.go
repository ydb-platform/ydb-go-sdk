package state

import (
	"context"
	"math"
	"strconv"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/endpoint"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/mock"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xslices"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xtest"
)

func TestState(t *testing.T) {
	ctx := xtest.Context(t)

	t.Run("Nil", func(t *testing.T) {
		var s *state

		require.Empty(t, s.All())

		e, err := s.Next(ctx)
		require.ErrorIs(t, err, ErrNilState)
		require.Nil(t, e)
	})

	t.Run("Empty", func(t *testing.T) {
		s := New(nil)

		require.Empty(t, s.All())

		e, err := s.Next(ctx)
		require.ErrorIs(t, err, ErrNoEndpoints)
		require.Nil(t, e)
	})

	t.Run("One", func(t *testing.T) {
		s := New([]endpoint.Endpoint{&mock.Endpoint{
			AddrField:   "1",
			NodeIDField: 1,
		}})

		require.Len(t, s.All(), 1)

		e, err := s.Next(ctx)
		require.NoError(t, err)
		require.NotNil(t, e)
		require.Equal(t, "1", e.Address())
		require.Equal(t, uint32(1), e.NodeID())
	})

	t.Run("ContextError", func(t *testing.T) {
		ctxWithCancel, cancel := context.WithCancel(ctx)
		cancel()

		s := New([]endpoint.Endpoint{&mock.Endpoint{
			AddrField:   "1",
			NodeIDField: 1,
		}})

		require.Len(t, s.All(), 1)

		e, err := s.Next(ctxWithCancel)
		require.ErrorIs(t, err, context.Canceled)
		require.Nil(t, e)
	})

	t.Run("Exclude", func(t *testing.T) {
		s := New([]endpoint.Endpoint{
			&mock.Endpoint{
				AddrField:   "1",
				NodeIDField: 1,
			},
			&mock.Endpoint{
				AddrField:   "2",
				NodeIDField: 2,
			},
			&mock.Endpoint{
				AddrField:   "3",
				NodeIDField: 3,
			},
			&mock.Endpoint{
				AddrField:   "4",
				NodeIDField: 4,
			},
			&mock.Endpoint{
				AddrField:   "5",
				NodeIDField: 5,
			},
		})

		var endpoints []endpoint.Endpoint

		for i := 0; i < 5; i++ {
			e, err := s.Next(ctx)
			require.NoError(t, err)
			require.NotNil(t, e)
			endpoints = append(endpoints, e)
			s = s.Exclude(e)
			require.Len(t, s.All(), 4-i)
		}

		e, err := s.Next(ctx)
		require.ErrorIs(t, err, ErrNoEndpoints)
		require.Nil(t, e)
		require.Empty(t, s.All())

		require.Equal(t,
			[]uint32{1, 2, 3, 4, 5},
			xslices.SortCopy(
				xslices.Transform(
					endpoints,
					func(e endpoint.Endpoint) uint32 {
						return e.NodeID()
					},
				), func(lhs, rhs uint32) int {
					return int(lhs) - int(rhs)
				},
			),
		)
	})

	t.Run("WithFilter", func(t *testing.T) {
		s := New([]endpoint.Endpoint{
			&mock.Endpoint{
				AddrField:   "1",
				NodeIDField: 1,
			},
			&mock.Endpoint{
				AddrField:   "2",
				NodeIDField: 2,
			},
			&mock.Endpoint{
				AddrField:   "3",
				NodeIDField: 3,
			},
			&mock.Endpoint{
				AddrField:   "4",
				NodeIDField: 4,
			},
		}, WithFilter(func(e endpoint.Info) bool {
			return e.NodeID()%2 == 0
		}))

		require.Len(t, s.index, 2)
		require.Len(t, s.All(), 2)
		require.Len(t, s.prefer, 2)
		require.Empty(t, s.fallback)
	})

	t.Run("WithFallback", func(t *testing.T) {
		t.Run("SplittedPreferAndFallback", func(t *testing.T) {
			s := New([]endpoint.Endpoint{
				&mock.Endpoint{
					AddrField:   "1",
					NodeIDField: 1,
				},
				&mock.Endpoint{
					AddrField:   "2",
					NodeIDField: 2,
				},
				&mock.Endpoint{
					AddrField:   "3",
					NodeIDField: 3,
				},
				&mock.Endpoint{
					AddrField:   "4",
					NodeIDField: 4,
				},
			}, WithFilter(func(e endpoint.Info) bool {
				return e.NodeID()%2 == 0
			}), WithFallback())

			require.Len(t, s.index, 4)
			require.Len(t, s.All(), 4)
			require.Len(t, s.prefer, 2)
			require.Len(t, s.fallback, 2)
		})

		t.Run("OnlyFallback", func(t *testing.T) {
			s := New([]endpoint.Endpoint{
				&mock.Endpoint{
					AddrField:   "1",
					NodeIDField: 1,
				},
			}, WithFilter(func(e endpoint.Info) bool {
				return false
			}), WithFallback())

			require.Len(t, s.index, 1)
			require.Len(t, s.All(), 1)
			require.Empty(t, s.prefer)
			require.Len(t, s.fallback, 1)

			e, err := s.Next(ctx)
			require.NoError(t, err)
			require.Equal(t, "1", e.Address())
			require.Equal(t, uint32(1), e.NodeID())
		})
	})

	t.Run("PreferByNodeID", func(t *testing.T) {
		s := New([]endpoint.Endpoint{
			&mock.Endpoint{
				AddrField:   "1",
				NodeIDField: 1,
			},
			&mock.Endpoint{
				AddrField:   "2",
				NodeIDField: 2,
			},
			&mock.Endpoint{
				AddrField:   "3",
				NodeIDField: 3,
			},
			&mock.Endpoint{
				AddrField:   "4",
				NodeIDField: 4,
			},
		})

		xtest.TestManyTimes(t, func(t testing.TB) {
			e, err := s.Next(endpoint.WithNodeID(ctx, 3))
			require.NoError(t, err)
			require.NotNil(t, e)
			require.Equal(t, "3", e.Address())
			require.Equal(t, uint32(3), e.NodeID())
		})
	})

	t.Run("NormalDistribution", func(t *testing.T) {
		const (
			buckets = 10
			total   = 1000000
			epsilon = int(float64(total) / float64(buckets) * 0.01)
		)
		endpoints := make([]endpoint.Endpoint, buckets)

		for i := 0; i < buckets; i++ {
			endpoints[i] = &mock.Endpoint{
				AddrField:   strconv.Itoa(i),
				NodeIDField: uint32(i),
			}
		}

		s := New(endpoints)

		distribution := make([]int, len(endpoints))
		for i := 0; i < total; i++ {
			e, err := s.Next(ctx)
			require.NoError(t, err)
			require.NotNil(t, e)
			distribution[e.NodeID()]++
		}

		for i := range distribution {
			if distribution[i] < total/buckets-epsilon || distribution[i] > total/buckets+epsilon {
				t.Errorf("unexpected distribuition[%d] = %0.1f%%", i,
					math.Abs(float64(distribution[i]-total/buckets)/float64(total/buckets)*100),
				)
			}
		}
	})
}

func BenchmarkNext1(b *testing.B) {
	benchmarkNextParallel(b, 1)
}

func BenchmarkNext4(b *testing.B) {
	benchmarkNextParallel(b, 4)
}

func BenchmarkNext16(b *testing.B) {
	benchmarkNextParallel(b, 16)
}

func BenchmarkNext32(b *testing.B) {
	benchmarkNextParallel(b, 32)
}

func BenchmarkNext64(b *testing.B) {
	benchmarkNextParallel(b, 64)
}

func BenchmarkNext128(b *testing.B) {
	benchmarkNextParallel(b, 128)
}

func BenchmarkNext256(b *testing.B) {
	benchmarkNextParallel(b, 256)
}

func BenchmarkNext512(b *testing.B) {
	benchmarkNextParallel(b, 512)
}

func benchmarkNextParallel(b *testing.B, parallelism int) {
	ctx := xtest.Context(b)

	s := New([]endpoint.Endpoint{
		&mock.Endpoint{
			AddrField:   "1",
			NodeIDField: 1,
		},
		&mock.Endpoint{
			AddrField:   "2",
			NodeIDField: 2,
		},
		&mock.Endpoint{
			AddrField:   "3",
			NodeIDField: 3,
		},
		&mock.Endpoint{
			AddrField:   "4",
			NodeIDField: 4,
		},
	}, WithFilter(func(e endpoint.Info) bool {
		return e.NodeID()%2 == 0
	}), WithFallback())

	b.ReportAllocs()

	b.ResetTimer()

	var wg sync.WaitGroup
	wg.Add(parallelism)
	for range make([]struct{}, parallelism) {
		go func() {
			defer wg.Done()

			for i := 0; i < b.N/parallelism; i++ {
				e, err := s.Next(ctx)
				require.NoError(b, err)
				require.NotNil(b, e)
			}
		}()
	}
	wg.Wait()
}
