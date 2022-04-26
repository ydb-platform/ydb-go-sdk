package rr

import (
	"context"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/balancer"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/balancer/mock"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/conn"
)

func TestRandomChoice_Create(t *testing.T) {
	conns := []conn.Conn{&mock.ConnMock{AddrField: "1"}, &mock.ConnMock{AddrField: "2"}}
	b := RandomChoice(conns).(*randomChoice)
	require.Equal(t, conns, b.conns)

	conns2 := []conn.Conn{&mock.ConnMock{AddrField: "3"}, &mock.ConnMock{AddrField: "4"}}
	b2 := b.Create(conns2).(*randomChoice)

	require.Equal(t, conns, b.conns) // check about not modify original balancer
	require.Equal(t, conns2, b2.conns)
}

func TestRandomChoice_Next(t *testing.T) {
	ctx := context.Background()

	t.Run("CheckResult", func(t *testing.T) {
		t.Run("Online", func(t *testing.T) {
			b := RandomChoice([]conn.Conn{
				&mock.ConnMock{AddrField: "1", State: conn.Online},
				&mock.ConnMock{AddrField: "2", State: conn.Online},
			})

			res := make(map[string]int)
			count := 100
			delta := 10.0
			for i := 0; i < count; i++ {
				c := b.Next(ctx)
				res[c.Endpoint().Address()]++
			}

			require.Len(t, res, 2)
			require.Equal(t, count, res["1"]+res["2"])
			require.InDelta(t, count/2, res["1"], delta)
			require.InDelta(t, count/2, res["2"], delta)
		})
		t.Run("PartiallyBanned", func(t *testing.T) {
			b := RandomChoice([]conn.Conn{
				&mock.ConnMock{AddrField: "1", State: conn.Online},
				&mock.ConnMock{AddrField: "2", State: conn.Banned},
				&mock.ConnMock{AddrField: "3", State: conn.Online},
			})

			t.Run("AllowBanned", func(t *testing.T) {
				res := make(map[string]int)
				count := 100
				delta := 10.0
				for i := 0; i < count; i++ {
					c := b.Next(ctx, balancer.WithAcceptBanned(true))
					res[c.Endpoint().Address()]++
				}

				require.Len(t, res, 3)
				require.Equal(t, count, res["1"]+res["2"]+res["3"])
				require.InDelta(t, count/3, res["1"], delta)
				require.InDelta(t, count/3, res["2"], delta)
				require.InDelta(t, count/3, res["3"], delta)
			})

			t.Run("DenyBanned", func(t *testing.T) {
				res := make(map[string]int)
				count := 100
				delta := 10.0
				for i := 0; i < count; i++ {
					c := b.Next(ctx)
					res[c.Endpoint().Address()]++
				}

				require.Len(t, res, 2)
				require.Equal(t, count, res["1"]+res["3"])
				require.InDelta(t, count/2, res["1"], delta)
				require.InDelta(t, count/2, res["3"], delta)
			})
		})
		t.Run("FullBanned", func(t *testing.T) {
			b := RandomChoice([]conn.Conn{
				&mock.ConnMock{AddrField: "1", State: conn.Banned},
				&mock.ConnMock{AddrField: "2", State: conn.Banned},
			})

			t.Run("AllowBanned", func(t *testing.T) {
				res := make(map[string]int)
				count := 100
				delta := 10.0
				for i := 0; i < count; i++ {
					c := b.Next(ctx, balancer.WithAcceptBanned(true))
					res[c.Endpoint().Address()]++
				}

				require.Len(t, res, 2)
				require.Equal(t, count, res["1"]+res["2"])
				require.InDelta(t, count/2, res["1"], delta)
				require.InDelta(t, count/2, res["2"], delta)
			})

			t.Run("DenyBanned", func(t *testing.T) {
				count := 100
				for i := 0; i < count; i++ {
					c := b.Next(ctx)
					require.Nil(t, c, i)
				}
			})
		})
	})

	t.Run("CheckDiscovery", func(t *testing.T) {
		t.Run("Empty", func(t *testing.T) {
			b := RandomChoice(nil)
			b.Next(ctx, balancer.WithOnBadState(func(ctx context.Context) {
				t.Error()
			}))
		})

		t.Run("FewBanned", func(t *testing.T) {
			conns := []conn.Conn{&mock.ConnMock{AddrField: "ban", State: conn.Banned}}
			// fill with many good connections
			for i := 0; i < 10; i++ {
				conns = append(conns, &mock.ConnMock{AddrField: strconv.Itoa(i), State: conn.Online})
			}

			b := RandomChoice(conns)

			// try next many times more then connections count - for `guarantee` about Next see a bad connection in internal loop
			for i := 0; i < 1000; i++ {
				c := b.Next(ctx, balancer.WithOnBadState(func(ctx context.Context) {
					t.Error(i)
				}))
				require.NotNil(t, c)
				require.NotEqual(t, conn.Banned, c.GetState())
			}
		})

		t.Run("ManyBanned", func(t *testing.T) {
			createBalancer := func() balancer.Balancer {
				conns := []conn.Conn{
					&mock.ConnMock{AddrField: "1", State: conn.Online},
					&mock.ConnMock{AddrField: "2", State: conn.Banned},
					&mock.ConnMock{AddrField: "3", State: conn.Banned},
				}
				return RandomChoice(conns)
			}
			t.Run("AllowBanned", func(t *testing.T) {
				count := 100
				for i := 0; i < count; i++ {
					b := createBalancer()
					b.Next(ctx, balancer.WithAcceptBanned(true), balancer.WithOnBadState(func(ctx context.Context) {
						t.Error(i)
					}))
				}
			})
			t.Run("DenyBanned", func(t *testing.T) {
				count := 100
				needRefresh := 0
				for i := 0; i < count; i++ {
					b := createBalancer()
					b.Next(ctx, balancer.WithOnBadState(func(ctx context.Context) {
						needRefresh++
					}))
				}
				require.Greater(t, needRefresh, 10)
				require.Less(t, needRefresh, count)
			})
		})
	})
}

func TestRoundRobin(t *testing.T) {
	conns := []conn.Conn{&mock.ConnMock{AddrField: "1"}}
	hasNonZeroPosition := false
	var b *roundRobin
	for i := 0; i < 100; i++ {
		b = RoundRobin(conns).(*roundRobin)
		if b.last > 0 {
			hasNonZeroPosition = true
			break
		}
	}
	require.True(t, hasNonZeroPosition)
	require.Equal(t, conns, b.conns)
}

func TestRoundRobinWithStartPosition(t *testing.T) {
	conns := []conn.Conn{&mock.ConnMock{AddrField: "1"}}
	b := RoundRobinWithStartPosition(conns, 5).(*roundRobin)
	require.Equal(t, int64(5), b.last)
	require.Equal(t, conns, b.conns)
}

func TestRoundRobin_Create(t *testing.T) {
	t.Run("NextIndex", func(t *testing.T) {
		for i := 0; i < 100; i++ {
			b := RoundRobin(nil).(*roundRobin)
			b1 := b.Create(nil).(*roundRobin)
			if b.last != 0 && b1.last != 0 && b.last != b1.last {
				return
			}
		}
		t.Errorf("Created balancer must start from random position")
	})
	t.Run("Conns", func(t *testing.T) {
		conns := []conn.Conn{&mock.ConnMock{AddrField: "1"}}
		conns2 := []conn.Conn{&mock.ConnMock{AddrField: "2"}}

		b := RoundRobin(conns).(*roundRobin)
		b2 := b.Create(conns2).(*roundRobin)

		require.Equal(t, conns, b.conns)
		require.Equal(t, conns2, b2.conns)
	})
}

func TestRoundRobin_Next(t *testing.T) {
	ctx := context.Background()
	t.Run("CheckResult", func(t *testing.T) {
		t.Run("Online", func(t *testing.T) {
			conns := []conn.Conn{
				&mock.ConnMock{AddrField: "1", State: conn.Online},
				&mock.ConnMock{AddrField: "2", State: conn.Online},
			}
			b := RoundRobin(conns).(*roundRobin)
			b.last = -1
			c := b.Next(ctx)
			require.Equal(t, conns[0], c)
			c = b.Next(ctx)
			require.Equal(t, conns[1], c)
			c = b.Next(ctx)
			require.Equal(t, conns[0], c)
		})
		t.Run("WithBanns", func(t *testing.T) {
			t.Run("InMiddle", func(t *testing.T) {
				conns := []conn.Conn{
					&mock.ConnMock{AddrField: "1", State: conn.Online},
					&mock.ConnMock{AddrField: "2", State: conn.Banned},
					&mock.ConnMock{AddrField: "3", State: conn.Online},
				}
				b := RoundRobin(conns).(*roundRobin)
				t.Run("AllowBanned", func(t *testing.T) {
					b.last = -1
					c := b.Next(ctx, balancer.WithAcceptBanned(true))
					require.Equal(t, conns[0], c)
					c = b.Next(ctx, balancer.WithAcceptBanned(true))
					require.Equal(t, conns[1], c)
					c = b.Next(ctx, balancer.WithAcceptBanned(true))
					require.Equal(t, conns[2], c)
					c = b.Next(ctx, balancer.WithAcceptBanned(true))
					require.Equal(t, conns[0], c)
				})
				t.Run("DenyBanned", func(t *testing.T) {
					b.last = -1
					c := b.Next(ctx)
					require.Equal(t, conns[0], c)
					c = b.Next(ctx)
					require.Equal(t, conns[2], c)

					// bad connection skipped, but counter has no additional moved on bad connection
					// that mean next call return same result
					c = b.Next(ctx)
					require.Equal(t, conns[2], c)

					c = b.Next(ctx)
					require.Equal(t, conns[0], c)
				})
			})
			t.Run("All", func(t *testing.T) {
				conns := []conn.Conn{
					&mock.ConnMock{AddrField: "1", State: conn.Banned},
					&mock.ConnMock{AddrField: "1", State: conn.Banned},
				}
				b := RoundRobin(conns).(*roundRobin)

				t.Run("AllowBanned", func(t *testing.T) {
					b.last = -1
					c := b.Next(ctx, balancer.WithAcceptBanned(true))
					require.Equal(t, conns[0], c)
					c = b.Next(ctx, balancer.WithAcceptBanned(true))
					require.Equal(t, conns[1], c)
					c = b.Next(ctx, balancer.WithAcceptBanned(true))
					require.Equal(t, conns[0], c)
				})
				t.Run("DenyBanned", func(t *testing.T) {
					c := b.Next(ctx)
					require.Nil(t, c)
					c = b.Next(ctx)
					require.Nil(t, c)
				})
			})
		})
	})

	t.Run("CheckNeedRefresh", func(t *testing.T) {
		t.Run("Online", func(t *testing.T) {
			b := RoundRobin([]conn.Conn{&mock.ConnMock{State: conn.Online}})
			b.Next(ctx, balancer.WithOnBadState(func(ctx context.Context) {
				t.Error()
			}))
		})

		t.Run("WithBanned", func(t *testing.T) {
			b := RoundRobin([]conn.Conn{
				&mock.ConnMock{State: conn.Online},
				&mock.ConnMock{State: conn.Banned},
				&mock.ConnMock{State: conn.Banned},
			}).(*roundRobin)

			b.last = 0
			b.Next(ctx, balancer.WithAcceptBanned(true), balancer.WithOnBadState(func(ctx context.Context) {
				t.Error()
			}))

			b.last = 0
			discoveryCalled := false
			b.Next(ctx, balancer.WithOnBadState(func(ctx context.Context) {
				discoveryCalled = true
			}))
			require.True(t, discoveryCalled)
		})
	})
}
