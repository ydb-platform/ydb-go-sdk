package rr

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/balancer"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/balancer/mock"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/conn"
)

func TestRandomChoice_Create(t *testing.T) {
	conns := []conn.Conn{&mock.ConnMock{Address: "1"}, &mock.ConnMock{Address: "2"}}
	b := RandomChoice(conns).(*randomChoice)
	require.Equal(t, conns, b.conns)

	conns2 := []conn.Conn{&mock.ConnMock{Address: "3"}, &mock.ConnMock{Address: "4"}}
	b2 := b.Create(conns2).(*randomChoice)

	require.Equal(t, conns, b.conns) // check about not modify original balancer
	require.Equal(t, conns2, b2.conns)
	require.NotEqual(t, b.needRefresh, b2.needRefresh)
}

func TestRandomChoice_NeedRefresh(t *testing.T) {
	soonExpireCtx := func() context.Context {
		ctx, _ := context.WithTimeout(context.Background(), time.Millisecond)
		return ctx
	}

	t.Run("Empty", func(t *testing.T) {

		t.Run("CancelledContext", func(t *testing.T) {
			b := RandomChoice(nil)
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			require.False(t, b.NeedRefresh(ctx))
		})

		t.Run("WorkContext", func(t *testing.T) {
			b := RandomChoice(nil)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			resultChan := make(chan bool, 1)
			go func() {
				resultChan <- b.NeedRefresh(ctx)
			}()

			tSleep()

			require.Len(t, resultChan, 0)

			require.Nil(t, b.Next(ctx, false))

			tSleep()

			require.Len(t, resultChan, 0)
		})
	})

	t.Run("FewBanned", func(t *testing.T) {
		var conns = []conn.Conn{&mock.ConnMock{Address: "ban", State: conn.Banned}}

		// fill with many good connections
		for i := 0; i < 10; i++ {
			conns = append(conns, &mock.ConnMock{Address: strconv.Itoa(i), State: conn.Online})
		}

		b := RandomChoice(conns)

		// try next many times more then connections count - for gurantee about Next see a bad connection in internal loop
		for i := 0; i < 1000; i++ {
			c := b.Next(nil, false)
			require.NotNil(t, c)
			require.NotEqual(t, conn.Banned, c.GetState())
			require.False(t, b.NeedRefresh(soonExpireCtx()))
		}
	})

	t.Run("ManyBanned", func(t *testing.T) {
		createBalancer := func() balancer.Balancer {
			conns := []conn.Conn{&mock.ConnMock{Address: "1", State: conn.Online}, &mock.ConnMock{Address: "2", State: conn.Banned}, &mock.ConnMock{Address: "3", State: conn.Banned}}
			return RandomChoice(conns)
		}
		t.Run("AllowBanned", func(t *testing.T) {
			count := 100
			for i := 0; i < count; i++ {
				b := createBalancer()
				res := make(chan bool)
				go func() { res <- b.NeedRefresh(soonExpireCtx()) }()
				b.Next(context.Background(), true)
				require.False(t, <-res)
			}
		})
		t.Run("DenyBanned", func(t *testing.T) {
			count := 100
			needRefresh := 0
			for i := 0; i < count; i++ {
				b := createBalancer()
				res := make(chan bool)
				go func() { res <- b.NeedRefresh(soonExpireCtx()) }()
				b.Next(context.Background(), false)
				if <-res {
					needRefresh++
				}
			}
			require.Greater(t, needRefresh, 10)
			require.Less(t, needRefresh, count)
		})
	})
}

func TestRandomChoice_Next(t *testing.T) {
	t.Run("Online", func(t *testing.T) {
		b := RandomChoice([]conn.Conn{&mock.ConnMock{Address: "1", State: conn.Online}, &mock.ConnMock{Address: "2", State: conn.Online}})

		res := make(map[string]int)
		count := 100
		delta := 10.0
		for i := 0; i < count; i++ {
			c := b.Next(context.Background(), false)
			res[c.Endpoint().Address()]++
		}

		require.Len(t, res, 2)
		require.Equal(t, count, res["1"]+res["2"])
		require.InDelta(t, count/2, res["1"], delta)
		require.InDelta(t, count/2, res["2"], delta)
	})
	t.Run("PartiallyBanned", func(t *testing.T) {
		b := RandomChoice([]conn.Conn{&mock.ConnMock{Address: "1", State: conn.Online}, &mock.ConnMock{Address: "2", State: conn.Banned}, &mock.ConnMock{Address: "3", State: conn.Online}})

		t.Run("AllowBanned", func(t *testing.T) {
			res := make(map[string]int)
			count := 100
			delta := 10.0
			for i := 0; i < count; i++ {
				c := b.Next(context.Background(), true)
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
				c := b.Next(context.Background(), false)
				res[c.Endpoint().Address()]++
			}

			require.Len(t, res, 2)
			require.Equal(t, count, res["1"]+res["3"])
			require.InDelta(t, count/2, res["1"], delta)
			require.InDelta(t, count/2, res["3"], delta)
		})

	})
	t.Run("FullBanned", func(t *testing.T) {
		b := RandomChoice([]conn.Conn{&mock.ConnMock{Address: "1", State: conn.Banned}, &mock.ConnMock{Address: "2", State: conn.Banned}})

		t.Run("AllowBanned", func(t *testing.T) {
			res := make(map[string]int)
			count := 100
			delta := 10.0
			for i := 0; i < count; i++ {
				c := b.Next(context.Background(), true)
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
				c := b.Next(context.Background(), false)
				require.Nil(t, c, i)
			}
		})
	})
}

func tSleep() {
	time.Sleep(time.Millisecond * 10)
}

func TestRoundRobin(t *testing.T) {
	conns := []conn.Conn{&mock.ConnMock{Address: "1"}}
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
	conns := []conn.Conn{&mock.ConnMock{Address: "1"}}
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
		conns := []conn.Conn{&mock.ConnMock{Address: "1"}}
		conns2 := []conn.Conn{&mock.ConnMock{Address: "2"}}

		b := RoundRobin(conns).(*roundRobin)
		b2 := b.Create(conns2).(*roundRobin)

		require.Equal(t, conns, b.conns)
		require.Equal(t, conns2, b2.conns)
	})
}

func TestRoundRobin_Next(t *testing.T) {
	t.Run("Online", func(t *testing.T) {
		conns := []conn.Conn{&mock.ConnMock{Address: "1", State: conn.Online}, &mock.ConnMock{Address: "2", State: conn.Online}}
		b := RoundRobin(conns).(*roundRobin)
		b.last = -1
		c := b.Next(nil, false)
		require.Equal(t, conns[0], c)
		c = b.Next(nil, false)
		require.Equal(t, conns[1], c)
		c = b.Next(nil, false)
		require.Equal(t, conns[0], c)
	})
	t.Run("WithBanns", func(t *testing.T) {
		t.Run("InMiddle", func(t *testing.T) {
			conns := []conn.Conn{&mock.ConnMock{Address: "1", State: conn.Online}, &mock.ConnMock{Address: "2", State: conn.Banned}, &mock.ConnMock{Address: "3", State: conn.Online}}
			b := RoundRobin(conns).(*roundRobin)
			b.last = -1
			c := b.Next(nil, false)
			require.Equal(t, conns[0], c)
			c = b.Next(nil, false)
			require.Equal(t, conns[2], c)

			// bad connection skipped, but counter has no additional moved on bad connection
			// that mean next call return same result
			c = b.Next(nil, false)
			require.Equal(t, conns[2], c)

			c = b.Next(nil, false)
			require.Equal(t, conns[0], c)
		})
	})
}
