package multi

import (
	"context"
	"fmt"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/balancer"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/balancer/mock"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/conn"
)

func TestCreate(t *testing.T) {
	t.Run("Empty", func(t *testing.T) {
		b := Balancer().(*multi)
		require.Empty(t, b.balancers)
		require.Empty(t, b.filters)

		b2 := b.Create(nil).(*multi)
		require.Empty(t, b2.balancers)
		require.Empty(t, b2.filters)
	})

	t.Run("Filled", func(t *testing.T) {
		var conns0, conns1 []conn.Conn
		m0 := mock.Balancer()
		m0.OnCreate = func(conns []conn.Conn) balancer.Balancer {
			conns0 = conns
			return mock.Balancer()
		}

		m1 := mock.Balancer()
		m1.OnCreate = func(conns []conn.Conn) balancer.Balancer {
			conns1 = conns
			newMock := mock.Balancer()
			newMock.OnCreate = func(conns []conn.Conn) balancer.Balancer {
				panic("must not called")
			}
			return newMock
		}

		b := Balancer(
			WithBalancer(m0, func(cc conn.Conn) bool {
				num, _ := strconv.Atoi(cc.Endpoint().Address())
				return num%2 == 0
			}),
			WithBalancer(m1, func(cc conn.Conn) bool {
				num, _ := strconv.Atoi(cc.Endpoint().Address())
				return num%2 == 1
			}),
		)

		connCount := 6
		var conns []conn.Conn
		for i := 0; i < connCount; i++ {
			conns = append(conns, &mock.Conn{AddrField: strconv.Itoa(i)})
		}

		b2 := b.Create(conns).(*multi)
		require.Len(t, b2.balancers, 2)
		require.Len(t, b2.filters, 2)

		// zero mock return balancer with nil oncreate
		require.Nil(t, b2.balancers[0].(*mock.BalancerMock).OnCreate)

		// first mock return balancer with non nil oncreate
		require.NotNil(t, b2.balancers[1].(*mock.BalancerMock).OnCreate)

		require.Len(t, conns0, connCount/2)
		for i := 0; i < len(conns0); i++ {
			require.Equal(t, strconv.Itoa(i*2), conns0[i].Endpoint().Address(), i)
		}

		require.Len(t, conns1, connCount/2)
		for i := 1; i < len(conns1); i++ {
			require.Equal(t, strconv.Itoa(i*2+1), conns1[i].Endpoint().Address(), i)
		}

		require.Len(t, b2.filters, 2)
		require.True(t, b2.filters[0](conns[0]))
		require.False(t, b2.filters[0](conns[1]))
		require.False(t, b2.filters[1](conns[0]))
		require.True(t, b2.filters[1](conns[1]))
	})
}

func TestNext(t *testing.T) {
	ctx := context.Background()

	t.Run("Empty", func(t *testing.T) {
		b := Balancer()
		res := b.Next(ctx)
		require.Nil(t, res)
	})

	t.Run("SelectFirstNonNilAnswer", func(t *testing.T) {
		answer := mock.Balancer()
		answer.OnNext = func(ctx context.Context, opts ...balancer.NextOption) conn.Conn {
			return &mock.Conn{AddrField: "ok"}
		}
		noanswer := mock.Balancer()
		noanswer.OnNext = func(ctx context.Context, opts ...balancer.NextOption) conn.Conn {
			return nil
		}

		t.Run("First", func(t *testing.T) {
			b := Balancer(WithBalancer(answer, nil), WithBalancer(noanswer, nil))
			res := b.Next(context.Background())
			require.Equal(t, "ok", res.Endpoint().Address())
		})

		t.Run("Second", func(t *testing.T) {
			b := Balancer(WithBalancer(noanswer, nil), WithBalancer(answer, nil))
			res := b.Next(context.Background())
			require.Equal(t, "ok", res.Endpoint().Address())
		})

		t.Run("None", func(t *testing.T) {
			b := Balancer(WithBalancer(noanswer, nil), WithBalancer(noanswer, nil))
			res := b.Next(context.Background())
			require.Nil(t, res)
		})
	})

	t.Run("ProxySameParams", func(t *testing.T) {
		createCheckParams := func(t *testing.T, needContext context.Context, opts ...balancer.NextOption) balancer.Balancer {
			b := mock.Balancer()
			opt := balancer.MakeNextOptions(opts...)
			b.OnNext = func(ctx context.Context, localOpts ...balancer.NextOption) conn.Conn {
				localOpt := balancer.MakeNextOptions(localOpts...)
				require.Equal(t, needContext, ctx)
				require.Equal(t, opt, localOpt)
				return nil
			}
			return b
		}

		type testKey struct{}
		ctx := context.WithValue(context.Background(), testKey{}, "test")
		for _, filter := range []bool{true, false} {
			t.Run(fmt.Sprint(filter), func(t *testing.T) {
				b := Balancer(
					WithBalancer(createCheckParams(t, ctx, balancer.WithAcceptBanned(filter)), nil),
					WithBalancer(createCheckParams(t, ctx, balancer.WithAcceptBanned(filter)), nil),
				)
				_ = b.Next(ctx, balancer.WithAcceptBanned(filter))
			})
		}
	})
}
