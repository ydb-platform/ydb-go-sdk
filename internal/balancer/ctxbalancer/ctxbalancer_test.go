package ctxbalancer

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/balancer"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/balancer/mock"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/conn"
)

func TestCtxBalancer_Create(t *testing.T) {
	b := Balancer([]conn.Conn{
		&mock.Conn{AddrField: "a1", NodeIDField: 1},
		&mock.Conn{AddrField: "a2", NodeIDField: 2},
	}).(*ctxBalancer)
	checkOriginalBalancer := func() {
		require.Len(t, b.connMap, 2)
		require.Equal(t, "a1", b.connMap[1].(*mock.Conn).AddrField)
		require.Equal(t, "a2", b.connMap[2].(*mock.Conn).AddrField)
	}
	checkOriginalBalancer()

	b1 := b.Create([]conn.Conn{
		&mock.Conn{AddrField: "a3", NodeIDField: 3},
		&mock.Conn{AddrField: "a4", NodeIDField: 4},
		&mock.Conn{AddrField: "a5", NodeIDField: 5},
	}).(*ctxBalancer)
	checkOriginalBalancer()

	require.Len(t, b1.connMap, 3)
	require.Equal(t, "a3", b1.connMap[3].(*mock.Conn).AddrField)
	require.Equal(t, "a4", b1.connMap[4].(*mock.Conn).AddrField)
	require.Equal(t, "a5", b1.connMap[5].(*mock.Conn).AddrField)
}

func TestCtxBalancer_Next(t *testing.T) {
	b := Balancer([]conn.Conn{
		&mock.Conn{NodeIDField: 1, State: conn.Online},
		&mock.Conn{NodeIDField: 2, State: conn.Banned},
	}).(*ctxBalancer)

	t.Run("EmptyContext", func(t *testing.T) {
		res := b.Next(context.Background(),
			balancer.WithOnBadState(func(ctx context.Context) {
				t.Error()
			}),
			balancer.WithAcceptBanned(true))
		require.Nil(t, res)
	})

	t.Run("WithPreferOnline", func(t *testing.T) {
		res := b.Next(WithEndpoint(context.Background(), &mock.Endpoint{NodeIDField: 1}),
			balancer.WithOnBadState(func(ctx context.Context) {
				t.Error()
			}),
			balancer.WithAcceptBanned(true)).(*mock.Conn)
		require.Equal(t, uint32(1), res.NodeIDField)
	})

	t.Run("WithPreferBanned", func(t *testing.T) {
		require.Equal(t, conn.Banned, b.connMap[2].GetState())

		for _, allowBanned := range []bool{true, false} {
			t.Run(fmt.Sprint(allowBanned), func(t *testing.T) {
				res := b.Next(WithEndpoint(context.Background(), &mock.Endpoint{NodeIDField: 2}),
					balancer.WithOnBadState(func(ctx context.Context) {
						t.Error()
					}),
					balancer.WithAcceptBanned(allowBanned)).(*mock.Conn)
				require.Equal(t, uint32(2), res.NodeIDField)
			})
		}
	})
}
