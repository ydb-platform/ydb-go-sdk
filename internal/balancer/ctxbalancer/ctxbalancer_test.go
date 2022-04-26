package ctxbalancer

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/balancer/mock"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/conn"
)

func TestCtxBalancer_Create(t *testing.T) {
	b := Balancer([]conn.Conn{
		&mock.ConnMock{AddrField: "a1", NodeIDField: 1},
		&mock.ConnMock{AddrField: "a2", NodeIDField: 2},
	}).(*ctxBalancer)
	checkOriginalBalancer := func() {
		require.Len(t, b.connMap, 2)
		require.Equal(t, "a1", b.connMap[1].(*mock.ConnMock).AddrField)
		require.Equal(t, "a2", b.connMap[2].(*mock.ConnMock).AddrField)
	}
	checkOriginalBalancer()

	b1 := b.Create([]conn.Conn{
		&mock.ConnMock{AddrField: "a3", NodeIDField: 3},
		&mock.ConnMock{AddrField: "a4", NodeIDField: 4},
		&mock.ConnMock{AddrField: "a5", NodeIDField: 5},
	}).(*ctxBalancer)
	checkOriginalBalancer()

	require.Len(t, b1.connMap, 3)
	require.Equal(t, "a3", b1.connMap[3].(*mock.ConnMock).AddrField)
	require.Equal(t, "a4", b1.connMap[4].(*mock.ConnMock).AddrField)
	require.Equal(t, "a5", b1.connMap[5].(*mock.ConnMock).AddrField)
}

func TestCtxBalancer_Next(t *testing.T) {
	b := Balancer([]conn.Conn{
		&mock.ConnMock{NodeIDField: 1, State: conn.Online},
		&mock.ConnMock{NodeIDField: 2, State: conn.Banned},
	}).(*ctxBalancer)

	t.Run("EmptyContext", func(t *testing.T) {
		res := b.Next(context.Background(), true)
		require.Nil(t, res)
	})

	t.Run("WithPreferOnline", func(t *testing.T) {
		res := b.Next(WithEndpoint(context.Background(), &mock.EndpointMock{NodeIDField: 1}), true).(*mock.ConnMock)
		require.Equal(t, uint32(1), res.NodeIDField)
	})

	t.Run("WithPreferBanned", func(t *testing.T) {
		require.Equal(t, conn.Banned, b.connMap[2].GetState())

		for _, allowBanned := range []bool{true, false} {
			res := b.Next(WithEndpoint(context.Background(), &mock.EndpointMock{NodeIDField: 2}), allowBanned).(*mock.ConnMock)
			require.Equal(t, uint32(2), res.NodeIDField)
		}
	})
}

func TestCtxBalancer_NeedRefresh(t *testing.T) {
	t.Run("Empty", func(t *testing.T) {
		require.False(t, Balancer(nil).NeedRefresh(context.Background()))
	})

	t.Run("Empty", func(t *testing.T) {
		b := Balancer([]conn.Conn{&mock.ConnMock{NodeIDField: 1}})
		require.False(t, b.NeedRefresh(context.Background()))
	})
}
