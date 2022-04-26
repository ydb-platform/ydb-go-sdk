package balancers

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/balancer/mock"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/conn"
)

func TestPreferLocalDC(t *testing.T) {
	ctx := context.Background()

	conns := []conn.Conn{
		&mock.ConnMock{AddrField: "1", LocalDCField: false},
		&mock.ConnMock{AddrField: "2", State: conn.Online, LocalDCField: true},
		&mock.ConnMock{AddrField: "3", State: conn.Online, LocalDCField: true},
	}
	rr := PreferLocalDC(RoundRobin()).Create(conns)
	require.Contains(t, []conn.Conn{conns[1], conns[2]}, rr.Next(ctx, false))
	require.Contains(t, []conn.Conn{conns[1], conns[2]}, rr.Next(ctx, false))
	require.Contains(t, []conn.Conn{conns[1], conns[2]}, rr.Next(ctx, false))

	// ban local connections
	conns[1].SetState(conn.Banned)
	conns[2].SetState(conn.Banned)
	require.Contains(t, []conn.Conn{conns[1], conns[2]}, rr.Next(ctx, true))
	require.Nil(t, rr.Next(ctx, false))
}

func TestPreferLocalDCWithFallBack(t *testing.T) {
	ctx := context.Background()

	conns := []conn.Conn{
		&mock.ConnMock{AddrField: "1", LocalDCField: false, State: conn.Online},
		&mock.ConnMock{AddrField: "2", State: conn.Online, LocalDCField: true},
		&mock.ConnMock{AddrField: "3", State: conn.Online, LocalDCField: true},
	}
	rr := PreferLocalDCWithFallBack(RoundRobin()).Create(conns)
	require.Contains(t, []conn.Conn{conns[1], conns[2]}, rr.Next(ctx, false))
	require.Contains(t, []conn.Conn{conns[1], conns[2]}, rr.Next(ctx, false))
	require.Contains(t, []conn.Conn{conns[1], conns[2]}, rr.Next(ctx, false))

	// ban connections
	conns[1].SetState(conn.Banned)
	conns[2].SetState(conn.Banned)
	require.Contains(t, []conn.Conn{conns[1], conns[2]}, rr.Next(ctx, true))
	require.Equal(t, conns[0], rr.Next(ctx, false))
}

func TestPreferLocations(t *testing.T) {
	ctx := context.Background()

	conns := []conn.Conn{
		&mock.ConnMock{AddrField: "1", LocationField: "zero", State: conn.Online},
		&mock.ConnMock{AddrField: "2", State: conn.Online, LocationField: "one"},
		&mock.ConnMock{AddrField: "3", State: conn.Online, LocationField: "two"},
	}

	rr := PreferLocations(RoundRobin(), "zero", "two").Create(conns)
	require.Contains(t, []conn.Conn{conns[0], conns[2]}, rr.Next(ctx, false))
	require.Contains(t, []conn.Conn{conns[0], conns[2]}, rr.Next(ctx, false))
	require.Contains(t, []conn.Conn{conns[0], conns[2]}, rr.Next(ctx, false))

	// ban zero, two
	conns[0].SetState(conn.Banned)
	conns[2].SetState(conn.Banned)
	require.Contains(t, []conn.Conn{conns[0], conns[2]}, rr.Next(ctx, true))
	require.Nil(t, rr.Next(ctx, false))
}

func TestPreferLocationsWithFallback(t *testing.T) {
	ctx := context.Background()

	conns := []conn.Conn{
		&mock.ConnMock{AddrField: "1", LocationField: "zero", State: conn.Online},
		&mock.ConnMock{AddrField: "2", State: conn.Online, LocationField: "one"},
		&mock.ConnMock{AddrField: "3", State: conn.Online, LocationField: "two"},
	}

	rr := PreferLocationsWithFallback(RoundRobin(), "zero", "two").Create(conns)
	require.Contains(t, []conn.Conn{conns[0], conns[2]}, rr.Next(ctx, false))
	require.Contains(t, []conn.Conn{conns[0], conns[2]}, rr.Next(ctx, false))
	require.Contains(t, []conn.Conn{conns[0], conns[2]}, rr.Next(ctx, false))

	// ban zero, two
	conns[0].SetState(conn.Banned)
	conns[2].SetState(conn.Banned)
	require.Contains(t, []conn.Conn{conns[0], conns[2]}, rr.Next(ctx, true))
	require.Equal(t, conns[1], rr.Next(ctx, false))
}
