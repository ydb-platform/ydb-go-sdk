package balancers

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/balancer"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/balancer/mock"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/conn"
)

func TestPreferLocalDC(t *testing.T) {
	ctx := context.Background()

	conns := []conn.Conn{
		&mock.Conn{AddrField: "1", LocalDCField: false},
		&mock.Conn{AddrField: "2", State: conn.Online, LocalDCField: true},
		&mock.Conn{AddrField: "3", State: conn.Online, LocalDCField: true},
	}
	rr := PreferLocalDC(RoundRobin()).Create(conns)
	require.Contains(t, []conn.Conn{conns[1], conns[2]}, rr.Next(ctx))
	require.Contains(t, []conn.Conn{conns[1], conns[2]}, rr.Next(ctx))
	require.Contains(t, []conn.Conn{conns[1], conns[2]}, rr.Next(ctx))

	// ban local connections
	conns[1].SetState(conn.Banned)
	conns[2].SetState(conn.Banned)
	require.Contains(t, []conn.Conn{conns[1], conns[2]}, rr.Next(ctx, balancer.WithAcceptBanned(true)))
	require.Nil(t, rr.Next(ctx))
}

func TestPreferLocalDCWithFallBack(t *testing.T) {
	ctx := context.Background()

	conns := []conn.Conn{
		&mock.Conn{AddrField: "1", LocalDCField: false, State: conn.Online},
		&mock.Conn{AddrField: "2", State: conn.Online, LocalDCField: true},
		&mock.Conn{AddrField: "3", State: conn.Online, LocalDCField: true},
	}
	rr := PreferLocalDCWithFallBack(RoundRobin()).Create(conns)
	require.Contains(t, []conn.Conn{conns[1], conns[2]}, rr.Next(ctx))
	require.Contains(t, []conn.Conn{conns[1], conns[2]}, rr.Next(ctx))
	require.Contains(t, []conn.Conn{conns[1], conns[2]}, rr.Next(ctx))

	// ban connections
	conns[1].SetState(conn.Banned)
	conns[2].SetState(conn.Banned)
	require.Contains(t, []conn.Conn{conns[1], conns[2]}, rr.Next(ctx, balancer.WithAcceptBanned(true)))
	require.Equal(t, conns[0], rr.Next(ctx))
}

func TestPreferLocations(t *testing.T) {
	ctx := context.Background()

	conns := []conn.Conn{
		&mock.Conn{AddrField: "1", LocationField: "zero", State: conn.Online},
		&mock.Conn{AddrField: "2", State: conn.Online, LocationField: "one"},
		&mock.Conn{AddrField: "3", State: conn.Online, LocationField: "two"},
	}

	rr := PreferLocations(RoundRobin(), "zero", "two").Create(conns)
	require.Contains(t, []conn.Conn{conns[0], conns[2]}, rr.Next(ctx))
	require.Contains(t, []conn.Conn{conns[0], conns[2]}, rr.Next(ctx))
	require.Contains(t, []conn.Conn{conns[0], conns[2]}, rr.Next(ctx))

	// ban zero, two
	conns[0].SetState(conn.Banned)
	conns[2].SetState(conn.Banned)
	require.Contains(t, []conn.Conn{conns[0], conns[2]}, rr.Next(ctx, balancer.WithAcceptBanned(true)))
	require.Nil(t, rr.Next(ctx))
}

func TestPreferLocationsWithFallback(t *testing.T) {
	ctx := context.Background()

	conns := []conn.Conn{
		&mock.Conn{AddrField: "1", LocationField: "zero", State: conn.Online},
		&mock.Conn{AddrField: "2", State: conn.Online, LocationField: "one"},
		&mock.Conn{AddrField: "3", State: conn.Online, LocationField: "two"},
	}

	rr := PreferLocationsWithFallback(RoundRobin(), "zero", "two").Create(conns)
	require.Contains(t, []conn.Conn{conns[0], conns[2]}, rr.Next(ctx))
	require.Contains(t, []conn.Conn{conns[0], conns[2]}, rr.Next(ctx))
	require.Contains(t, []conn.Conn{conns[0], conns[2]}, rr.Next(ctx))

	// ban zero, two
	conns[0].SetState(conn.Banned)
	conns[2].SetState(conn.Banned)
	require.Contains(t, []conn.Conn{conns[0], conns[2]}, rr.Next(ctx, balancer.WithAcceptBanned(true)))
	require.Equal(t, conns[1], rr.Next(ctx))
}
