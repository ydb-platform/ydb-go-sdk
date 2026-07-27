package balancer

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	balancerConfig "github.com/ydb-platform/ydb-go-sdk/v3/internal/balancer/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/conn"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/conn/state"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/endpoint"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/mock"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xrand"
)

type localDCFilter struct{}

func (localDCFilter) Allow(info balancerConfig.Info, e endpoint.Info) bool {
	return e.Location() == info.SelfLocation
}

func (localDCFilter) String() string { return "LocalDC" }

func discoveredEndpoints(n int) []endpoint.Endpoint {
	out := make([]endpoint.Endpoint, n)
	for i := range out {
		out[i] = endpoint.New(
			fmt.Sprintf("e%d.example:2135", i),
			endpoint.WithID(uint32(i+1)),
		)
	}

	return out
}

func discoveredWithLocations(remote, local int, localDC string) []endpoint.Endpoint {
	out := make([]endpoint.Endpoint, 0, remote+local)
	for i := range remote {
		out = append(out, endpoint.New(
			fmt.Sprintf("remote%d.example:2135", i),
			endpoint.WithID(uint32(i+1)),
			endpoint.WithLocation("remote"),
		))
	}
	for i := range local {
		out = append(out, endpoint.New(
			fmt.Sprintf("local%d.example:2135", i),
			endpoint.WithID(uint32(remote+i+1)),
			endpoint.WithLocation(localDC),
		))
	}

	return out
}

func selectAll(
	previous []conn.Conn,
	discovered []endpoint.Endpoint,
	max int,
	rnd xrand.Rand,
) []endpoint.Endpoint {
	return selectEndpoints(previous, discovered, max, nil, balancerConfig.Info{}, false, rnd)
}

func TestSelectEndpoints(t *testing.T) {
	discovered := discoveredEndpoints(20)

	t.Run("Unlimited", func(t *testing.T) {
		selected := selectAll(nil, discovered, 0, xrand.New(xrand.WithSeed(1)))
		require.Equal(t, discovered, selected)
	})

	t.Run("BelowCap", func(t *testing.T) {
		small := discovered[:3]
		selected := selectAll(nil, small, 10, xrand.New(xrand.WithSeed(1)))
		require.Equal(t, small, selected)
	})

	t.Run("Cap", func(t *testing.T) {
		selected := selectAll(nil, discovered, 5, xrand.New(xrand.WithSeed(1)))
		require.Len(t, selected, 5)
	})

	t.Run("StickyKeep", func(t *testing.T) {
		previous := []conn.Conn{
			&mock.Conn{AddrField: "e0.example:2135", NodeIDField: 1, StateField: state.Online},
			&mock.Conn{AddrField: "e1.example:2135", NodeIDField: 2, StateField: state.Online},
			&mock.Conn{AddrField: "e2.example:2135", NodeIDField: 3, StateField: state.Online},
		}
		selected := selectAll(previous, discovered, 3, xrand.New(xrand.WithSeed(1)))
		require.Len(t, selected, 3)
		require.Equal(t, "e0.example:2135", selected[0].Address())
		require.Equal(t, "e1.example:2135", selected[1].Address())
		require.Equal(t, "e2.example:2135", selected[2].Address())
	})

	t.Run("SkipBannedInSticky", func(t *testing.T) {
		previous := []conn.Conn{
			&mock.Conn{AddrField: "e0.example:2135", NodeIDField: 1, StateField: state.Banned},
			&mock.Conn{AddrField: "e1.example:2135", NodeIDField: 2, StateField: state.Online},
		}
		selected := selectAll(previous, discovered, 2, xrand.New(xrand.WithSeed(1)))
		require.Len(t, selected, 2)
		require.Equal(t, "e1.example:2135", selected[0].Address())
		for _, e := range selected {
			require.NotEqual(t, "e0.example:2135", e.Address(),
				"banned previous endpoint must not be sticky-kept or re-filled")
		}
	})

	t.Run("FillAfterDrop", func(t *testing.T) {
		previous := []conn.Conn{
			&mock.Conn{AddrField: "e0.example:2135", NodeIDField: 1, StateField: state.Online},
			&mock.Conn{AddrField: "gone.example:2135", NodeIDField: 99, StateField: state.Online},
		}
		selected := selectAll(previous, discovered, 2, xrand.New(xrand.WithSeed(1)))
		require.Len(t, selected, 2)
		require.Equal(t, "e0.example:2135", selected[0].Address())
		require.NotEqual(t, "gone.example:2135", selected[1].Address())
	})

	t.Run("IgnoreNilAndDuplicatePrevious", func(t *testing.T) {
		existing := &mock.Conn{
			AddrField:   "e0.example:2135",
			NodeIDField: 1,
			StateField:  state.Online,
		}
		selected := selectAll(
			[]conn.Conn{nil, existing, existing},
			discovered,
			2,
			xrand.New(xrand.WithSeed(1)),
		)

		require.Len(t, selected, 2)
		require.Equal(t, existing.Endpoint().Key(), selected[0].Key())
		require.NotEqual(t, selected[0].Key(), selected[1].Key())
	})
}

func TestSelectEndpointsPrefersFilter(t *testing.T) {
	const localDC = "local"
	info := balancerConfig.Info{SelfLocation: localDC}
	filter := localDCFilter{}
	rnd := xrand.New(xrand.WithLock())

	// Many remote, few local — without filter awareness MaxConnections could
	// exclude all local endpoints and PreferNearestDC would permanently fail.
	discovered := discoveredWithLocations(100, 3, localDC)

	t.Run("NoFallbackUsesOnlyPreferred", func(t *testing.T) {
		for range 20 {
			selected := selectEndpoints(nil, discovered, 9, filter, info, false, rnd)
			require.Len(t, selected, 3, "only 3 local endpoints exist")
			for _, e := range selected {
				require.Equal(t, localDC, e.Location())
			}
		}
	})

	t.Run("WithFallbackFillsPreferredFirst", func(t *testing.T) {
		for range 20 {
			selected := selectEndpoints(nil, discovered, 9, filter, info, true, rnd)
			require.Len(t, selected, 9)
			localCount := 0
			for _, e := range selected {
				if e.Location() == localDC {
					localCount++
				}
			}
			require.Equal(t, 3, localCount, "all local endpoints must be included before remotes")
		}
	})

	t.Run("WithFallbackPreferredAloneFillsCap", func(t *testing.T) {
		manyLocal := discoveredWithLocations(5, 20, localDC)
		selected := selectEndpoints(nil, manyLocal, 9, filter, info, true, rnd)
		require.Len(t, selected, 9)
		for _, e := range selected {
			require.Equal(t, localDC, e.Location())
		}
	})

	t.Run("NoPreferredReturnsEmptyWithoutFallback", func(t *testing.T) {
		remoteOnly := discoveredWithLocations(10, 0, localDC)
		selected := selectEndpoints(nil, remoteOnly, 5, filter, info, false, rnd)
		require.Empty(t, selected)
	})

	t.Run("StickyDoesNotDisplacePreferred", func(t *testing.T) {
		// Previous active set is all remote (bad sticky set from before the fix).
		previous := make([]conn.Conn, 9)
		for i := range previous {
			previous[i] = &mock.Conn{
				AddrField:     fmt.Sprintf("remote%d.example:2135", i),
				NodeIDField:   uint32(i + 1),
				LocationField: "remote",
				StateField:    state.Online,
			}
		}
		selected := selectEndpoints(previous, discovered, 9, filter, info, false, rnd)
		require.Len(t, selected, 3)
		for _, e := range selected {
			require.Equal(t, localDC, e.Location())
		}
	})
}

func TestEndpointByNodeID(t *testing.T) {
	endpoints := discoveredEndpoints(3)

	require.Equal(t, endpoints[1], endpointByNodeID(endpoints, endpoints[1].NodeID()))
	require.Nil(t, endpointByNodeID(endpoints, 404))
}

func TestConnKeys(t *testing.T) {
	cc := &mock.Conn{AddrField: "e0.example:2135", NodeIDField: 1}

	require.Equal(t, map[endpoint.Key]struct{}{
		cc.Endpoint().Key(): {},
	}, connKeys([]conn.Conn{nil, cc}))
}

func TestReplacementEndpoint(t *testing.T) {
	discovered := discoveredEndpoints(3)
	active := []conn.Conn{
		&mock.Conn{
			AddrField:   discovered[0].Address(),
			NodeIDField: discovered[0].NodeID(),
		},
	}

	replacement := replacementEndpoint(discovered, active, discovered[1].Key(), nil, balancerConfig.Info{}, false, nil)
	require.Equal(t, discovered[2].Key(), replacement.Key())

	require.Nil(t, replacementEndpoint(
		discovered[:2], active, discovered[1].Key(), nil, balancerConfig.Info{}, false, nil),
	)
}

func TestReplacementEndpointPrefersFilter(t *testing.T) {
	const localDC = "local"
	info := balancerConfig.Info{SelfLocation: localDC}
	discovered := discoveredWithLocations(5, 2, localDC)
	active := []conn.Conn{
		&mock.Conn{
			AddrField:     discovered[5].Address(), // local0
			NodeIDField:   discovered[5].NodeID(),
			LocationField: localDC,
		},
	}

	replacement := replacementEndpoint(discovered, active, discovered[5].Key(), localDCFilter{}, info, false, nil)
	require.NotNil(t, replacement)
	require.Equal(t, localDC, replacement.Location())
	require.Equal(t, discovered[6].Key(), replacement.Key())

	// Without fallback, do not replace with remote when no other local remains.
	activeBothLocal := []conn.Conn{
		&mock.Conn{AddrField: discovered[5].Address(), NodeIDField: discovered[5].NodeID()},
		&mock.Conn{AddrField: discovered[6].Address(), NodeIDField: discovered[6].NodeID()},
	}
	withoutBanned := activeBothLocal[1:] // local1 still active
	require.Nil(t, replacementEndpoint(
		discovered, withoutBanned, discovered[5].Key(), localDCFilter{}, info, false, nil,
	))
	require.Equal(t, "remote", replacementEndpoint(
		discovered, withoutBanned, discovered[5].Key(), localDCFilter{}, info, true, nil,
	).Location())
}

func TestReplacementEndpointPicksRandomCandidate(t *testing.T) {
	discovered := discoveredEndpoints(5)
	active := []conn.Conn{
		&mock.Conn{AddrField: discovered[0].Address(), NodeIDField: discovered[0].NodeID()},
	}
	// Seeded RNG that returns index 1 among 3 eligible candidates (e1,e2,e3 after
	// excluding active e0 and banned e4) → e2.
	rnd := xrand.New(xrand.WithSeed(1), xrand.WithLock())
	got := replacementEndpoint(
		discovered, active, discovered[4].Key(), nil, balancerConfig.Info{}, false, rnd,
	)
	require.NotNil(t, got)
	require.NotEqual(t, discovered[0].Key(), got.Key())
	require.NotEqual(t, discovered[4].Key(), got.Key())

	// With a fixed seed, two calls with fresh RNGs of the same seed agree.
	again := replacementEndpoint(
		discovered, active, discovered[4].Key(), nil, balancerConfig.Info{}, false,
		xrand.New(xrand.WithSeed(1), xrand.WithLock()),
	)
	require.Equal(t, got.Key(), again.Key())
}
