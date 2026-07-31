package balancer

import (
	"sort"
	"testing"

	"github.com/stretchr/testify/require"

	balancerConfig "github.com/ydb-platform/ydb-go-sdk/v3/internal/balancer/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/conn"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/conn/state"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/endpoint"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/mock"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xrand"
)

// testRand returns a deterministic Rand so shuffle-based selection is reproducible.
func testRand() xrand.Rand {
	return xrand.New(xrand.WithSeed(42))
}

// ep is a small helper to build a mock endpoint with an address and node id.
func ep(addr string, nodeID uint32) endpoint.Endpoint {
	return &mock.Endpoint{AddrField: addr, NodeIDField: nodeID}
}

// cn is a small helper to build a mock connection with an address, node id and state.
func cn(addr string, nodeID uint32, st state.State) conn.Conn {
	return &mock.Conn{AddrField: addr, NodeIDField: nodeID, StateField: st}
}

// addrsOf extracts a sorted slice of addresses for order-independent comparison.
func addrsOf(endpoints []endpoint.Endpoint) []string {
	res := make([]string, 0, len(endpoints))
	for _, e := range endpoints {
		res = append(res, e.Address())
	}
	sort.Strings(res)

	return res
}

// locationFilter allows only endpoints whose Location equals info.SelfLocation.
func locationFilter() balancerConfig.Filter {
	return filterFunc(func(info balancerConfig.Info, e endpoint.Info) bool {
		return info.SelfLocation == e.Location()
	})
}

func TestSelectEndpoints_Unlimited(t *testing.T) {
	discovered := []endpoint.Endpoint{ep("1", 1), ep("2", 2), ep("3", 3)}

	t.Run("MaxZeroReturnsAll", func(t *testing.T) {
		got := selectEndpoints(nil, discovered, 0, nil, balancerConfig.Info{}, false, testRand())
		require.Equal(t, discovered, got)
	})

	t.Run("MaxNegativeReturnsAll", func(t *testing.T) {
		got := selectEndpoints(nil, discovered, -5, nil, balancerConfig.Info{}, false, testRand())
		require.Equal(t, discovered, got)
	})

	t.Run("DiscoveredWithinLimitReturnsAll", func(t *testing.T) {
		got := selectEndpoints(nil, discovered, 5, nil, balancerConfig.Info{}, false, testRand())
		require.Equal(t, discovered, got)
	})

	t.Run("DiscoveredEqualLimitReturnsAll", func(t *testing.T) {
		got := selectEndpoints(nil, discovered, 3, nil, balancerConfig.Info{}, false, testRand())
		require.Equal(t, discovered, got)
	})
}

func TestSelectEndpoints_NoFilter(t *testing.T) {
	discovered := []endpoint.Endpoint{ep("1", 1), ep("2", 2), ep("3", 3), ep("4", 4)}

	got := selectEndpoints(nil, discovered, 2, nil, balancerConfig.Info{}, false, testRand())
	require.Len(t, got, 2)
}

func TestSelectEndpoints_FilterNoFallback(t *testing.T) {
	// preferred: location "t", other: location "f"
	discovered := []endpoint.Endpoint{
		&mock.Endpoint{AddrField: "t1", NodeIDField: 1, LocationField: "t"},
		&mock.Endpoint{AddrField: "t2", NodeIDField: 2, LocationField: "t"},
		&mock.Endpoint{AddrField: "t3", NodeIDField: 3, LocationField: "t"},
		&mock.Endpoint{AddrField: "f1", NodeIDField: 4, LocationField: "f"},
		&mock.Endpoint{AddrField: "f2", NodeIDField: 5, LocationField: "f"},
	}

	got := selectEndpoints(
		nil, discovered, 2, locationFilter(), balancerConfig.Info{SelfLocation: "t"}, false, testRand(),
	)

	require.Len(t, got, 2)
	for _, e := range got {
		require.Equal(t, "t", e.Location(), "no-fallback must only pick preferred endpoints")
	}
}

func TestSelectEndpoints_FilterWithFallback(t *testing.T) {
	discovered := []endpoint.Endpoint{
		&mock.Endpoint{AddrField: "t1", NodeIDField: 1, LocationField: "t"},
		&mock.Endpoint{AddrField: "f1", NodeIDField: 2, LocationField: "f"},
		&mock.Endpoint{AddrField: "f2", NodeIDField: 3, LocationField: "f"},
		&mock.Endpoint{AddrField: "f3", NodeIDField: 4, LocationField: "f"},
	}

	t.Run("FillsFromOtherWhenPreferredNotEnough", func(t *testing.T) {
		// Only 1 preferred ("t1"), max=3 -> need to fill 2 from other.
		got := selectEndpoints(
			nil, discovered, 3, locationFilter(), balancerConfig.Info{SelfLocation: "t"}, true, testRand(),
		)
		require.Len(t, got, 3)

		var preferredCount int
		for _, e := range got {
			if e.Location() == "t" {
				preferredCount++
			}
		}
		require.Equal(t, 1, preferredCount, "the single preferred endpoint must be kept")
	})

	t.Run("NeverExceedsMaxConnectionsWhenFilling", func(t *testing.T) {
		// 1 preferred + 3 other, max=2. Fallback fill must respect the remaining
		// budget (max - selected) and never overflow the limit.
		got := selectEndpoints(
			nil, discovered, 2, locationFilter(), balancerConfig.Info{SelfLocation: "t"}, true, testRand(),
		)
		require.Len(t, got, 2, "total selected must not exceed maxConnections")
	})

	t.Run("StopsWhenPreferredAlreadyMeetsLimit", func(t *testing.T) {
		many := []endpoint.Endpoint{
			&mock.Endpoint{AddrField: "t1", NodeIDField: 1, LocationField: "t"},
			&mock.Endpoint{AddrField: "t2", NodeIDField: 2, LocationField: "t"},
			&mock.Endpoint{AddrField: "t3", NodeIDField: 3, LocationField: "t"},
			&mock.Endpoint{AddrField: "f1", NodeIDField: 4, LocationField: "f"},
		}
		// preferred=3, max=2 -> selectFrom(preferred) yields 2 (>= max), no fallback fill.
		got := selectEndpoints(
			nil, many, 2, locationFilter(), balancerConfig.Info{SelfLocation: "t"}, true, testRand(),
		)
		require.Len(t, got, 2)
		for _, e := range got {
			require.Equal(t, "t", e.Location())
		}
	})
}

func TestSelectionCtx_selectFrom(t *testing.T) {
	t.Run("LimitZeroReturnsNil", func(t *testing.T) {
		c := &selectionCtx{rnd: testRand()}
		require.Nil(t, c.selectFrom([]endpoint.Endpoint{ep("1", 1)}, 0))
	})

	t.Run("EmptyCandidatesReturnsNil", func(t *testing.T) {
		c := &selectionCtx{rnd: testRand()}
		require.Nil(t, c.selectFrom(nil, 3))
	})

	t.Run("CandidatesWithinLimitReturnedAsIs", func(t *testing.T) {
		candidates := []endpoint.Endpoint{ep("1", 1), ep("2", 2)}
		c := &selectionCtx{rnd: testRand()}
		require.Equal(t, candidates, c.selectFrom(candidates, 3))
	})

	t.Run("KeepsExistingAndFillsRemaining", func(t *testing.T) {
		candidates := []endpoint.Endpoint{ep("1", 1), ep("2", 2), ep("3", 3), ep("4", 4)}
		// previous holds an active conn for "2" that is still in candidates -> must be kept.
		previous := []conn.Conn{cn("2", 2, state.Online)}
		c := &selectionCtx{previous: previous, rnd: testRand()}

		got := c.selectFrom(candidates, 2)
		require.Len(t, got, 2)
		require.Contains(t, addrsOf(got), "2", "existing active endpoint must be kept")
	})
}

func TestSelectionCtx_keepExisting(t *testing.T) {
	candidates := []endpoint.Endpoint{ep("1", 1), ep("2", 2), ep("3", 3)}
	keys := toKeySet(candidates)

	t.Run("SkipsNilBannedAndAbsent", func(t *testing.T) {
		previous := []conn.Conn{
			nil,                      // nil -> skip
			cn("2", 2, state.Banned), // banned -> skip
			cn("9", 9, state.Online), // not in candidates -> skip
			cn("1", 1, state.Online), // kept
		}
		c := &selectionCtx{previous: previous, rnd: testRand()}

		kept := c.keepExisting(keys, 5)
		require.Equal(t, []string{"1"}, addrsOf(kept))
	})

	t.Run("BreaksWhenLimitReached", func(t *testing.T) {
		previous := []conn.Conn{
			cn("1", 1, state.Online),
			cn("2", 2, state.Online),
			cn("3", 3, state.Online),
		}
		c := &selectionCtx{previous: previous, rnd: testRand()}

		kept := c.keepExisting(keys, 2)
		require.Len(t, kept, 2, "keepExisting must stop once the limit is reached")
	})
}

func TestSelectionCtx_fillCandidates(t *testing.T) {
	candidates := []endpoint.Endpoint{ep("1", 1), ep("2", 2), ep("3", 3), ep("4", 4)}
	kept := []endpoint.Endpoint{ep("1", 1)}
	bannedKeys := map[endpoint.Key]struct{}{
		ep("3", 3).Key(): {},
	}

	c := &selectionCtx{}
	fill := c.fillCandidates(candidates, kept, bannedKeys)

	// "1" excluded (kept), "3" excluded (banned) -> remaining "2","4".
	require.Equal(t, []string{"2", "4"}, addrsOf(fill))
}

func TestPartition(t *testing.T) {
	discovered := []endpoint.Endpoint{
		&mock.Endpoint{AddrField: "t1", LocationField: "t"},
		&mock.Endpoint{AddrField: "f1", LocationField: "f"},
		&mock.Endpoint{AddrField: "t2", LocationField: "t"},
	}

	t.Run("NilFilterReturnsAllAsPreferred", func(t *testing.T) {
		preferred, other := partition(discovered, nil, balancerConfig.Info{})
		require.Equal(t, discovered, preferred)
		require.Nil(t, other)
	})

	t.Run("SplitsByFilter", func(t *testing.T) {
		preferred, other := partition(discovered, locationFilter(), balancerConfig.Info{SelfLocation: "t"})
		require.Equal(t, []string{"t1", "t2"}, addrsOf(preferred))
		require.Equal(t, []string{"f1"}, addrsOf(other))
	})
}

func TestToKeySet(t *testing.T) {
	endpoints := []endpoint.Endpoint{ep("1", 1), ep("2", 2)}
	got := toKeySet(endpoints)

	require.Len(t, got, 2)
	require.Contains(t, got, ep("1", 1).Key())
	require.Contains(t, got, ep("2", 2).Key())
}

func TestBannedKeysFrom(t *testing.T) {
	conns := []conn.Conn{
		nil,                      // nil -> ignored
		cn("1", 1, state.Online), // not banned -> ignored
		cn("2", 2, state.Banned), // banned -> included
	}
	got := bannedKeysFrom(conns)

	require.Len(t, got, 1)
	require.Contains(t, got, ep("2", 2).Key())
}
