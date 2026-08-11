package strategy

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/endpoint"
)

func TestPreferEstimator(t *testing.T) {
	endpoints := strategyEndpoints("local", "remote")
	withoutFallback := PreferNearestDC(RandomChoice(), "Location(local)", locationMatch("local"), false)

	require.Equal(t, "Prefer{Filter=Location(local),AllowFallback=false,Child=RandomChoice}",
		withoutFallback.String(),
	)
	require.Equal(t, []Estimation{{Key: endpoints[0].Key(), Weight: 1}},
		withoutFallback.Estimate(Info{}, endpoints),
	)

	withFallback := Prefer(SingleConn(), "Location(local)", locationMatch("local"), true)
	require.Equal(t, []Estimation{
		{Key: endpoints[0].Key(), Weight: 1},
		{Key: endpoints[1].Key(), Penalty: 1, Weight: 1},
	}, withFallback.Estimate(Info{}, endpoints))

	noPreferred := Prefer(RandomChoice(), "missing", locationMatch("missing"), true)
	require.Equal(t, []Estimation{
		{Key: endpoints[0].Key(), Weight: 1},
		{Key: endpoints[1].Key(), Weight: 1},
	}, noPreferred.Estimate(Info{}, endpoints))

	normalized := Prefer(nil, "Location(local)", locationMatch("local"), false)
	require.Equal(t, "Prefer{Filter=Location(local),AllowFallback=false,Child=RandomChoice}", normalized.String())
}

func TestPreferPenaltyComposition(t *testing.T) {
	endpoints := strategyEndpoints("local", "remote", "other")
	child := fixedEstimator{estimates: []Estimation{
		{Key: endpoints[0].Key(), Penalty: 2, Weight: 3},
		{Key: endpoints[1].Key(), Penalty: 5, Weight: 4},
	}}
	estimator := Prefer(child, "local", locationMatch("local"), true)

	require.Equal(t, []Estimation{
		{Key: endpoints[0].Key(), Weight: 3},
		{Key: endpoints[1].Key(), Penalty: 2, Weight: 4},
	}, estimator.Estimate(Info{}, endpoints))

	overflowSafe := Prefer(fixedEstimator{estimates: []Estimation{
		{Key: endpoints[0].Key(), Penalty: math.MaxUint64, Weight: 1},
		{Key: endpoints[1].Key(), Weight: 1},
	}}, "local", locationMatch("local"), true)
	require.Equal(t, []Estimation{
		{Key: endpoints[0].Key(), Penalty: 1, Weight: 1},
		{Key: endpoints[1].Key(), Penalty: 2, Weight: 1},
	}, overflowSafe.Estimate(Info{}, endpoints))
}

func TestPreferCallsChildOnceWithFullSnapshot(t *testing.T) {
	endpoints := strategyEndpoints("local", "remote")
	child := &recordingEstimator{}
	estimator := Prefer(child, "local", locationMatch("local"), true)

	estimator.Estimate(Info{}, endpoints)

	require.Equal(t, 1, child.calls)
	require.Equal(t, endpoints, child.endpoints)
}

func TestPartitionEstimates(t *testing.T) {
	endpoints := strategyEndpoints("local", "remote")
	estimates := RandomChoice().Estimate(Info{}, endpoints)

	preferred, fallback := partitionEstimates(endpoints, estimates, nil, Info{})
	require.Equal(t, estimates, preferred)
	require.Nil(t, fallback)

	preferred, fallback = partitionEstimates(endpoints, estimates, locationMatch("local"), Info{})
	require.Equal(t, estimates[:1], preferred)
	require.Equal(t, estimates[1:], fallback)
	require.Nil(t, compactPenalties(nil))
}

func TestEstimatorCanUseEndpointMetadata(t *testing.T) {
	endpoints := []endpoint.Endpoint{
		endpoint.New("primary", endpoint.WithMetadata(endpoint.Metadata{
			BridgePileState: endpoint.PileStatePrimary,
		})),
		endpoint.New("secondary", endpoint.WithMetadata(endpoint.Metadata{
			BridgePileState: endpoint.PileStateSynchronized,
		})),
	}
	estimator := Prefer(RandomChoice(), "PrimaryPile", func(_ Info, candidate endpoint.Info) bool {
		return candidate.Metadata().BridgePileState == endpoint.PileStatePrimary
	}, false)

	require.Equal(t, []Estimation{{Key: endpoints[0].Key(), Weight: 1}}, estimator.Estimate(Info{}, endpoints))
}

type recordingEstimator struct {
	calls     int
	endpoints []endpoint.Endpoint
}

func (r *recordingEstimator) Estimate(_ Info, endpoints []endpoint.Endpoint) []Estimation {
	r.calls++
	r.endpoints = endpoints

	return randomChoice{}.Estimate(Info{}, endpoints)
}

func (*recordingEstimator) String() string {
	return "Recording"
}
