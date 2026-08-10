package strategy

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/endpoint"
)

func TestMaxConnectionsKeepsStickyActiveSet(t *testing.T) {
	endpoints := maxConnectionEndpoints(1, 2, 3, 4)
	estimator := WithMaxConnections(RandomChoice(), 2)
	plan := Compile(estimator)
	estimates := estimator.Estimate(Info{}, endpoints)
	selected := plan.Active(Info{Rand: testRand{}}, estimates)
	require.Len(t, selected, 2)
	require.Equal(t, endpoints[len(endpoints)-1].Key(), selected[0].Key)

	selectedAgain := plan.Active(Info{
		PreviousActive: previousEstimationKeys(selected...),
		Rand:           testRand{},
	}, estimator.Estimate(Info{}, maxConnectionEndpoints(1, 2, 3, 4)))
	require.Equal(t, estimationKeys(selected), estimationKeys(selectedAgain))

	selectedAfterBan := plan.Active(Info{
		PreviousActive: []PreviousEndpoint{
			{Key: selected[0].Key, Banned: true},
			{Key: selected[1].Key},
		},
		Rand: testRand{},
	}, estimates)
	require.Len(t, selectedAfterBan, 2)
	require.NotContains(t, estimationKeys(selectedAfterBan), selected[0].Key)
}

func TestMaxConnectionsUsesChildPenalties(t *testing.T) {
	endpoints := []endpoint.Endpoint{
		endpoint.New("local-1", endpoint.WithID(1), endpoint.WithLocation("local")),
		endpoint.New("remote-1", endpoint.WithID(2), endpoint.WithLocation("remote")),
		endpoint.New("local-2", endpoint.WithID(3), endpoint.WithLocation("local")),
		endpoint.New("remote-2", endpoint.WithID(4), endpoint.WithLocation("remote")),
	}
	child := Prefer(RandomChoice(), "Location(local)", locationMatch("local"), true)
	estimator := WithMaxConnections(child, 3)
	plan := Compile(estimator)
	estimates := estimator.Estimate(Info{}, endpoints)
	selected := plan.Active(Info{}, estimates)

	require.Equal(t, "MaxConnections{Limit=3,Child="+child.String()+"}", estimator.String())
	require.Equal(t, 3, plan.MaxConnections())
	require.Len(t, selected, 3)
	require.Equal(t, []endpoint.Key{endpoints[0].Key(), endpoints[2].Key(), endpoints[1].Key()},
		[]endpoint.Key{selected[0].Key, selected[1].Key, selected[2].Key},
	)
}

func TestMaxConnectionsFillsWithLowestPenaltyBannedEndpoint(t *testing.T) {
	endpoints := maxConnectionEndpoints(1, 2, 3)
	estimates := []Estimation{
		{Key: endpoints[0].Key(), Penalty: 5, Weight: 1},
		{Key: endpoints[1].Key(), Penalty: 1, Weight: 1},
		{Key: endpoints[2].Key(), Weight: 1},
	}

	selected := selectActiveEstimates(Info{
		PreviousActive: []PreviousEndpoint{
			{Key: endpoints[0].Key(), Banned: true},
			{Key: endpoints[1].Key(), Banned: true},
		},
	}, estimates, 2)

	require.Equal(t, []Estimation{estimates[2], estimates[1]}, selected)
}

func TestMaxConnectionsNonPositiveAndNestedLimits(t *testing.T) {
	endpoints := maxConnectionEndpoints(1, 2, 3)
	estimates := RandomChoice().Estimate(Info{}, endpoints)

	require.Equal(t, estimates, Compile(WithMaxConnections(RandomChoice(), 0)).Active(Info{}, estimates))
	require.Equal(t, estimates, Compile(WithMaxConnections(RandomChoice(), -1)).Active(Info{}, estimates))
	require.Equal(t, 2, Compile(WithMaxConnections(WithMaxConnections(RandomChoice(), 3), 2)).MaxConnections())
	require.Equal(t, 2, Compile(WithMaxConnections(WithMaxConnections(RandomChoice(), 2), 0)).MaxConnections())
}

func TestSelectActiveEstimatesEdgeCases(t *testing.T) {
	endpoints := maxConnectionEndpoints(1, 2, 3, 4)
	estimates := RandomChoice().Estimate(Info{}, endpoints)
	estimates[3].Weight = 0

	require.Nil(t, selectActiveEstimates(Info{}, nil, 1))
	require.Equal(t, estimates, selectActiveEstimates(Info{}, estimates, 0))
	require.Equal(t, estimates, selectActiveEstimates(Info{}, estimates, len(estimates)))

	selected := selectActiveEstimates(Info{
		PreviousActive: []PreviousEndpoint{
			{Key: endpoint.New("outside", endpoint.WithID(9)).Key()},
			{Key: endpoints[0].Key()},
			{Key: endpoints[0].Key()},
			{Key: endpoints[2].Key(), Banned: true},
		},
	}, estimates, 3)
	require.Equal(t, []endpoint.Key{endpoints[0].Key(), endpoints[1].Key(), endpoints[2].Key()},
		[]endpoint.Key{selected[0].Key, selected[1].Key, selected[2].Key},
	)

	shuffleEqualPenaltyRuns(Info{}, estimates, nil)
}

func TestMaxConnectionsPreservesChildLifecycle(t *testing.T) {
	estimator := WithMaxConnections(
		PreferNearestDC(SingleConn(), "Location(local)", locationMatch("local"), true), 2,
	)
	plan := Compile(estimator)
	runtime := &recordingRuntime{}

	_, err := plan.Start(t.Context(), runtime)
	require.NoError(t, err)
	require.Equal(t, "configured", runtime.source)

	resolved, err := plan.ResolveLocation(
		t.Context(), nil, "discovered",
		func(_ context.Context, _ []endpoint.Endpoint) (string, error) {
			return "detected", nil
		},
	)
	require.NoError(t, err)
	require.Equal(t, "detected", resolved.SelfLocation)
	require.True(t, resolved.NeedLocalDC)
}

func maxConnectionEndpoints(nodeIDs ...uint32) []endpoint.Endpoint {
	result := make([]endpoint.Endpoint, len(nodeIDs))
	for i, nodeID := range nodeIDs {
		result[i] = endpoint.New(string(rune('a'+i)), endpoint.WithID(nodeID))
	}

	return result
}

func previousEstimationKeys(estimates ...Estimation) []PreviousEndpoint {
	result := make([]PreviousEndpoint, len(estimates))
	for i, estimation := range estimates {
		result[i] = PreviousEndpoint{Key: estimation.Key}
	}

	return result
}

func estimationKeys(estimates []Estimation) map[endpoint.Key]struct{} {
	result := make(map[endpoint.Key]struct{}, len(estimates))
	for _, estimation := range estimates {
		result[estimation.Key] = struct{}{}
	}

	return result
}
