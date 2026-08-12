package policy

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/endpoint"
)

func TestPolicy(t *testing.T) {
	endpoints := strategyEndpoints("local", "remote")

	t.Run("default", func(t *testing.T) {
		policy := Policy{}

		require.Equal(t, "Priority", policy.String())
		require.False(t, policy.SingleConnection())
		require.False(t, policy.DetectsNearestDC())
		require.Equal(t, []EndpointPriority{
			{Key: endpoints[0].Key()},
			{Key: endpoints[1].Key()},
		}, policy.Prioritize(Info{}, endpoints))
	})

	t.Run("single connection", func(t *testing.T) {
		policy := SingleConn()

		require.Equal(t, "SingleConn", policy.String())
		require.True(t, policy.SingleConnection())
		require.False(t, policy.DetectsNearestDC())
	})

	t.Run("nearest DC", func(t *testing.T) {
		policy := PreferNearestDC(Policy{}, "LocalDC", locationMatch("local"))

		require.Equal(t, "Priority{Preferences=[LocalDC]}", policy.String())
		require.False(t, policy.SingleConnection())
		require.True(t, policy.DetectsNearestDC())
	})

	t.Run("preference preserves mode", func(t *testing.T) {
		policy := PreferNearestDC(SingleConn(), "LocalDC", locationMatch("local"))

		require.Equal(t, "SingleConn{Preferences=[LocalDC]}", policy.String())
		require.True(t, policy.SingleConnection())
		require.True(t, policy.DetectsNearestDC())
	})
}

func TestPolicyIsImmutable(t *testing.T) {
	base := Prefer(Policy{}, "LocalDC", locationMatch("local"))
	composed := Prefer(base, "RemoteDC", locationMatch("remote"))

	require.Equal(t, "Priority{Preferences=[LocalDC]}", base.String())
	require.Equal(t, "Priority{Preferences=[RemoteDC,LocalDC]}", composed.String())
}

func TestApplyPreference(t *testing.T) {
	endpoints := strategyEndpoints("local", "remote", "remote")
	priorities := Policy{}.Prioritize(Info{}, endpoints)

	applyPreference(Info{}, endpoints, priorities, preference{
		name:  "LocalDC",
		match: locationMatch("local"),
	})

	require.Equal(t, []EndpointPriority{
		{Key: endpoints[0].Key()},
		{Key: endpoints[1].Key(), Priority: 1},
		{Key: endpoints[2].Key(), Priority: 1},
	}, priorities)
}

func TestApplyPreferenceKeepsSingleBucketWhenAllEndpointsMatchEqually(t *testing.T) {
	endpoints := strategyEndpoints("local", "local")
	priorities := Policy{}.Prioritize(Info{}, endpoints)

	applyPreference(Info{}, endpoints, priorities, preference{
		name:  "LocalDC",
		match: locationMatch("local"),
	})

	require.Equal(t, []EndpointPriority{
		{Key: endpoints[0].Key()},
		{Key: endpoints[1].Key()},
	}, priorities)
}

func TestPolicyPrioritizeComposesPreferencesOutermostFirst(t *testing.T) {
	endpoints := []endpoint.Endpoint{
		strategyEndpoint("local-primary", "local", endpoint.PileStatePrimary),
		strategyEndpoint("local-secondary", "local", endpoint.PileStateSynchronized),
		strategyEndpoint("remote-primary", "remote", endpoint.PileStatePrimary),
		strategyEndpoint("remote-secondary", "remote", endpoint.PileStateSynchronized),
	}
	primary := func(_ Info, candidate endpoint.Info) bool {
		return candidate.Metadata().BridgePileState == endpoint.PileStatePrimary
	}
	policy := Prefer(
		Prefer(Policy{}, "PrimaryPile", primary),
		"LocalDC", locationMatch("local"),
	)

	require.Equal(t, "Priority{Preferences=[LocalDC,PrimaryPile]}", policy.String())
	require.Equal(t, []EndpointPriority{
		{Key: endpoints[0].Key()},
		{Key: endpoints[1].Key(), Priority: 1},
		{Key: endpoints[2].Key(), Priority: 2},
		{Key: endpoints[3].Key(), Priority: 3},
	}, policy.Prioritize(Info{}, endpoints))
}

func TestPolicyPrioritizeEmptyEndpoints(t *testing.T) {
	policy := Prefer(Policy{}, "LocalDC", locationMatch("local"))

	require.Empty(t, policy.Prioritize(Info{}, nil))
}

func strategyEndpoints(locations ...string) []endpoint.Endpoint {
	result := make([]endpoint.Endpoint, len(locations))
	for i, location := range locations {
		result[i] = endpoint.New(location, endpoint.WithID(uint32(i+1)), endpoint.WithLocation(location))
	}

	return result
}

func locationMatch(location string) func(Info, endpoint.Info) bool {
	return func(_ Info, candidate endpoint.Info) bool {
		return candidate.Location() == location
	}
}

func strategyEndpoint(address, location string, pileState endpoint.PileState) endpoint.Endpoint {
	return endpoint.New(address,
		endpoint.WithLocation(location),
		endpoint.WithMetadata(endpoint.Metadata{BridgePileState: pileState}),
	)
}
