package strategy

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/endpoint"
)

func TestCompilePlanBehavior(t *testing.T) {
	tests := []struct {
		name              string
		estimator         Estimator
		expectedSource    string
		expectedLocation  string
		expectedDetection bool
	}{
		{
			name:             "random choice uses cluster discovery location",
			estimator:        RandomChoice(),
			expectedSource:   "cluster",
			expectedLocation: "discovered",
		},
		{
			name:             "single connection uses configured endpoint",
			estimator:        SingleConn(),
			expectedSource:   "configured",
			expectedLocation: "discovered",
		},
		{
			name: "nearest DC decorates cluster discovery",
			estimator: PreferNearestDC(
				RandomChoice(), "Location(local)", locationMatch("local"), false,
			),
			expectedSource:    "cluster",
			expectedLocation:  "detected",
			expectedDetection: true,
		},
		{
			name: "nearest DC preserves nested configured source",
			estimator: PreferNearestDC(
				SingleConn(), "Location(local)", locationMatch("local"), true,
			),
			expectedSource:    "configured",
			expectedLocation:  "detected",
			expectedDetection: true,
		},
		{
			name: "outer location preference preserves nearest DC resolver",
			estimator: Prefer(
				PreferNearestDC(RandomChoice(), "Location(local)", locationMatch("local"), true),
				"Location(remote)", locationMatch("remote"), false,
			),
			expectedSource:    "cluster",
			expectedLocation:  "detected",
			expectedDetection: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			plan := Compile(test.estimator)
			runtime := &recordingRuntime{}

			controller, err := plan.Start(t.Context(), runtime)
			require.NoError(t, err)
			require.NotNil(t, controller)
			require.Equal(t, test.expectedSource, runtime.source)

			detectorCalls := 0
			resolved, err := plan.ResolveLocation(
				t.Context(),
				[]endpoint.Endpoint{endpoint.New("node:2135")},
				"discovered",
				func(context.Context, []endpoint.Endpoint) (string, error) {
					detectorCalls++

					return "detected", nil
				},
			)
			require.NoError(t, err)
			require.Equal(t, test.expectedLocation, resolved.SelfLocation)
			require.Equal(t, test.expectedDetection, resolved.NeedLocalDC)
			if test.expectedDetection {
				require.Equal(t, 1, detectorCalls)
			} else {
				require.Zero(t, detectorCalls)
			}
		})
	}
}

func TestCompileDefaultsUnknownBalancerToDynamicDiscovery(t *testing.T) {
	estimator := externalStyleEstimator{Estimator: RandomChoice()}
	plan := Compile(estimator)
	runtime := &recordingRuntime{}

	_, err := plan.Start(t.Context(), runtime)
	require.NoError(t, err)
	require.Equal(t, "cluster", runtime.source)
	require.Equal(t, estimator, plan.Estimator())
	require.Equal(t,
		[]Estimation{{Key: strategyEndpoints("local")[0].Key(), Weight: 1}},
		plan.Estimator().Estimate(Info{}, strategyEndpoints("local")),
	)
}

func TestZeroPlanUsesDynamicDiscoveryDefaults(t *testing.T) {
	var plan Plan
	runtime := &recordingRuntime{}

	_, err := plan.Start(t.Context(), runtime)
	require.NoError(t, err)
	require.Equal(t, "cluster", runtime.source)
	require.Equal(t, "RandomChoice", plan.Estimator().String())

	resolved, err := plan.ResolveLocation(t.Context(), nil, "discovered", nil)
	require.NoError(t, err)
	require.Equal(t, ResolvedLocation{SelfLocation: "discovered"}, resolved)
}

func TestNearestDCResolverReturnsDetectorError(t *testing.T) {
	expectedErr := errors.New("detector failed")
	plan := Compile(PreferNearestDC(RandomChoice(), "Location(local)", locationMatch("local"), false))

	resolved, err := plan.ResolveLocation(
		t.Context(), nil, "discovered",
		func(context.Context, []endpoint.Endpoint) (string, error) {
			return "", expectedErr
		},
	)

	require.ErrorIs(t, err, expectedErr)
	require.Equal(t, ResolvedLocation{}, resolved)
}

type recordingRuntime struct {
	source string
}

func (r *recordingRuntime) StartClusterDiscovery(context.Context) (Controller, error) {
	r.source = "cluster"

	return recordingController{}, nil
}

func (r *recordingRuntime) UseConfiguredEndpoint(context.Context) (Controller, error) {
	r.source = "configured"

	return recordingController{}, nil
}

type recordingController struct{}

func (recordingController) Force() {}
func (recordingController) Stop()  {}

type externalStyleEstimator struct {
	Estimator
}
