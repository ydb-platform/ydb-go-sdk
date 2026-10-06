//go:build darwin || linux

package main

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestSummarizeIncludesErrorsAndScheduledLatency(t *testing.T) {
	r := summarize([]sample{
		{latency: time.Millisecond, rpc: time.Millisecond, inWindow: true},
		{latency: 2 * time.Millisecond, rpc: time.Millisecond, dispatch: time.Millisecond, inWindow: true, phase: 1},
		{latency: 3 * time.Millisecond, rpc: 2 * time.Millisecond, phase: 2},
		{latency: time.Second, err: errors.New("timeout")},
	})
	require.Equal(t, 4, r.Requests)
	require.Equal(t, 1, r.Errors)
	require.Equal(t, 2, r.CompletedInWindow)
	require.Equal(t, float64(2), r.P50MS)
	require.Equal(t, float64(3), r.P95MS)
	require.Equal(t, float64(3), r.P99MS)
	require.Equal(t, []float64{1, 2, 3}, r.PhaseP95MS)
}

func TestPercentileNearestRank(t *testing.T) {
	require.Zero(t, percentile(nil, 0.95))
	require.Equal(t, float64(3), percentile([]float64{5, 1, 3, 2, 4}, 0.5))
	require.Equal(t, float64(5), percentile([]float64{5, 1, 3, 2, 4}, 0.95))
}
