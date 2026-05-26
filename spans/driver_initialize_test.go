package spans

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/repeater"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/stack"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

type fakeEndpoint struct{}

func (fakeEndpoint) String() string         { return "node-1:2136" }
func (fakeEndpoint) NodeID() uint32         { return 42 }
func (fakeEndpoint) Address() string        { return "node-1:2136" }
func (fakeEndpoint) Location() string       { return "local" }
func (fakeEndpoint) LoadFactor() float32    { return 0 }
func (fakeEndpoint) LastUpdated() time.Time { return time.Time{} }
func (fakeEndpoint) LocalDC() bool          { return true }

func TestDriverInitializeSpan(t *testing.T) {
	const (
		outerCallID = "github.com/ydb-platform/ydb-go-sdk/v3/internal/balancer.(*Balancer).clusterDiscoveryAttemptWithDial"
		innerCallID = "github.com/ydb-platform/ydb-go-sdk/v3/internal/balancer.(*Balancer).clusterDiscoveryAttempt"
	)

	cases := []struct {
		name      string
		event     repeater.Event
		callID    string
		wantSpans int
	}{
		{
			name:      "init outer: emits ydb.Driver.Initialize",
			event:     repeater.EventInit,
			callID:    outerCallID,
			wantSpans: 1,
		},
		{
			name:      "init inner: skipped (only outer is named)",
			event:     repeater.EventInit,
			callID:    innerCallID,
			wantSpans: 0,
		},
		{
			name:      "periodic tick: skipped (not initial)",
			event:     repeater.EventTick,
			callID:    outerCallID,
			wantSpans: 0,
		},
		{
			name:      "force refresh: skipped (not initial)",
			event:     repeater.EventForce,
			callID:    outerCallID,
			wantSpans: 0,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			adapter := &recordingAdapter{}
			d := driver(adapter)

			ctx := repeater.WithEvent(context.Background(), tc.event)
			done := d.OnBalancerClusterDiscoveryAttempt(
				trace.DriverBalancerClusterDiscoveryAttemptStartInfo{
					Context:  &ctx,
					Call:     stack.FunctionID(tc.callID),
					Address:  "ydb:2136",
					Database: "/local",
				},
			)
			if done != nil {
				done(trace.DriverBalancerClusterDiscoveryAttemptDoneInfo{})
			}

			require.Len(t, adapter.byName(SpanNameDriverInitialize), tc.wantSpans)
		})
	}
}

func TestAnnotateNetworkPeerSetsEndpointAttributes(t *testing.T) {
	span := &recordedSpan{}

	annotateNetworkPeer(span, fakeEndpoint{})

	require.Equal(t, "node-1", span.attr(AttrNetworkPeerAddress))
	require.Equal(t, 2136, span.attr(AttrNetworkPeerPort))
	require.Equal(t, "local", span.attr(AttrYDBNodeDC))
	require.Equal(t, int64(42), span.attr(AttrYDBNodeID))
}
