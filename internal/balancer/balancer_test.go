package balancer

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	balancerConfig "github.com/ydb-platform/ydb-go-sdk/v3/internal/balancer/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/conn"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/endpoint"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/mock"
)

type fakePool struct {
	connections map[string]*mock.Conn
}

func (fp *fakePool) EndpointsToConnections(eps []endpoint.Endpoint) []conn.Conn {
	var conns []conn.Conn
	for _, ep := range eps {
		if c, ok := fp.connections[ep.Address()]; ok {
			conns = append(conns, c)
		}
	}

	return conns
}

func (fp *fakePool) Allow(_ context.Context, _ conn.Conn) {}

func (fp *fakePool) GetIfPresent(ep endpoint.Endpoint) conn.Conn {
	if c, ok := fp.connections[ep.Address()]; ok {
		return c
	}

	return nil
}

func TestBuildConnectionsState(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name         string
		newEndpoints []endpoint.Endpoint
		oldEndpoints []endpoint.Endpoint
		initialConns map[string]*mock.Conn
		conf         balancerConfig.Config
		selfLoc      balancerConfig.Info
		expectPinged []string
		expectClosed []string
	}{
		{
			name:         "single new and old endpoint",
			newEndpoints: []endpoint.Endpoint{&mock.Endpoint{AddrField: "127.0.0.1"}},
			oldEndpoints: []endpoint.Endpoint{&mock.Endpoint{AddrField: "127.0.0.2"}},
			initialConns: map[string]*mock.Conn{
				"127.0.0.1": {
					AddrField: "127.0.0.1",
					State:     conn.Online,
				},
				"127.0.0.2": {
					AddrField: "127.0.0.2",
					State:     conn.Offline,
				},
			},
			conf: balancerConfig.Config{
				AllowFallback:   true,
				DetectNearestDC: true,
			},
			selfLoc:      balancerConfig.Info{SelfLocation: "local"},
			expectPinged: []string{"127.0.0.1"},
			expectClosed: []string{"127.0.0.2"},
		},
		{
			newEndpoints: []endpoint.Endpoint{&mock.Endpoint{AddrField: "a1"}, &mock.Endpoint{AddrField: "a2"}},
			oldEndpoints: []endpoint.Endpoint{&mock.Endpoint{AddrField: "a3"}},
			initialConns: map[string]*mock.Conn{
				"a1": {
					AddrField:     "a1",
					LocationField: "local",
					State:         conn.Offline,
				},
				"a2": {
					AddrField: "a2",
					State:     conn.Offline,
				},
				"a3": {
					AddrField: "a3",
					State:     conn.Online,
				},
			},
			conf: balancerConfig.Config{
				AllowFallback:   true,
				DetectNearestDC: true,
			},
			selfLoc:      balancerConfig.Info{SelfLocation: "local"},
			expectPinged: []string{"a1", "a2"},
			expectClosed: []string{"a3"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fp := &fakePool{connections: make(map[string]*mock.Conn)}
			for addr, c := range tt.initialConns {
				fp.connections[addr] = c
			}

			state := buildConnectionsState(ctx, fp, tt.newEndpoints, tt.oldEndpoints, tt.conf, tt.selfLoc)
			assert.NotNil(t, state)
			for _, addr := range tt.expectPinged {
				c := fp.connections[addr]
				assert.True(t, c.Pinged.Load(), "connection %s should be pinged", addr)
				assert.True(t, c.State == conn.Online || c.PingErr != nil)
			}
			for _, addr := range tt.expectClosed {
				c := fp.connections[addr]
				assert.True(t, c.Closed.Load(), "connection %s should be closed", addr)
				assert.True(t, c.State == conn.Offline, "connection %s should be offline", addr)
			}
		})
	}
}
