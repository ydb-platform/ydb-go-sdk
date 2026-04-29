package mock

import (
	"context"
	"time"

	"google.golang.org/grpc"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/conn/state"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/endpoint"
)

//go:generate mockgen -destination grpc_client_conn_interface_mock.go --typed -package mock -write_package_comment=false google.golang.org/grpc ClientConnInterface
//go:generate mockgen -destination grpc_client_stream_mock.go --typed -package mock -write_package_comment=false google.golang.org/grpc ClientStream

type Conn struct {
	grpc.ClientConnInterface

	AddrField     string
	LocationField string
	NodeIDField   uint32
	State         state.State
	LocalDCField  bool
}

func (c *Conn) Endpoint() endpoint.Endpoint {
	return &Endpoint{
		AddrField:     c.AddrField,
		LocalDCField:  c.LocalDCField,
		LocationField: c.LocationField,
		NodeIDField:   c.NodeIDField,
	}
}

func (c *Conn) LastUsage() time.Time {
	panic("not implemented in mock")
}

func (c *Conn) GetState() state.State {
	return c.State
}

func (c *Conn) SetState(ctx context.Context, state state.State) state.State {
	c.State = state

	return c.State
}

func (c *Conn) Unban(ctx context.Context) state.State {
	c.SetState(ctx, state.Online)

	return state.Online
}

type Endpoint struct {
	AddrField         string
	LocationField     string
	NodeIDField       uint32
	LocalDCField      bool
	OverrideHostField string
}

func (e *Endpoint) Key() endpoint.Key {
	return endpoint.Key{
		Address:      e.AddrField,
		NodeID:       e.NodeIDField,
		HostOverride: e.OverrideHostField,
	}
}

func (e *Endpoint) Choose(bool) {
}

func (e *Endpoint) NodeID() uint32 {
	return e.NodeIDField
}

func (e *Endpoint) Address() string {
	return e.AddrField
}

// Deprecated: LocalDC check "local" by compare endpoint location with discovery "selflocation" field.
// It work good only if connection url always point to local dc.
// Will be removed after Oct 2024.
// Read about versioning policy: https://github.com/ydb-platform/ydb-go-sdk/blob/master/VERSIONING.md#deprecated
func (e *Endpoint) LocalDC() bool {
	return e.LocalDCField
}

func (e *Endpoint) Location() string {
	return e.LocationField
}

func (e *Endpoint) LastUpdated() time.Time {
	panic("not implemented in mock")
}

func (e *Endpoint) LoadFactor() float32 {
	panic("not implemented in mock")
}

func (e *Endpoint) OverrideHost() string {
	return e.OverrideHostField
}

func (e *Endpoint) String() string {
	panic("not implemented in mock")
}

func (e *Endpoint) Copy() endpoint.Endpoint {
	c := *e

	return &c
}

func (e *Endpoint) Touch(opts ...endpoint.Option) {
}
