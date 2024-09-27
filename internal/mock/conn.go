package mock

import (
	"context"
	"time"

	"google.golang.org/grpc"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/conn"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/endpoint"
)

type Conn struct {
	PingErr       error
	AddrField     string
	LocationField string
	NodeIDField   uint32
	State         conn.State
	LocalDCField  bool
}

func (c *Conn) Invoke(
	ctx context.Context,
	method string,
	args interface{},
	reply interface{},
	opts ...grpc.CallOption,
) error {
	panic("not implemented in mock")
}

func (c *Conn) NewStream(ctx context.Context,
	desc *grpc.StreamDesc, method string,
	opts ...grpc.CallOption,
) (grpc.ClientStream, error) {
	panic("not implemented in mock")
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

func (c *Conn) Park(ctx context.Context) (err error) {
	panic("not implemented in mock")
}

func (c *Conn) Ping(ctx context.Context) error {
	return c.PingErr
}

func (c *Conn) IsState(states ...conn.State) bool {
	panic("not implemented in mock")
}

func (c *Conn) GetState() conn.State {
	return c.State
}

func (c *Conn) SetState(ctx context.Context, state conn.State) conn.State {
	c.State = state

	return c.State
}

func (c *Conn) Unban(ctx context.Context) conn.State {
	c.SetState(ctx, conn.Online)

	return conn.Online
}

type Endpoint struct {
	AddrField     string
	LocationField string
	NodeIDField   uint32
	LocalDCField  bool
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
	panic("not implemented in mock")
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
