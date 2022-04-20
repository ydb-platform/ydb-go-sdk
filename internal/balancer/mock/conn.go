package mock

import (
	"context"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/conn"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/endpoint"
	"google.golang.org/grpc"
)

type ConnMock struct {
	Address string
}

func NewConn(addr string) *ConnMock {
	return &ConnMock{Address: addr}
}

func (c *ConnMock) Invoke(ctx context.Context, method string, args interface{}, reply interface{}, opts ...grpc.CallOption) error {
	panic("not implemented in mock")
}

func (c *ConnMock) NewStream(ctx context.Context, desc *grpc.StreamDesc, method string, opts ...grpc.CallOption) (grpc.ClientStream, error) {
	panic("not implemented in mock")
}

func (c *ConnMock) Endpoint() endpoint.Endpoint {
	return &EndpointMock{Addr: c.Address}
}

func (c *ConnMock) LastUsage() time.Time {
	panic("not implemented in mock")
}

func (c *ConnMock) Park(ctx context.Context) (err error) {
	panic("not implemented in mock")
}

func (c *ConnMock) Ping(ctx context.Context) error {
	panic("not implemented in mock")
}

func (c *ConnMock) IsState(states ...conn.State) bool {
	panic("not implemented in mock")
}

func (c *ConnMock) GetState() conn.State {
	panic("not implemented in mock")
}

func (c *ConnMock) SetState(state conn.State) conn.State {
	panic("not implemented in mock")
}

func (c *ConnMock) Release(ctx context.Context) error {
	panic("not implemented in mock")
}

type EndpointMock struct {
	Addr string
}

func (e *EndpointMock) NodeID() uint32 {
	panic("not implemented in mock")
}

func (e *EndpointMock) Address() string {
	return e.Addr
}

func (e *EndpointMock) LocalDC() bool {
	panic("not implemented in mock")
}

func (e *EndpointMock) Location() string {
	panic("not implemented in mock")
}

func (e *EndpointMock) LastUpdated() time.Time {
	panic("not implemented in mock")
}

func (e *EndpointMock) LoadFactor() float32 {
	panic("not implemented in mock")
}

func (e *EndpointMock) String() string {
	panic("not implemented in mock")
}

func (e *EndpointMock) Copy() endpoint.Endpoint {
	panic("not implemented in mock")
}

func (e *EndpointMock) Touch(opts ...endpoint.Option) {
	panic("not implemented in mock")
}
