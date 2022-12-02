package conn

import (
	"context"

	"google.golang.org/grpc"
)

type contextModifierMiddleware struct {
	cc        grpc.ClientConnInterface
	modifyCtx func(ctx context.Context) context.Context
}

func (c *contextModifierMiddleware) Invoke(
	ctx context.Context, method string, args interface{}, reply interface{}, opts ...grpc.CallOption,
) error {
	return c.cc.Invoke(c.modifyCtx(ctx), method, args, reply, opts...)
}

func (c *contextModifierMiddleware) NewStream(
	ctx context.Context, desc *grpc.StreamDesc, method string, opts ...grpc.CallOption,
) (grpc.ClientStream, error) {
	return c.cc.NewStream(c.modifyCtx(ctx), desc, method, opts...)
}

func WithContextModifier(
	cc grpc.ClientConnInterface,
	modifyCtx func(ctx context.Context) context.Context,
) grpc.ClientConnInterface {
	return &contextModifierMiddleware{
		cc:        cc,
		modifyCtx: modifyCtx,
	}
}

type optionsAppenderMiddleware struct {
	cc   grpc.ClientConnInterface
	opts []grpc.CallOption
}

func (c *optionsAppenderMiddleware) Invoke(
	ctx context.Context, method string, args interface{}, reply interface{}, opts ...grpc.CallOption,
) error {
	return c.cc.Invoke(ctx, method, args, reply, append(opts, c.opts...)...)
}

func (c *optionsAppenderMiddleware) NewStream(
	ctx context.Context, desc *grpc.StreamDesc, method string, opts ...grpc.CallOption,
) (grpc.ClientStream, error) {
	return c.cc.NewStream(ctx, desc, method, append(opts, c.opts...)...)
}

func WithAppendOptions(cc grpc.ClientConnInterface, opts ...grpc.CallOption) grpc.ClientConnInterface {
	return &optionsAppenderMiddleware{
		cc:   cc,
		opts: opts,
	}
}
