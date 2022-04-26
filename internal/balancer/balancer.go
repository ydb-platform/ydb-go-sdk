package balancer

import (
	"context"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/conn"
)

// Balancer is an interface that implements particular load-balancing
// algorithm.
//
// Balancer methods called synchronized. That is, implementations must not
// provide additional goroutine safety.
type Balancer interface {
	// Next returns next connection for request.
	// return Err
	Next(ctx context.Context, opts ...NextOption) conn.Conn

	// Create makes empty balancer with same implementation
	Create(conns []conn.Conn) Balancer
}

func IsOkConnection(c conn.Conn, bannedIsOk bool) bool {
	switch c.GetState() {
	case conn.Online, conn.Created, conn.Offline:
		return true
	case conn.Banned:
		return bannedIsOk
	default:
		return false
	}
}

type (
	NextOption            func(o *NextOptions)
	NeedDiscoveryCallback func(ctx context.Context)

	NextOptions struct {
		OnNeedRediscovery NeedDiscoveryCallback
		WantPessimized    bool
	}
)

func NewNextOptions(opts ...NextOption) NextOptions {
	var o NextOptions
	for _, f := range opts {
		f(&o)
	}
	return o
}

func (o *NextOptions) Discovery(ctx context.Context) {
	if o.OnNeedRediscovery != nil {
		o.OnNeedRediscovery(ctx)
	}
}

func WithWantPessimized(val ...bool) NextOption {
	return func(o *NextOptions) {
		if len(val) == 0 {
			o.WantPessimized = true
		} else {
			o.WantPessimized = val[0]
		}
	}
}

func WithOnNeedRediscovery(callback NeedDiscoveryCallback) NextOption {
	return func(o *NextOptions) {
		o.OnNeedRediscovery = callback
	}
}
