package conn

import (
	"context"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
)

type ccGuard struct {
	dial func(ctx context.Context) (cc *grpc.ClientConn, err error)
	cc   *grpc.ClientConn
	mu   sync.RWMutex
}

func newCcGuard(dial func(ctx context.Context) (cc *grpc.ClientConn, err error)) *ccGuard {
	return &ccGuard{
		dial: dial,
	}
}

func (g *ccGuard) Get(ctx context.Context) (*grpc.ClientConn, error) {
	g.mu.Lock()
	defer g.mu.Unlock()

	if g.cc != nil {
		return g.cc, nil
	}

	cc, err := g.dial(ctx)
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}

	g.cc = cc

	return cc, nil
}

func (g *ccGuard) Ready() bool {
	g.mu.Lock()
	defer g.mu.Unlock()

	return g.ready()
}

func (g *ccGuard) ready() bool {
	return g.cc != nil && g.cc.GetState() == connectivity.Ready
}

func (g *ccGuard) State() State {
	g.mu.RLock()
	defer g.mu.RUnlock()

	if g.cc == nil {
		return connectivity.Idle
	}

	return g.cc.GetState()
}

func (g *ccGuard) Close(ctx context.Context) error {
	g.mu.Lock()
	defer g.mu.Unlock()

	if g.cc == nil {
		return nil
	}

	err := g.cc.Close()
	g.cc = nil

	if err != nil {
		return xerrors.WithStackTrace(err)
	}

	return nil
}
