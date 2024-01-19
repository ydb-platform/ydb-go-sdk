package mocked

import (
	"context"
	"fmt"
	"net"
	"testing"

	"github.com/rekby/fixenv"
	"github.com/rekby/fixenv/sf"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

type scopeT struct {
	Ctx context.Context
	fixenv.Env
	Require *require.Assertions
	t       testing.TB
}

func newScope(t *testing.T) *scopeT {
	at := require.New(t)
	fEnv := fixenv.NewEnv(t)
	ctx, ctxCancel := context.WithCancel(context.Background())
	t.Cleanup(func() {
		ctxCancel()
	})
	res := &scopeT{
		Ctx:     ctx,
		Env:     fEnv,
		Require: at,
		t:       t,
	}
	return res
}

func (scope *scopeT) T() testing.TB {
	return scope.t
}

func (scope *scopeT) Logf(format string, args ...interface{}) {
	scope.t.Helper()
	scope.t.Logf(format, args...)
}

func (scope *scopeT) Failed() bool {
	return scope.t.Failed()
}

type serviceImplementationMap map[*grpc.ServiceDesc]any // service description -> service implementation

func (scope *scopeT) StartYDBMock(services serviceImplementationMap) string {
	grpcServer := grpc.NewServer()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		scope.T().Fatal("Failed to listen port for ydb mock: %+v", err)
	}
	scope.T().Cleanup(func() {
		grpcServer.Stop()
		_ = listener.Close()
	})

	for service, impl := range services {
		grpcServer.RegisterService(service, impl)
	}

	go func() {
		err := grpcServer.Serve(listener)
		if err != nil {
			scope.T().Fatal("failed to start grpc server: %+v", err)
		}
	}()

	connString := fmt.Sprintf("grpc://%s/local", listener.Addr())
	return connString
}

func (scope *scopeT) MockYdbListener() net.Listener {
	return sf.LocalTCPListenerNamed(scope.Env, "ydb-mock")
}
