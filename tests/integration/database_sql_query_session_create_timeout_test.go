//go:build integration
// +build integration

package integration

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/Ydb_Query_V1"
	"google.golang.org/grpc"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/balancers"
	"github.com/ydb-platform/ydb-go-sdk/v3/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
)

func TestDatabaseSQLQueryServiceRetriesCreateSessionAttemptTimeout(t *testing.T) {
	const createSessionTimeout = 100 * time.Millisecond

	var createSessionCalls atomic.Int32
	interceptor := func(
		ctx context.Context,
		method string,
		req, reply any,
		cc *grpc.ClientConn,
		invoker grpc.UnaryInvoker,
		opts ...grpc.CallOption,
	) error {
		if method != Ydb_Query_V1.QueryService_CreateSession_FullMethodName {
			return invoker(ctx, method, req, reply, cc, opts...)
		}

		if createSessionCalls.Add(1) == 1 {
			<-ctx.Done()

			return ctx.Err()
		}

		return invoker(ctx, method, req, reply, cc, opts...)
	}

	scope := newScope(t)
	scope.Driver(
		// Keep the only local endpoint usable after the simulated attempt deadline.
		ydb.WithBalancer(balancers.SingleConn()),
		ydb.WithSessionPoolCreateSessionTimeout(createSessionTimeout),
		ydb.With(config.WithGrpcOptions(
			grpc.WithChainUnaryInterceptor(interceptor),
		)),
	)
	db := scope.SQLDriver()

	connectCtx, cancel := context.WithTimeout(scope.Ctx, 5*time.Second)
	defer cancel()

	err := db.PingContext(connectCtx)
	connectCtxErr := connectCtx.Err()

	require.NoError(t, connectCtxErr, "the caller context must remain active after the attempt timeout")
	require.NoError(t, err, "the internal session creation timeout must be retried")
}

func TestQuerySessionPoolKeepsCreatingSessionAfterCallerContextCancellation(t *testing.T) {
	scope := newScope(t)
	requestCtx, cancel := context.WithCancel(scope.Ctx)
	defer cancel()

	var createSessionCalls atomic.Int32
	createSessionStarted := make(chan struct{})
	interceptor := func(
		ctx context.Context,
		method string,
		req, reply any,
		cc *grpc.ClientConn,
		invoker grpc.UnaryInvoker,
		opts ...grpc.CallOption,
	) error {
		if method != Ydb_Query_V1.QueryService_CreateSession_FullMethodName {
			return invoker(ctx, method, req, reply, cc, opts...)
		}

		if createSessionCalls.Add(1) == 1 {
			close(createSessionStarted)
			<-requestCtx.Done()
		}

		return invoker(ctx, method, req, reply, cc, opts...)
	}

	driver := scope.Driver(
		ydb.WithBalancer(balancers.SingleConn()),
		ydb.WithSessionPoolCreateSessionTimeout(5*time.Second),
		ydb.With(config.WithGrpcOptions(
			grpc.WithChainUnaryInterceptor(interceptor),
		)),
	)

	firstRequestDone := make(chan error, 1)
	go func() {
		firstRequestDone <- driver.Query().Do(requestCtx, func(ctx context.Context, _ query.Session) error {
			return ctx.Err()
		})
	}()

	<-createSessionStarted
	cancel()

	require.ErrorIs(t, <-firstRequestDone, context.Canceled)
	require.NoError(t, driver.Query().Do(scope.Ctx, func(context.Context, query.Session) error {
		return nil
	}))
	require.EqualValues(t, 1, createSessionCalls.Load(),
		"the session created for the canceled request must be reused",
	)
}
