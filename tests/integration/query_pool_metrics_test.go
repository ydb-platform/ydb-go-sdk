//go:build integration

package integration

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/Ydb_Query_V1"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/metrics"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func TestQueryPoolMetricsWhileSessionIsCheckedOut(t *testing.T) {
	// Regression: https://github.com/ydb-platform/ydb-go-sdk/issues/2295
	scope := newScope(t)

	registry := newRegistryConfig(trace.QueryPoolEvents)
	db := scope.Driver(
		ydb.WithSessionPoolSizeLimit(1),
		metrics.WithTraces(registry),
	)

	require.NoError(t, db.Query().Do(scope.Ctx, func(ctx context.Context, s query.Session) error {
		if err := s.Exec(ctx, "SELECT 1"); err != nil {
			return err
		}

		// Do retains the session until this callback returns, even after Exec completes.
		// Inspect the actual metrics adapter output while the only pool slot is occupied.
		registry.gauges.AssertEqual(t, "ydb.query.pool.size.in_use", 1)

		return nil
	}, query.WithIdempotent()))

	require.NoError(t, db.Query().Do(scope.Ctx, func(ctx context.Context, s query.Session) error {
		if err := s.Exec(ctx, "SELECT 1"); err != nil {
			return err
		}

		// The reused session is checked out again and must no longer be counted as idle.
		registry.gauges.AssertEqual(t, "ydb.query.pool.size.idle", 0)

		return nil
	}, query.WithIdempotent()))
}

func TestQueryPoolMetricsWhileSessionIsBeingCreated(t *testing.T) {
	scope := newScope(t)
	registry := newRegistryConfig(trace.QueryPoolEvents)
	stopper := NewGrpcStopper(errors.New("stop gRPC for test"))

	db := scope.Driver(
		ydb.WithSessionPoolSizeLimit(1),
		metrics.WithTraces(registry),
		ydb.With(config.WithGrpcOptions(grpc.WithChainUnaryInterceptor(
			stopper.UnaryClientInterceptor,
		))),
	)
	client := db.Query()
	paused := stopper.PauseOnlyOnMethods(Ydb_Query_V1.QueryService_CreateSession_FullMethodName)

	go func() {
		select {
		case <-paused:
			registry.gauges.AssertEqual(t, "ydb.query.pool.size.concurrency", 1)
			registry.gauges.AssertEqual(t, "ydb.query.pool.size.create_in_progress", 1)
		case <-scope.Ctx.Done():
		}
		stopper.Start()
	}()
	require.NoError(t, client.Do(scope.Ctx, func(ctx context.Context, s query.Session) error {
		return s.Exec(ctx, "SELECT 1")
	}, query.WithIdempotent()))
}

func TestQueryPoolMetricsBetweenRetryAttempts(t *testing.T) {
	scope := newScope(t)
	registry := newRegistryConfig(trace.QueryPoolEvents)
	stopper := NewGrpcStopper(status.Error(codes.ResourceExhausted, "retry operation"))
	stopper.Stop(Ydb_Query_V1.QueryService_ExecuteQuery_FullMethodName)
	db := scope.Driver(
		ydb.WithSessionPoolSizeLimit(1),
		metrics.WithTraces(registry),
		ydb.With(config.WithGrpcOptions(grpc.WithChainStreamInterceptor(
			stopper.StreamClientInterceptor,
		))),
	)
	require.NoError(t, db.Query().Do(scope.Ctx, func(ctx context.Context, s query.Session) error {
		return s.Exec(ctx, "SELECT 1")
	}, query.WithIdempotent(), query.WithRetryBudget(retryBudgetFunc(func(context.Context) error {
		// Acquire runs after the session is returned and before the next attempt.
		registry.gauges.AssertEqual(t, "ydb.query.pool.size.idle", 1)
		registry.gauges.AssertEqual(t, "ydb.query.pool.size.in_use", 0)
		stopper.Start()

		return nil
	}))))
}

func TestQueryPoolMetricsWhileWaitingForSession(t *testing.T) {
	scope := newScope(t)
	registry := newRegistryConfig(trace.QueryPoolEvents)
	stopper := NewGrpcStopper(errors.New("stop gRPC for test"))
	db := scope.Driver(
		ydb.WithSessionPoolSizeLimit(1),
		metrics.WithTraces(registry),
		ydb.With(config.WithGrpcOptions(grpc.WithChainStreamInterceptor(
			stopper.StreamClientInterceptor,
		))),
	)
	client := db.Query()
	paused := stopper.PauseOnlyOnMethods(Ydb_Query_V1.QueryService_ExecuteQuery_FullMethodName)
	group, ctx := errgroup.WithContext(scope.Ctx)
	group.Go(func() error {
		return client.Do(ctx, func(ctx context.Context, s query.Session) error {
			return s.Exec(ctx, "SELECT 1")
		}, query.WithIdempotent())
	})
	// Let the initial checkout publish its metrics before starting the next Do.
	select {
	case <-paused:
	case <-ctx.Done():
	}
	group.Go(func() error {
		return client.Do(ctx, func(ctx context.Context, s query.Session) error {
			return s.Exec(ctx, "SELECT 1")
		}, query.WithIdempotent())
	})
	group.Go(func() error {
		defer stopper.Start()
		if err := ctx.Err(); err != nil {
			return err
		}
		// One Do holds the only session until these checks finish.
		assert.EventuallyWithT(t, func(t *assert.CollectT) {
			registry.gauges.AssertEqual(t, "ydb.query.pool.size.concurrency", 2)
			registry.gauges.AssertEqual(t, "ydb.query.pool.size.waiters_queue", 1)
		}, time.Second, time.Millisecond)

		return nil
	})
	require.NoError(t, group.Wait())
}

type retryBudgetFunc func(context.Context) error

func (f retryBudgetFunc) Acquire(ctx context.Context) error {
	return f(ctx)
}
