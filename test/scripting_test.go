//go:build !fast
// +build !fast

package test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/balancer"
	"github.com/ydb-platform/ydb-go-sdk/v3/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/retry"
	"github.com/ydb-platform/ydb-go-sdk/v3/scripting"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func TestScriptingService(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	db, err := ydb.New(
		ctx,
		ydb.WithConnectionString(os.Getenv("YDB_CONNECTION_STRING")),
		ydb.WithAnonymousCredentials(),
		ydb.With(
			config.WithRequestTimeout(time.Second*2),
			config.WithStreamTimeout(time.Second*2),
			config.WithOperationTimeout(time.Second*2),
			config.WithOperationCancelAfter(time.Second*2),
		),
		ydb.WithBalancer(balancer.SingleConn()),
		ydb.WithConnectionTTL(time.Millisecond*50),
		ydb.WithLogger(
			trace.DriverConnEvents,
			ydb.WithNamespace("ydb"),
			ydb.WithOutWriter(os.Stdout),
			ydb.WithErrWriter(os.Stderr),
			ydb.WithMinLevel(ydb.TRACE),
		),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = db.Close(ctx)
	}()
	t.Run("ExecuteYql", func(t *testing.T) {
		if err := retry.Retry(ctx, true, func(ctx context.Context) (err error) {
			res, err := db.Scripting().ExecuteYql(ctx, "SELECT 1+1", table.NewQueryParameters())
			if err != nil {
				return err
			}
			defer func() {
				_ = res.Close()
			}()
			if !res.NextResultSet(ctx) {
				return retry.RetryableError(
					"no result sets",
					retry.WithBackoff(retry.BackoffTypeNoBackoff),
				)
			}
			if !res.NextRow() {
				return retry.RetryableError(
					"no rows",
					retry.WithBackoff(retry.BackoffTypeSlowBackoff),
				)
			}
			var sum int32
			if err = res.Scan(&sum); err != nil {
				return fmt.Errorf("scan failed: %w", err)
			}
			if sum != 2 {
				return fmt.Errorf("unexpected sum: %v", sum)
			}
			return nil
		}); err != nil {
			t.Fatalf("ExecuteYql failed: %v", err)
		}
	})
	t.Run("StreamExecuteYql", func(t *testing.T) {
		if err := retry.Retry(ctx, true, func(ctx context.Context) (err error) {
			res, err := db.Scripting().StreamExecuteYql(ctx, "SELECT 1+1", table.NewQueryParameters())
			if err != nil {
				return err
			}
			defer func() {
				_ = res.Close()
			}()
			if !res.NextResultSet(ctx) {
				return retry.RetryableError(
					"no result sets",
					retry.WithBackoff(retry.BackoffTypeNoBackoff),
					retry.WithDeleteSession(),
				)
			}
			if !res.NextRow() {
				return retry.RetryableError(
					"no rows",
					retry.WithBackoff(retry.BackoffTypeFastBackoff),
				)
			}
			var sum int32
			if err = res.Scan(&sum); err != nil {
				return fmt.Errorf("scan failed: %w", err)
			}
			if sum != 2 {
				return fmt.Errorf("unexpected sum: %v", sum)
			}
			return nil
		}); err != nil {
			t.Fatalf("StreamExecuteYql failed: %v", err)
		}
	})
	t.Run("ExplainYql", func(t *testing.T) {
		if err := retry.Retry(ctx, true, func(ctx context.Context) (err error) {
			res, err := db.Scripting().ExplainYql(ctx, "SELECT 1+1", scripting.ExplainModeDefault)
			if err != nil {
				return err
			}
			if res.Plan == "" {
				return fmt.Errorf("empty plan")
			}
			return nil
		}); err != nil {
			t.Fatalf("ExplainYql failed: %v", err)
		}
	})
}
