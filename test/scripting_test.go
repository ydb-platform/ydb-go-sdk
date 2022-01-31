//go:build !fast
// +build !fast

package test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	ydb "github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/balancers"
	"github.com/ydb-platform/ydb-go-sdk/v3/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/retry"
	"github.com/ydb-platform/ydb-go-sdk/v3/scripting"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func TestScripting(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	db, err := ydb.New(
		ctx,
		ydb.WithConnectionString(os.Getenv("YDB_CONNECTION_STRING")),
		ydb.WithAnonymousCredentials(),
		ydb.With(
			ydb_config.WithRequestTimeout(time.Second*2),
			ydb_config.WithStreamTimeout(time.Second*2),
			ydb_config.WithOperationTimeout(time.Second*2),
			ydb_config.WithOperationCancelAfter(time.Second*2),
		),
		ydb.WithBalancer(ydb_balancers.SingleConn()),
		ydb.WithConnectionTTL(time.Millisecond*10000),
		ydb.WithLogger(
			ydb_trace.DriverConnEvents,
			ydb.WithNamespace("ydb"),
			ydb.WithOutWriter(os.Stdout),
			ydb.WithErrWriter(os.Stderr),
			ydb.WithMinLevel(ydb.TRACE),
		),
		ydb.WithUserAgent("scripting"),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer t.Run("CleanupConnection", func(t *testing.T) {
		if e := db.Close(ctx); e != nil {
			t.Fatalf("db close failed: %+v", e)
		}
	})
	t.Run("Execute", func(t *testing.T) {
		if err := ydb_retry.Retry(ctx, true, func(ctx context.Context) (err error) {
			res, err := db.Scripting().Execute(
				ctx,
				"SELECT 1+1",
				ydb_table.NewQueryParameters(),
			)
			if err != nil {
				return err
			}
			defer func() {
				_ = res.Close()
			}()
			if !res.NextResultSet(ctx) {
				return ydb_retry.RetryableError(
					fmt.Errorf("no result sets"),
					ydb_retry.WithBackoff(ydb_retry.BackoffTypeNoBackoff),
				)
			}
			if !res.NextRow() {
				return ydb_retry.RetryableError(
					fmt.Errorf("no rows"),
					ydb_retry.WithBackoff(ydb_retry.BackoffTypeSlowBackoff),
				)
			}
			var sum int32
			if err = res.Scan(&sum); err != nil {
				return fmt.Errorf("scan failed: %w", err)
			}
			if sum != 2 {
				return fmt.Errorf("unexpected sum: %v", sum)
			}
			return res.Err()
		}); err != nil {
			t.Fatalf("Execute failed: %v", err)
		}
	})
	t.Run("StreamExecute", func(t *testing.T) {
		if err := ydb_retry.Retry(ctx, true, func(ctx context.Context) (err error) {
			res, err := db.Scripting().StreamExecute(
				ctx,
				"SELECT 1+1",
				ydb_table.NewQueryParameters(),
			)
			if err != nil {
				return err
			}
			defer func() {
				_ = res.Close()
			}()
			if !res.NextResultSet(ctx) {
				return ydb_retry.RetryableError(
					fmt.Errorf("no result sets"),
					ydb_retry.WithBackoff(ydb_retry.BackoffTypeNoBackoff),
					ydb_retry.WithDeleteSession(),
				)
			}
			if !res.NextRow() {
				return ydb_retry.RetryableError(
					fmt.Errorf("no rows"),
					ydb_retry.WithBackoff(ydb_retry.BackoffTypeFastBackoff),
				)
			}
			var sum int32
			if err = res.Scan(&sum); err != nil {
				return fmt.Errorf("scan failed: %w", err)
			}
			if sum != 2 {
				return fmt.Errorf("unexpected sum: %v", sum)
			}
			return res.Err()
		}); err != nil {
			t.Fatalf("StreamExecute failed: %v", err)
		}
	})
	t.Run("ExplainPlan", func(t *testing.T) {
		if err := ydb_retry.Retry(ctx, true, func(ctx context.Context) (err error) {
			res, err := db.Scripting().Explain(
				ctx,
				"SELECT 1+1",
				ydb_scripting.ExplainModePlan,
			)
			if err != nil {
				return err
			}
			if res.Plan == "" {
				return fmt.Errorf("empty plan")
			}
			return nil
		}); err != nil {
			t.Fatalf("Explain failed: %v", err)
		}
	})
	t.Run("ExplainValidate", func(t *testing.T) {
		if err := ydb_retry.Retry(ctx, true, func(ctx context.Context) (err error) {
			res, err := db.Scripting().Explain(
				ctx,
				"SELECT 1+1",
				ydb_scripting.ExplainModeValidate,
			)
			if err != nil {
				return err
			}
			if len(res.ParameterTypes) > 0 {
				return fmt.Errorf("unexpected parameter types")
			}
			return nil
		}); err != nil {
			t.Fatalf("Explain failed: %v", err)
		}
	})
}
