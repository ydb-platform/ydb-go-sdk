//go:build integration
// +build integration

package integration

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
)

// https://github.com/ydb-platform/ydb-go-sdk/issues/259
func TestIssue259IntervalFromDuration(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	db, err := ydb.Open(ctx,
		os.Getenv("YDB_CONNECTION_STRING"),
		ydb.WithAccessTokenCredentials(os.Getenv("YDB_ACCESS_TOKEN_CREDENTIALS")),
	)
	require.NoError(t, err)

	t.Run("Check about interval work with microseconds", func(t *testing.T) {
		err := db.Table().DoTx(ctx, func(ctx context.Context, tx table.TransactionActor) error {
			//
			res, err := tx.Execute(ctx, `DECLARE $ts as Interval;
			$ten_micro = CAST(10 as Interval);
			SELECT $ts == $ten_micro, $ten_micro;`, table.NewQueryParameters(
				table.ValueParam(`$ts`, types.IntervalValueFromDuration(10*time.Microsecond)),
			))
			if err != nil {
				return err
			}
			if err = res.NextResultSetErr(ctx); err != nil {
				return err
			}
			if !res.NextRow() {
				return fmt.Errorf("unexpected no rows in result set (err = %w)", res.Err())
			}
			var (
				valuesEqual bool
				tenMicro    time.Duration
			)
			if err = res.Scan(&valuesEqual, &tenMicro); err != nil {
				return err
			}
			if !valuesEqual {
				return fmt.Errorf("unexpected values equal (err = %w)", res.Err())
			}
			if tenMicro != 10*time.Microsecond {
				return fmt.Errorf("unexpected ten micro equal: %v (err = %w)", tenMicro, res.Err())
			}
			return res.Err()
		}, table.WithIdempotent())
		require.NoError(t, err)
	})

	t.Run("Check about parse interval represent date interval", func(t *testing.T) {
		err := db.Table().DoTx(ctx, func(ctx context.Context, tx table.TransactionActor) error {
			//
			query := `
		SELECT 
			DateTime::MakeTimestamp(DateTime::ParseIso8601("2009-02-14T02:31:30+0000")) - 
			DateTime::MakeTimestamp(DateTime::ParseIso8601("2009-02-14T01:31:30+0000")) 
		`
			res, err := tx.Execute(ctx, query, nil)
			if err != nil {
				return err
			}
			if err = res.NextResultSetErr(ctx); err != nil {
				return err
			}
			if !res.NextRow() {
				return fmt.Errorf("unexpected no rows in result set (err = %w)", res.Err())
			}
			var delta time.Duration
			if err = res.ScanWithDefaults(&delta); err != nil {
				return err
			}
			if delta != time.Hour {
				return fmt.Errorf("unexpected ten micro equal: %v (err = %w)", delta, res.Err())
			}
			return res.Err()
		}, table.WithIdempotent())
		require.NoError(t, err)
	})

	t.Run("check about send interval work find with dates", func(t *testing.T) {
		err := db.Table().DoTx(ctx, func(ctx context.Context, tx table.TransactionActor) error {
			//
			query := `
		DECLARE $delta AS Interval;
	
		SELECT 
			DateTime::MakeTimestamp(DateTime::ParseIso8601("2009-02-14T01:31:30+0000")) + $delta ==
			DateTime::MakeTimestamp(DateTime::ParseIso8601("2009-02-14T02:31:30+0000"))
		`
			res, err := tx.Execute(ctx, query, table.NewQueryParameters(
				table.ValueParam("$delta", types.IntervalValueFromDuration(time.Hour))),
			)
			if err != nil {
				return err
			}
			if err = res.NextResultSetErr(ctx); err != nil {
				return err
			}
			if !res.NextRow() {
				return fmt.Errorf("unexpected no rows in result set (err = %w)", res.Err())
			}
			var valuesEqual bool
			if err = res.ScanWithDefaults(&valuesEqual); err != nil {
				return err
			}
			if !valuesEqual {
				return fmt.Errorf("unexpected values equal (err = %w)", res.Err())
			}
			return res.Err()
		}, table.WithIdempotent())
		require.NoError(t, err)
	})
}
