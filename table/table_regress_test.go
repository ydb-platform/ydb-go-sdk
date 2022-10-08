//go:build !fast
// +build !fast

package table_test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/xerrors"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result/named"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
)

type issue229Struct struct{}

// UnmarshalJSON implements json.Unmarshaler
func (i *issue229Struct) UnmarshalJSON(_ []byte) error {
	return nil
}

func TestIssue229UnexpectedNullWhileParseNilJsonDocumentValue(t *testing.T) {
	// https://github.com/ydb-platform/ydb-go-sdk/issues/229
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	db := connect(t)
	defer db.Close(ctx)
	err := db.Table().DoTx(ctx, func(ctx context.Context, tx table.TransactionActor) error {
		res, err := tx.Execute(ctx, `SELECT Nothing(JsonDocument?) AS r`, nil)
		require.NoError(t, err)
		require.NoError(t, res.NextResultSetErr(ctx))
		require.True(t, res.NextRow())

		var val issue229Struct
		require.NoError(t, res.Scan(&val))
		return nil
	})
	require.NoError(t, err)
}

func connect(t testing.TB) ydb.Connection {
	db, err := ydb.Open(
		context.Background(),
		os.Getenv("YDB_CONNECTION_STRING"),
		ydb.WithAccessTokenCredentials(os.Getenv("YDB_ACCESS_TOKEN_CREDENTIALS")))
	require.NoError(t, err)
	return db
}

func TestIssue259IntervalFromDuration(t *testing.T) {
	// https://github.com/ydb-platform/ydb-go-sdk/issues/259
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	db := connect(t)
	defer db.Close(ctx)
	err := db.Table().DoTx(ctx, func(ctx context.Context, tx table.TransactionActor) error {
		// Check about interval work with microseconds
		res, err := tx.Execute(ctx, `DECLARE $ts as Interval;
			$ten_micro = CAST(10 as Interval);
			SELECT $ts == $ten_micro, $ten_micro;`, table.NewQueryParameters(
			table.ValueParam(`$ts`, types.IntervalValueFromDuration(10*time.Microsecond)),
		))
		require.NoError(t, err)
		require.NoError(t, res.NextResultSetErr(ctx))
		require.True(t, res.NextRow())

		var (
			valuesEqual bool
			tenMicro    time.Duration
		)
		require.NoError(t, res.Scan(&valuesEqual, &tenMicro))
		require.True(t, valuesEqual)
		require.Equal(t, 10*time.Microsecond, tenMicro)

		// Check about parse interval represent date interval
		query := `
		SELECT 
			DateTime::MakeTimestamp(DateTime::ParseIso8601("2009-02-14T02:31:30+0000")) - 
			DateTime::MakeTimestamp(DateTime::ParseIso8601("2009-02-14T01:31:30+0000")) 
		`
		res, err = tx.Execute(ctx, query, nil)
		require.NoError(t, err)
		require.NoError(t, res.NextResultSetErr(ctx))
		require.True(t, res.NextRow())

		var delta time.Duration
		require.NoError(t, res.ScanWithDefaults(&delta))
		require.Equal(t, time.Hour, delta)

		// check about send interval work find with dates
		query = `
		DECLARE $delta AS Interval;
	
		SELECT 
			DateTime::MakeTimestamp(DateTime::ParseIso8601("2009-02-14T01:31:30+0000")) + $delta ==
			DateTime::MakeTimestamp(DateTime::ParseIso8601("2009-02-14T02:31:30+0000"))
		`
		res, err = tx.Execute(ctx, query, table.NewQueryParameters(
			table.ValueParam("$delta", types.IntervalValueFromDuration(time.Hour))),
		)
		require.NoError(t, err)
		require.NoError(t, res.NextResultSetErr(ctx))
		require.True(t, res.NextRow())

		require.NoError(t, res.ScanWithDefaults(&valuesEqual))
		require.True(t, valuesEqual)

		return nil
	})
	require.NoError(t, err)
}

func TestIssue415ScanError(t *testing.T) {
	// https://github.com/ydb-platform/ydb-go-sdk/issues/415
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	db := connect(t)
	defer db.Close(ctx)
	err := db.Table().DoTx(ctx, func(ctx context.Context, tx table.TransactionActor) error {
		res, err := tx.Execute(ctx, `SELECT 1 as abc, 2 as def;`, nil)
		if err != nil {
			return err
		}
		err = res.NextResultSetErr(ctx)
		if err != nil {
			return err
		}
		if !res.NextRow() {
			if err = res.Err(); err != nil {
				return err
			}
			return fmt.Errorf("unexpected empty result set")
		}
		var abc, def int32
		err = res.ScanNamed(
			named.Required("abc", &abc),
			named.Required("ghi", &def),
		)
		if err != nil {
			return err
		}
		fmt.Println(abc, def)
		return res.Err()
	}, table.WithTxSettings(table.TxSettings(table.WithSnapshotReadOnly())))
	require.Error(t, err)
	err = func(err error) error {
		for {
			//nolint:errorlint
			if unwrappedErr, has := err.(xerrors.Wrapper); has {
				err = unwrappedErr.Unwrap()
			} else {
				return err
			}
		}
	}(err)
	require.Equal(t, "not found column 'ghi'", err.Error())
}
