//go:build integration
// +build integration

package integration

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result/indexed"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result/named"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
)

type rawInt64 struct {
	val int64
}

func (r *rawInt64) UnmarshalYDB(raw types.RawValue) error {
	raw.Unwrap()
	r.val = raw.Int64()
	return nil
}

func TestIssueWideResultWithUnwrap(t *testing.T) {
	const columnsCount = 200
	var columns []string
	for i := 0; i < columnsCount; i++ {
		column := fmt.Sprintf("CAST(%v AS Int64?) as c%v", i, i)
		columns = append(columns, column)
	}

	query := "SELECT " + strings.Join(columns, ", ")
	t.Run("named", func(t *testing.T) {
		scope := newScope(t)
		var res result.Result
		var err error
		err = scope.Driver().Table().DoTx(scope.Ctx, func(ctx context.Context, tx table.TransactionActor) error {
			res, err = tx.Execute(ctx, query, nil)
			return err
		})
		require.NoError(t, err)

		res.NextResultSet(scope.Ctx)
		require.NoError(t, res.Err())
		res.NextRow()
		require.NoError(t, res.Err())

		results := make([]rawInt64, columnsCount)
		resultsPointers := make([]named.Value, columnsCount)
		for i := range results {
			resultsPointers[i] = named.Required("c"+strconv.Itoa(i), &results[i])
		}
		err = res.ScanNamed(resultsPointers...)
		require.NoError(t, err)

		for i, val := range results {
			require.Equal(t, int64(i), val.val)
		}
	})
	t.Run("indexed", func(t *testing.T) {
		scope := newScope(t)
		var res result.Result
		var err error
		err = scope.Driver().Table().DoTx(scope.Ctx, func(ctx context.Context, tx table.TransactionActor) error {
			res, err = tx.Execute(ctx, query, nil)
			return err
		})
		require.NoError(t, err)

		res.NextResultSet(scope.Ctx)
		require.NoError(t, res.Err())
		res.NextRow()
		require.NoError(t, res.Err())

		results := make([]rawInt64, columnsCount)
		resultsPointers := make([]indexed.RequiredOrOptional, columnsCount)
		for i := range results {
			resultsPointers[i] = &results[i]
		}
		err = res.Scan(resultsPointers...)
		require.NoError(t, err)

		for i, val := range results {
			require.Equal(t, int64(i), val.val)
		}
	})
}

// https://github.com/ydb-platform/ydb-go-sdk/issues/1227
func TestRegressionIssue1227RetryBadSession(t *testing.T) {
	var (
		scope = newScope(t)
		cnt   = 0
	)
	err := scope.Driver().Table().Do(scope.Ctx, func(ctx context.Context, s table.Session) error {
		cnt++
		if cnt < 100 {
			return xerrors.WithStackTrace(xerrors.Operation(xerrors.WithStatusCode(Ydb.StatusIds_BAD_SESSION)))
		}

		return nil
	})
	require.NoError(t, err)
	require.EqualValues(t, 100, cnt)
}
