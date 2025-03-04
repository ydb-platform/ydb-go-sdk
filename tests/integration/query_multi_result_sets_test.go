//go:build integration
// +build integration

package integration

import (
	"context"
	"errors"
	"fmt"
	"io"
	"testing"

	"github.com/ydb-platform/ydb-go-sdk/v3/query"
)

func TestQueryMultiResultSets(t *testing.T) {
	scope := newScope(t)
	var i, j, k int
	db := scope.Driver()
	err := db.Query().Do(scope.Ctx, func(ctx context.Context, s query.Session) (err error) {
		res, err := s.Query(ctx, `SELECT 42; SELECT 43, 44;`,
			query.WithIdempotent(),
		)
		if err != nil {
			return fmt.Errorf("can't get result: %w", err)
		}
		defer res.Close(ctx)
		
		set, err := res.NextResultSet(ctx)
		if err != nil {
			return fmt.Errorf("set 0: get next result set: %w", err)
		}

		for row, err := set.NextRow(ctx); !errors.Is(err, io.EOF); row, err = set.NextRow(ctx) {
			if err != nil {
				return fmt.Errorf("set 0: get next row: %w", err)
			}
			if err := row.Scan(&i); err != nil {
				return fmt.Errorf("set 0: scan row: %w", err)
			}
			fmt.Println(i)
		}

		set, err = res.NextResultSet(ctx)
		if err != nil {
			return err
		}

		for row, err := set.NextRow(ctx); !errors.Is(err, io.EOF); row, err = set.NextRow(ctx) {
			if err != nil {
				return fmt.Errorf("set 1: get next row: %w", err)
			}

			if err := row.Scan(&j, &k); err != nil {
				return fmt.Errorf("set 1: scan row: %w", err)
			}
			fmt.Println(j, k)
		}
		_, err = res.NextResultSet(ctx)
		if !errors.Is(err, io.EOF) {
			return fmt.Errorf("get next result set: %w", err)
		}

		return nil
	}, query.WithIdempotent())

	scope.Require.NoError(err)

	scope.Require.Equal(42, i)
	scope.Require.Equal(43, j)
	scope.Require.Equal(44, k)
}
