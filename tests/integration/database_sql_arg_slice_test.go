//go:build integration
// +build integration

package integration

import (
	"context"
	"database/sql"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/retry"
)

func TestDatabaseSqlArgSlice(t *testing.T) {
	scope := newScope(t)
	db := scope.SQLDriverWithFolder()

	t.Run("single slice", func(t *testing.T) {
		values, err := retry.RetryWithResult(scope.Ctx, func(ctx context.Context) (values []uint64, err error) {
			rows, err := db.QueryContext(ctx,
				`SELECT * FROM AS_TABLE(ListMap($list, ($id) -> (<|id: $id|>))) 
			        WHERE id IN $list
					ORDER BY t.id;`,
				sql.Named("list", []any{1, 2, 3}),
			)
			if err != nil {
				return nil, err
			}
			defer rows.Close()
			for rows.Next() {
				var v uint64
				if err := rows.Scan(&v); err != nil {
					return nil, err
				}
				values = append(values, v)
			}
			return values, rows.Err()
		})
		scope.Require.NoError(err)

		require.Equal(t, []uint64{1, 2, 3}, values)
	})

	t.Run("two slices", func(t *testing.T) {
		values, err := retry.RetryWithResult(scope.Ctx, func(ctx context.Context) (values []uint64, err error) {
			rows, err := db.QueryContext(ctx,
				`SELECT * FROM AS_TABLE(ListMap($list1, ($id) -> (<|id: $id|>))) 
         			WHERE id IN $list2
					ORDER BY t.id;`,
				sql.Named("list1", []int{1, 2, 3}),
				sql.Named("list2", []any{2, 3, 4}),
			)
			if err != nil {
				return nil, err
			}
			defer rows.Close()
			for rows.Next() {
				var v uint64
				if err := rows.Scan(&v); err != nil {
					return nil, err
				}
				values = append(values, v)
			}
			return values, rows.Err()
		})
		scope.Require.NoError(err)

		require.Equal(t, []uint64{2, 3}, values)
	})

	t.Run("tree slices with any args", func(t *testing.T) {
		values, err := retry.RetryWithResult(scope.Ctx, func(ctx context.Context) (values []uint64, err error) {
			rows, err := db.QueryContext(ctx,
				`SELECT t.* FROM AS_TABLE(ListMap($list1, ($id) -> (<|id: $id|>))) AS t
             		WHERE $b 
             		  AND t.id IN $list2
					  AND $a NOT IN $list3
					ORDER BY t.id;`,
				sql.Named("a", "test"),
				sql.Named("list1", []any{1, 2, 3}),
				sql.Named("b", true),
				sql.Named("list2", []int{2, 3, 4}),
				sql.Named("list3", []string{"1", "2", "3"}),
			)
			if err != nil {
				return nil, err
			}
			defer rows.Close()
			for rows.Next() {
				var v uint64
				if err := rows.Scan(&v); err != nil {
					return nil, err
				}
				values = append(values, v)
			}
			return values, rows.Err()
		})
		scope.Require.NoError(err)

		require.Equal(t, []uint64{2, 3}, values)
	})
}
