//go:build integration
// +build integration

package integration

import (
		"io"
		"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
)

func TestQueryReadRow(t *testing.T) {
	scope := newScope(t)
	db := scope.Driver()
	ctx := scope.Ctx

	t.Run("HappyWay", func(t *testing.T) {
		row, err := db.Query().QueryRow(ctx, `
		DECLARE $p1 AS Text;
		DECLARE $p2 AS Uint64;
		DECLARE $p3 AS Interval;
		SELECT $p1, $p2, $p3;`,
			query.WithParameters(
				ydb.ParamsBuilder().
					Param("$p1").Text("test").
					Param("$p2").Uint64(100500000000).
					Param("$p3").Interval(time.Duration(100500000000)).
					Build(),
			),
			query.WithSyntax(query.SyntaxYQL),
			query.WithIdempotent(),
		)
		require.NoError(t, err)

		var (
			p1 string
			p2 uint64
			p3 time.Duration
		)
		err = row.Scan(&p1, &p2, &p3)
		require.NoError(t, err)
		require.EqualValues(t, "test", p1)
		require.EqualValues(t, 100500000000, p2)
		require.EqualValues(t, time.Duration(100500000000), p3)
	})

	t.Run("ErrNoRows", func(t *testing.T) {
		scope := newScope(t)
		db := scope.Driver()
		ctx := scope.Ctx

		_, err := db.Query().QueryRow(ctx, `SELECT * FROM (SELECT 1) WHERE 1 = 0;`)
		require.ErrorIs(t, err, query.ErrNoRows)
		require.ErrorIs(t, err, io.EOF)
	})

	t.Run("ExactlyOneRow", func(t *testing.T) {
		scope := newScope(t)
		db := scope.Driver()
		ctx := scope.Ctx

		row, err := db.Query().QueryRow(ctx, `SELECT 1, "test", 3.14;`)
		require.NoError(t, err)

		var (
			col1 int32
			col2 string
			col3 float64
		)
		err = row.Scan(&col1, &col2, &col3)
		require.NoError(t, err)
		require.EqualValues(t, 1, col1)
		require.EqualValues(t, "test", col2)
		require.EqualValues(t, 3.14, col3)
	})

	t.Run("ErrMoreThanOneResultSet", func(t *testing.T) {
		scope := newScope(t)
		db := scope.Driver()
		ctx := scope.Ctx

		_, err := db.Query().QueryRow(ctx, `SELECT 1; SELECT 2;`)
		require.ErrorIs(t, err, query.ErrMoreThanOneResultSet)
	})

	t.Run("ErrMoreThanOneRow", func(t *testing.T) {
		scope := newScope(t)
		db := scope.Driver()
		ctx := scope.Ctx

		_, err := db.Query().QueryRow(ctx, `SELECT 1 UNION ALL SELECT 2;`)
		require.ErrorIs(t, err, query.ErrMoreThanOneRow)
	})
}
