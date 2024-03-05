//go:build integration
// +build integration

package integration

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/version"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
)

func TestQueryExecute(t *testing.T) {
	if version.Lt(os.Getenv("YDB_VERSION"), "24.1") {
		t.Skip("query service not allowed in YDB version '" + os.Getenv("YDB_VERSION") + "'")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	db, err := ydb.Open(ctx,
		os.Getenv("YDB_CONNECTION_STRING"),
		ydb.WithAccessTokenCredentials(os.Getenv("YDB_ACCESS_TOKEN_CREDENTIALS")),
	)
	require.NoError(t, err)
	t.Run("Scan", func(t *testing.T) {
		var (
			p1 string
			p2 uint64
			p3 time.Duration
		)
		err = db.Query().Do(ctx, func(ctx context.Context, s query.Session) (err error) {
			_, res, err := s.Execute(ctx, `
				DECLARE $p1 AS Text;
				DECLARE $p2 AS Uint64;
				DECLARE $p3 AS Interval;
				SELECT $p1, $p2, $p3;
				`,
				query.WithParameters(
					ydb.ParamsBuilder().
						Param("$p1").Text("test").
						Param("$p2").Uint64(100500000000).
						Param("$p3").Interval(time.Duration(100500000000)).
						Build(),
				),
				query.WithSyntax(query.SyntaxYQL),
			)
			if err != nil {
				return err
			}
			rs, err := res.NextResultSet(ctx)
			if err != nil {
				return err
			}
			row, err := rs.NextRow(ctx)
			if err != nil {
				return err
			}
			err = row.Scan(&p1, &p2, &p3)
			if err != nil {
				return err
			}
			return res.Err()
		}, query.WithIdempotent())
		require.NoError(t, err)
		require.EqualValues(t, "test", p1)
		require.EqualValues(t, 100500000000, p2)
		require.EqualValues(t, time.Duration(100500000000), p3)
	})
	t.Run("ScanNamed", func(t *testing.T) {
		var (
			p1 string
			p2 uint64
			p3 time.Duration
		)
		err = db.Query().Do(ctx, func(ctx context.Context, s query.Session) (err error) {
			_, res, err := s.Execute(ctx, `
				DECLARE $p1 AS Text;
				DECLARE $p2 AS Uint64;
				DECLARE $p3 AS Interval;
				SELECT $p1 AS p1, $p2 AS p2, $p3 AS p3;
				`,
				query.WithParameters(
					ydb.ParamsBuilder().
						Param("$p1").Text("test").
						Param("$p2").Uint64(100500000000).
						Param("$p3").Interval(time.Duration(100500000000)).
						Build(),
				),
				query.WithSyntax(query.SyntaxYQL),
			)
			if err != nil {
				return err
			}
			rs, err := res.NextResultSet(ctx)
			if err != nil {
				return err
			}
			row, err := rs.NextRow(ctx)
			if err != nil {
				return err
			}
			err = row.ScanNamed(
				query.Named("p1", &p1),
				query.Named("p2", &p2),
				query.Named("p3", &p3),
			)
			if err != nil {
				return err
			}
			return res.Err()
		}, query.WithIdempotent())
		require.NoError(t, err)
		require.EqualValues(t, "test", p1)
		require.EqualValues(t, 100500000000, p2)
		require.EqualValues(t, time.Duration(100500000000), p3)
	})
	t.Run("ScanStruct", func(t *testing.T) {
		var data struct {
			P1 *string       `sql:"p1"`
			P2 uint64        `sql:"p2"`
			P3 time.Duration `sql:"p3"`
			P4 *string       `sql:"p4"`
		}
		err = db.Query().Do(ctx, func(ctx context.Context, s query.Session) (err error) {
			_, res, err := s.Execute(ctx, `
				DECLARE $p1 AS Text;
				DECLARE $p2 AS Uint64;
				DECLARE $p3 AS Interval;
				SELECT CAST($p1 AS Optional<Text>) AS p1, $p2 AS p2, $p3 AS p3, CAST(NULL AS Optional<Text>) AS p4;
				`,
				query.WithParameters(
					ydb.ParamsBuilder().
						Param("$p1").Text("test").
						Param("$p2").Uint64(100500000000).
						Param("$p3").Interval(time.Duration(100500000000)).
						Build(),
				),
				query.WithSyntax(query.SyntaxYQL),
			)
			if err != nil {
				return err
			}
			rs, err := res.NextResultSet(ctx)
			if err != nil {
				return err
			}
			row, err := rs.NextRow(ctx)
			if err != nil {
				return err
			}
			err = row.ScanStruct(&data)
			if err != nil {
				return err
			}
			return res.Err()
		}, query.WithIdempotent())
		require.NoError(t, err)
		require.NotNil(t, data.P1)
		require.EqualValues(t, "test", *data.P1)
		require.EqualValues(t, 100500000000, data.P2)
		require.EqualValues(t, time.Duration(100500000000), data.P3)
		require.Nil(t, data.P4)
	})
}
