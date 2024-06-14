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
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/version"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xtest"
	"github.com/ydb-platform/ydb-go-sdk/v3/log"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func TestQueryExecute(t *testing.T) {
	if version.Lt(os.Getenv("YDB_VERSION"), "24.1") {
		t.Skip("query service not allowed in YDB version '" + os.Getenv("YDB_VERSION") + "'")
	}

	ctx, cancel := context.WithCancel(xtest.Context(t))
	defer cancel()

	db, err := ydb.Open(ctx,
		os.Getenv("YDB_CONNECTION_STRING"),
		ydb.WithAccessTokenCredentials(os.Getenv("YDB_ACCESS_TOKEN_CREDENTIALS")),
		ydb.WithTraceQuery(
			log.Query(
				log.Default(os.Stdout,
					log.WithLogQuery(),
					log.WithColoring(),
					log.WithMinLevel(log.INFO),
				),
				trace.QueryEvents,
			),
		),
	)
	require.NoError(t, err)
	t.Run("Execute", func(t *testing.T) {
		var (
			p1 string
			p2 uint64
			p3 time.Duration
		)
		res, err := db.Query().Execute(ctx, `
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
		require.NoError(t, err)
		rs, err := res.NextResultSet(ctx)
		require.NoError(t, err)
		row, err := rs.NextRow(ctx)
		require.NoError(t, err)
		err = row.Scan(&p1, &p2, &p3)
		require.NoError(t, err)
		require.NoError(t, res.Err())
		require.EqualValues(t, "test", p1)
		require.EqualValues(t, 100500000000, p2)
		require.EqualValues(t, time.Duration(100500000000), p3)
	})
	t.Run("Stats", func(t *testing.T) {
		s, err := query.Stats(db.Query())
		require.NoError(t, err)
		require.EqualValues(t, -1, s.Limit)
	})
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
	t.Run("Tx", func(t *testing.T) {
		t.Run("Explicit", func(t *testing.T) {
			err = db.Query().Do(ctx, func(ctx context.Context, s query.Session) (err error) {
				tx, err := s.Begin(ctx, query.TxSettings(query.WithSerializableReadWrite()))
				if err != nil {
					return err
				}
				res, err := tx.Execute(ctx, `SELECT 1`)
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
				var v int32
				err = row.Scan(&v)
				if err != nil {
					return err
				}
				if v != 1 {
					return fmt.Errorf("unexpected value from database: %d", v)
				}
				if err = res.Err(); err != nil {
					return err
				}
				return tx.CommitTx(ctx)
			}, query.WithIdempotent())
			require.NoError(t, err)
		})
		t.Run("Lazy", func(t *testing.T) {
			err = db.Query().Do(ctx, func(ctx context.Context, s query.Session) (err error) {
				tx, res, err := s.Execute(ctx, `SELECT 1`,
					query.WithTxControl(query.TxControl(query.BeginTx(query.WithSerializableReadWrite()))),
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
				var v int32
				err = row.Scan(&v)
				if err != nil {
					return err
				}
				if v != 1 {
					return fmt.Errorf("unexpected value from database: %d", v)
				}
				if err = res.Err(); err != nil {
					return err
				}
				res, err = tx.Execute(ctx, `SELECT 2`, query.WithCommit())
				if err != nil {
					return err
				}
				rs, err = res.NextResultSet(ctx)
				if err != nil {
					return err
				}
				row, err = rs.NextRow(ctx)
				if err != nil {
					return err
				}
				err = row.Scan(&v)
				if err != nil {
					return err
				}
				if v != 2 {
					return fmt.Errorf("unexpected value from database: %d", v)
				}
				return res.Err()
			}, query.WithIdempotent())
			require.NoError(t, err)
		})
	})
}
