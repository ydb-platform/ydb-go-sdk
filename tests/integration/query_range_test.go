//go:build integration && go1.22 && goexperiment.rangefunc
// +build integration,go1.22,goexperiment.rangefunc

package integration

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/version"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xtest"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
)

func TestQueryRange(t *testing.T) {
	if version.Lt(os.Getenv("YDB_VERSION"), "24.1") {
		t.Skip("query service not allowed in YDB version '" + os.Getenv("YDB_VERSION") + "'")
	}

	ctx, cancel := context.WithCancel(xtest.Context(t))
	defer cancel()

	db, err := ydb.Open(ctx,
		os.Getenv("YDB_CONNECTION_STRING"),
		ydb.WithAccessTokenCredentials(os.Getenv("YDB_ACCESS_TOKEN_CREDENTIALS")),
	)
	require.NoError(t, err)
	t.Run("Execute", func(t *testing.T) {
		listItems := make([]value.Value, 1000)
		for i := range make([]struct{}, 1000) {
			listItems[i] = value.StructValue(
				value.StructValueField{
					Name: "p1",
					V:    value.TextValue(strconv.Itoa(i)),
				},
				value.StructValueField{
					Name: "p2",
					V:    value.Uint64Value(uint64(i)),
				},
				value.StructValueField{
					Name: "p3",
					V:    value.IntervalValueFromDuration(time.Duration(i * 1000)),
				},
			)
		}
		r, err := db.Query().Execute(ctx, `
				DECLARE $values AS List<Struct<p1:Text,p2:Uint64,p3:Interval>>;
				SELECT p1, p2, p3 FROM AS_TABLE($values);
			`,
			query.WithParameters(
				ydb.ParamsBuilder().Param("$values").BeginList().AddItems(listItems...).EndList().Build(),
			),
		)
		require.NoError(t, err)
		count := 0
		for rs, err := range r.Range(ctx) {
			require.NoError(t, err)
			for row, err := range rs.Range(ctx) {
				require.NoError(t, err)

				var (
					p1 string
					p2 uint64
					p3 time.Duration
				)

				err = row.Scan(&p1, &p2, &p3)
				require.NoError(t, err)

				require.EqualValues(t, strconv.Itoa(count), p1)
				require.EqualValues(t, count, p2)
				require.EqualValues(t, time.Duration(count*1000), p3)

				count++
			}
		}
		require.EqualValues(t, 1000, count)
	})
	t.Run("Do", func(t *testing.T) {
		var (
			p1 string
			p2 uint64
			p3 time.Duration
		)
		err := db.Query().Do(ctx, func(ctx context.Context, s query.Session) error {
			_, r, err := s.Execute(ctx, `
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
			for rs, err := range r.Range(ctx) {
				if err != nil {
					return err
				}
				for row, err := range rs.Range(ctx) {
					if err != nil {
						return err
					}
					err = row.Scan(&p1, &p2, &p3)
					if err != nil {
						return err
					}

					if p1 != "test" {
						return fmt.Errorf("unexpected p1 value: %v", p1)
					}
					if p2 != 100500000000 {
						return fmt.Errorf("unexpected p2 value: %v", p2)
					}
					if p3 != time.Duration(100500000000) {
						return fmt.Errorf("unexpected p3 value: %v", p3)
					}
				}
			}
			return r.Err()
		}, query.WithIdempotent())
		require.NoError(t, err)
	})
	t.Run("DoTx", func(t *testing.T) {
		var (
			p1 string
			p2 uint64
			p3 time.Duration
		)
		err := db.Query().DoTx(ctx, func(ctx context.Context, tx query.TxActor) error {
			r, err := tx.Execute(ctx, `
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
			for rs, err := range r.Range(ctx) {
				if err != nil {
					return err
				}
				for row, err := range rs.Range(ctx) {
					if err != nil {
						return err
					}
					err = row.Scan(&p1, &p2, &p3)
					if err != nil {
						return err
					}

					if p1 != "test" {
						return fmt.Errorf("unexpected p1 value: %v", p1)
					}
					if p2 != 100500000000 {
						return fmt.Errorf("unexpected p2 value: %v", p2)
					}
					if p3 != time.Duration(100500000000) {
						return fmt.Errorf("unexpected p3 value: %v", p3)
					}
				}
			}
			return r.Err()
		}, query.WithIdempotent())
		require.NoError(t, err)
	})
}
