//go:build !fast
// +build !fast

package test

import (
	"context"
	"errors"
	"fmt"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
	"io"
	"os"
	"path"
	"sync/atomic"
	"testing"
	"time"

	ydb "github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
)

func TestLongStream(t *testing.T) {
	var (
		discoveryInterval = 10 * time.Second
		db                ydb.Connection
		err               error
		upsertRowsCount   = 10000
		batchSize         = 1000
	)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	t.Run("make connection", func(t *testing.T) {
		db, err = ydb.Open(
			ctx,
			os.Getenv("YDB_CONNECTION_STRING"),
			ydb.WithAccessTokenCredentials(
				os.Getenv("YDB_ACCESS_TOKEN_CREDENTIALS"),
			),
			ydb.WithDiscoveryInterval(0), // disable re-discovery on upsert time
		)
		if err != nil {
			t.Fatal(err)
		}
	})

	defer func(db ydb.Connection) {
		// cleanup
		_ = db.Close(ctx)
	}(db)

	t.Run("creating stream table", func(t *testing.T) {
		if err = db.Table().Do(
			ctx,
			func(ctx context.Context, s table.Session) (err error) {
				return s.ExecuteSchemeQuery(
					ctx,
					`CREATE TABLE long_stream_query (val Int32, PRIMARY KEY (val))`,
				)
			},
		); err != nil {
			t.Fatalf("create table failed: %v\n", err)
		}
	})

	t.Run("check batch size", func(t *testing.T) {
		if upsertRowsCount%batchSize != 0 {
			t.Fatalf("wrong batch size: (%d mod %d = %d) != 0", upsertRowsCount, batchSize, upsertRowsCount%batchSize)
		}
	})

	t.Run("upserting rows", func(t *testing.T) {
		var upserted uint32 = 0
		for i := 0; i < (upsertRowsCount / batchSize); i++ {
			var (
				from = int32(i * batchSize)
				to   = int32((i + 1) * batchSize)
			)
			t.Run(fmt.Sprintf("upserting %d..%d", from, to-1), func(t *testing.T) {
				values := make([]types.Value, 0, batchSize)
				for j := from; j < to; j++ {
					values = append(
						values,
						types.StructValue(
							types.StructFieldValue("val", types.Int32Value(j)),
						),
					)
				}
				if err = db.Table().Do(
					ctx,
					func(ctx context.Context, s table.Session) (err error) {
						_, _, err = s.Execute(
							ctx,
							table.TxControl(
								table.BeginTx(
									table.WithSerializableReadWrite(),
								),
								table.CommitTx(),
							), `
								DECLARE $values AS List<Struct<
									val: Int32,
								>>;
								UPSERT INTO long_stream_query
								SELECT
									val 
								FROM
									AS_TABLE($values);            
							`, table.NewQueryParameters(
								table.ValueParam(
									"$values",
									types.ListValue(values...),
								),
							),
						)
						return err
					},
				); err != nil {
					t.Fatalf("upsert failed: %v\n", err)
				} else {
					atomic.AddUint32(&upserted, uint32(batchSize))
				}
			})
		}
		if upserted != uint32(upsertRowsCount) {
			t.Fatalf("wrong rows count: %v, expected: %d", upserted, upsertRowsCount)
		}
	})

	t.Run("make child discovered connection", func(t *testing.T) {
		db, err = db.With(ctx, ydb.WithDiscoveryInterval(discoveryInterval))
		if err != nil {
			t.Fatal(err)
		}
	})

	defer func(db ydb.Connection) {
		// cleanup
		_ = db.Close(ctx)
	}(db)

	t.Run("execute stream query", func(t *testing.T) {
		if err = db.Table().Do(
			ctx,
			func(ctx context.Context, s table.Session) (err error) {
				var (
					start     = time.Now()
					rowsCount = 0
				)
				res, err := s.StreamExecuteScanQuery(ctx, "SELECT val FROM long_stream_query", table.NewQueryParameters())
				if err != nil {
					return err
				}
				t.Run("receiving result sets", func(t *testing.T) {
					for err == nil {
						t.Run("", func(t *testing.T) {
							if err = res.NextResultSetErr(ctx); err != nil {
								if errors.Is(err, io.EOF) {
									return
								}
								t.Fatalf("unexpected error: %v (rowsCount: %d, duration: %v)", err, rowsCount, time.Since(start))
							}
							for res.NextRow() {
								rowsCount++
							}
							time.Sleep(discoveryInterval)
						})
					}
				})
				if err = res.Err(); err != nil {
					return fmt.Errorf("received error: %w (duration: %v)", err, time.Since(start))
				}
				if rowsCount != upsertRowsCount {
					return fmt.Errorf("wrong rows count: %v, expected: %d (duration: %v)", rowsCount, upsertRowsCount, time.Since(start))
				}
				return nil
			},
		); err != nil {
			t.Fatalf("stream query failed: %v\n", err)
		}
	})

	t.Run("stream read table", func(t *testing.T) {
		if err = db.Table().Do(
			ctx,
			func(ctx context.Context, s table.Session) (err error) {
				var (
					start     = time.Now()
					rowsCount = 0
				)
				res, err := s.StreamReadTable(ctx, path.Join(db.Name(), "long_stream_query"), options.ReadColumn("val"))
				if err != nil {
					return err
				}
				t.Run("receiving result sets", func(t *testing.T) {
					for err == nil {
						t.Run("", func(t *testing.T) {
							if err = res.NextResultSetErr(ctx); err != nil {
								if errors.Is(err, io.EOF) {
									return
								}
								t.Fatalf("unexpected error: %v (rowsCount: %d, duration: %v)", err, rowsCount, time.Since(start))
							}
							for res.NextRow() {
								rowsCount++
							}
							time.Sleep(discoveryInterval)
						})
					}
				})
				if err = res.Err(); err != nil {
					return fmt.Errorf("received error: %w (duration: %v)", err, time.Since(start))
				}
				if rowsCount != upsertRowsCount {
					return fmt.Errorf("wrong rows count: %v, expected: %d (duration: %v)", rowsCount, upsertRowsCount, time.Since(start))
				}
				return nil
			},
		); err != nil {
			t.Fatalf("stream query failed: %v\n", err)
		}
	})
}
