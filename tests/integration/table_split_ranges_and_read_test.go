//go:build integration
// +build integration

package integration

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"os"
	"path"
	"testing"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xtest"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
)

func TestSplitRangesAndRead(t *testing.T) {
	var (
		tableName       = `ranges_table`
		db              *ydb.Driver
		err             error
		upsertRowsCount = 100000
		batchSize       = 10000
	)

	ctx := xtest.Context(t)

	db, err = ydb.Open(ctx,
		os.Getenv("YDB_CONNECTION_STRING"),
		ydb.WithAccessTokenCredentials(
			os.Getenv("YDB_ACCESS_TOKEN_CREDENTIALS"),
		),
		ydb.WithDiscoveryInterval(0), // disable re-discovery on upsert time
	)
	if err != nil {
		t.Fatal(err)
	}
	defer func(db *ydb.Driver) {
		// cleanup
		_ = db.Close(ctx)
	}(db)

	t.Run("creating table", func(t *testing.T) {
		if err = db.Table().Do(ctx,
			func(ctx context.Context, s table.Session) (err error) {
				_, err = s.DescribeTable(ctx, path.Join(db.Name(), tableName))
				if err == nil {
					if err = s.DropTable(ctx, path.Join(db.Name(), tableName)); err != nil {
						return err
					}
				}
				return s.ExecuteSchemeQuery(
					ctx,
					`CREATE TABLE `+tableName+` (
						id Uint64,
						PRIMARY KEY (id)
					)
					WITH (
						UNIFORM_PARTITIONS = 8
					)`,
				)
			},
			table.WithIdempotent(),
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
		var upserted uint32
		for i := 0; i < (upsertRowsCount / batchSize); i++ {
			from, to := uint32(i*batchSize), uint32((i+1)*batchSize)
			t.Run(fmt.Sprintf("upserting %v...%v", from, to-1), func(t *testing.T) {
				values := make([]types.Value, 0, batchSize)
				for j := from; j < to; j++ {
					b := make([]byte, 4)
					binary.BigEndian.PutUint32(b, j)
					s := sha256.Sum224(b)
					values = append(
						values,
						types.StructValue(
							types.StructFieldValue("id", types.Uint64Value(binary.BigEndian.Uint64(s[:]))),
						),
					)
				}
				if err = db.Table().Do(ctx,
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
									id: Uint64,
								>>;
								UPSERT INTO `+"`"+path.Join(db.Name(), tableName)+"`"+`
								SELECT
									id 
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
					table.WithIdempotent(),
				); err != nil {
					t.Fatalf("upsert failed: %v\n", err)
				} else {
					upserted += to - from
				}
			})
		}
		t.Run("check upserted rows", func(t *testing.T) {
			if upserted != uint32(upsertRowsCount) {
				t.Fatalf("wrong rows count: %v, expected: %d", upserted, upsertRowsCount)
			}
		})
	})

	var ranges []options.KeyRange

	t.Run("make ranges", func(t *testing.T) {
		if err = db.Table().Do(ctx,
			func(ctx context.Context, s table.Session) (err error) {
				d, err := s.DescribeTable(ctx,
					path.Join(db.Name(), tableName),
					options.WithShardKeyBounds(),
				)
				if err != nil {
					return err
				}
				for _, r := range d.KeyRanges {
					if r.From == nil || r.To == nil {
						ranges = append(ranges, r)
					} else {
						var from, to uint64
						if err := types.CastTo(r.From, &from); err != nil {
							return err
						}
						if err := types.CastTo(r.To, &to); err != nil {
							return err
						}
						ranges = append(ranges,
							options.KeyRange{
								From: r.From,
								To: types.TupleValue(
									types.OptionalValue(types.Uint64Value(from + (to-from)/2)),
								),
							},
							options.KeyRange{
								From: types.TupleValue(
									types.OptionalValue(types.Uint64Value(from + (to-from)/2)),
								),
								To: r.To,
							},
						)
					}
				}
				return nil
			},
			table.WithIdempotent(),
		); err != nil {
			t.Fatalf("stream query failed: %v\n", err)
		}
	})

	t.Run("read ranges", func(t *testing.T) {
		var (
			start     = time.Now()
			rowsCount = 0
		)
		for _, r := range ranges {
			if err = db.Table().Do(ctx,
				func(ctx context.Context, s table.Session) (err error) {
					res, err := s.StreamReadTable(ctx, path.Join(db.Name(), tableName), options.ReadKeyRange(r))
					if err != nil {
						return err
					}
					defer func() {
						_ = res.Close()
					}()
					for res.NextResultSet(ctx) {
						count := 0
						for res.NextRow() {
							count++
						}
						rowsCount += count
					}
					if err = res.Err(); err != nil {
						return fmt.Errorf("received error (duration: %v): %w", time.Since(start), err)
					}
					return nil
				},
				table.WithIdempotent(),
			); err != nil {
				t.Fatalf("stream query failed: %v\n", err)
			}
		}
		if rowsCount != upsertRowsCount {
			t.Errorf("wrong rows count: %v, expected: %d (duration: %v, ranges: %v)",
				rowsCount,
				upsertRowsCount,
				time.Since(start),
				ranges,
			)
		}
	})
}
