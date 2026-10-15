//go:build integration && (darwin || linux)

package witharrow

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/types"
)

func BenchmarkFormats(b *testing.B) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	db, err := ydb.Open(ctx, connectionString(), ydb.WithAnonymousCredentials())
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close(ctx)
	if err := db.Query().Exec(ctx, `CREATE TABLE query_arrow_benchmark (
  id Uint64 NOT NULL, score Int32, active Bool, amount Double, name Utf8, payload String, PRIMARY KEY(id)
);`); err != nil {
		b.Fatal(err)
	}
	defer func() {
		if err := db.Query().Exec(ctx, "DROP TABLE query_arrow_benchmark;"); err != nil {
			b.Error(err)
		}
	}()
	fixture := make([]types.Value, 10000)
	for i := range fixture {
		score := types.OptionalValue(types.Int32Value(int32(i % 1000)))
		name := types.OptionalValue(types.TextValue(fmt.Sprintf("customer-%05d", i)))
		if i%10 == 0 {
			score = types.NullValue(types.TypeInt32)
			name = types.NullValue(types.TypeText)
		}
		fixture[i] = types.StructValue(
			types.StructFieldValue("id", types.Uint64Value(uint64(i))),
			types.StructFieldValue("score", score),
			types.StructFieldValue("active", types.OptionalValue(types.BoolValue(i%2 == 0))),
			types.StructFieldValue("amount", types.OptionalValue(types.DoubleValue(float64(i)/4))),
			types.StructFieldValue("name", name),
			types.StructFieldValue("payload", types.OptionalValue(types.BytesValue([]byte(strings.Repeat("x", 64))))),
		)
	}
	for i := 0; i < len(fixture); i += 1000 {
		if err := db.Table().BulkUpsert(ctx, "/local/query_arrow_benchmark",
			table.BulkUpsertDataRows(types.ListValue(fixture[i:i+1000]...))); err != nil {
			b.Fatal(err)
		}
	}
	arrowOption := query.WithResultFormatArrow(ipc.NewReader)
	err = db.Query().Do(ctx, func(ctx context.Context, s query.Session) error {
		for _, size := range []int{1, 10, 100, 1000, 10000} {
			sql := fmt.Sprintf(
				"SELECT id,score,active,amount,name,payload FROM query_arrow_benchmark WHERE id < %d ORDER BY id;", size,
			)
			for _, variant := range []string{"Value", "QueryArrow", "WithResultFormatArrow"} {
				b.Run(fmt.Sprintf("%d/%s", size, variant), func(b *testing.B) {
					opts := []query.ExecuteOption{query.WithResponsePartLimitSizeBytes(32 << 10)}
					if variant == "WithResultFormatArrow" {
						opts = append(opts, arrowOption)
					}
					run := func() error { return consumeRows(ctx, s, sql, opts) }
					if variant == "QueryArrow" {
						run = func() error { return consumeArrow(ctx, s, sql, opts) }
					}
					for range 10 {
						if err := run(); err != nil {
							b.Fatal(err)
						}
					}
					b.ReportAllocs()
					for b.Loop() {
						if err := run(); err != nil {
							b.Fatal(err)
						}
					}
				})
			}
		}

		return nil
	})
	if err != nil {
		b.Fatal(err)
	}
}

func consumeRows(ctx context.Context, s query.Session, sql string, opts []query.ExecuteOption) error {
	res, err := s.Query(ctx, sql, opts...)
	if err != nil {
		return err
	}
	defer res.Close(ctx)
	var id uint64
	var score *int32
	var active *bool
	var amount *float64
	var name *string
	var payload *[]byte
	dst := []any{&id, &score, &active, &amount, &name, &payload}
	for rs, err := range res.ResultSets(ctx) {
		if err != nil {
			return err
		}
		for row, err := range rs.Rows(ctx) {
			if err != nil {
				return err
			}
			if err := row.Scan(dst...); err != nil {
				return err
			}
			consumeValues(id, score, active, amount, name, payload)
		}
	}

	return res.Close(ctx)
}

func consumeArrow(ctx context.Context, s query.Session, sql string, opts []query.ExecuteOption) error {
	res, err := s.QueryArrow(ctx, sql, opts...)
	if err != nil {
		return err
	}
	defer res.Close(ctx)
	for part, err := range res.Parts(ctx) {
		if err != nil {
			return err
		}
		reader, err := ipc.NewReader(part)
		if err != nil {
			return err
		}
		for reader.Next() {
			batch := reader.RecordBatch()
			ids := batch.Column(0).(*array.Uint64)
			scores := batch.Column(1).(*array.Int32)
			actives := batch.Column(2).(*array.Uint8)
			amounts := batch.Column(3).(*array.Float64)
			names := batch.Column(4).(*array.String)
			payloads := batch.Column(5).(*array.Binary)
			for i := 0; i < int(batch.NumRows()); i++ {
				var score *int32
				var active *bool
				var amount *float64
				var name *string
				var payload *[]byte
				if !scores.IsNull(i) {
					v := scores.Value(i)
					score = &v
				}
				if !actives.IsNull(i) {
					v := actives.Value(i) != 0
					active = &v
				}
				if !amounts.IsNull(i) {
					v := amounts.Value(i)
					amount = &v
				}
				if !names.IsNull(i) {
					v := names.Value(i)
					name = &v
				}
				if !payloads.IsNull(i) {
					v := payloads.Value(i)
					payload = &v
				}
				consumeValues(ids.Value(i), score, active, amount, name, payload)
			}
		}
		err = reader.Err()
		reader.Release()
		if err != nil {
			return err
		}
	}

	return res.Close(ctx)
}

type benchmarkRow struct {
	id      uint64
	score   int32
	active  bool
	amount  float64
	name    string
	payload []byte
}

var benchmarkSink benchmarkRow

func consumeValues(id uint64, score *int32, active *bool, amount *float64, name *string, payload *[]byte) {
	benchmarkSink = benchmarkRow{id: id}
	if score != nil {
		benchmarkSink.score = *score
	}
	if active != nil {
		benchmarkSink.active = *active
	}
	if amount != nil {
		benchmarkSink.amount = *amount
	}
	if name != nil {
		benchmarkSink.name = *name
	}
	if payload != nil {
		benchmarkSink.payload = *payload
	}
}
