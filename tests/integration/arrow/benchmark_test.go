//go:build integration && (darwin || linux)

package witharrow

import (
	"context"
	"fmt"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"

	"github.com/ydb-platform/ydb-go-sdk/v3/query"
	"github.com/ydb-platform/ydb-go-sdk/v3/tests/integration/internal/querybench"
)

func BenchmarkFormats(b *testing.B) {
	ctx, db := querybench.Setup(b, connectionString())
	arrowOption := query.WithResultFormatArrow(ipc.NewReader)
	if err := db.Query().Do(ctx, func(ctx context.Context, s query.Session) error {
		for _, size := range []int{1, 10, 100, 1000, 10000} {
			sql := querybench.SQL(size)
			expected := querybench.VerifyRows(ctx, b, s, sql, size)
			for _, variant := range []string{"QueryArrow", "WithResultFormatArrow"} {
				b.Run(fmt.Sprintf("%d/%s", size, variant), func(b *testing.B) {
					querybench.Measure(b, size, expected, func() (int, uint64, error) {
						if variant == "QueryArrow" {
							return consumeArrow(ctx, s, sql)
						}

						return querybench.ConsumeRows(ctx, s, sql, []query.ExecuteOption{arrowOption})
					})
				})
			}
		}

		return nil
	}); err != nil {
		b.Fatal(err)
	}
}

func consumeArrow(ctx context.Context, s query.Session, sql string) (int, uint64, error) {
	res, err := s.QueryArrow(ctx, sql, query.WithResponsePartLimitSizeBytes(32<<10))
	if err != nil {
		return 0, 0, err
	}
	defer res.Close(ctx)
	n := 0
	hash := uint64(14695981039346656037)
	for part, err := range res.Parts(ctx) {
		if err != nil {
			return 0, 0, err
		}
		reader, err := ipc.NewReader(part)
		if err != nil {
			return 0, 0, err
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
				hash = querybench.Checksum(hash, ids.Value(i), score, active, amount, name, payload)
				n++
			}
		}
		err = reader.Err()
		reader.Release()
		if err != nil {
			return 0, 0, err
		}
	}

	return n, hash, res.Close(ctx)
}
