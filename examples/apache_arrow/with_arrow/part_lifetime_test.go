//go:build integration

package witharrow

import (
	"context"
	"fmt"
	"io"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/ydb-platform/ydb-go-sdk/v3"
	arrowinternal "github.com/ydb-platform/ydb-go-sdk/v3/internal/query/arrow"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/query/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
)

func TestArrowPartLifetime(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	db, err := ydb.Open(ctx, connectionString(), ydb.WithAnonymousCredentials())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close(ctx)
	sql := fmt.Sprintf(`SELECT id, CAST(id %% 1000 AS Int32?) AS score,
CAST(id %% 2 == 0 AS Bool?) AS active, CAST(CAST(id AS Double)/4 AS Double?) AS amount,
CAST("owned"u AS Utf8?) AS name, CAST("%s" AS String?) AS payload
FROM AS_TABLE(ListMap(ListFromRange(0, 10000), ($x) -> { RETURN AsStruct($x AS id); }))
ORDER BY id;`, strings.Repeat("x", 64))
	for _, test := range []struct {
		name         string
		materialized bool
		prefetch     int
	}{
		{name: "streaming"},
		{name: "streaming prefetch", prefetch: 4},
		{name: "materialized", materialized: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			var active, activeBytes atomic.Int64
			var parts, peak, peakBytes, largestPart, largestPartBytes, totalBytes int64
			decode := func(ctx context.Context, columns []arrowinternal.Column, part io.Reader) ([]arrowinternal.Batch, error) {
				if !test.materialized && active.Load() != 0 {
					return nil, fmt.Errorf("previous part still owns %d batches", active.Load())
				}
				var sizes []int64
				decode := arrowinternal.NewDecoder(func(part io.Reader, opts ...ipc.Option) (*lifetimeReader, error) {
					reader, err := ipc.NewReader(part, opts...)
					if err != nil {
						return nil, err
					}
					return &lifetimeReader{Reader: reader, sizes: &sizes}, nil
				})
				batches, err := decode(ctx, columns, part)
				if err != nil {
					return nil, err
				}
				parts++
				var bytesInPart int64
				for i, b := range batches {
					if b.NumCols() != 6 {
						t.Fatalf("part %d has %d columns, want 6", parts, b.NumCols())
					}
					size := sizes[i]
					bytesInPart += size
					active.Add(1)
					activeBytes.Add(size)
					batches[i] = &lifetimeBatch{Batch: b, active: &active, activeBytes: &activeBytes, size: size}
				}
				largestPart = max(largestPart, int64(len(batches)))
				largestPartBytes = max(largestPartBytes, bytesInPart)
				totalBytes += bytesInPart
				peak = max(peak, active.Load())
				peakBytes = max(peakBytes, activeBytes.Load())

				return batches, nil
			}
			read := func(executor query.Executor) error {
				res, err := executor.Query(ctx, sql, options.WithArrow(decode),
					query.WithResponsePartLimitSizeBytes(4<<10), query.WithResponsePartPrefetch(test.prefetch))
				if err != nil {
					return err
				}
				defer res.Close(ctx)
				var count int32
				var savedName *string
				var savedPayload *[]byte
				for rs, err := range res.ResultSets(ctx) {
					if err != nil {
						return err
					}
					for row, err := range rs.Rows(ctx) {
						if err != nil {
							return err
						}
						var id int32
						var score *int32
						var active *bool
						var amount *float64
						if err := row.Scan(&id, &score, &active, &amount, &savedName, &savedPayload); err != nil {
							return err
						}
						if id != count || *score != id%1000 || *active != (id%2 == 0) || *amount != float64(id)/4 {
							return fmt.Errorf("invalid numeric values at row %d", count)
						}
						if *savedName != "owned" || string(*savedPayload) != strings.Repeat("x", 64) {
							return fmt.Errorf("invalid strings at row %d", count)
						}
						count++
					}
				}
				if count != 10000 || parts <= 1 {
					return fmt.Errorf("rows=%d, parts=%d", count, parts)
				}
				if !test.materialized && (active.Load() != 0 || peak > largestPart || peakBytes > largestPartBytes) {
					return fmt.Errorf("stream retains batches outside the current part")
				}
				if test.materialized && (active.Load() == 0 || peak <= largestPart) {
					return fmt.Errorf("materialized result lost its retained batches")
				}
				if err := res.Close(ctx); err != nil {
					return err
				}
				if *savedName != "owned" || string(*savedPayload) != strings.Repeat("x", 64) {
					return fmt.Errorf("detached destinations changed after Close")
				}

				return nil
			}
			if test.materialized {
				err = read(db.Query())
			} else {
				err = db.Query().Do(ctx, func(ctx context.Context, s query.Session) error { return read(s) })
			}
			if err != nil {
				t.Fatal(err)
			}
			if active.Load() != 0 || activeBytes.Load() != 0 {
				t.Fatalf("leaked batches=%d bytes=%d", active.Load(), activeBytes.Load())
			}
			t.Logf("rows=10000 parts=%d peak_batches=%d peak_buffer_bytes=%d total_buffer_bytes=%d",
				parts, peak, peakBytes, totalBytes)
		})
	}
}

type lifetimeBatch struct {
	arrowinternal.Batch
	active      *atomic.Int64
	activeBytes *atomic.Int64
	size        int64
}

func (b *lifetimeBatch) Release() {
	b.Batch.Release()
	b.active.Add(-1)
	b.activeBytes.Add(-b.size)
}

type lifetimeReader struct {
	*ipc.Reader
	sizes *[]int64
}

func (r *lifetimeReader) Read() (arrow.RecordBatch, error) {
	record, err := r.Reader.Read()
	if err != nil {
		return nil, err
	}
	var size int64
	for _, column := range record.Columns() {
		for _, buffer := range column.Data().Buffers() {
			if buffer != nil {
				size += int64(buffer.Len())
			}
		}
	}
	*r.sizes = append(*r.sizes, size)
	return record, nil
}
