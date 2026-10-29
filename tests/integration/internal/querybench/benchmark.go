//go:build integration && (darwin || linux)

// Package querybench shares the Query and Arrow result benchmark fixture.
package querybench

import (
	"context"
	"fmt"
	"math"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/types"
)

// Setup opens the SDK and loads the common six-column fixture.
func Setup(b *testing.B, connectionString string) (context.Context, *ydb.Driver) {
	b.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	b.Cleanup(cancel)
	db, err := ydb.Open(ctx, connectionString, ydb.WithAnonymousCredentials())
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() {
		if err := db.Close(ctx); err != nil {
			b.Error(err)
		}
	})
	if err := db.Query().Exec(ctx, `CREATE TABLE query_arrow_benchmark (
  id Uint64 NOT NULL, score Int32, active Bool, amount Double, name Utf8, payload String, PRIMARY KEY(id)
);`); err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() {
		if err := db.Query().Exec(ctx, "DROP TABLE query_arrow_benchmark;"); err != nil {
			b.Error(err)
		}
	})
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

	return ctx, db
}

// SQL selects the requested number of ordered fixture rows.
func SQL(size int) string {
	return fmt.Sprintf(
		"SELECT id,score,active,amount,name,payload FROM query_arrow_benchmark WHERE id < %d ORDER BY id;", size,
	)
}

// VerifyRows checks the fixture and obtains the checksum shared by all consumers.
func VerifyRows(ctx context.Context, b *testing.B, s query.Session, sql string, size int) uint64 {
	b.Helper()
	count, expected, err := ConsumeRows(ctx, s, sql, nil)
	if err != nil {
		b.Fatal(err)
	}
	if count != size {
		b.Fatalf("fixture rows=%d, want %d", count, size)
	}
	b.Logf("rows=%d checksum=%016x", size, expected)

	return expected
}

// Measure warms up a result consumer and reports elapsed time, CPU and allocations.
func Measure(b *testing.B, size int, expected uint64, consume func() (int, uint64, error)) {
	b.Helper()
	for range 10 {
		n, h, err := consume()
		if err != nil || n != size || h != expected {
			b.Fatalf("warmup: rows=%d checksum=%016x err=%v", n, h, err)
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	startCPU := processCPU(b)
	for i := 0; i < b.N; i++ {
		n, h, err := consume()
		if err != nil || n != size || h != expected {
			b.Fatalf("rows=%d checksum=%016x err=%v", n, h, err)
		}
	}
	cpu := processCPU(b) - startCPU
	b.StopTimer()
	b.ReportMetric(float64(cpu.Nanoseconds())/float64(b.N), "cpu-ns/op")
}

// ConsumeRows scans and checksums every returned column with reusable destinations.
func ConsumeRows(ctx context.Context, s query.Session, sql string, opts []query.ExecuteOption) (int, uint64, error) {
	res, err := s.Query(ctx, sql, append(opts, query.WithResponsePartLimitSizeBytes(32<<10))...)
	if err != nil {
		return 0, 0, err
	}
	defer res.Close(ctx)
	n := 0
	hash := uint64(14695981039346656037)
	var id uint64
	var score *int32
	var active *bool
	var amount *float64
	var name *string
	var payload *[]byte
	dst := []any{&id, &score, &active, &amount, &name, &payload}
	for rs, err := range res.ResultSets(ctx) {
		if err != nil {
			return 0, 0, err
		}
		for row, err := range rs.Rows(ctx) {
			if err != nil {
				return 0, 0, err
			}
			if err := row.Scan(dst...); err != nil {
				return 0, 0, err
			}
			hash = Checksum(hash, id, score, active, amount, name, payload)
			n++
		}
	}

	return n, hash, res.Close(ctx)
}

// Checksum includes the value and null state of all six fixture columns.
func Checksum(h, id uint64, score *int32, active *bool, amount *float64, name *string, payload *[]byte) uint64 {
	mix := func(v uint64) { h = (h ^ v) * 1099511628211 }
	mix(id)
	if score == nil {
		mix(0)
	} else {
		mix(1)
		mix(uint64(*score))
	}
	if active == nil {
		mix(0)
	} else {
		mix(1)
		if *active {
			mix(1)
		} else {
			mix(0)
		}
	}
	if amount == nil {
		mix(0)
	} else {
		mix(1)
		mix(math.Float64bits(*amount))
	}
	if name == nil {
		mix(0)
	} else {
		mix(1)
		for _, c := range []byte(*name) {
			mix(uint64(c))
		}
	}
	if payload == nil {
		mix(0)
	} else {
		mix(1)
		for _, c := range *payload {
			mix(uint64(c))
		}
	}

	return h
}

func processCPU(b *testing.B) time.Duration {
	var usage syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &usage); err != nil {
		b.Fatal(err)
	}

	return time.Duration(usage.Utime.Sec+usage.Stime.Sec)*time.Second +
		time.Duration(usage.Utime.Usec+usage.Stime.Usec)*time.Microsecond
}
