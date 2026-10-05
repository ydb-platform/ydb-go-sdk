//go:build integration

package witharrow

import (
	"context"
	"io"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
)

func TestExecutors(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	var calls atomic.Int32
	decoder := query.ArrowDecoder(func(ctx context.Context, cols []query.ArrowColumn, part io.Reader) ([]query.ArrowBatch, error) {
		calls.Add(1)
		return Decode(ctx, cols, part)
	})
	db, err := ydb.Open(ctx, connectionString(), ydb.WithAnonymousCredentials(), ydb.WithQueryDefaultResultFormatArrow(decoder))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close(ctx)
	verifyExecutor(t, ctx, db.Query())
	if err := db.Query().Do(ctx, func(ctx context.Context, s query.Session) error { verifyExecutor(t, ctx, s); return nil }); err != nil {
		t.Fatal(err)
	}
	if err := db.Query().DoTx(ctx, func(ctx context.Context, tx query.TxActor) error { verifyExecutor(t, ctx, tx); return nil }); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 9 {
		t.Fatalf("decode calls=%d, want 9", calls.Load())
	}
	row, err := db.Query().QueryRow(ctx, "SELECT 7 AS id", query.WithArrow(nil))
	if err != nil {
		t.Fatal(err)
	}
	var id int32
	if err := row.Scan(&id); err != nil || id != 7 {
		t.Fatalf("override: %d %v", id, err)
	}
	if calls.Load() != 9 {
		t.Fatal("nil override called decoder")
	}
	verifyExecutor(t, ctx, db.Query())
	if calls.Load() != 12 {
		t.Fatalf("decode calls=%d after override, want 12", calls.Load())
	}
}

func verifyExecutor(t *testing.T, ctx context.Context, executor query.Executor) {
	t.Helper()
	const sql = `SELECT CAST(42 AS Uint64) AS id, "owned"u AS name, CAST(NULL AS Int32?) AS score;`
	res, err := executor.Query(ctx, sql)
	if err != nil {
		t.Fatal(err)
	}
	var rows []query.Row
	for rs, err := range res.ResultSets(ctx) {
		if err != nil {
			t.Fatal(err)
		}
		for row, err := range rs.Rows(ctx) {
			if err != nil {
				t.Fatal(err)
			}
			rows = append(rows, row)
			verifyRow(t, row)
		}
	}
	if len(rows) != 1 {
		t.Fatalf("rows=%d", len(rows))
	}
	if err := res.Close(ctx); err != nil {
		t.Fatal(err)
	}
	row, err := executor.QueryRow(ctx, sql)
	if err != nil {
		t.Fatal(err)
	}
	verifyRow(t, row)
	rs, err := executor.QueryResultSet(ctx, sql)
	if err != nil {
		t.Fatal(err)
	}
	row, err = rs.NextRow(ctx)
	if err != nil {
		t.Fatal(err)
	}
	verifyRow(t, row)
	if err := rs.Close(ctx); err != nil {
		t.Fatal(err)
	}
}

func verifyRow(t *testing.T, row query.Row) {
	t.Helper()
	var id uint64
	var name string
	var score *int32
	if err := row.Scan(&id, &name, &score); err != nil {
		t.Fatal(err)
	}
	if id != 42 || name != "owned" || score != nil {
		t.Fatalf("values: %d %q %v", id, name, score)
	}
	if err := row.ScanNamed(query.Named("name", &name)); err != nil {
		t.Fatal(err)
	}
	dst := struct {
		ID    uint64 `sql:"id"`
		Name  string `sql:"name"`
		Score *int32 `sql:"score"`
	}{}
	if err := row.ScanStruct(&dst); err != nil {
		t.Fatal(err)
	}
	if dst.ID != id || dst.Name != name || dst.Score != nil {
		t.Fatal("ScanStruct differs")
	}
	if len(row.Values()) != 3 {
		t.Fatal("Values differs")
	}
}

func connectionString() string {
	if s := os.Getenv("YDB_CONNECTION_STRING"); s != "" {
		return s
	}
	return "grpc://localhost:2136/local"
}
