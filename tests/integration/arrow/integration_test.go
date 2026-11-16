//go:build integration

package witharrow

import (
	"context"
	"database/sql"
	"io"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/ipc"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
)

func TestExecutors(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	var calls atomic.Int32
	newReader := func(part io.Reader, opts ...ipc.Option) (*ipc.Reader, error) {
		calls.Add(1)

		return ipc.NewReader(part, opts...)
	}
	db, err := ydb.Open(ctx, connectionString(), ydb.WithAnonymousCredentials(),
		ydb.WithQueryDefaultResultFormatArrow(newReader))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close(ctx)
	verifyExecutor(ctx, t, db.Query())
	if err := db.Query().Do(ctx, func(ctx context.Context, s query.Session) error {
		verifyExecutor(ctx, t, s)

		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.Query().DoTx(ctx, func(ctx context.Context, tx query.TxActor) error {
		verifyExecutor(ctx, t, tx)

		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 9 {
		t.Fatalf("decode calls=%d, want 9", calls.Load())
	}
	row, err := db.Query().QueryRow(ctx, "SELECT 7 AS id", query.WithYdbValue())
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
	verifyExecutor(ctx, t, db.Query())
	if calls.Load() != 12 {
		t.Fatalf("decode calls=%d after override, want 12", calls.Load())
	}
}

func TestDatabaseSQLDriverDefault(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	var calls atomic.Int32
	newReader := func(part io.Reader, opts ...ipc.Option) (*ipc.Reader, error) {
		calls.Add(1)

		return ipc.NewReader(part, opts...)
	}
	db, err := ydb.Open(ctx, connectionString(), ydb.WithAnonymousCredentials(),
		ydb.WithQueryDefaultResultFormatArrow(newReader))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close(ctx)
	connector, err := ydb.Connector(db, ydb.WithQueryService(true))
	if err != nil {
		t.Fatal(err)
	}
	defer connector.Close()
	sqlDB := sql.OpenDB(connector)
	defer sqlDB.Close()
	const statement = `SELECT CAST(42 AS Uint64) AS id, "owned"u AS name,
CAST(NULL AS Int32?) AS score, "bytes" AS payload;`
	rows, err := sqlDB.QueryContext(ctx, statement)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	if !rows.Next() {
		t.Fatalf("expected row: %v", rows.Err())
	}
	var id uint64
	var name string
	var score *int32
	var payload []byte
	if err := rows.Scan(&id, &name, &score, &payload); err != nil {
		t.Fatal(err)
	}
	if rows.Next() || rows.Err() != nil {
		t.Fatalf("unexpected next row: %v", rows.Err())
	}
	if id != 42 || name != "owned" || score != nil || string(payload) != "bytes" {
		t.Fatalf("values after EOF: %d %q %v %q", id, name, score, payload)
	}
	if calls.Load() != 1 {
		t.Fatalf("decode calls=%d, want 1", calls.Load())
	}
	if _, err := sqlDB.ExecContext(ctx, statement); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 1 {
		t.Fatal("ExecContext called decoder")
	}
}

func TestDatabaseSQLLiteralNull(t *testing.T) {
	for _, format := range []string{"YdbValue", "Arrow"} {
		t.Run(format, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
			defer cancel()
			var calls atomic.Int32
			newReader := func(part io.Reader, opts ...ipc.Option) (*ipc.Reader, error) {
				calls.Add(1)

				return ipc.NewReader(part, opts...)
			}
			opts := []ydb.Option{ydb.WithAnonymousCredentials()}
			if format == "Arrow" {
				opts = append(opts, ydb.WithQueryDefaultResultFormatArrow(newReader))
			}
			db, err := ydb.Open(ctx, connectionString(), opts...)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close(ctx)
			connector, err := ydb.Connector(db, ydb.WithQueryService(true))
			if err != nil {
				t.Fatal(err)
			}
			defer connector.Close()
			sqlDB := sql.OpenDB(connector)
			defer sqlDB.Close()
			text := sql.NullString{String: "previous", Valid: true}
			integer := sql.NullInt64{Int64: 42, Valid: true}
			var generic any = "previous"
			if err := sqlDB.QueryRowContext(ctx, "SELECT NULL AS text_null, NULL AS int_null, NULL AS value_null;").
				Scan(&text, &integer, &generic); err != nil {
				t.Fatal(err)
			}
			if text.Valid || text.String != "" || integer.Valid || integer.Int64 != 0 || generic != nil {
				t.Fatalf("literal NULL: text=%+v integer=%+v generic=%#v", text, integer, generic)
			}
			wantCalls := int32(0)
			if format == "Arrow" {
				wantCalls = 1
			}
			if calls.Load() != wantCalls {
				t.Fatalf("decode calls=%d, want %d", calls.Load(), wantCalls)
			}
		})
	}
}

func verifyExecutor(ctx context.Context, t *testing.T, executor query.Executor) {
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
