//go:build integration
// +build integration

package integration

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/retry"
)

// jsonRawScanner wraps a private json.RawMessage field and implements
// json.Marshaler (for binding as a query parameter) and sql.Scanner
// (for scanning result columns). It deliberately does NOT embed
// json.RawMessage so it does not inherit driver.Valuer, which would
// cause the driver to unwrap the value to []byte before the
// json.Marshaler check in toValue.
type jsonRawScanner struct {
	raw json.RawMessage
}

var (
	_ json.Marshaler   = jsonRawScanner{}
	_ json.Unmarshaler = (*jsonRawScanner)(nil)
	_ sql.Scanner      = (*jsonRawScanner)(nil)
)

func (j jsonRawScanner) MarshalJSON() ([]byte, error) {
	return j.raw, nil
}

func (j *jsonRawScanner) UnmarshalJSON(data []byte) error {
	j.raw = data

	return nil
}

func (j *jsonRawScanner) Scan(src any) error {
	switch v := src.(type) {
	case nil:
		j.raw = nil
	case []byte:
		j.raw = v
	case string:
		j.raw = []byte(v)
	default:
		return fmt.Errorf("cannot scan %T into jsonRawScanner", src)
	}

	return nil
}

// Bytes returns the underlying raw JSON bytes.
func (j jsonRawScanner) Bytes() []byte {
	return j.raw
}

// TestDatabaseSqlJSONScanner verifies end-to-end insert and select of a
// json.Marshaler / sql.Scanner type via database/sql with autodeclare.
func TestDatabaseSqlJSONScanner(t *testing.T) {
	var (
		scope = newScope(t)
		// ddlDB is used only for scheme (DDL) queries. WithQueryService is not
		// used here because the query service handles SchemeQueryMode differently.
		ddlDB = scope.SQLDriverWithFolder()
		// db is used for all DML / SELECT queries with autodeclare.
		db = scope.SQLDriverWithFolder(
			ydb.WithQueryService(true),
			ydb.WithAutoDeclare(),
		)
	)

	// Create the test table.
	require.NoError(t, retry.Do(scope.Ctx, ddlDB, func(ctx context.Context, cc *sql.Conn) error {
		_, err := cc.ExecContext(
			ydb.WithQueryMode(ctx, ydb.SchemeQueryMode),
			`CREATE TABLE IF NOT EXISTS json_scanner_test (
				id   Uint64,
				data Json,
				PRIMARY KEY (id)
			)`,
		)

		return err
	}, retry.WithIdempotent(true)))

	t.Cleanup(func() {
		_ = retry.Do(scope.Ctx, ddlDB, func(ctx context.Context, cc *sql.Conn) error {
			_, err := cc.ExecContext(
				ydb.WithQueryMode(ctx, ydb.SchemeQueryMode),
				`DROP TABLE IF EXISTS json_scanner_test`,
			)

			return err
		}, retry.WithIdempotent(true))
	})

	const rawJSON = `{"key":"value","num":42}`

	t.Run("insert value param", func(t *testing.T) {
		// Insert using a jsonRawScanner value as a named parameter.
		// toValue must route it through case json.Marshaler → JSONValue.
		src := jsonRawScanner{raw: json.RawMessage(rawJSON)}
		require.NoError(t, retry.DoTx(scope.Ctx, db, func(ctx context.Context, tx *sql.Tx) error {
			_, err := tx.ExecContext(ctx,
				`UPSERT INTO json_scanner_test (id, data) VALUES ($id, $data)`,
				sql.Named("id", uint64(1)),
				sql.Named("data", src),
			)

			return err
		}, retry.WithIdempotent(true)))
	})

	t.Run("insert pointer param", func(t *testing.T) {
		// Insert using a pointer to jsonRawScanner — must produce Optional<Json>.
		src := &jsonRawScanner{raw: json.RawMessage(rawJSON)}
		require.NoError(t, retry.DoTx(scope.Ctx, db, func(ctx context.Context, tx *sql.Tx) error {
			_, err := tx.ExecContext(ctx,
				`UPSERT INTO json_scanner_test (id, data) VALUES ($id, $data)`,
				sql.Named("id", uint64(2)),
				sql.Named("data", src),
			)

			return err
		}, retry.WithIdempotent(true)))
	})

	t.Run("select into sql.Scanner", func(t *testing.T) {
		// Read back both rows and scan the JSON column into a jsonRawScanner
		// via its sql.Scanner implementation.
		rows, err := db.QueryContext(scope.Ctx,
			`SELECT id, data FROM json_scanner_test WHERE id = $id1 OR id = $id2 ORDER BY id`,
			sql.Named("id1", uint64(1)),
			sql.Named("id2", uint64(2)),
		)
		require.NoError(t, err)
		defer func() { _ = rows.Close() }()

		type row struct {
			id   uint64
			data jsonRawScanner
		}
		var got []row
		for rows.Next() {
			var r row
			require.NoError(t, rows.Scan(&r.id, &r.data))
			got = append(got, r)
		}
		require.NoError(t, rows.Err())
		require.Len(t, got, 2)

		// Both rows must contain valid JSON equal to what was inserted.
		for _, r := range got {
			require.True(t, json.Valid(r.data.raw),
				"row %d: scanned value is not valid JSON: %q", r.id, r.data.raw)
			require.JSONEq(t, rawJSON, string(r.data.raw),
				"row %d: unexpected JSON content", r.id)
		}
	})

	t.Run("check ydb type is Json", func(t *testing.T) {
		// Confirm the driver reports the column as Json, not Bytes.
		src := jsonRawScanner{raw: json.RawMessage(rawJSON)}
		row := db.QueryRowContext(scope.Ctx,
			`SELECT FormatType(TypeOf($data))`,
			sql.Named("data", src),
		)
		var typeName string
		require.NoError(t, row.Scan(&typeName))
		require.NoError(t, row.Err())
		require.Equal(t, "Json", typeName)
	})

	t.Run("nil pointer produces NullValue", func(t *testing.T) {
		// A nil *jsonRawScanner must bind as Null<Json>, not panic.
		var nilPtr *jsonRawScanner
		require.NoError(t, retry.DoTx(scope.Ctx, db, func(ctx context.Context, tx *sql.Tx) error {
			_, err := tx.ExecContext(ctx,
				`UPSERT INTO json_scanner_test (id, data) VALUES ($id, $data)`,
				sql.Named("id", uint64(3)),
				sql.Named("data", nilPtr),
			)

			return err
		}, retry.WithIdempotent(true)))

		// Read back and confirm the column is NULL.
		row := db.QueryRowContext(scope.Ctx,
			`SELECT data FROM json_scanner_test WHERE id = $id`,
			sql.Named("id", uint64(3)),
		)
		var dst jsonRawScanner
		require.NoError(t, row.Scan(&dst))
		require.Nil(t, dst.raw, "expected nil JSON for row inserted with nil pointer")
	})
}
