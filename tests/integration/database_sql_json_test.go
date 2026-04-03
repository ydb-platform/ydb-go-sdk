//go:build integration
// +build integration

package integration

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
)

var _ json.Marshaler = (*testJson)(nil)

type testJson struct {
	a int64  `json:"a"`
	b string `json:"b"`
}

func (t testJson) MarshalJSON() ([]byte, error) {
	return []byte(fmt.Sprintf(`{"a":%d,"b":"%s"}`, t.a, t.b)), nil
}

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

func TestDatabaseSqlJson(t *testing.T) {
	t.Run("table/types", func(t *testing.T) {
		var (
			scope = newScope(t)
			db    = scope.SQLDriver(
				ydb.WithQueryService(true),
				ydb.WithAutoDeclare(),
				ydb.WithNumericArgs(),
			)
		)

		t.Run("named param", func(t *testing.T) {
			t.Run("check ydb type", func(t *testing.T) {
				row := db.QueryRowContext(scope.Ctx, "SELECT FormatType(TypeOf($a))",
					table.ValueParam("$a", types.JSONDocumentValue(`{"a":1,"b":"2"}`)),
				)
				var act string
				require.NoError(t, row.Scan(&act))
				require.NoError(t, row.Err())
				require.Equal(t, `JsonDocument`, act)
			})
			t.Run("get json value", func(t *testing.T) {
				row := db.QueryRowContext(scope.Ctx, "SELECT $a",
					table.ValueParam("$a", types.JSONDocumentValue(`{"a":1,"b":"2"}`)),
				)
				var act string
				require.NoError(t, row.Scan(&act))
				require.NoError(t, row.Err())
				require.Equal(t, `{"a":1,"b":"2"}`, act)
			})
		})
	})

	t.Run("ydb.ParamsBuilder()", func(t *testing.T) {
		var (
			scope = newScope(t)
			db    = scope.SQLDriver(
				ydb.WithQueryService(true),
				ydb.WithAutoDeclare(),
				ydb.WithNumericArgs(),
			)
		)

		t.Run("check ydb type for json string", func(t *testing.T) {
			row := db.QueryRowContext(scope.Ctx, "SELECT FormatType(TypeOf($a))",
				ydb.ParamsBuilder().Param("$a").JSON(`{"a":1,"b":"2"}`).Build(),
			)
			var act string
			require.NoError(t, row.Scan(&act))
			require.NoError(t, row.Err())
			require.Equal(t, `Json`, act)
		})
		t.Run("get json value from json string", func(t *testing.T) {
			row := db.QueryRowContext(scope.Ctx, "SELECT $a",
				ydb.ParamsBuilder().Param("$a").JSON(`{"a":1,"b":"2"}`).Build(),
			)
			var act string
			require.NoError(t, row.Scan(&act))
			require.NoError(t, row.Err())
			require.Equal(t, `{"a":1,"b":"2"}`, act)
		})
		t.Run("check ydb type for json bytes", func(t *testing.T) {
			row := db.QueryRowContext(scope.Ctx, "SELECT FormatType(TypeOf($a))",
				ydb.ParamsBuilder().Param("$a").JSONFromBytes([]byte(`{"a":1,"b":"2"}`)).Build(),
			)
			var act string
			require.NoError(t, row.Scan(&act))
			require.NoError(t, row.Err())
			require.Equal(t, `Json`, act)
		})
		t.Run("get json value from json bytes", func(t *testing.T) {
			row := db.QueryRowContext(scope.Ctx, "SELECT $a",
				ydb.ParamsBuilder().Param("$a").JSONFromBytes([]byte(`{"a":1,"b":"2"}`)).Build(),
			)
			var act string
			require.NoError(t, row.Scan(&act))
			require.NoError(t, row.Err())
			require.Equal(t, `{"a":1,"b":"2"}`, act)
		})
	})

	t.Run("sql.NamedArg", func(t *testing.T) {
		var (
			scope = newScope(t)
			db    = scope.SQLDriver(
				ydb.WithQueryService(true),
				ydb.WithAutoDeclare(),
			)
		)

		t.Run("table/types", func(t *testing.T) {
			t.Run("check ydb type", func(t *testing.T) {
				row := db.QueryRowContext(scope.Ctx, "SELECT FormatType(TypeOf($a))",
					sql.Named("a", types.JSONDocumentValue(`{"a":1,"b":"2"}`)),
				)
				var act string
				require.NoError(t, row.Scan(&act))
				require.NoError(t, row.Err())
				require.Equal(t, `JsonDocument`, act)
			})
			t.Run("get json value", func(t *testing.T) {
				row := db.QueryRowContext(scope.Ctx, "SELECT $a",
					sql.Named("a", types.JSONDocumentValue(`{"a":1,"b":"2"}`)),
				)
				var act string
				require.NoError(t, row.Scan(&act))
				require.NoError(t, row.Err())
				require.Equal(t, `{"a":1,"b":"2"}`, act)
			})
		})
		t.Run("json.Marshaler", func(t *testing.T) {
			t.Run("check ydb type", func(t *testing.T) {
				row := db.QueryRowContext(scope.Ctx, "SELECT FormatType(TypeOf($a))",
					sql.Named("a", testJson{a: 1, b: "2"}),
				)
				var act string
				require.NoError(t, row.Scan(&act))
				require.NoError(t, row.Err())
				require.Equal(t, `Json`, act)
			})
			t.Run("struct param", func(t *testing.T) {
				row := db.QueryRowContext(scope.Ctx, "SELECT $a",
					sql.Named("a", testJson{a: 1, b: "2"}),
				)
				var act string
				require.NoError(t, row.Scan(&act))
				require.NoError(t, row.Err())
				require.Equal(t, `{"a":1,"b":"2"}`, act)
			})
			t.Run("pointer to struct param", func(t *testing.T) {
				row := db.QueryRowContext(scope.Ctx, "SELECT $a",
					sql.Named("a", &testJson{a: 1, b: "2"}),
				)
				var act string
				require.NoError(t, row.Scan(&act))
				require.NoError(t, row.Err())
				require.Equal(t, `{"a":1,"b":"2"}`, act)
			})
		})
	})

	t.Run("unnamed param", func(t *testing.T) {
		t.Run("numeric args", func(t *testing.T) {
			var (
				scope = newScope(t)
				db    = scope.SQLDriver(
					ydb.WithQueryService(true),
					ydb.WithAutoDeclare(),
					ydb.WithNumericArgs(),
				)
			)

			t.Run("table/types", func(t *testing.T) {
				t.Run("check ydb type", func(t *testing.T) {
					row := db.QueryRowContext(scope.Ctx, "SELECT FormatType(TypeOf($1))",
						types.JSONDocumentValue(`{"a":1,"b":"2"}`),
					)
					var act string
					require.NoError(t, row.Scan(&act))
					require.NoError(t, row.Err())
					require.Equal(t, `JsonDocument`, act)
				})
				t.Run("get json value", func(t *testing.T) {
					row := db.QueryRowContext(scope.Ctx, "SELECT $1",
						types.JSONDocumentValue(`{"a":1,"b":"2"}`),
					)
					var act string
					require.NoError(t, row.Scan(&act))
					require.NoError(t, row.Err())
					require.Equal(t, `{"a":1,"b":"2"}`, act)
				})
			})
			t.Run("json.Marshaler", func(t *testing.T) {
				t.Run("check ydb type", func(t *testing.T) {
					row := db.QueryRowContext(scope.Ctx, "SELECT FormatType(TypeOf($1))", testJson{a: 1, b: "2"})
					var act string
					require.NoError(t, row.Scan(&act))
					require.NoError(t, row.Err())
					require.Equal(t, `Json`, act)
				})
				t.Run("struct param", func(t *testing.T) {
					row := db.QueryRowContext(scope.Ctx, "SELECT $1", testJson{a: 1, b: "2"})
					var act string
					require.NoError(t, row.Scan(&act))
					require.NoError(t, row.Err())
					require.Equal(t, `{"a":1,"b":"2"}`, act)
				})
				t.Run("pointer to struct param", func(t *testing.T) {
					row := db.QueryRowContext(scope.Ctx, "SELECT $1", &testJson{a: 1, b: "2"})
					var act string
					require.NoError(t, row.Scan(&act))
					require.NoError(t, row.Err())
					require.Equal(t, `{"a":1,"b":"2"}`, act)
				})
			})
		})
		t.Run("positional args", func(t *testing.T) {
			var (
				scope = newScope(t)
				db    = scope.SQLDriver(
					ydb.WithQueryService(true),
					ydb.WithAutoDeclare(),
					ydb.WithPositionalArgs(),
				)
			)

			t.Run("table/types", func(t *testing.T) {
				t.Run("check ydb type", func(t *testing.T) {
					row := db.QueryRowContext(scope.Ctx, "SELECT FormatType(TypeOf(?))",
						types.JSONDocumentValue(`{"a":1,"b":"2"}`),
					)
					var act string
					require.NoError(t, row.Scan(&act))
					require.NoError(t, row.Err())
					require.Equal(t, `JsonDocument`, act)
				})
				t.Run("get json value", func(t *testing.T) {
					row := db.QueryRowContext(scope.Ctx, "SELECT ?",
						types.JSONDocumentValue(`{"a":1,"b":"2"}`),
					)
					var act string
					require.NoError(t, row.Scan(&act))
					require.NoError(t, row.Err())
					require.Equal(t, `{"a":1,"b":"2"}`, act)
				})
			})

			t.Run("json.Marshaler", func(t *testing.T) {
				t.Run("check ydb type", func(t *testing.T) {
					row := db.QueryRowContext(scope.Ctx, "SELECT FormatType(TypeOf(?))", testJson{a: 1, b: "2"})
					var act string
					require.NoError(t, row.Scan(&act))
					require.NoError(t, row.Err())
					require.Equal(t, `Json`, act)
				})
				t.Run("struct param", func(t *testing.T) {
					row := db.QueryRowContext(scope.Ctx, "SELECT ?", testJson{a: 1, b: "2"})
					var act string
					require.NoError(t, row.Scan(&act))
					require.NoError(t, row.Err())
					require.Equal(t, `{"a":1,"b":"2"}`, act)
				})
				t.Run("pointer to struct param", func(t *testing.T) {
					row := db.QueryRowContext(scope.Ctx, "SELECT ?", &testJson{a: 1, b: "2"})
					var act string
					require.NoError(t, row.Scan(&act))
					require.NoError(t, row.Err())
					require.Equal(t, `{"a":1,"b":"2"}`, act)
				})
			})
		})
	})

	t.Run("json.Marshaler", func(t *testing.T) {
		// Tests for a type that implements json.Marshaler with a private field
		// (no embedded json.RawMessage, so driver.Valuer is not inherited).
		// The type also implements sql.Scanner to read back Json columns.
		var (
			scope = newScope(t)
			db    = scope.SQLDriver(
				ydb.WithQueryService(true),
				ydb.WithAutoDeclare(),
			)
		)

		const rawJSON = `{"key":"value","num":42}`

		t.Run("value param", func(t *testing.T) {
			t.Run("check ydb type", func(t *testing.T) {
				// toType must return types.JSON for a json.Marshaler, not fall
				// through to the reflect.Struct path which panics on unexported fields.
				row := db.QueryRowContext(scope.Ctx,
					`SELECT FormatType(TypeOf($data))`,
					sql.Named("data", jsonRawScanner{raw: json.RawMessage(rawJSON)}),
				)
				var act string
				require.NoError(t, row.Scan(&act))
				require.NoError(t, row.Err())
				require.Equal(t, `Json`, act)
			})
			t.Run("get json value", func(t *testing.T) {
				// toValue routes a json.Marshaler value → JSONValue; AS_TABLE
				// round-trips it through YDB without a persistent table.
				row := db.QueryRowContext(scope.Ctx,
					`SELECT data FROM AS_TABLE(AsList(AsStruct($data AS data)))`,
					sql.Named("data", jsonRawScanner{raw: json.RawMessage(rawJSON)}),
				)
				var dst jsonRawScanner
				require.NoError(t, row.Scan(&dst))
				require.NoError(t, row.Err())
				require.JSONEq(t, rawJSON, string(dst.raw))
			})
		})

		t.Run("pointer param", func(t *testing.T) {
			t.Run("check ydb type", func(t *testing.T) {
				// A non-nil *json.Marshaler must produce Optional<Json>.
				row := db.QueryRowContext(scope.Ctx,
					`SELECT FormatType(TypeOf($data))`,
					sql.Named("data", &jsonRawScanner{raw: json.RawMessage(rawJSON)}),
				)
				var act string
				require.NoError(t, row.Scan(&act))
				require.NoError(t, row.Err())
				require.Equal(t, `Optional<Json>`, act)
			})
			t.Run("get json value", func(t *testing.T) {
				// AS_TABLE round-trips Optional<Json> through YDB without a persistent table.
				row := db.QueryRowContext(scope.Ctx,
					`SELECT data FROM AS_TABLE(AsList(AsStruct($data AS data)))`,
					sql.Named("data", &jsonRawScanner{raw: json.RawMessage(rawJSON)}),
				)
				var dst jsonRawScanner
				require.NoError(t, row.Scan(&dst))
				require.NoError(t, row.Err())
				require.JSONEq(t, rawJSON, string(dst.raw))
			})
		})

		t.Run("nil pointer param", func(t *testing.T) {
			t.Run("check ydb type", func(t *testing.T) {
				// A nil *json.Marshaler must bind as Null<Json>, not panic.
				var nilPtr *jsonRawScanner
				row := db.QueryRowContext(scope.Ctx,
					`SELECT FormatType(TypeOf($data))`,
					sql.Named("data", nilPtr),
				)
				var act string
				require.NoError(t, row.Scan(&act))
				require.NoError(t, row.Err())
				require.Equal(t, `Optional<Json>`, act)
			})
			t.Run("get json value", func(t *testing.T) {
				var nilPtr *jsonRawScanner
				row := db.QueryRowContext(scope.Ctx,
					`SELECT data FROM AS_TABLE(AsList(AsStruct($data AS data)))`,
					sql.Named("data", nilPtr),
				)
				var dst jsonRawScanner
				require.NoError(t, row.Scan(&dst))
				require.NoError(t, row.Err())
				require.Nil(t, dst.raw)
			})
		})
	})
}
