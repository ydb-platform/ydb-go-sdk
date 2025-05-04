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

		t.Run("check ydb type", func(t *testing.T) {
			row := db.QueryRowContext(scope.Ctx, "SELECT FormatType(TypeOf($a))",
				ydb.ParamsBuilder().Param("$a").JSON(`{"a":1,"b":"2"}`).Build(),
			)
			var act string
			require.NoError(t, row.Scan(&act))
			require.NoError(t, row.Err())
			require.Equal(t, `Json`, act)
		})
		t.Run("get json value", func(t *testing.T) {
			row := db.QueryRowContext(scope.Ctx, "SELECT $a",
				ydb.ParamsBuilder().Param("$a").JSON(`{"a":1,"b":"2"}`).Build(),
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
}
