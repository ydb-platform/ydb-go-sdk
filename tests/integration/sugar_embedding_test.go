//go:build integration
// +build integration

package integration

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/query"
	"github.com/ydb-platform/ydb-go-sdk/v3/sugar"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/types"
)

func TestSugarEmbeddingMatchesKnn(t *testing.T) {
	scope := newScope(t)
	db := scope.Driver()

	tests := []struct {
		name     string
		yqlType  string
		values   types.Value
		embedded types.Value
	}{
		{"int", "Int64", types.ListValue(types.Int64Value(-2), types.Int64Value(16777217)), sugar.Embedding(int(-2), int(16777217))},
		{"int16", "Int16", types.ListValue(types.Int16Value(-2), types.Int16Value(3)), sugar.Embedding(int16(-2), int16(3))},
		{"int32", "Int32", types.ListValue(types.Int32Value(-2), types.Int32Value(16777217)), sugar.Embedding(int32(-2), int32(16777217))},
		{"int64", "Int64", types.ListValue(types.Int64Value(-2), types.Int64Value(16777217)), sugar.Embedding(int64(-2), int64(16777217))},
		{"uint", "Uint64", types.ListValue(types.Uint64Value(2), types.Uint64Value(16777217)), sugar.Embedding(uint(2), uint(16777217))},
		{"uint16", "Uint16", types.ListValue(types.Uint16Value(2), types.Uint16Value(65535)), sugar.Embedding(uint16(2), uint16(65535))},
		{"uint32", "Uint32", types.ListValue(types.Uint32Value(2), types.Uint32Value(16777217)), sugar.Embedding(uint32(2), uint32(16777217))},
		{"uint64", "Uint64", types.ListValue(types.Uint64Value(2), types.Uint64Value(16777217)), sugar.Embedding(uint64(2), uint64(16777217))},
		{"float32", "Float", types.ListValue(types.FloatValue(-2.5), types.FloatValue(1.25)), sugar.Embedding(float32(-2.5), float32(1.25))},
		{"float64", "Double", types.ListValue(types.DoubleValue(-2.5), types.DoubleValue(1.00000001)), sugar.Embedding(float64(-2.5), float64(1.00000001))},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			row, err := db.Query().QueryRow(scope.Ctx, fmt.Sprintf(`
				DECLARE $values AS List<%s>;
				DECLARE $embedded AS String;
				SELECT $embedded = Untag(Knn::ToBinaryStringFloat(
					ListMap($values, ($value) -> (CAST($value AS Float)))
				), "FloatVector");`, test.yqlType),
				query.WithParameters(table.NewQueryParameters(
					table.ValueParam("$values", test.values),
					table.ValueParam("$embedded", test.embedded),
				)),
				query.WithIdempotent(),
			)
			require.NoError(t, err)

			var equal bool
			require.NoError(t, row.Scan(&equal))
			require.True(t, equal)
		})
	}
}
