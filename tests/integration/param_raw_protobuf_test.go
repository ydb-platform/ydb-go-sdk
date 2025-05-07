//go:build integration
// +build integration

package integration

import (
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3"
)

func TestRawProtobuf(t *testing.T) {
	raw := &Ydb.TypedValue{
		Type: &Ydb.Type{
			Type: &Ydb.Type_TypeId{
				TypeId: Ydb.Type_UINT64,
			},
		},
		Value: &Ydb.Value{
			Value: &Ydb.Value_Uint64Value{
				Uint64Value: 123,
			},
		},
	}

	t.Run("query", func(t *testing.T) {
		var (
			scope = newScope(t)
			db    = scope.Driver()
		)

		row, err := db.Query().QueryRow(scope.Ctx, `
			DECLARE $raw AS Uint64;
			SELECT $raw;`,
			query.WithParameters(
				ydb.ParamsBuilder().Param("$raw").FromProtobuf(raw).Build(),
			),
		)
		require.NoError(t, err)

		t.Run("*Ydb.TypedValue", func(t *testing.T) {
			var act *Ydb.TypedValue
			require.NoError(t, row.Scan(&act))
			require.Equal(t, raw.String(), act.String())
		})
		t.Run("**Ydb.TypedValue", func(t *testing.T) {
			var act *Ydb.TypedValue
			require.NoError(t, row.Scan(&act))
			require.Equal(t, raw.String(), act.String())
		})
		t.Run("*Ydb.Value", func(t *testing.T) {
			var act *Ydb.Value
			require.NoError(t, row.Scan(&act))
			require.Equal(t, raw.Value.String(), act.String())
		})
		t.Run("**Ydb.Value", func(t *testing.T) {
			var act *Ydb.Value
			require.NoError(t, row.Scan(&act))
			require.Equal(t, raw.Value.String(), act.String())
		})
	})
	t.Run("database/sql", func(t *testing.T) {
		var (
			scope = newScope(t)
			db    = scope.SQLDriver(
				ydb.WithAutoDeclare(),
				ydb.WithPositionalArgs(),
			)
		)

		row := db.QueryRowContext(scope.Ctx, `SELECT ?`, raw)
		require.NoError(t, row.Err())

		t.Run("*Ydb.TypedValue", func(t *testing.T) {
			var act *Ydb.TypedValue
			require.NoError(t, row.Scan(&act))
			require.Equal(t, raw.String(), act.String())
		})
		t.Run("**Ydb.TypedValue", func(t *testing.T) {
			var act *Ydb.TypedValue
			require.NoError(t, row.Scan(&act))
			require.Equal(t, raw.String(), act.String())
		})
		t.Run("*Ydb.Value", func(t *testing.T) {
			var act *Ydb.Value
			require.NoError(t, row.Scan(&act))
			require.Equal(t, raw.Value.String(), act.String())
		})
		t.Run("**Ydb.Value", func(t *testing.T) {
			var act *Ydb.Value
			require.NoError(t, row.Scan(&act))
			require.Equal(t, raw.Value.String(), act.String())
		})
	})
}
