//go:build integration
// +build integration

package integration

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"google.golang.org/protobuf/proto"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
)

func TestRawProtobuf(t *testing.T) {
	raw := &Ydb.TypedValue{
		Type:  &Ydb.Type{Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_UINT64}},
		Value: &Ydb.Value{Value: &Ydb.Value_Uint64Value{Uint64Value: 123}},
	}

	t.Run("query", func(t *testing.T) {
		scope := newScope(t)
		db := scope.Driver()
		for _, tt := range []struct {
			name string
			scan func(t *testing.T, row query.Row)
		}{
			{
				name: "TypedValue",
				scan: func(t *testing.T, row query.Row) {
					var act Ydb.TypedValue
					require.NoError(t, row.Scan(&act))
					require.True(t, proto.Equal(raw, &act))
				},
			},
			{
				name: "TypedValuePointer",
				scan: func(t *testing.T, row query.Row) {
					var act *Ydb.TypedValue
					require.NoError(t, row.Scan(&act))
					require.True(t, proto.Equal(raw, act))
				},
			},
			{
				name: "Value",
				scan: func(t *testing.T, row query.Row) {
					var act Ydb.Value
					require.NoError(t, row.Scan(&act))
					require.True(t, proto.Equal(raw.GetValue(), &act))
				},
			},
			{
				name: "ValuePointer",
				scan: func(t *testing.T, row query.Row) {
					var act *Ydb.Value
					require.NoError(t, row.Scan(&act))
					require.True(t, proto.Equal(raw.GetValue(), act))
				},
			},
		} {
			t.Run(tt.name, func(t *testing.T) {
				row, err := db.Query().QueryRow(scope.Ctx, "SELECT $raw;",
					query.WithParameters(ydb.ParamsBuilder().Param("$raw").FromProtobuf(raw).Build()),
				)
				require.NoError(t, err)
				tt.scan(t, row)
			})
		}
	})
	t.Run("database/sql", func(t *testing.T) {
		scope := newScope(t)
		db := scope.SQLDriver(ydb.WithAutoDeclare(), ydb.WithPositionalArgs())
		for _, tt := range []struct {
			name string
			arg  any
		}{
			{name: "TypedValue", arg: Ydb.TypedValue{Type: raw.Type, Value: raw.Value}},
			{name: "TypedValuePointer", arg: raw},
		} {
			t.Run(tt.name, func(t *testing.T) {
				var act uint64
				require.NoError(t, db.QueryRowContext(scope.Ctx, "SELECT ?", tt.arg).Scan(&act))
				require.Equal(t, uint64(123), act)
			})
		}
	})
}
