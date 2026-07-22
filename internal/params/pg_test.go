package params

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"google.golang.org/protobuf/proto"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/pg"
	xtest "github.com/ydb-platform/ydb-go-sdk/v3/pkg/xtest"
)

func TestPg(t *testing.T) {
	type expected struct {
		Type  *Ydb.Type
		Value *Ydb.Value
	}

	tests := []struct {
		method string
		args   []any

		expected expected
	}{
		{
			method: "Unknown",
			args:   []any{"123"},

			expected: expected{
				Type: Ydb.Type_builder{
					PgType: Ydb.PgType_builder{
						Oid: pg.OIDUnknown,
					}.Build(),
				}.Build(),
				Value: Ydb.Value_builder{
					TextValue: proto.String("123"),
				}.Build(),
			},
		},
		{
			method: "Int4",
			args:   []any{int32(123)},

			expected: expected{
				Type: Ydb.Type_builder{
					PgType: Ydb.PgType_builder{
						Oid: pg.OIDInt4,
					}.Build(),
				}.Build(),
				Value: Ydb.Value_builder{
					TextValue: proto.String("123"),
				}.Build(),
			},
		},
		{
			method: "Int8",
			args:   []any{int64(123)},

			expected: expected{
				Type: Ydb.Type_builder{
					PgType: Ydb.PgType_builder{
						Oid: pg.OIDInt8,
					}.Build(),
				}.Build(),
				Value: Ydb.Value_builder{
					TextValue: proto.String("123"),
				}.Build(),
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.method, func(t *testing.T) {
			item := Builder{}.Param("$x").Pg()

			result, ok := xtest.CallMethod(item, tc.method, tc.args...)[0].(Builder)
			require.True(t, ok)

			params := result.build().toYDB()

			require.Equal(t, xtest.ToJSON(
				map[string]*Ydb.TypedValue{
					"$x": Ydb.TypedValue_builder{
						Type:  tc.expected.Type,
						Value: tc.expected.Value,
					}.Build(),
				}), xtest.ToJSON(params))
		})
	}
}
