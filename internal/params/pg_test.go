package params

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/pg"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xtest"
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
				Type: &Ydb.Type{
					Type: &Ydb.Type_PgType{
						PgType: &Ydb.PgType{
							Oid: pg.OIDUnknown,
						},
					},
				},
				Value: &Ydb.Value{
					Value: &Ydb.Value_TextValue{TextValue: "123"},
				},
			},
		},
		{
			method: "Int4",
			args:   []any{int32(123)},

			expected: expected{
				Type: &Ydb.Type{
					Type: &Ydb.Type_PgType{
						PgType: &Ydb.PgType{
							Oid: pg.OIDInt4,
						},
					},
				},
				Value: &Ydb.Value{
					Value: &Ydb.Value_TextValue{TextValue: "123"},
				},
			},
		},
		{
			method: "Int8",
			args:   []any{int64(123)},

			expected: expected{
				Type: &Ydb.Type{
					Type: &Ydb.Type_PgType{
						PgType: &Ydb.PgType{
							Oid: pg.OIDInt8,
						},
					},
				},
				Value: &Ydb.Value{
					Value: &Ydb.Value_TextValue{TextValue: "123"},
				},
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
					"$x": {
						Type:  tc.expected.Type,
						Value: tc.expected.Value,
					},
				}), xtest.ToJSON(params))
		})
	}
}
