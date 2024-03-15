package params

import (
	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/allocator"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/pg"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xtest"
	"testing"
)

func TestPg(t *testing.T) {
	type expected struct {
		kind  *Ydb.Type
		value *Ydb.Value
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
				kind: &Ydb.Type{
					Type: &Ydb.Type_PgType{
						PgType: &Ydb.PgType{
							Oid: pg.OIDUnknown,
						},
					},
				},
				value: &Ydb.Value{
					Value: &Ydb.Value_TextValue{TextValue: "123"},
				},
			},
		},
		{
			method: "Int4",
			args:   []any{int32(123)},

			expected: expected{
				kind: &Ydb.Type{
					Type: &Ydb.Type_PgType{
						PgType: &Ydb.PgType{
							Oid: pg.OIDInt4,
						},
					},
				},
				value: &Ydb.Value{
					Value: &Ydb.Value_TextValue{TextValue: "123"},
				},
			},
		},
		{
			method: "Int8",
			args:   []any{int64(123)},

			expected: expected{
				kind: &Ydb.Type{
					Type: &Ydb.Type_PgType{
						PgType: &Ydb.PgType{
							Oid: pg.OIDInt8,
						},
					},
				},
				value: &Ydb.Value{
					Value: &Ydb.Value_TextValue{TextValue: "123"},
				},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.method, func(t *testing.T) {
			a := allocator.New()
			defer a.Free()

			item := Builder{}.Param("$x").Pg()

			result, ok := xtest.CallMethod(item, tc.method, tc.args...)[0].(Builder)
			require.True(t, ok)

			params := result.Build().ToYDB(a)

			require.Equal(t, paramsToJSON(
				map[string]*Ydb.TypedValue{
					"$x": {
						Type:  tc.expected.kind,
						Value: tc.expected.value,
					},
				}), paramsToJSON(params))
		})
	}
}
