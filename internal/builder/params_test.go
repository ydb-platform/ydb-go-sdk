package builder

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/allocator"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xtest"
)

func paramsToJson(params map[string]*Ydb.TypedValue) string {
	b, _ := json.MarshalIndent(params, "", "\t")
	return string(b)
}

func TestParams(t *testing.T) {
	for _, tt := range []struct {
		name    string
		builder Parameters
		params  map[string]*Ydb.TypedValue
	}{
		{
			name:    xtest.CurrentFileLine(),
			builder: Params().Param("$a").Text("A").Build(),
			params: map[string]*Ydb.TypedValue{
				"$a": {
					Type: &Ydb.Type{
						Type: &Ydb.Type_TypeId{
							TypeId: Ydb.Type_UTF8,
						},
					},
					Value: &Ydb.Value{
						Value: &Ydb.Value_TextValue{
							TextValue: "A",
						},
					},
				},
			},
		},
		{
			name:    xtest.CurrentFileLine(),
			builder: Params().Param("$a").Uint64(123).Build(),
			params: map[string]*Ydb.TypedValue{
				"$a": {
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
				},
			},
		},
		{
			name:    xtest.CurrentFileLine(),
			builder: Params().Param("$a").Uint64(123).Param("$b").Text("B").Build(),
			params: map[string]*Ydb.TypedValue{
				"$b": {
					Type: &Ydb.Type{
						Type: &Ydb.Type_TypeId{
							TypeId: Ydb.Type_UTF8,
						},
					},
					Value: &Ydb.Value{
						Value: &Ydb.Value_TextValue{
							TextValue: "B",
						},
					},
				},
				"$a": {
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
				},
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			a := allocator.New()
			defer a.Free()
			require.Equal(t, paramsToJson(tt.params), paramsToJson(tt.builder.ToYDB(a)))
		})
	}
}
