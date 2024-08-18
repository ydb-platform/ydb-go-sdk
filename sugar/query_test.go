package sugar_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"

	internalQuery "github.com/ydb-platform/ydb-go-sdk/v3/internal/query"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
	"github.com/ydb-platform/ydb-go-sdk/v3/sugar"
)

func TestUnmarshallRow(t *testing.T) {
	type myStruct struct {
		ID  uint64 `sql:"id"`
		Str string `sql:"myStr"`
	}
	v, err := sugar.UnmarshallRow[myStruct](func() query.Row {
		return internalQuery.NewRow([]*Ydb.Column{
			{
				Name: "id",
				Type: &Ydb.Type{
					Type: &Ydb.Type_TypeId{
						TypeId: Ydb.Type_UINT64,
					},
				},
			},
			{
				Name: "myStr",
				Type: &Ydb.Type{
					Type: &Ydb.Type_TypeId{
						TypeId: Ydb.Type_UTF8,
					},
				},
			},
		}, &Ydb.Value{
			Items: []*Ydb.Value{{
				Value: &Ydb.Value_Uint64Value{
					Uint64Value: 123,
				},
			}, {
				Value: &Ydb.Value_TextValue{
					TextValue: "my string",
				},
			}},
		})
	}())
	require.NoError(t, err)
	require.EqualValues(t, 123, v.ID)
	require.EqualValues(t, "my string", v.Str)
}

func TestUnmarshallResultSet(t *testing.T) {
	type myStruct struct {
		ID  uint64 `sql:"id"`
		Str string `sql:"myStr"`
	}
	v, err := sugar.UnmarshallResultSet[myStruct](internalQuery.MaterializedResultSet(-1, nil, nil,
		[]query.Row{
			func() query.Row {
				return internalQuery.NewRow([]*Ydb.Column{
					{
						Name: "id",
						Type: &Ydb.Type{
							Type: &Ydb.Type_TypeId{
								TypeId: Ydb.Type_UINT64,
							},
						},
					},
					{
						Name: "myStr",
						Type: &Ydb.Type{
							Type: &Ydb.Type_TypeId{
								TypeId: Ydb.Type_UTF8,
							},
						},
					},
				}, &Ydb.Value{
					Items: []*Ydb.Value{{
						Value: &Ydb.Value_Uint64Value{
							Uint64Value: 123,
						},
					}, {
						Value: &Ydb.Value_TextValue{
							TextValue: "my string 1",
						},
					}},
				})
			}(),
			func() query.Row {
				return internalQuery.NewRow([]*Ydb.Column{
					{
						Name: "id",
						Type: &Ydb.Type{
							Type: &Ydb.Type_TypeId{
								TypeId: Ydb.Type_UINT64,
							},
						},
					},
					{
						Name: "myStr",
						Type: &Ydb.Type{
							Type: &Ydb.Type_TypeId{
								TypeId: Ydb.Type_UTF8,
							},
						},
					},
				}, &Ydb.Value{
					Items: []*Ydb.Value{{
						Value: &Ydb.Value_Uint64Value{
							Uint64Value: 456,
						},
					}, {
						Value: &Ydb.Value_TextValue{
							TextValue: "my string 2",
						},
					}},
				})
			}(),
		},
	))
	require.NoError(t, err)
	require.Len(t, v, 2)
	require.EqualValues(t, 123, v[0].ID)
	require.EqualValues(t, "my string 1", v[0].Str)
	require.EqualValues(t, 456, v[1].ID)
	require.EqualValues(t, "my string 2", v[1].Str)
}
