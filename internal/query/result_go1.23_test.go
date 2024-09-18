//go:build go1.23

package query

import (
	"context"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Query"
	"go.uber.org/mock/gomock"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xtest"
)

func TestResultRangeResultSets(t *testing.T) {
	ctx, cancel := context.WithCancel(xtest.Context(t))
	defer cancel()
	ctrl := gomock.NewController(t)
	stream := NewMockQueryService_ExecuteQueryClient(ctrl)
	stream.EXPECT().Recv().Return(&Ydb_Query.ExecuteQueryResponsePart{
		Status:         Ydb.StatusIds_SUCCESS,
		ResultSetIndex: 0,
		ResultSet: &Ydb.ResultSet{
			Columns: []*Ydb.Column{
				{
					Name: "a",
					Type: &Ydb.Type{
						Type: &Ydb.Type_TypeId{
							TypeId: Ydb.Type_UINT64,
						},
					},
				},
				{
					Name: "b",
					Type: &Ydb.Type{
						Type: &Ydb.Type_TypeId{
							TypeId: Ydb.Type_UTF8,
						},
					},
				},
			},
			Rows: []*Ydb.Value{
				{
					Items: []*Ydb.Value{{
						Value: &Ydb.Value_Uint64Value{
							Uint64Value: 1,
						},
					}, {
						Value: &Ydb.Value_TextValue{
							TextValue: "1",
						},
					}},
				},
				{
					Items: []*Ydb.Value{{
						Value: &Ydb.Value_Uint64Value{
							Uint64Value: 2,
						},
					}, {
						Value: &Ydb.Value_TextValue{
							TextValue: "2",
						},
					}},
				},
				{
					Items: []*Ydb.Value{{
						Value: &Ydb.Value_Uint64Value{
							Uint64Value: 3,
						},
					}, {
						Value: &Ydb.Value_TextValue{
							TextValue: "3",
						},
					}},
				},
			},
		},
	}, nil)
	stream.EXPECT().Recv().Return(&Ydb_Query.ExecuteQueryResponsePart{
		Status:         Ydb.StatusIds_SUCCESS,
		ResultSetIndex: 0,
		ResultSet: &Ydb.ResultSet{
			Rows: []*Ydb.Value{
				{
					Items: []*Ydb.Value{{
						Value: &Ydb.Value_Uint64Value{
							Uint64Value: 4,
						},
					}, {
						Value: &Ydb.Value_TextValue{
							TextValue: "4",
						},
					}},
				},
				{
					Items: []*Ydb.Value{{
						Value: &Ydb.Value_Uint64Value{
							Uint64Value: 5,
						},
					}, {
						Value: &Ydb.Value_TextValue{
							TextValue: "5",
						},
					}},
				},
			},
		},
	}, nil)
	stream.EXPECT().Recv().Return(&Ydb_Query.ExecuteQueryResponsePart{
		Status:         Ydb.StatusIds_SUCCESS,
		ResultSetIndex: 1,
		ResultSet: &Ydb.ResultSet{
			Columns: []*Ydb.Column{
				{
					Name: "c",
					Type: &Ydb.Type{
						Type: &Ydb.Type_TypeId{
							TypeId: Ydb.Type_UINT64,
						},
					},
				},
				{
					Name: "d",
					Type: &Ydb.Type{
						Type: &Ydb.Type_TypeId{
							TypeId: Ydb.Type_UTF8,
						},
					},
				},
				{
					Name: "e",
					Type: &Ydb.Type{
						Type: &Ydb.Type_TypeId{
							TypeId: Ydb.Type_BOOL,
						},
					},
				},
			},
			Rows: []*Ydb.Value{
				{
					Items: []*Ydb.Value{{
						Value: &Ydb.Value_Uint64Value{
							Uint64Value: 1,
						},
					}, {
						Value: &Ydb.Value_TextValue{
							TextValue: "1",
						},
					}, {
						Value: &Ydb.Value_BoolValue{
							BoolValue: true,
						},
					}},
				},
				{
					Items: []*Ydb.Value{{
						Value: &Ydb.Value_Uint64Value{
							Uint64Value: 2,
						},
					}, {
						Value: &Ydb.Value_TextValue{
							TextValue: "2",
						},
					}, {
						Value: &Ydb.Value_BoolValue{
							BoolValue: false,
						},
					}},
				},
			},
		},
	}, nil)
	stream.EXPECT().Recv().Return(&Ydb_Query.ExecuteQueryResponsePart{
		Status:         Ydb.StatusIds_SUCCESS,
		ResultSetIndex: 1,
		ResultSet: &Ydb.ResultSet{
			Rows: []*Ydb.Value{
				{
					Items: []*Ydb.Value{{
						Value: &Ydb.Value_Uint64Value{
							Uint64Value: 3,
						},
					}, {
						Value: &Ydb.Value_TextValue{
							TextValue: "3",
						},
					}, {
						Value: &Ydb.Value_BoolValue{
							BoolValue: true,
						},
					}},
				},
				{
					Items: []*Ydb.Value{{
						Value: &Ydb.Value_Uint64Value{
							Uint64Value: 4,
						},
					}, {
						Value: &Ydb.Value_TextValue{
							TextValue: "4",
						},
					}, {
						Value: &Ydb.Value_BoolValue{
							BoolValue: false,
						},
					}},
				},
				{
					Items: []*Ydb.Value{{
						Value: &Ydb.Value_Uint64Value{
							Uint64Value: 5,
						},
					}, {
						Value: &Ydb.Value_TextValue{
							TextValue: "5",
						},
					}, {
						Value: &Ydb.Value_BoolValue{
							BoolValue: false,
						},
					}},
				},
			},
		},
	}, nil)
	stream.EXPECT().Recv().Return(&Ydb_Query.ExecuteQueryResponsePart{
		Status:         Ydb.StatusIds_SUCCESS,
		ResultSetIndex: 2,
		ResultSet: &Ydb.ResultSet{
			Columns: []*Ydb.Column{
				{
					Name: "c",
					Type: &Ydb.Type{
						Type: &Ydb.Type_TypeId{
							TypeId: Ydb.Type_UINT64,
						},
					},
				},
				{
					Name: "d",
					Type: &Ydb.Type{
						Type: &Ydb.Type_TypeId{
							TypeId: Ydb.Type_UTF8,
						},
					},
				},
				{
					Name: "e",
					Type: &Ydb.Type{
						Type: &Ydb.Type_TypeId{
							TypeId: Ydb.Type_BOOL,
						},
					},
				},
			},
			Rows: []*Ydb.Value{
				{
					Items: []*Ydb.Value{{
						Value: &Ydb.Value_Uint64Value{
							Uint64Value: 1,
						},
					}, {
						Value: &Ydb.Value_TextValue{
							TextValue: "1",
						},
					}, {
						Value: &Ydb.Value_BoolValue{
							BoolValue: true,
						},
					}},
				},
				{
					Items: []*Ydb.Value{{
						Value: &Ydb.Value_Uint64Value{
							Uint64Value: 2,
						},
					}, {
						Value: &Ydb.Value_TextValue{
							TextValue: "2",
						},
					}, {
						Value: &Ydb.Value_BoolValue{
							BoolValue: false,
						},
					}},
				},
			},
		},
	}, nil)
	stream.EXPECT().Recv().Return(&Ydb_Query.ExecuteQueryResponsePart{
		Status:         Ydb.StatusIds_SUCCESS,
		ResultSetIndex: 2,
		ResultSet: &Ydb.ResultSet{
			Rows: []*Ydb.Value{
				{
					Items: []*Ydb.Value{{
						Value: &Ydb.Value_Uint64Value{
							Uint64Value: 3,
						},
					}, {
						Value: &Ydb.Value_TextValue{
							TextValue: "3",
						},
					}, {
						Value: &Ydb.Value_BoolValue{
							BoolValue: true,
						},
					}},
				},
				{
					Items: []*Ydb.Value{{
						Value: &Ydb.Value_Uint64Value{
							Uint64Value: 4,
						},
					}, {
						Value: &Ydb.Value_TextValue{
							TextValue: "4",
						},
					}, {
						Value: &Ydb.Value_BoolValue{
							BoolValue: false,
						},
					}},
				},
				{
					Items: []*Ydb.Value{{
						Value: &Ydb.Value_Uint64Value{
							Uint64Value: 5,
						},
					}, {
						Value: &Ydb.Value_TextValue{
							TextValue: "5",
						},
					}, {
						Value: &Ydb.Value_BoolValue{
							BoolValue: false,
						},
					}},
				},
			},
		},
	}, nil)
	stream.EXPECT().Recv().Return(nil, io.EOF)
	r, err := newResult(ctx, stream, nil)
	require.NoError(t, err)
	defer r.Close(ctx)
	rsCount := 0
	for rs, err := range r.ResultSets(ctx) {
		require.NoError(t, err)
		rowsCount := 0
		for _, err := range rs.Rows(ctx) {
			require.NoError(t, err)
			rowsCount++
		}
		require.EqualValues(t, 5, rowsCount)
		rsCount++
	}
	require.EqualValues(t, 3, rsCount)
}
