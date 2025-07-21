package query

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/Ydb_Query_V1"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Query"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_TableStats"
	"go.uber.org/mock/gomock"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/stats"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xtest"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func TestResultNextResultSet(t *testing.T) {
	t.Run("HappyWay", func(t *testing.T) {
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
		{
			t.Log("nextResultSet")
			rs, err := r.nextResultSet(ctx)
			require.NoError(t, err)
			require.EqualValues(t, 0, rs.Index())
			{
				t.Log("next (row=1)")
				_, err := rs.nextRow(ctx)
				require.NoError(t, err)
				require.EqualValues(t, 0, rs.rowIndex)
			}
			{
				t.Log("next (row=2)")
				_, err := rs.nextRow(ctx)
				require.NoError(t, err)
				require.EqualValues(t, 1, rs.rowIndex)
			}
			{
				t.Log("next (row=3)")
				_, err := rs.nextRow(ctx)
				require.NoError(t, err)
				require.EqualValues(t, 2, rs.rowIndex)
			}
			{
				t.Log("next (row=4)")
				_, err := rs.nextRow(ctx)
				require.NoError(t, err)
				require.EqualValues(t, 0, rs.rowIndex)
			}
			{
				t.Log("next (row=5)")
				_, err := rs.nextRow(ctx)
				require.NoError(t, err)
				require.EqualValues(t, 1, rs.rowIndex)
			}
			{
				t.Log("next (row=6)")
				_, err := rs.nextRow(ctx)
				require.ErrorIs(t, err, io.EOF)
			}
		}
		{
			t.Log("nextResultSet")
			rs, err := r.nextResultSet(ctx)
			require.NoError(t, err)
			require.EqualValues(t, 1, rs.Index())
		}
		{
			t.Log("nextResultSet")
			rs, err := r.nextResultSet(ctx)
			require.NoError(t, err)
			require.EqualValues(t, 2, rs.Index())
			{
				t.Log("next (row=1)")
				_, err := rs.nextRow(ctx)
				require.NoError(t, err)
				require.EqualValues(t, 0, rs.rowIndex)
			}
			{
				t.Log("next (row=2)")
				_, err := rs.nextRow(ctx)
				require.NoError(t, err)
				require.EqualValues(t, 1, rs.rowIndex)
			}
			{
				t.Log("next (row=3)")
				_, err := rs.nextRow(ctx)
				require.NoError(t, err)
				require.EqualValues(t, 0, rs.rowIndex)
			}
			{
				t.Log("next (row=4)")
				_, err := rs.nextRow(ctx)
				require.NoError(t, err)
				require.EqualValues(t, 1, rs.rowIndex)
			}
			{
				t.Log("next (row=5)")
				_, err := rs.nextRow(ctx)
				require.NoError(t, err)
				require.EqualValues(t, 2, rs.rowIndex)
			}
			{
				t.Log("next (row=6)")
				_, err := rs.nextRow(ctx)
				require.ErrorIs(t, err, io.EOF)
			}
		}
		{
			t.Log("close result")
			r.Close(context.Background())
		}
		{
			t.Log("nextResultSet")
			rs, err := r.nextResultSet(context.Background())
			require.ErrorIs(t, err, io.EOF)
			require.Nil(t, rs)
			require.Equal(t, -1, rs.Index())
		}
	})
	t.Run("InterruptStream", func(t *testing.T) {
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
		r, err := newResult(ctx, stream, nil)
		require.NoError(t, err)
		defer r.Close(ctx)
		{
			t.Log("nextResultSet")
			rs, err := r.nextResultSet(ctx)
			require.NoError(t, err)
			require.EqualValues(t, 0, rs.Index())
			{
				t.Log("next (row=1)")
				_, err := rs.nextRow(ctx)
				require.NoError(t, err)
				require.EqualValues(t, 0, rs.rowIndex)
			}
			{
				t.Log("next (row=2)")
				_, err := rs.nextRow(ctx)
				require.NoError(t, err)
				require.EqualValues(t, 1, rs.rowIndex)
			}
			t.Log("explicit interrupt stream")
			r.closeOnce()
			{
				t.Log("next (row=3)")
				_, err := rs.nextRow(context.Background())
				require.NoError(t, err)
				require.EqualValues(t, 2, rs.rowIndex)
			}
			{
				t.Log("next (row=4)")
				_, err := rs.nextRow(context.Background())
				require.ErrorIs(t, err, io.EOF)
			}
		}
		{
			t.Log("nextResultSet")
			_, err := r.nextResultSet(context.Background())
			require.ErrorIs(t, err, io.EOF)
		}
	})
	t.Run("WrongResultSetIndex", func(t *testing.T) {
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
		r, err := newResult(ctx, stream, nil)
		require.NoError(t, err)
		defer r.Close(ctx)
		{
			t.Log("nextResultSet")
			rs, err := r.nextResultSet(ctx)
			require.NoError(t, err)
			require.EqualValues(t, 0, rs.Index())
			{
				t.Log("next (row=1)")
				_, err := rs.nextRow(ctx)
				require.NoError(t, err)
				require.EqualValues(t, 0, rs.rowIndex)
			}
			{
				t.Log("next (row=2)")
				_, err := rs.nextRow(ctx)
				require.NoError(t, err)
				require.EqualValues(t, 1, rs.rowIndex)
			}
			{
				t.Log("next (row=3)")
				_, err := rs.nextRow(ctx)
				require.NoError(t, err)
				require.EqualValues(t, 2, rs.rowIndex)
			}
			{
				t.Log("next (row=4)")
				_, err := rs.nextRow(ctx)
				require.NoError(t, err)
				require.EqualValues(t, 0, rs.rowIndex)
			}
			{
				t.Log("next (row=5)")
				_, err := rs.nextRow(ctx)
				require.NoError(t, err)
				require.EqualValues(t, 1, rs.rowIndex)
			}
			{
				t.Log("next (row=6)")
				_, err := rs.nextRow(ctx)
				require.ErrorIs(t, err, io.EOF)
			}
		}
		{
			t.Log("nextResultSet")
			rs, err := r.nextResultSet(ctx)
			require.NoError(t, err)
			require.EqualValues(t, 2, rs.Index())
		}
		{
			t.Log("nextResultSet")
			_, err := r.nextResultSet(ctx)
			require.ErrorIs(t, err, errWrongNextResultSetIndex)
		}
	})
}

func TestExactlyOneRowFromResult(t *testing.T) {
	ctx := xtest.Context(t)
	t.Run("HappyWay", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		stream := NewMockQueryService_ExecuteQueryClient(ctrl)
		stream.EXPECT().Recv().Return(&Ydb_Query.ExecuteQueryResponsePart{
			Status: Ydb.StatusIds_SUCCESS,
			TxMeta: &Ydb_Query.TransactionMeta{
				Id: "456",
			},
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
				},
			},
		}, nil)
		stream.EXPECT().Recv().Return(nil, io.EOF)
		r, err := newResult(ctx, stream, nil)
		require.NoError(t, err)

		row, err := exactlyOneRowFromResult(ctx, r)
		require.NoError(t, err)
		var (
			a uint64
			b string
		)
		err = row.Scan(&a, &b)
		require.NoError(t, err)
		require.EqualValues(t, 1, a)
		require.EqualValues(t, "1", b)
	})
	t.Run("MoreThanOneRow", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		stream := NewMockQueryService_ExecuteQueryClient(ctrl)
		stream.EXPECT().Recv().Return(&Ydb_Query.ExecuteQueryResponsePart{
			Status: Ydb.StatusIds_SUCCESS,
			TxMeta: &Ydb_Query.TransactionMeta{
				Id: "456",
			},
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
				},
			},
		}, nil)
		r, err := newResult(ctx, stream, nil)
		require.NoError(t, err)

		row, err := exactlyOneRowFromResult(ctx, r)
		require.ErrorIs(t, err, errMoreThanOneRow)
		require.Nil(t, row)
	})
	t.Run("MoreThanOneRowErrorOnNextRow", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		stream := NewMockQueryService_ExecuteQueryClient(ctrl)
		stream.EXPECT().Recv().Return(&Ydb_Query.ExecuteQueryResponsePart{
			Status: Ydb.StatusIds_SUCCESS,
			TxMeta: &Ydb_Query.TransactionMeta{
				Id: "456",
			},
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
				},
			},
		}, nil)
		testErr := errors.New("test-err")
		stream.EXPECT().Recv().Return(nil, testErr)
		r, err := newResult(ctx, stream, nil)
		require.NoError(t, err)

		row, err := exactlyOneRowFromResult(ctx, r)
		require.ErrorIs(t, err, testErr)
		require.Nil(t, row)
	})
	t.Run("MoreThanOneResultSet", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		stream := NewMockQueryService_ExecuteQueryClient(ctrl)
		stream.EXPECT().Recv().Return(&Ydb_Query.ExecuteQueryResponsePart{
			Status: Ydb.StatusIds_SUCCESS,
			TxMeta: &Ydb_Query.TransactionMeta{
				Id: "456",
			},
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
				},
			},
		}, nil)
		stream.EXPECT().Recv().Return(&Ydb_Query.ExecuteQueryResponsePart{
			Status: Ydb.StatusIds_SUCCESS,
			TxMeta: &Ydb_Query.TransactionMeta{
				Id: "456",
			},
			ResultSetIndex: 1,
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
				},
			},
		}, nil)
		r, err := newResult(ctx, stream, nil)
		require.NoError(t, err)

		row, err := exactlyOneResultSetFromResult(ctx, r)
		require.ErrorIs(t, err, errMoreThanOneResultSet)
		require.Nil(t, row)
	})
	t.Run("MoreThanOneResultSetErrorOnNextResultSet", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		stream := NewMockQueryService_ExecuteQueryClient(ctrl)
		stream.EXPECT().Recv().Return(&Ydb_Query.ExecuteQueryResponsePart{
			Status: Ydb.StatusIds_SUCCESS,
			TxMeta: &Ydb_Query.TransactionMeta{
				Id: "456",
			},
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
				},
			},
		}, nil)
		testErr := errors.New("test-err")
		stream.EXPECT().Recv().Return(nil, testErr)
		r, err := newResult(ctx, stream, nil)
		require.NoError(t, err)

		row, err := exactlyOneRowFromResult(ctx, r)
		require.ErrorIs(t, err, testErr)
		require.Nil(t, row)
	})
}

func TestExactlyOneResultSetFromResult(t *testing.T) {
	ctx := xtest.Context(t)
	t.Run("HappyWay", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		stream := NewMockQueryService_ExecuteQueryClient(ctrl)
		stream.EXPECT().Recv().Return(&Ydb_Query.ExecuteQueryResponsePart{
			Status: Ydb.StatusIds_SUCCESS,
			TxMeta: &Ydb_Query.TransactionMeta{
				Id: "456",
			},
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
				},
			},
		}, nil)
		stream.EXPECT().Recv().Return(nil, io.EOF)
		r, err := newResult(ctx, stream, nil)
		require.NoError(t, err)

		rs, err := exactlyOneResultSetFromResult(ctx, r)
		require.NoError(t, err)
		var (
			a uint64
			b string
		)
		r1, err1 := rs.NextRow(ctx)
		require.NoError(t, err1)
		require.NotNil(t, r1)
		scanErr1 := r1.Scan(&a, &b)
		require.NoError(t, scanErr1)
		require.EqualValues(t, 1, a)
		require.EqualValues(t, "1", b)
		r2, err2 := rs.NextRow(ctx)
		require.NoError(t, err2)
		require.NotNil(t, r2)
		scanErr2 := r2.Scan(&a, &b)
		require.NoError(t, scanErr2)
		require.EqualValues(t, 2, a)
		require.EqualValues(t, "2", b)
		r3, err3 := rs.NextRow(ctx)
		require.ErrorIs(t, err3, io.EOF)
		require.Nil(t, r3)
	})
	t.Run("MoreThanOneResultSet", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		stream := NewMockQueryService_ExecuteQueryClient(ctrl)
		stream.EXPECT().Recv().Return(&Ydb_Query.ExecuteQueryResponsePart{
			Status: Ydb.StatusIds_SUCCESS,
			TxMeta: &Ydb_Query.TransactionMeta{
				Id: "456",
			},
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
				},
			},
		}, nil)
		stream.EXPECT().Recv().Return(&Ydb_Query.ExecuteQueryResponsePart{
			Status: Ydb.StatusIds_SUCCESS,
			TxMeta: &Ydb_Query.TransactionMeta{
				Id: "456",
			},
			ResultSetIndex: 1,
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
				},
			},
		}, nil)
		r, err := newResult(ctx, stream, nil)
		require.NoError(t, err)

		rs, err := exactlyOneResultSetFromResult(ctx, r)
		require.ErrorIs(t, err, errMoreThanOneResultSet)
		require.Nil(t, rs)
	})
	t.Run("MoreThanOneResultSetErrorOnNextResultSet", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		stream := NewMockQueryService_ExecuteQueryClient(ctrl)
		stream.EXPECT().Recv().Return(&Ydb_Query.ExecuteQueryResponsePart{
			Status: Ydb.StatusIds_SUCCESS,
			TxMeta: &Ydb_Query.TransactionMeta{
				Id: "456",
			},
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
				},
			},
		}, nil)
		testErr := errors.New("test-err")
		stream.EXPECT().Recv().Return(nil, testErr)
		r, err := newResult(ctx, stream, nil)
		require.NoError(t, err)

		rs, err := exactlyOneResultSetFromResult(ctx, r)
		require.ErrorIs(t, err, testErr)
		require.Nil(t, rs)
	})
}

func TestCloseResultOnCloseClosableResultSet(t *testing.T) {
	ctx := xtest.Context(t)
	ctrl := gomock.NewController(t)
	stream := NewMockQueryService_ExecuteQueryClient(ctrl)
	stream.EXPECT().Recv().Return(&Ydb_Query.ExecuteQueryResponsePart{
		Status: Ydb.StatusIds_SUCCESS,
		TxMeta: &Ydb_Query.TransactionMeta{
			Id: "456",
		},
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
			},
		},
	}, nil)
	stream.EXPECT().Recv().Return(&Ydb_Query.ExecuteQueryResponsePart{
		Status: Ydb.StatusIds_SUCCESS,
		TxMeta: &Ydb_Query.TransactionMeta{
			Id: "456",
		},
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
							Uint64Value: 3,
						},
					}, {
						Value: &Ydb.Value_TextValue{
							TextValue: "3",
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
					}},
				},
			},
		},
	}, nil)
	stream.EXPECT().Recv().Return(nil, io.EOF)
	var closed bool
	r, err := newResult(ctx, stream, withStreamResultTrace(&trace.Query{
		OnResultClose: func(info trace.QueryResultCloseStartInfo) func(info trace.QueryResultCloseDoneInfo) {
			require.False(t, closed)
			closed = true

			return nil
		},
	}))

	require.NoError(t, err)

	rs, err := readResultSet(ctx, r)
	require.NoError(t, err)
	var (
		a uint64
		b string
	)
	r1, err1 := rs.NextRow(ctx)
	require.NoError(t, err1)
	require.NotNil(t, r1)
	scanErr1 := r1.Scan(&a, &b)
	require.NoError(t, scanErr1)
	require.EqualValues(t, 1, a)
	require.EqualValues(t, "1", b)
	r2, err2 := rs.NextRow(ctx)
	require.NoError(t, err2)
	require.NotNil(t, r2)
	scanErr2 := r2.Scan(&a, &b)
	require.NoError(t, scanErr2)
	require.EqualValues(t, 2, a)
	require.EqualValues(t, "2", b)
	r3, err3 := rs.NextRow(ctx)
	require.NoError(t, err3)
	scanErr3 := r3.Scan(&a, &b)
	require.EqualValues(t, 3, a)
	require.EqualValues(t, "3", b)
	require.NoError(t, scanErr3)
	r4, err4 := rs.NextRow(ctx)
	require.NoError(t, err4)
	scanErr4 := r4.Scan(&a, &b)
	require.EqualValues(t, 4, a)
	require.EqualValues(t, "4", b)
	require.NoError(t, scanErr4)
	r5, err5 := rs.NextRow(ctx)
	require.ErrorIs(t, err5, io.EOF)
	require.Nil(t, r5)
	err = rs.Close(ctx)
	require.NoError(t, err)
	require.True(t, closed)
}

func TestResultStats(t *testing.T) {
	t.Run("Stats", func(t *testing.T) {
		t.Run("Never", func(t *testing.T) {
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
			var s stats.QueryStats
			result, err := newResult(ctx, stream, withStreamResultStatsCallback(func(queryStats stats.QueryStats) {
				s = queryStats
			}))
			require.NoError(t, err)
			require.NotNil(t, result)
			defer result.Close(ctx)
			for {
				resultSet, err := result.nextResultSet(ctx)
				if err != nil && xerrors.Is(err, io.EOF) {
					break
				}
				for {
					_, err := resultSet.nextRow(ctx)
					if err != nil && xerrors.Is(err, io.EOF) {
						break
					}
				}
			}
			require.Nil(t, s)
		})
		t.Run("SeparatedLastPart", func(t *testing.T) {
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
			stream.EXPECT().Recv().Return(&Ydb_Query.ExecuteQueryResponsePart{
				Status:         Ydb.StatusIds_SUCCESS,
				ResultSetIndex: 0,
				ExecStats: &Ydb_TableStats.QueryStats{
					ProcessCpuTimeUs: 300,
					QueryPlan:        "123",
					QueryAst:         "456",
					TotalDurationUs:  100,
					TotalCpuTimeUs:   200,
				},
			}, nil)
			stream.EXPECT().Recv().Return(nil, io.EOF)
			var s stats.QueryStats
			result, err := newResult(ctx, stream, withStreamResultStatsCallback(func(queryStats stats.QueryStats) {
				s = queryStats
			}))
			require.NoError(t, err)
			require.NotNil(t, result)
			defer result.Close(ctx)
			for {
				resultSet, err := result.nextResultSet(ctx)
				if err != nil && xerrors.Is(err, io.EOF) {
					break
				}
				for {
					_, err := resultSet.nextRow(ctx)
					if err != nil && xerrors.Is(err, io.EOF) {
						break
					}
				}
			}
			require.NotNil(t, s)
			require.Equal(t, "123", s.QueryPlan())
			require.Equal(t, "456", s.QueryAST())
			require.Equal(t, time.Microsecond*100, s.TotalDuration())
			require.Equal(t, time.Microsecond*200, s.TotalCPUTime())
			require.Equal(t, time.Microsecond*300, s.ProcessCPUTime())
		})
		t.Run("WithLastPart", func(t *testing.T) {
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
				ExecStats: &Ydb_TableStats.QueryStats{
					ProcessCpuTimeUs: 300,
					QueryPlan:        "123",
					QueryAst:         "456",
					TotalDurationUs:  100,
					TotalCpuTimeUs:   200,
				},
			}, nil)
			stream.EXPECT().Recv().Return(nil, io.EOF)
			var s stats.QueryStats
			result, err := newResult(ctx, stream, withStreamResultStatsCallback(func(queryStats stats.QueryStats) {
				s = queryStats
			}))
			require.NoError(t, err)
			require.NotNil(t, result)
			defer result.Close(ctx)
			for {
				resultSet, err := result.nextResultSet(ctx)
				if err != nil && xerrors.Is(err, io.EOF) {
					break
				}
				for {
					_, err := resultSet.nextRow(ctx)
					if err != nil && xerrors.Is(err, io.EOF) {
						break
					}
				}
			}
			require.NotNil(t, s)
			require.Equal(t, "123", s.QueryPlan())
			require.Equal(t, "456", s.QueryAST())
			require.Equal(t, time.Microsecond*100, s.TotalDuration())
			require.Equal(t, time.Microsecond*200, s.TotalCPUTime())
			require.Equal(t, time.Microsecond*300, s.ProcessCPUTime())
		})
		t.Run("EveryPart", func(t *testing.T) {
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
				ExecStats: &Ydb_TableStats.QueryStats{
					ProcessCpuTimeUs: 1,
					QueryPlan:        "sdsd",
					QueryAst:         "sdffds",
					TotalDurationUs:  2,
					TotalCpuTimeUs:   3,
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
				ExecStats: &Ydb_TableStats.QueryStats{
					ProcessCpuTimeUs: 332400,
					QueryPlan:        "12wesfse3",
					QueryAst:         "sedfefs",
					TotalDurationUs:  10324320,
					TotalCpuTimeUs:   234,
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
				ExecStats: &Ydb_TableStats.QueryStats{
					ProcessCpuTimeUs: 323400,
					QueryPlan:        "sdfgrdg",
					QueryAst:         "sdgsrg",
					TotalDurationUs:  43554,
					TotalCpuTimeUs:   235643,
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
				ExecStats: &Ydb_TableStats.QueryStats{
					ProcessCpuTimeUs: 3034564570,
					QueryPlan:        "sdgsrgsg",
					QueryAst:         "dghytjf",
					TotalDurationUs:  45676,
					TotalCpuTimeUs:   4562342,
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
				ExecStats: &Ydb_TableStats.QueryStats{
					ProcessCpuTimeUs: 300,
					QueryPlan:        "123",
					QueryAst:         "456",
					TotalDurationUs:  100,
					TotalCpuTimeUs:   200,
				},
			}, nil)
			stream.EXPECT().Recv().Return(nil, io.EOF)
			var s stats.QueryStats
			result, err := newResult(ctx, stream, withStreamResultStatsCallback(func(queryStats stats.QueryStats) {
				s = queryStats
			}))
			require.NoError(t, err)
			require.NotNil(t, result)
			defer result.Close(ctx)
			for {
				resultSet, err := result.nextResultSet(ctx)
				if err != nil && xerrors.Is(err, io.EOF) {
					break
				}
				for {
					_, err := resultSet.nextRow(ctx)
					if err != nil && xerrors.Is(err, io.EOF) {
						break
					}
				}
			}
			require.NotNil(t, s)
			require.Equal(t, "123", s.QueryPlan())
			require.Equal(t, "456", s.QueryAST())
			require.Equal(t, time.Microsecond*100, s.TotalDuration())
			require.Equal(t, time.Microsecond*200, s.TotalCPUTime())
			require.Equal(t, time.Microsecond*300, s.ProcessCPUTime())
		})
	})
}

func TestMaterializedResultStats(t *testing.T) {
	newResult := func(
		ctx context.Context,
		stream Ydb_Query_V1.QueryService_ExecuteQueryClient,
		opts ...resultOption,
	) (query.Result, error) {
		r, err := newResult(ctx, stream, opts...)
		if err != nil {
			return nil, err
		}

		return r, nil
	}
	t.Run("Stats", func(t *testing.T) {
		t.Run("Never", func(t *testing.T) {
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
			var s stats.QueryStats
			result, err := newResult(ctx, stream, withStreamResultStatsCallback(func(queryStats stats.QueryStats) {
				s = queryStats
			}))
			require.NoError(t, err)
			require.NotNil(t, result)
			defer result.Close(ctx)
			for {
				resultSet, err := result.NextResultSet(ctx)
				if err != nil && xerrors.Is(err, io.EOF) {
					break
				}
				for {
					_, err := resultSet.NextRow(ctx)
					if err != nil && xerrors.Is(err, io.EOF) {
						break
					}
				}
			}
			require.Nil(t, s)
		})
		t.Run("SeparatedLastPart", func(t *testing.T) {
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
			stream.EXPECT().Recv().Return(&Ydb_Query.ExecuteQueryResponsePart{
				Status:         Ydb.StatusIds_SUCCESS,
				ResultSetIndex: 0,
				ExecStats: &Ydb_TableStats.QueryStats{
					ProcessCpuTimeUs: 300,
					QueryPlan:        "123",
					QueryAst:         "456",
					TotalDurationUs:  100,
					TotalCpuTimeUs:   200,
				},
			}, nil)
			stream.EXPECT().Recv().Return(nil, io.EOF)
			var s stats.QueryStats
			result, err := newResult(ctx, stream, withStreamResultStatsCallback(func(queryStats stats.QueryStats) {
				s = queryStats
			}))
			require.NoError(t, err)
			require.NotNil(t, result)
			defer result.Close(ctx)
			for {
				resultSet, err := result.NextResultSet(ctx)
				if err != nil && xerrors.Is(err, io.EOF) {
					break
				}
				for {
					_, err := resultSet.NextRow(ctx)
					if err != nil && xerrors.Is(err, io.EOF) {
						break
					}
				}
			}
			require.NotNil(t, s)
			require.Equal(t, "123", s.QueryPlan())
			require.Equal(t, "456", s.QueryAST())
			require.Equal(t, time.Microsecond*100, s.TotalDuration())
			require.Equal(t, time.Microsecond*200, s.TotalCPUTime())
			require.Equal(t, time.Microsecond*300, s.ProcessCPUTime())
		})
		t.Run("WithLastPart", func(t *testing.T) {
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
				ExecStats: &Ydb_TableStats.QueryStats{
					ProcessCpuTimeUs: 300,
					QueryPlan:        "123",
					QueryAst:         "456",
					TotalDurationUs:  100,
					TotalCpuTimeUs:   200,
				},
			}, nil)
			stream.EXPECT().Recv().Return(nil, io.EOF)
			var s stats.QueryStats
			result, err := newResult(ctx, stream, withStreamResultStatsCallback(func(queryStats stats.QueryStats) {
				s = queryStats
			}))
			require.NoError(t, err)
			require.NotNil(t, result)
			defer result.Close(ctx)
			for {
				resultSet, err := result.NextResultSet(ctx)
				if err != nil && xerrors.Is(err, io.EOF) {
					break
				}
				for {
					_, err := resultSet.NextRow(ctx)
					if err != nil && xerrors.Is(err, io.EOF) {
						break
					}
				}
			}
			require.NotNil(t, s)
			require.Equal(t, "123", s.QueryPlan())
			require.Equal(t, "456", s.QueryAST())
			require.Equal(t, time.Microsecond*100, s.TotalDuration())
			require.Equal(t, time.Microsecond*200, s.TotalCPUTime())
			require.Equal(t, time.Microsecond*300, s.ProcessCPUTime())
		})
		t.Run("EveryPart", func(t *testing.T) {
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
				ExecStats: &Ydb_TableStats.QueryStats{
					ProcessCpuTimeUs: 1,
					QueryPlan:        "sdsd",
					QueryAst:         "sdffds",
					TotalDurationUs:  2,
					TotalCpuTimeUs:   3,
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
				ExecStats: &Ydb_TableStats.QueryStats{
					ProcessCpuTimeUs: 332400,
					QueryPlan:        "12wesfse3",
					QueryAst:         "sedfefs",
					TotalDurationUs:  10324320,
					TotalCpuTimeUs:   234,
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
				ExecStats: &Ydb_TableStats.QueryStats{
					ProcessCpuTimeUs: 323400,
					QueryPlan:        "sdfgrdg",
					QueryAst:         "sdgsrg",
					TotalDurationUs:  43554,
					TotalCpuTimeUs:   235643,
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
				ExecStats: &Ydb_TableStats.QueryStats{
					ProcessCpuTimeUs: 3034564570,
					QueryPlan:        "sdgsrgsg",
					QueryAst:         "dghytjf",
					TotalDurationUs:  45676,
					TotalCpuTimeUs:   4562342,
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
				ExecStats: &Ydb_TableStats.QueryStats{
					ProcessCpuTimeUs: 300,
					QueryPlan:        "123",
					QueryAst:         "456",
					TotalDurationUs:  100,
					TotalCpuTimeUs:   200,
				},
			}, nil)
			stream.EXPECT().Recv().Return(nil, io.EOF)
			var s stats.QueryStats
			result, err := newResult(ctx, stream, withStreamResultStatsCallback(func(queryStats stats.QueryStats) {
				s = queryStats
			}))
			require.NoError(t, err)
			require.NotNil(t, result)
			defer result.Close(ctx)
			for {
				resultSet, err := result.NextResultSet(ctx)
				if err != nil && xerrors.Is(err, io.EOF) {
					break
				}
				for {
					_, err := resultSet.NextRow(ctx)
					if err != nil && xerrors.Is(err, io.EOF) {
						break
					}
				}
			}
			require.NotNil(t, s)
			require.Equal(t, "123", s.QueryPlan())
			require.Equal(t, "456", s.QueryAST())
			require.Equal(t, time.Microsecond*100, s.TotalDuration())
			require.Equal(t, time.Microsecond*200, s.TotalCPUTime())
			require.Equal(t, time.Microsecond*300, s.ProcessCPUTime())
		})
	})
}
