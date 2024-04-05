package query

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Query"
	"go.uber.org/mock/gomock"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xtest"
)

func TestResultNextResultSet(t *testing.T) {
	t.Run("HappyWay", func(t *testing.T) {
		xtest.TestManyTimes(t, func(t testing.TB) {
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
			r, _, err := newResult(ctx, stream, nil, nil)
			require.NoError(t, err)
			defer r.Close(ctx)
			{
				t.Log("nextResultSet")
				rs, err := r.nextResultSet(ctx)
				require.NoError(t, err)
				require.EqualValues(t, 0, rs.index)
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
				require.EqualValues(t, 1, rs.index)
			}
			{
				t.Log("nextResultSet")
				rs, err := r.nextResultSet(ctx)
				require.NoError(t, err)
				require.EqualValues(t, 2, rs.index)
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
				_, err := r.nextResultSet(context.Background())
				require.ErrorIs(t, err, errClosedResult)
			}
			t.Log("check final error")
			require.NoError(t, r.Err())
		}, xtest.StopAfter(time.Second))
	})
	t.Run("InterruptStream", func(t *testing.T) {
		xtest.TestManyTimes(t, func(t testing.TB) {
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
			r, _, err := newResult(ctx, stream, nil, nil)
			require.NoError(t, err)
			defer r.Close(ctx)
			{
				t.Log("nextResultSet")
				rs, err := r.nextResultSet(ctx)
				require.NoError(t, err)
				require.EqualValues(t, 0, rs.index)
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
				require.NoError(t, r.closeOnce(ctx))
				{
					t.Log("next (row=3)")
					_, err := rs.nextRow(context.Background())
					require.NoError(t, err)
					require.EqualValues(t, 2, rs.rowIndex)
				}
				{
					t.Log("next (row=4)")
					_, err := rs.nextRow(context.Background())
					require.ErrorIs(t, err, errClosedResult)
				}
			}
			{
				t.Log("nextResultSet")
				_, err := r.nextResultSet(context.Background())
				require.ErrorIs(t, err, errClosedResult)
			}
			t.Log("check final error")
			require.ErrorIs(t, r.Err(), errClosedResult)
		}, xtest.StopAfter(time.Second))
	})
	t.Run("WrongResultSetIndex", func(t *testing.T) {
		xtest.TestManyTimes(t, func(t testing.TB) {
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
			r, _, err := newResult(ctx, stream, nil, nil)
			require.NoError(t, err)
			defer r.Close(ctx)
			{
				t.Log("nextResultSet")
				rs, err := r.nextResultSet(ctx)
				require.NoError(t, err)
				require.EqualValues(t, 0, rs.index)
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
				require.EqualValues(t, 2, rs.index)
			}
			{
				t.Log("nextResultSet")
				_, err := r.nextResultSet(ctx)
				require.ErrorIs(t, err, errWrongNextResultSetIndex)
			}
			t.Log("check final error")
			require.ErrorIs(t, r.Err(), errWrongNextResultSetIndex)
		}, xtest.StopAfter(time.Second))
	})
}
