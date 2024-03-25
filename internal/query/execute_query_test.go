package query

import (
	"context"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Query"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
	grpcCodes "google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	grpcStatus "google.golang.org/grpc/status"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/allocator"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/params"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/query/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xtest"
)

func TestExecute(t *testing.T) {
	t.Run("HappyWay", func(t *testing.T) {
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
		service := NewMockQueryServiceClient(ctrl)
		service.EXPECT().ExecuteQuery(gomock.Any(), gomock.Any()).Return(stream, nil)
		tx, r, err := execute(ctx, newTestSession("123"), service, "", options.ExecuteSettings())
		require.NoError(t, err)
		defer r.Close(ctx)
		require.EqualValues(t, "456", tx.id)
		require.EqualValues(t, "123", tx.s.id)
		require.EqualValues(t, -1, r.resultSetIndex)
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
	})
	t.Run("TransportError", func(t *testing.T) {
		t.Run("OnCall", func(t *testing.T) {
			ctx := xtest.Context(t)
			ctrl := gomock.NewController(t)
			service := NewMockQueryServiceClient(ctrl)
			service.EXPECT().ExecuteQuery(gomock.Any(), gomock.Any()).Return(nil, grpcStatus.Error(grpcCodes.Unavailable, ""))
			t.Log("execute")
			_, _, err := execute(ctx, newTestSession("123"), service, "", options.ExecuteSettings())
			require.Error(t, err)
			require.True(t, xerrors.IsTransportError(err, grpcCodes.Unavailable))
		})
		t.Run("OnStream", func(t *testing.T) {
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
			stream.EXPECT().Recv().Return(nil, grpcStatus.Error(grpcCodes.Unavailable, ""))
			service := NewMockQueryServiceClient(ctrl)
			service.EXPECT().ExecuteQuery(gomock.Any(), gomock.Any()).Return(stream, nil)
			t.Log("execute")
			tx, r, err := execute(ctx, newTestSession("123"), service, "", options.ExecuteSettings())
			require.NoError(t, err)
			defer r.Close(ctx)
			require.EqualValues(t, "456", tx.id)
			require.EqualValues(t, "123", tx.s.id)
			require.EqualValues(t, -1, r.resultSetIndex)
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
					require.Error(t, err)
					require.True(t, xerrors.IsTransportError(err, grpcCodes.Unavailable))
				}
			}
			t.Log("check final error")
			require.Error(t, r.Err())
			require.True(t, xerrors.IsTransportError(r.Err(), grpcCodes.Unavailable))
		})
	})
	t.Run("OperationError", func(t *testing.T) {
		t.Run("OnCall", func(t *testing.T) {
			ctx := xtest.Context(t)
			ctrl := gomock.NewController(t)
			stream := NewMockQueryService_ExecuteQueryClient(ctrl)
			stream.EXPECT().Recv().Return(nil, xerrors.Operation(xerrors.WithStatusCode(
				Ydb.StatusIds_UNAVAILABLE,
			)))
			service := NewMockQueryServiceClient(ctrl)
			service.EXPECT().ExecuteQuery(gomock.Any(), gomock.Any()).Return(stream, nil)
			t.Log("execute")
			_, _, err := execute(ctx, newTestSession("123"), service, "", options.ExecuteSettings())
			require.Error(t, err)
			require.True(t, xerrors.IsOperationError(err, Ydb.StatusIds_UNAVAILABLE))
		})
		t.Run("OnStream", func(t *testing.T) {
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
			stream.EXPECT().Recv().Return(nil, xerrors.Operation(xerrors.WithStatusCode(
				Ydb.StatusIds_UNAVAILABLE,
			)))
			service := NewMockQueryServiceClient(ctrl)
			service.EXPECT().ExecuteQuery(gomock.Any(), gomock.Any()).Return(stream, nil)
			t.Log("execute")
			tx, r, err := execute(ctx, newTestSession("123"), service, "", options.ExecuteSettings())
			require.NoError(t, err)
			defer r.Close(ctx)
			require.EqualValues(t, "456", tx.id)
			require.EqualValues(t, "123", tx.s.id)
			require.EqualValues(t, -1, r.resultSetIndex)
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
					require.Error(t, err)
					require.True(t, xerrors.IsOperationError(err, Ydb.StatusIds_UNAVAILABLE))
				}
			}
			t.Log("check final error")
			require.Error(t, r.Err())
			require.True(t, xerrors.IsOperationError(r.Err(), Ydb.StatusIds_UNAVAILABLE))
		})
	})
}

func TestExecuteQueryRequest(t *testing.T) {
	a := allocator.New()
	for _, tt := range []struct {
		name        string
		opts        []options.ExecuteOption
		request     *Ydb_Query.ExecuteQueryRequest
		callOptions []grpc.CallOption
	}{
		{
			name: "WithoutOptions",
			request: &Ydb_Query.ExecuteQueryRequest{
				SessionId: "WithoutOptions",
				ExecMode:  Ydb_Query.ExecMode_EXEC_MODE_EXECUTE,
				TxControl: &Ydb_Query.TransactionControl{
					TxSelector: &Ydb_Query.TransactionControl_BeginTx{
						BeginTx: &Ydb_Query.TransactionSettings{
							TxMode: &Ydb_Query.TransactionSettings_SerializableReadWrite{
								SerializableReadWrite: &Ydb_Query.SerializableModeSettings{},
							},
						},
					},
					CommitTx: true,
				},
				Query: &Ydb_Query.ExecuteQueryRequest_QueryContent{
					QueryContent: &Ydb_Query.QueryContent{
						Syntax: Ydb_Query.Syntax_SYNTAX_YQL_V1,
						Text:   "WithoutOptions",
					},
				},
				StatsMode:            Ydb_Query.StatsMode_STATS_MODE_NONE,
				ConcurrentResultSets: false,
			},
		},
		{
			name: "WithParams",
			opts: []options.ExecuteOption{
				options.WithParameters(
					params.Builder{}.
						Param("$a").Text("A").
						Param("$b").Text("B").
						Param("$c").Text("C").
						Build(),
				),
			},
			request: &Ydb_Query.ExecuteQueryRequest{
				SessionId: "WithParams",
				ExecMode:  Ydb_Query.ExecMode_EXEC_MODE_EXECUTE,
				TxControl: &Ydb_Query.TransactionControl{
					TxSelector: &Ydb_Query.TransactionControl_BeginTx{
						BeginTx: &Ydb_Query.TransactionSettings{
							TxMode: &Ydb_Query.TransactionSettings_SerializableReadWrite{
								SerializableReadWrite: &Ydb_Query.SerializableModeSettings{},
							},
						},
					},
					CommitTx: true,
				},
				Query: &Ydb_Query.ExecuteQueryRequest_QueryContent{
					QueryContent: &Ydb_Query.QueryContent{
						Syntax: Ydb_Query.Syntax_SYNTAX_YQL_V1,
						Text:   "WithParams",
					},
				},
				Parameters: map[string]*Ydb.TypedValue{
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
					"$c": {
						Type: &Ydb.Type{
							Type: &Ydb.Type_TypeId{
								TypeId: Ydb.Type_UTF8,
							},
						},
						Value: &Ydb.Value{
							Value: &Ydb.Value_TextValue{
								TextValue: "C",
							},
						},
					},
				},
				StatsMode:            Ydb_Query.StatsMode_STATS_MODE_NONE,
				ConcurrentResultSets: false,
			},
		},
		{
			name: "WithExplain",
			opts: []options.ExecuteOption{
				options.WithExecMode(options.ExecModeExplain),
			},
			request: &Ydb_Query.ExecuteQueryRequest{
				SessionId: "WithExplain",
				ExecMode:  Ydb_Query.ExecMode_EXEC_MODE_EXPLAIN,
				TxControl: &Ydb_Query.TransactionControl{
					TxSelector: &Ydb_Query.TransactionControl_BeginTx{
						BeginTx: &Ydb_Query.TransactionSettings{
							TxMode: &Ydb_Query.TransactionSettings_SerializableReadWrite{
								SerializableReadWrite: &Ydb_Query.SerializableModeSettings{},
							},
						},
					},
					CommitTx: true,
				},
				Query: &Ydb_Query.ExecuteQueryRequest_QueryContent{
					QueryContent: &Ydb_Query.QueryContent{
						Syntax: Ydb_Query.Syntax_SYNTAX_YQL_V1,
						Text:   "WithExplain",
					},
				},
				StatsMode:            Ydb_Query.StatsMode_STATS_MODE_NONE,
				ConcurrentResultSets: false,
			},
		},
		{
			name: "WithValidate",
			opts: []options.ExecuteOption{
				options.WithExecMode(options.ExecModeValidate),
			},
			request: &Ydb_Query.ExecuteQueryRequest{
				SessionId: "WithValidate",
				ExecMode:  Ydb_Query.ExecMode_EXEC_MODE_VALIDATE,
				TxControl: &Ydb_Query.TransactionControl{
					TxSelector: &Ydb_Query.TransactionControl_BeginTx{
						BeginTx: &Ydb_Query.TransactionSettings{
							TxMode: &Ydb_Query.TransactionSettings_SerializableReadWrite{
								SerializableReadWrite: &Ydb_Query.SerializableModeSettings{},
							},
						},
					},
					CommitTx: true,
				},
				Query: &Ydb_Query.ExecuteQueryRequest_QueryContent{
					QueryContent: &Ydb_Query.QueryContent{
						Syntax: Ydb_Query.Syntax_SYNTAX_YQL_V1,
						Text:   "WithValidate",
					},
				},
				StatsMode:            Ydb_Query.StatsMode_STATS_MODE_NONE,
				ConcurrentResultSets: false,
			},
		},
		{
			name: "WithValidate",
			opts: []options.ExecuteOption{
				options.WithExecMode(options.ExecModeParse),
			},
			request: &Ydb_Query.ExecuteQueryRequest{
				SessionId: "WithValidate",
				ExecMode:  Ydb_Query.ExecMode_EXEC_MODE_PARSE,
				TxControl: &Ydb_Query.TransactionControl{
					TxSelector: &Ydb_Query.TransactionControl_BeginTx{
						BeginTx: &Ydb_Query.TransactionSettings{
							TxMode: &Ydb_Query.TransactionSettings_SerializableReadWrite{
								SerializableReadWrite: &Ydb_Query.SerializableModeSettings{},
							},
						},
					},
					CommitTx: true,
				},
				Query: &Ydb_Query.ExecuteQueryRequest_QueryContent{
					QueryContent: &Ydb_Query.QueryContent{
						Syntax: Ydb_Query.Syntax_SYNTAX_YQL_V1,
						Text:   "WithValidate",
					},
				},
				StatsMode:            Ydb_Query.StatsMode_STATS_MODE_NONE,
				ConcurrentResultSets: false,
			},
		},
		{
			name: "WithStatsFull",
			opts: []options.ExecuteOption{
				options.WithStatsMode(options.StatsModeFull),
			},
			request: &Ydb_Query.ExecuteQueryRequest{
				SessionId: "WithStatsFull",
				ExecMode:  Ydb_Query.ExecMode_EXEC_MODE_EXECUTE,
				TxControl: &Ydb_Query.TransactionControl{
					TxSelector: &Ydb_Query.TransactionControl_BeginTx{
						BeginTx: &Ydb_Query.TransactionSettings{
							TxMode: &Ydb_Query.TransactionSettings_SerializableReadWrite{
								SerializableReadWrite: &Ydb_Query.SerializableModeSettings{},
							},
						},
					},
					CommitTx: true,
				},
				Query: &Ydb_Query.ExecuteQueryRequest_QueryContent{
					QueryContent: &Ydb_Query.QueryContent{
						Syntax: Ydb_Query.Syntax_SYNTAX_YQL_V1,
						Text:   "WithStatsFull",
					},
				},
				StatsMode:            Ydb_Query.StatsMode_STATS_MODE_FULL,
				ConcurrentResultSets: false,
			},
		},
		{
			name: "WithStatsBasic",
			opts: []options.ExecuteOption{
				options.WithStatsMode(options.StatsModeBasic),
			},
			request: &Ydb_Query.ExecuteQueryRequest{
				SessionId: "WithStatsBasic",
				ExecMode:  Ydb_Query.ExecMode_EXEC_MODE_EXECUTE,
				TxControl: &Ydb_Query.TransactionControl{
					TxSelector: &Ydb_Query.TransactionControl_BeginTx{
						BeginTx: &Ydb_Query.TransactionSettings{
							TxMode: &Ydb_Query.TransactionSettings_SerializableReadWrite{
								SerializableReadWrite: &Ydb_Query.SerializableModeSettings{},
							},
						},
					},
					CommitTx: true,
				},
				Query: &Ydb_Query.ExecuteQueryRequest_QueryContent{
					QueryContent: &Ydb_Query.QueryContent{
						Syntax: Ydb_Query.Syntax_SYNTAX_YQL_V1,
						Text:   "WithStatsBasic",
					},
				},
				StatsMode:            Ydb_Query.StatsMode_STATS_MODE_BASIC,
				ConcurrentResultSets: false,
			},
		},
		{
			name: "WithStatsProfile",
			opts: []options.ExecuteOption{
				options.WithStatsMode(options.StatsModeProfile),
			},
			request: &Ydb_Query.ExecuteQueryRequest{
				SessionId: "WithStatsProfile",
				ExecMode:  Ydb_Query.ExecMode_EXEC_MODE_EXECUTE,
				TxControl: &Ydb_Query.TransactionControl{
					TxSelector: &Ydb_Query.TransactionControl_BeginTx{
						BeginTx: &Ydb_Query.TransactionSettings{
							TxMode: &Ydb_Query.TransactionSettings_SerializableReadWrite{
								SerializableReadWrite: &Ydb_Query.SerializableModeSettings{},
							},
						},
					},
					CommitTx: true,
				},
				Query: &Ydb_Query.ExecuteQueryRequest_QueryContent{
					QueryContent: &Ydb_Query.QueryContent{
						Syntax: Ydb_Query.Syntax_SYNTAX_YQL_V1,
						Text:   "WithStatsProfile",
					},
				},
				StatsMode:            Ydb_Query.StatsMode_STATS_MODE_PROFILE,
				ConcurrentResultSets: false,
			},
		},
		{
			name: "WithGrpcCallOptions",
			opts: []options.ExecuteOption{
				options.WithCallOptions(grpc.Header(&metadata.MD{
					"ext-header": []string{"test"},
				})),
			},
			request: &Ydb_Query.ExecuteQueryRequest{
				SessionId: "WithGrpcCallOptions",
				ExecMode:  Ydb_Query.ExecMode_EXEC_MODE_EXECUTE,
				TxControl: &Ydb_Query.TransactionControl{
					TxSelector: &Ydb_Query.TransactionControl_BeginTx{
						BeginTx: &Ydb_Query.TransactionSettings{
							TxMode: &Ydb_Query.TransactionSettings_SerializableReadWrite{
								SerializableReadWrite: &Ydb_Query.SerializableModeSettings{},
							},
						},
					},
					CommitTx: true,
				},
				Query: &Ydb_Query.ExecuteQueryRequest_QueryContent{
					QueryContent: &Ydb_Query.QueryContent{
						Syntax: Ydb_Query.Syntax_SYNTAX_YQL_V1,
						Text:   "WithGrpcCallOptions",
					},
				},
				StatsMode:            Ydb_Query.StatsMode_STATS_MODE_NONE,
				ConcurrentResultSets: false,
			},
			callOptions: []grpc.CallOption{
				grpc.Header(&metadata.MD{
					"ext-header": []string{"test"},
				}),
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			request, callOptions := executeQueryRequest(a, tt.name, tt.name, options.ExecuteSettings(tt.opts...))
			require.Equal(t, request.String(), tt.request.String())
			require.Equal(t, tt.callOptions, callOptions)
		})
	}
}
