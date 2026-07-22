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
	"google.golang.org/protobuf/proto"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/stats"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func TestResultNextResultSet(t *testing.T) {
	t.Run("HappyWay", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		ctrl := gomock.NewController(t)
		stream := newExecuteQueryStreamMock(ctrl)
		stream.EXPECT().Recv().Return(Ydb_Query.ExecuteQueryResponsePart_builder{
			Status:         Ydb.StatusIds_SUCCESS,
			ResultSetIndex: 0,
			ResultSet: Ydb.ResultSet_builder{
				Columns: []*Ydb.Column{
					Ydb.Column_builder{
						Name: "a",
						Type: Ydb.Type_builder{
							TypeId: Ydb.Type_UINT64.Enum(),
						}.Build(),
					}.Build(),
					Ydb.Column_builder{
						Name: "b",
						Type: Ydb.Type_builder{
							TypeId: Ydb.Type_UTF8.Enum(),
						}.Build(),
					}.Build(),
				},
				Rows: []*Ydb.Value{
					Ydb.Value_builder{
						Items: []*Ydb.Value{Ydb.Value_builder{
							Uint64Value: proto.Uint64(1),
						}.Build(), Ydb.Value_builder{
							TextValue: proto.String("1"),
						}.Build()},
					}.Build(),
					Ydb.Value_builder{
						Items: []*Ydb.Value{Ydb.Value_builder{
							Uint64Value: proto.Uint64(2),
						}.Build(), Ydb.Value_builder{
							TextValue: proto.String("2"),
						}.Build()},
					}.Build(),
					Ydb.Value_builder{
						Items: []*Ydb.Value{Ydb.Value_builder{
							Uint64Value: proto.Uint64(3),
						}.Build(), Ydb.Value_builder{
							TextValue: proto.String("3"),
						}.Build()},
					}.Build(),
				},
			}.Build(),
		}.Build(), nil)
		stream.EXPECT().Recv().Return(Ydb_Query.ExecuteQueryResponsePart_builder{
			Status:         Ydb.StatusIds_SUCCESS,
			ResultSetIndex: 0,
			ResultSet: Ydb.ResultSet_builder{
				Rows: []*Ydb.Value{
					Ydb.Value_builder{
						Items: []*Ydb.Value{Ydb.Value_builder{
							Uint64Value: proto.Uint64(4),
						}.Build(), Ydb.Value_builder{
							TextValue: proto.String("4"),
						}.Build()},
					}.Build(),
					Ydb.Value_builder{
						Items: []*Ydb.Value{Ydb.Value_builder{
							Uint64Value: proto.Uint64(5),
						}.Build(), Ydb.Value_builder{
							TextValue: proto.String("5"),
						}.Build()},
					}.Build(),
				},
			}.Build(),
		}.Build(), nil)
		stream.EXPECT().Recv().Return(Ydb_Query.ExecuteQueryResponsePart_builder{
			Status:         Ydb.StatusIds_SUCCESS,
			ResultSetIndex: 1,
			ResultSet: Ydb.ResultSet_builder{
				Columns: []*Ydb.Column{
					Ydb.Column_builder{
						Name: "c",
						Type: Ydb.Type_builder{
							TypeId: Ydb.Type_UINT64.Enum(),
						}.Build(),
					}.Build(),
					Ydb.Column_builder{
						Name: "d",
						Type: Ydb.Type_builder{
							TypeId: Ydb.Type_UTF8.Enum(),
						}.Build(),
					}.Build(),
					Ydb.Column_builder{
						Name: "e",
						Type: Ydb.Type_builder{
							TypeId: Ydb.Type_BOOL.Enum(),
						}.Build(),
					}.Build(),
				},
				Rows: []*Ydb.Value{
					Ydb.Value_builder{
						Items: []*Ydb.Value{Ydb.Value_builder{
							Uint64Value: proto.Uint64(1),
						}.Build(), Ydb.Value_builder{
							TextValue: proto.String("1"),
						}.Build(), Ydb.Value_builder{
							BoolValue: proto.Bool(true),
						}.Build()},
					}.Build(),
					Ydb.Value_builder{
						Items: []*Ydb.Value{Ydb.Value_builder{
							Uint64Value: proto.Uint64(2),
						}.Build(), Ydb.Value_builder{
							TextValue: proto.String("2"),
						}.Build(), Ydb.Value_builder{
							BoolValue: proto.Bool(false),
						}.Build()},
					}.Build(),
				},
			}.Build(),
		}.Build(), nil)
		stream.EXPECT().Recv().Return(Ydb_Query.ExecuteQueryResponsePart_builder{
			Status:         Ydb.StatusIds_SUCCESS,
			ResultSetIndex: 1,
			ResultSet: Ydb.ResultSet_builder{
				Rows: []*Ydb.Value{
					Ydb.Value_builder{
						Items: []*Ydb.Value{Ydb.Value_builder{
							Uint64Value: proto.Uint64(3),
						}.Build(), Ydb.Value_builder{
							TextValue: proto.String("3"),
						}.Build(), Ydb.Value_builder{
							BoolValue: proto.Bool(true),
						}.Build()},
					}.Build(),
					Ydb.Value_builder{
						Items: []*Ydb.Value{Ydb.Value_builder{
							Uint64Value: proto.Uint64(4),
						}.Build(), Ydb.Value_builder{
							TextValue: proto.String("4"),
						}.Build(), Ydb.Value_builder{
							BoolValue: proto.Bool(false),
						}.Build()},
					}.Build(),
					Ydb.Value_builder{
						Items: []*Ydb.Value{Ydb.Value_builder{
							Uint64Value: proto.Uint64(5),
						}.Build(), Ydb.Value_builder{
							TextValue: proto.String("5"),
						}.Build(), Ydb.Value_builder{
							BoolValue: proto.Bool(false),
						}.Build()},
					}.Build(),
				},
			}.Build(),
		}.Build(), nil)
		stream.EXPECT().Recv().Return(Ydb_Query.ExecuteQueryResponsePart_builder{
			Status:         Ydb.StatusIds_SUCCESS,
			ResultSetIndex: 2,
			ResultSet: Ydb.ResultSet_builder{
				Columns: []*Ydb.Column{
					Ydb.Column_builder{
						Name: "c",
						Type: Ydb.Type_builder{
							TypeId: Ydb.Type_UINT64.Enum(),
						}.Build(),
					}.Build(),
					Ydb.Column_builder{
						Name: "d",
						Type: Ydb.Type_builder{
							TypeId: Ydb.Type_UTF8.Enum(),
						}.Build(),
					}.Build(),
					Ydb.Column_builder{
						Name: "e",
						Type: Ydb.Type_builder{
							TypeId: Ydb.Type_BOOL.Enum(),
						}.Build(),
					}.Build(),
				},
				Rows: []*Ydb.Value{
					Ydb.Value_builder{
						Items: []*Ydb.Value{Ydb.Value_builder{
							Uint64Value: proto.Uint64(1),
						}.Build(), Ydb.Value_builder{
							TextValue: proto.String("1"),
						}.Build(), Ydb.Value_builder{
							BoolValue: proto.Bool(true),
						}.Build()},
					}.Build(),
					Ydb.Value_builder{
						Items: []*Ydb.Value{Ydb.Value_builder{
							Uint64Value: proto.Uint64(2),
						}.Build(), Ydb.Value_builder{
							TextValue: proto.String("2"),
						}.Build(), Ydb.Value_builder{
							BoolValue: proto.Bool(false),
						}.Build()},
					}.Build(),
				},
			}.Build(),
		}.Build(), nil)
		stream.EXPECT().Recv().Return(Ydb_Query.ExecuteQueryResponsePart_builder{
			Status:         Ydb.StatusIds_SUCCESS,
			ResultSetIndex: 2,
			ResultSet: Ydb.ResultSet_builder{
				Rows: []*Ydb.Value{
					Ydb.Value_builder{
						Items: []*Ydb.Value{Ydb.Value_builder{
							Uint64Value: proto.Uint64(3),
						}.Build(), Ydb.Value_builder{
							TextValue: proto.String("3"),
						}.Build(), Ydb.Value_builder{
							BoolValue: proto.Bool(true),
						}.Build()},
					}.Build(),
					Ydb.Value_builder{
						Items: []*Ydb.Value{Ydb.Value_builder{
							Uint64Value: proto.Uint64(4),
						}.Build(), Ydb.Value_builder{
							TextValue: proto.String("4"),
						}.Build(), Ydb.Value_builder{
							BoolValue: proto.Bool(false),
						}.Build()},
					}.Build(),
					Ydb.Value_builder{
						Items: []*Ydb.Value{Ydb.Value_builder{
							Uint64Value: proto.Uint64(5),
						}.Build(), Ydb.Value_builder{
							TextValue: proto.String("5"),
						}.Build(), Ydb.Value_builder{
							BoolValue: proto.Bool(false),
						}.Build()},
					}.Build(),
				},
			}.Build(),
		}.Build(), nil)
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
			r.Close(t.Context())
		}
		{
			t.Log("nextResultSet")
			rs, err := r.nextResultSet(t.Context())
			require.ErrorIs(t, err, io.EOF)
			require.Nil(t, rs)
		}
	})
	t.Run("InterruptStream", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		ctrl := gomock.NewController(t)
		stream := newExecuteQueryStreamMock(ctrl)
		stream.EXPECT().Recv().Return(Ydb_Query.ExecuteQueryResponsePart_builder{
			Status:         Ydb.StatusIds_SUCCESS,
			ResultSetIndex: 0,
			ResultSet: Ydb.ResultSet_builder{
				Columns: []*Ydb.Column{
					Ydb.Column_builder{
						Name: "a",
						Type: Ydb.Type_builder{
							TypeId: Ydb.Type_UINT64.Enum(),
						}.Build(),
					}.Build(),
					Ydb.Column_builder{
						Name: "b",
						Type: Ydb.Type_builder{
							TypeId: Ydb.Type_UTF8.Enum(),
						}.Build(),
					}.Build(),
				},
				Rows: []*Ydb.Value{
					Ydb.Value_builder{
						Items: []*Ydb.Value{Ydb.Value_builder{
							Uint64Value: proto.Uint64(1),
						}.Build(), Ydb.Value_builder{
							TextValue: proto.String("1"),
						}.Build()},
					}.Build(),
					Ydb.Value_builder{
						Items: []*Ydb.Value{Ydb.Value_builder{
							Uint64Value: proto.Uint64(2),
						}.Build(), Ydb.Value_builder{
							TextValue: proto.String("2"),
						}.Build()},
					}.Build(),
					Ydb.Value_builder{
						Items: []*Ydb.Value{Ydb.Value_builder{
							Uint64Value: proto.Uint64(3),
						}.Build(), Ydb.Value_builder{
							TextValue: proto.String("3"),
						}.Build()},
					}.Build(),
				},
			}.Build(),
		}.Build(), nil)
		stream.EXPECT().Recv().Return(nil, xerrors.Operation(xerrors.WithStatusCode(Ydb.StatusIds_CANCELLED)))
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
				_, err := rs.nextRow(t.Context())
				require.NoError(t, err)
				require.EqualValues(t, 2, rs.rowIndex)
			}
			{
				t.Log("next (row=4)")
				_, err := rs.nextRow(t.Context())
				require.Error(t, err)
				require.True(t, xerrors.IsOperationError(err, Ydb.StatusIds_CANCELLED))
			}
		}
		{
			t.Log("nextResultSet")
			_, err := r.nextResultSet(t.Context())
			require.Error(t, err)
			require.True(t, xerrors.IsOperationError(err, Ydb.StatusIds_CANCELLED))
		}
	})
	t.Run("WrongResultSetIndex", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		ctrl := gomock.NewController(t)
		stream := newExecuteQueryStreamMock(ctrl)
		stream.EXPECT().Recv().Return(Ydb_Query.ExecuteQueryResponsePart_builder{
			Status:         Ydb.StatusIds_SUCCESS,
			ResultSetIndex: 0,
			ResultSet: Ydb.ResultSet_builder{
				Columns: []*Ydb.Column{
					Ydb.Column_builder{
						Name: "a",
						Type: Ydb.Type_builder{
							TypeId: Ydb.Type_UINT64.Enum(),
						}.Build(),
					}.Build(),
					Ydb.Column_builder{
						Name: "b",
						Type: Ydb.Type_builder{
							TypeId: Ydb.Type_UTF8.Enum(),
						}.Build(),
					}.Build(),
				},
				Rows: []*Ydb.Value{
					Ydb.Value_builder{
						Items: []*Ydb.Value{Ydb.Value_builder{
							Uint64Value: proto.Uint64(1),
						}.Build(), Ydb.Value_builder{
							TextValue: proto.String("1"),
						}.Build()},
					}.Build(),
					Ydb.Value_builder{
						Items: []*Ydb.Value{Ydb.Value_builder{
							Uint64Value: proto.Uint64(2),
						}.Build(), Ydb.Value_builder{
							TextValue: proto.String("2"),
						}.Build()},
					}.Build(),
					Ydb.Value_builder{
						Items: []*Ydb.Value{Ydb.Value_builder{
							Uint64Value: proto.Uint64(3),
						}.Build(), Ydb.Value_builder{
							TextValue: proto.String("3"),
						}.Build()},
					}.Build(),
				},
			}.Build(),
		}.Build(), nil)
		stream.EXPECT().Recv().Return(Ydb_Query.ExecuteQueryResponsePart_builder{
			Status:         Ydb.StatusIds_SUCCESS,
			ResultSetIndex: 0,
			ResultSet: Ydb.ResultSet_builder{
				Rows: []*Ydb.Value{
					Ydb.Value_builder{
						Items: []*Ydb.Value{Ydb.Value_builder{
							Uint64Value: proto.Uint64(4),
						}.Build(), Ydb.Value_builder{
							TextValue: proto.String("4"),
						}.Build()},
					}.Build(),
					Ydb.Value_builder{
						Items: []*Ydb.Value{Ydb.Value_builder{
							Uint64Value: proto.Uint64(5),
						}.Build(), Ydb.Value_builder{
							TextValue: proto.String("5"),
						}.Build()},
					}.Build(),
				},
			}.Build(),
		}.Build(), nil)
		stream.EXPECT().Recv().Return(Ydb_Query.ExecuteQueryResponsePart_builder{
			Status:         Ydb.StatusIds_SUCCESS,
			ResultSetIndex: 2,
			ResultSet: Ydb.ResultSet_builder{
				Columns: []*Ydb.Column{
					Ydb.Column_builder{
						Name: "c",
						Type: Ydb.Type_builder{
							TypeId: Ydb.Type_UINT64.Enum(),
						}.Build(),
					}.Build(),
					Ydb.Column_builder{
						Name: "d",
						Type: Ydb.Type_builder{
							TypeId: Ydb.Type_UTF8.Enum(),
						}.Build(),
					}.Build(),
					Ydb.Column_builder{
						Name: "e",
						Type: Ydb.Type_builder{
							TypeId: Ydb.Type_BOOL.Enum(),
						}.Build(),
					}.Build(),
				},
				Rows: []*Ydb.Value{
					Ydb.Value_builder{
						Items: []*Ydb.Value{Ydb.Value_builder{
							Uint64Value: proto.Uint64(1),
						}.Build(), Ydb.Value_builder{
							TextValue: proto.String("1"),
						}.Build(), Ydb.Value_builder{
							BoolValue: proto.Bool(true),
						}.Build()},
					}.Build(),
					Ydb.Value_builder{
						Items: []*Ydb.Value{Ydb.Value_builder{
							Uint64Value: proto.Uint64(2),
						}.Build(), Ydb.Value_builder{
							TextValue: proto.String("2"),
						}.Build(), Ydb.Value_builder{
							BoolValue: proto.Bool(false),
						}.Build()},
					}.Build(),
				},
			}.Build(),
		}.Build(), nil)
		stream.EXPECT().Recv().Return(Ydb_Query.ExecuteQueryResponsePart_builder{
			Status:         Ydb.StatusIds_SUCCESS,
			ResultSetIndex: 2,
			ResultSet: Ydb.ResultSet_builder{
				Rows: []*Ydb.Value{
					Ydb.Value_builder{
						Items: []*Ydb.Value{Ydb.Value_builder{
							Uint64Value: proto.Uint64(3),
						}.Build(), Ydb.Value_builder{
							TextValue: proto.String("3"),
						}.Build(), Ydb.Value_builder{
							BoolValue: proto.Bool(true),
						}.Build()},
					}.Build(),
					Ydb.Value_builder{
						Items: []*Ydb.Value{Ydb.Value_builder{
							Uint64Value: proto.Uint64(4),
						}.Build(), Ydb.Value_builder{
							TextValue: proto.String("4"),
						}.Build(), Ydb.Value_builder{
							BoolValue: proto.Bool(false),
						}.Build()},
					}.Build(),
					Ydb.Value_builder{
						Items: []*Ydb.Value{Ydb.Value_builder{
							Uint64Value: proto.Uint64(5),
						}.Build(), Ydb.Value_builder{
							TextValue: proto.String("5"),
						}.Build(), Ydb.Value_builder{
							BoolValue: proto.Bool(false),
						}.Build()},
					}.Build(),
				},
			}.Build(),
		}.Build(), nil)
		stream.EXPECT().Recv().Return(Ydb_Query.ExecuteQueryResponsePart_builder{
			Status:         Ydb.StatusIds_SUCCESS,
			ResultSetIndex: 1,
			ResultSet: Ydb.ResultSet_builder{
				Columns: []*Ydb.Column{
					Ydb.Column_builder{
						Name: "c",
						Type: Ydb.Type_builder{
							TypeId: Ydb.Type_UINT64.Enum(),
						}.Build(),
					}.Build(),
					Ydb.Column_builder{
						Name: "d",
						Type: Ydb.Type_builder{
							TypeId: Ydb.Type_UTF8.Enum(),
						}.Build(),
					}.Build(),
					Ydb.Column_builder{
						Name: "e",
						Type: Ydb.Type_builder{
							TypeId: Ydb.Type_BOOL.Enum(),
						}.Build(),
					}.Build(),
				},
				Rows: []*Ydb.Value{
					Ydb.Value_builder{
						Items: []*Ydb.Value{Ydb.Value_builder{
							Uint64Value: proto.Uint64(1),
						}.Build(), Ydb.Value_builder{
							TextValue: proto.String("1"),
						}.Build(), Ydb.Value_builder{
							BoolValue: proto.Bool(true),
						}.Build()},
					}.Build(),
					Ydb.Value_builder{
						Items: []*Ydb.Value{Ydb.Value_builder{
							Uint64Value: proto.Uint64(2),
						}.Build(), Ydb.Value_builder{
							TextValue: proto.String("2"),
						}.Build(), Ydb.Value_builder{
							BoolValue: proto.Bool(false),
						}.Build()},
					}.Build(),
				},
			}.Build(),
		}.Build(), nil)
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

	t.Run("context canceling and closing issues", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		ctrl := gomock.NewController(t)
		stream := newExecuteQueryStreamMock(ctrl)
		stream.EXPECT().Recv().Return(Ydb_Query.ExecuteQueryResponsePart_builder{
			Status:    Ydb.StatusIds_SUCCESS,
			ResultSet: &Ydb.ResultSet{},
		}.Build(), nil).AnyTimes()

		r, err := newResult(ctx, stream, withStreamResultTrace(&trace.Query{
			OnResultNextPart: func(trace.QueryResultNextPartStartInfo) func(trace.QueryResultNextPartDoneInfo) {
				cancel()

				return func(trace.QueryResultNextPartDoneInfo) {}
			},
		}))
		require.ErrorIs(t, err, context.Canceled)
		require.Nil(t, r)
	})
}

func TestExactlyOneRowFromResult(t *testing.T) {
	ctx := t.Context()
	t.Run("HappyWay", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		stream := newExecuteQueryStreamMock(ctrl)
		stream.EXPECT().Recv().Return(Ydb_Query.ExecuteQueryResponsePart_builder{
			Status: Ydb.StatusIds_SUCCESS,
			TxMeta: Ydb_Query.TransactionMeta_builder{
				Id: "456",
			}.Build(),
			ResultSetIndex: 0,
			ResultSet: Ydb.ResultSet_builder{
				Columns: []*Ydb.Column{
					Ydb.Column_builder{
						Name: "a",
						Type: Ydb.Type_builder{
							TypeId: Ydb.Type_UINT64.Enum(),
						}.Build(),
					}.Build(),
					Ydb.Column_builder{
						Name: "b",
						Type: Ydb.Type_builder{
							TypeId: Ydb.Type_UTF8.Enum(),
						}.Build(),
					}.Build(),
				},
				Rows: []*Ydb.Value{
					Ydb.Value_builder{
						Items: []*Ydb.Value{Ydb.Value_builder{
							Uint64Value: proto.Uint64(1),
						}.Build(), Ydb.Value_builder{
							TextValue: proto.String("1"),
						}.Build()},
					}.Build(),
				},
			}.Build(),
		}.Build(), nil)
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
		stream := newExecuteQueryStreamMock(ctrl)
		stream.EXPECT().Recv().Return(Ydb_Query.ExecuteQueryResponsePart_builder{
			Status: Ydb.StatusIds_SUCCESS,
			TxMeta: Ydb_Query.TransactionMeta_builder{
				Id: "456",
			}.Build(),
			ResultSetIndex: 0,
			ResultSet: Ydb.ResultSet_builder{
				Columns: []*Ydb.Column{
					Ydb.Column_builder{
						Name: "a",
						Type: Ydb.Type_builder{
							TypeId: Ydb.Type_UINT64.Enum(),
						}.Build(),
					}.Build(),
					Ydb.Column_builder{
						Name: "b",
						Type: Ydb.Type_builder{
							TypeId: Ydb.Type_UTF8.Enum(),
						}.Build(),
					}.Build(),
				},
				Rows: []*Ydb.Value{
					Ydb.Value_builder{
						Items: []*Ydb.Value{Ydb.Value_builder{
							Uint64Value: proto.Uint64(1),
						}.Build(), Ydb.Value_builder{
							TextValue: proto.String("1"),
						}.Build()},
					}.Build(),
					Ydb.Value_builder{
						Items: []*Ydb.Value{Ydb.Value_builder{
							Uint64Value: proto.Uint64(2),
						}.Build(), Ydb.Value_builder{
							TextValue: proto.String("2"),
						}.Build()},
					}.Build(),
				},
			}.Build(),
		}.Build(), nil)
		r, err := newResult(ctx, stream, nil)
		require.NoError(t, err)

		row, err := exactlyOneRowFromResult(ctx, r)
		require.ErrorIs(t, err, ErrMoreThanOneRow)
		require.Nil(t, row)
	})
	t.Run("MoreThanOneRowErrorOnNextRow", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		stream := newExecuteQueryStreamMock(ctrl)
		stream.EXPECT().Recv().Return(Ydb_Query.ExecuteQueryResponsePart_builder{
			Status: Ydb.StatusIds_SUCCESS,
			TxMeta: Ydb_Query.TransactionMeta_builder{
				Id: "456",
			}.Build(),
			ResultSetIndex: 0,
			ResultSet: Ydb.ResultSet_builder{
				Columns: []*Ydb.Column{
					Ydb.Column_builder{
						Name: "a",
						Type: Ydb.Type_builder{
							TypeId: Ydb.Type_UINT64.Enum(),
						}.Build(),
					}.Build(),
					Ydb.Column_builder{
						Name: "b",
						Type: Ydb.Type_builder{
							TypeId: Ydb.Type_UTF8.Enum(),
						}.Build(),
					}.Build(),
				},
				Rows: []*Ydb.Value{
					Ydb.Value_builder{
						Items: []*Ydb.Value{Ydb.Value_builder{
							Uint64Value: proto.Uint64(1),
						}.Build(), Ydb.Value_builder{
							TextValue: proto.String("1"),
						}.Build()},
					}.Build(),
				},
			}.Build(),
		}.Build(), nil)
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
		stream := newExecuteQueryStreamMock(ctrl)
		stream.EXPECT().Recv().Return(Ydb_Query.ExecuteQueryResponsePart_builder{
			Status: Ydb.StatusIds_SUCCESS,
			TxMeta: Ydb_Query.TransactionMeta_builder{
				Id: "456",
			}.Build(),
			ResultSetIndex: 0,
			ResultSet: Ydb.ResultSet_builder{
				Columns: []*Ydb.Column{
					Ydb.Column_builder{
						Name: "a",
						Type: Ydb.Type_builder{
							TypeId: Ydb.Type_UINT64.Enum(),
						}.Build(),
					}.Build(),
					Ydb.Column_builder{
						Name: "b",
						Type: Ydb.Type_builder{
							TypeId: Ydb.Type_UTF8.Enum(),
						}.Build(),
					}.Build(),
				},
				Rows: []*Ydb.Value{
					Ydb.Value_builder{
						Items: []*Ydb.Value{Ydb.Value_builder{
							Uint64Value: proto.Uint64(1),
						}.Build(), Ydb.Value_builder{
							TextValue: proto.String("1"),
						}.Build()},
					}.Build(),
				},
			}.Build(),
		}.Build(), nil)
		stream.EXPECT().Recv().Return(Ydb_Query.ExecuteQueryResponsePart_builder{
			Status: Ydb.StatusIds_SUCCESS,
			TxMeta: Ydb_Query.TransactionMeta_builder{
				Id: "456",
			}.Build(),
			ResultSetIndex: 1,
			ResultSet: Ydb.ResultSet_builder{
				Columns: []*Ydb.Column{
					Ydb.Column_builder{
						Name: "a",
						Type: Ydb.Type_builder{
							TypeId: Ydb.Type_UINT64.Enum(),
						}.Build(),
					}.Build(),
					Ydb.Column_builder{
						Name: "b",
						Type: Ydb.Type_builder{
							TypeId: Ydb.Type_UTF8.Enum(),
						}.Build(),
					}.Build(),
				},
				Rows: []*Ydb.Value{
					Ydb.Value_builder{
						Items: []*Ydb.Value{Ydb.Value_builder{
							Uint64Value: proto.Uint64(1),
						}.Build(), Ydb.Value_builder{
							TextValue: proto.String("1"),
						}.Build()},
					}.Build(),
				},
			}.Build(),
		}.Build(), nil)
		r, err := newResult(ctx, stream, nil)
		require.NoError(t, err)

		row, err := exactlyOneResultSetFromResult(ctx, r)
		require.ErrorIs(t, err, ErrMoreThanOneResultSet)
		require.Nil(t, row)
	})
	t.Run("MoreThanOneResultSetErrorOnNextResultSet", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		stream := newExecuteQueryStreamMock(ctrl)
		stream.EXPECT().Recv().Return(Ydb_Query.ExecuteQueryResponsePart_builder{
			Status: Ydb.StatusIds_SUCCESS,
			TxMeta: Ydb_Query.TransactionMeta_builder{
				Id: "456",
			}.Build(),
			ResultSetIndex: 0,
			ResultSet: Ydb.ResultSet_builder{
				Columns: []*Ydb.Column{
					Ydb.Column_builder{
						Name: "a",
						Type: Ydb.Type_builder{
							TypeId: Ydb.Type_UINT64.Enum(),
						}.Build(),
					}.Build(),
					Ydb.Column_builder{
						Name: "b",
						Type: Ydb.Type_builder{
							TypeId: Ydb.Type_UTF8.Enum(),
						}.Build(),
					}.Build(),
				},
				Rows: []*Ydb.Value{
					Ydb.Value_builder{
						Items: []*Ydb.Value{Ydb.Value_builder{
							Uint64Value: proto.Uint64(1),
						}.Build(), Ydb.Value_builder{
							TextValue: proto.String("1"),
						}.Build()},
					}.Build(),
				},
			}.Build(),
		}.Build(), nil)
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
	ctx := t.Context()
	t.Run("HappyWay", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		stream := newExecuteQueryStreamMock(ctrl)
		stream.EXPECT().Recv().Return(Ydb_Query.ExecuteQueryResponsePart_builder{
			Status: Ydb.StatusIds_SUCCESS,
			TxMeta: Ydb_Query.TransactionMeta_builder{
				Id: "456",
			}.Build(),
			ResultSetIndex: 0,
			ResultSet: Ydb.ResultSet_builder{
				Columns: []*Ydb.Column{
					Ydb.Column_builder{
						Name: "a",
						Type: Ydb.Type_builder{
							TypeId: Ydb.Type_UINT64.Enum(),
						}.Build(),
					}.Build(),
					Ydb.Column_builder{
						Name: "b",
						Type: Ydb.Type_builder{
							TypeId: Ydb.Type_UTF8.Enum(),
						}.Build(),
					}.Build(),
				},
				Rows: []*Ydb.Value{
					Ydb.Value_builder{
						Items: []*Ydb.Value{Ydb.Value_builder{
							Uint64Value: proto.Uint64(1),
						}.Build(), Ydb.Value_builder{
							TextValue: proto.String("1"),
						}.Build()},
					}.Build(),
					Ydb.Value_builder{
						Items: []*Ydb.Value{Ydb.Value_builder{
							Uint64Value: proto.Uint64(2),
						}.Build(), Ydb.Value_builder{
							TextValue: proto.String("2"),
						}.Build()},
					}.Build(),
				},
			}.Build(),
		}.Build(), nil)
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
		stream := newExecuteQueryStreamMock(ctrl)
		stream.EXPECT().Recv().Return(Ydb_Query.ExecuteQueryResponsePart_builder{
			Status: Ydb.StatusIds_SUCCESS,
			TxMeta: Ydb_Query.TransactionMeta_builder{
				Id: "456",
			}.Build(),
			ResultSetIndex: 0,
			ResultSet: Ydb.ResultSet_builder{
				Columns: []*Ydb.Column{
					Ydb.Column_builder{
						Name: "a",
						Type: Ydb.Type_builder{
							TypeId: Ydb.Type_UINT64.Enum(),
						}.Build(),
					}.Build(),
					Ydb.Column_builder{
						Name: "b",
						Type: Ydb.Type_builder{
							TypeId: Ydb.Type_UTF8.Enum(),
						}.Build(),
					}.Build(),
				},
				Rows: []*Ydb.Value{
					Ydb.Value_builder{
						Items: []*Ydb.Value{Ydb.Value_builder{
							Uint64Value: proto.Uint64(1),
						}.Build(), Ydb.Value_builder{
							TextValue: proto.String("1"),
						}.Build()},
					}.Build(),
				},
			}.Build(),
		}.Build(), nil)
		stream.EXPECT().Recv().Return(Ydb_Query.ExecuteQueryResponsePart_builder{
			Status: Ydb.StatusIds_SUCCESS,
			TxMeta: Ydb_Query.TransactionMeta_builder{
				Id: "456",
			}.Build(),
			ResultSetIndex: 1,
			ResultSet: Ydb.ResultSet_builder{
				Columns: []*Ydb.Column{
					Ydb.Column_builder{
						Name: "a",
						Type: Ydb.Type_builder{
							TypeId: Ydb.Type_UINT64.Enum(),
						}.Build(),
					}.Build(),
					Ydb.Column_builder{
						Name: "b",
						Type: Ydb.Type_builder{
							TypeId: Ydb.Type_UTF8.Enum(),
						}.Build(),
					}.Build(),
				},
				Rows: []*Ydb.Value{
					Ydb.Value_builder{
						Items: []*Ydb.Value{Ydb.Value_builder{
							Uint64Value: proto.Uint64(1),
						}.Build(), Ydb.Value_builder{
							TextValue: proto.String("1"),
						}.Build()},
					}.Build(),
				},
			}.Build(),
		}.Build(), nil)
		r, err := newResult(ctx, stream, nil)
		require.NoError(t, err)

		rs, err := exactlyOneResultSetFromResult(ctx, r)
		require.ErrorIs(t, err, ErrMoreThanOneResultSet)
		require.Nil(t, rs)
	})
	t.Run("MoreThanOneResultSetErrorOnNextResultSet", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		stream := newExecuteQueryStreamMock(ctrl)
		stream.EXPECT().Recv().Return(Ydb_Query.ExecuteQueryResponsePart_builder{
			Status: Ydb.StatusIds_SUCCESS,
			TxMeta: Ydb_Query.TransactionMeta_builder{
				Id: "456",
			}.Build(),
			ResultSetIndex: 0,
			ResultSet: Ydb.ResultSet_builder{
				Columns: []*Ydb.Column{
					Ydb.Column_builder{
						Name: "a",
						Type: Ydb.Type_builder{
							TypeId: Ydb.Type_UINT64.Enum(),
						}.Build(),
					}.Build(),
					Ydb.Column_builder{
						Name: "b",
						Type: Ydb.Type_builder{
							TypeId: Ydb.Type_UTF8.Enum(),
						}.Build(),
					}.Build(),
				},
				Rows: []*Ydb.Value{
					Ydb.Value_builder{
						Items: []*Ydb.Value{Ydb.Value_builder{
							Uint64Value: proto.Uint64(1),
						}.Build(), Ydb.Value_builder{
							TextValue: proto.String("1"),
						}.Build()},
					}.Build(),
				},
			}.Build(),
		}.Build(), nil)
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
	ctx := t.Context()
	ctrl := gomock.NewController(t)
	stream := newExecuteQueryStreamMock(ctrl)
	stream.EXPECT().Recv().Return(Ydb_Query.ExecuteQueryResponsePart_builder{
		Status: Ydb.StatusIds_SUCCESS,
		TxMeta: Ydb_Query.TransactionMeta_builder{
			Id: "456",
		}.Build(),
		ResultSetIndex: 0,
		ResultSet: Ydb.ResultSet_builder{
			Columns: []*Ydb.Column{
				Ydb.Column_builder{
					Name: "a",
					Type: Ydb.Type_builder{
						TypeId: Ydb.Type_UINT64.Enum(),
					}.Build(),
				}.Build(),
				Ydb.Column_builder{
					Name: "b",
					Type: Ydb.Type_builder{
						TypeId: Ydb.Type_UTF8.Enum(),
					}.Build(),
				}.Build(),
			},
			Rows: []*Ydb.Value{
				Ydb.Value_builder{
					Items: []*Ydb.Value{Ydb.Value_builder{
						Uint64Value: proto.Uint64(1),
					}.Build(), Ydb.Value_builder{
						TextValue: proto.String("1"),
					}.Build()},
				}.Build(),
				Ydb.Value_builder{
					Items: []*Ydb.Value{Ydb.Value_builder{
						Uint64Value: proto.Uint64(2),
					}.Build(), Ydb.Value_builder{
						TextValue: proto.String("2"),
					}.Build()},
				}.Build(),
			},
		}.Build(),
	}.Build(), nil)
	stream.EXPECT().Recv().Return(Ydb_Query.ExecuteQueryResponsePart_builder{
		Status: Ydb.StatusIds_SUCCESS,
		TxMeta: Ydb_Query.TransactionMeta_builder{
			Id: "456",
		}.Build(),
		ResultSetIndex: 0,
		ResultSet: Ydb.ResultSet_builder{
			Columns: []*Ydb.Column{
				Ydb.Column_builder{
					Name: "a",
					Type: Ydb.Type_builder{
						TypeId: Ydb.Type_UINT64.Enum(),
					}.Build(),
				}.Build(),
				Ydb.Column_builder{
					Name: "b",
					Type: Ydb.Type_builder{
						TypeId: Ydb.Type_UTF8.Enum(),
					}.Build(),
				}.Build(),
			},
			Rows: []*Ydb.Value{
				Ydb.Value_builder{
					Items: []*Ydb.Value{Ydb.Value_builder{
						Uint64Value: proto.Uint64(3),
					}.Build(), Ydb.Value_builder{
						TextValue: proto.String("3"),
					}.Build()},
				}.Build(),
				Ydb.Value_builder{
					Items: []*Ydb.Value{Ydb.Value_builder{
						Uint64Value: proto.Uint64(4),
					}.Build(), Ydb.Value_builder{
						TextValue: proto.String("4"),
					}.Build()},
				}.Build(),
			},
		}.Build(),
	}.Build(), nil)
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
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			ctrl := gomock.NewController(t)
			stream := newExecuteQueryStreamMock(ctrl)
			stream.EXPECT().Recv().Return(Ydb_Query.ExecuteQueryResponsePart_builder{
				Status:         Ydb.StatusIds_SUCCESS,
				ResultSetIndex: 0,
				ResultSet: Ydb.ResultSet_builder{
					Columns: []*Ydb.Column{
						Ydb.Column_builder{
							Name: "a",
							Type: Ydb.Type_builder{
								TypeId: Ydb.Type_UINT64.Enum(),
							}.Build(),
						}.Build(),
						Ydb.Column_builder{
							Name: "b",
							Type: Ydb.Type_builder{
								TypeId: Ydb.Type_UTF8.Enum(),
							}.Build(),
						}.Build(),
					},
					Rows: []*Ydb.Value{
						Ydb.Value_builder{
							Items: []*Ydb.Value{Ydb.Value_builder{
								Uint64Value: proto.Uint64(1),
							}.Build(), Ydb.Value_builder{
								TextValue: proto.String("1"),
							}.Build()},
						}.Build(),
						Ydb.Value_builder{
							Items: []*Ydb.Value{Ydb.Value_builder{
								Uint64Value: proto.Uint64(2),
							}.Build(), Ydb.Value_builder{
								TextValue: proto.String("2"),
							}.Build()},
						}.Build(),
						Ydb.Value_builder{
							Items: []*Ydb.Value{Ydb.Value_builder{
								Uint64Value: proto.Uint64(3),
							}.Build(), Ydb.Value_builder{
								TextValue: proto.String("3"),
							}.Build()},
						}.Build(),
					},
				}.Build(),
			}.Build(), nil)
			stream.EXPECT().Recv().Return(Ydb_Query.ExecuteQueryResponsePart_builder{
				Status:         Ydb.StatusIds_SUCCESS,
				ResultSetIndex: 0,
				ResultSet: Ydb.ResultSet_builder{
					Rows: []*Ydb.Value{
						Ydb.Value_builder{
							Items: []*Ydb.Value{Ydb.Value_builder{
								Uint64Value: proto.Uint64(4),
							}.Build(), Ydb.Value_builder{
								TextValue: proto.String("4"),
							}.Build()},
						}.Build(),
						Ydb.Value_builder{
							Items: []*Ydb.Value{Ydb.Value_builder{
								Uint64Value: proto.Uint64(5),
							}.Build(), Ydb.Value_builder{
								TextValue: proto.String("5"),
							}.Build()},
						}.Build(),
					},
				}.Build(),
			}.Build(), nil)
			stream.EXPECT().Recv().Return(Ydb_Query.ExecuteQueryResponsePart_builder{
				Status:         Ydb.StatusIds_SUCCESS,
				ResultSetIndex: 1,
				ResultSet: Ydb.ResultSet_builder{
					Columns: []*Ydb.Column{
						Ydb.Column_builder{
							Name: "c",
							Type: Ydb.Type_builder{
								TypeId: Ydb.Type_UINT64.Enum(),
							}.Build(),
						}.Build(),
						Ydb.Column_builder{
							Name: "d",
							Type: Ydb.Type_builder{
								TypeId: Ydb.Type_UTF8.Enum(),
							}.Build(),
						}.Build(),
						Ydb.Column_builder{
							Name: "e",
							Type: Ydb.Type_builder{
								TypeId: Ydb.Type_BOOL.Enum(),
							}.Build(),
						}.Build(),
					},
					Rows: []*Ydb.Value{
						Ydb.Value_builder{
							Items: []*Ydb.Value{Ydb.Value_builder{
								Uint64Value: proto.Uint64(1),
							}.Build(), Ydb.Value_builder{
								TextValue: proto.String("1"),
							}.Build(), Ydb.Value_builder{
								BoolValue: proto.Bool(true),
							}.Build()},
						}.Build(),
						Ydb.Value_builder{
							Items: []*Ydb.Value{Ydb.Value_builder{
								Uint64Value: proto.Uint64(2),
							}.Build(), Ydb.Value_builder{
								TextValue: proto.String("2"),
							}.Build(), Ydb.Value_builder{
								BoolValue: proto.Bool(false),
							}.Build()},
						}.Build(),
					},
				}.Build(),
			}.Build(), nil)
			stream.EXPECT().Recv().Return(Ydb_Query.ExecuteQueryResponsePart_builder{
				Status:         Ydb.StatusIds_SUCCESS,
				ResultSetIndex: 1,
				ResultSet: Ydb.ResultSet_builder{
					Rows: []*Ydb.Value{
						Ydb.Value_builder{
							Items: []*Ydb.Value{Ydb.Value_builder{
								Uint64Value: proto.Uint64(3),
							}.Build(), Ydb.Value_builder{
								TextValue: proto.String("3"),
							}.Build(), Ydb.Value_builder{
								BoolValue: proto.Bool(true),
							}.Build()},
						}.Build(),
						Ydb.Value_builder{
							Items: []*Ydb.Value{Ydb.Value_builder{
								Uint64Value: proto.Uint64(4),
							}.Build(), Ydb.Value_builder{
								TextValue: proto.String("4"),
							}.Build(), Ydb.Value_builder{
								BoolValue: proto.Bool(false),
							}.Build()},
						}.Build(),
						Ydb.Value_builder{
							Items: []*Ydb.Value{Ydb.Value_builder{
								Uint64Value: proto.Uint64(5),
							}.Build(), Ydb.Value_builder{
								TextValue: proto.String("5"),
							}.Build(), Ydb.Value_builder{
								BoolValue: proto.Bool(false),
							}.Build()},
						}.Build(),
					},
				}.Build(),
			}.Build(), nil)
			stream.EXPECT().Recv().Return(Ydb_Query.ExecuteQueryResponsePart_builder{
				Status:         Ydb.StatusIds_SUCCESS,
				ResultSetIndex: 2,
				ResultSet: Ydb.ResultSet_builder{
					Columns: []*Ydb.Column{
						Ydb.Column_builder{
							Name: "c",
							Type: Ydb.Type_builder{
								TypeId: Ydb.Type_UINT64.Enum(),
							}.Build(),
						}.Build(),
						Ydb.Column_builder{
							Name: "d",
							Type: Ydb.Type_builder{
								TypeId: Ydb.Type_UTF8.Enum(),
							}.Build(),
						}.Build(),
						Ydb.Column_builder{
							Name: "e",
							Type: Ydb.Type_builder{
								TypeId: Ydb.Type_BOOL.Enum(),
							}.Build(),
						}.Build(),
					},
					Rows: []*Ydb.Value{
						Ydb.Value_builder{
							Items: []*Ydb.Value{Ydb.Value_builder{
								Uint64Value: proto.Uint64(1),
							}.Build(), Ydb.Value_builder{
								TextValue: proto.String("1"),
							}.Build(), Ydb.Value_builder{
								BoolValue: proto.Bool(true),
							}.Build()},
						}.Build(),
						Ydb.Value_builder{
							Items: []*Ydb.Value{Ydb.Value_builder{
								Uint64Value: proto.Uint64(2),
							}.Build(), Ydb.Value_builder{
								TextValue: proto.String("2"),
							}.Build(), Ydb.Value_builder{
								BoolValue: proto.Bool(false),
							}.Build()},
						}.Build(),
					},
				}.Build(),
			}.Build(), nil)
			stream.EXPECT().Recv().Return(Ydb_Query.ExecuteQueryResponsePart_builder{
				Status:         Ydb.StatusIds_SUCCESS,
				ResultSetIndex: 2,
				ResultSet: Ydb.ResultSet_builder{
					Rows: []*Ydb.Value{
						Ydb.Value_builder{
							Items: []*Ydb.Value{Ydb.Value_builder{
								Uint64Value: proto.Uint64(3),
							}.Build(), Ydb.Value_builder{
								TextValue: proto.String("3"),
							}.Build(), Ydb.Value_builder{
								BoolValue: proto.Bool(true),
							}.Build()},
						}.Build(),
						Ydb.Value_builder{
							Items: []*Ydb.Value{Ydb.Value_builder{
								Uint64Value: proto.Uint64(4),
							}.Build(), Ydb.Value_builder{
								TextValue: proto.String("4"),
							}.Build(), Ydb.Value_builder{
								BoolValue: proto.Bool(false),
							}.Build()},
						}.Build(),
						Ydb.Value_builder{
							Items: []*Ydb.Value{Ydb.Value_builder{
								Uint64Value: proto.Uint64(5),
							}.Build(), Ydb.Value_builder{
								TextValue: proto.String("5"),
							}.Build(), Ydb.Value_builder{
								BoolValue: proto.Bool(false),
							}.Build()},
						}.Build(),
					},
				}.Build(),
			}.Build(), nil)
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
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			ctrl := gomock.NewController(t)
			stream := newExecuteQueryStreamMock(ctrl)
			stream.EXPECT().Recv().Return(Ydb_Query.ExecuteQueryResponsePart_builder{
				Status:         Ydb.StatusIds_SUCCESS,
				ResultSetIndex: 0,
				ResultSet: Ydb.ResultSet_builder{
					Columns: []*Ydb.Column{
						Ydb.Column_builder{
							Name: "a",
							Type: Ydb.Type_builder{
								TypeId: Ydb.Type_UINT64.Enum(),
							}.Build(),
						}.Build(),
						Ydb.Column_builder{
							Name: "b",
							Type: Ydb.Type_builder{
								TypeId: Ydb.Type_UTF8.Enum(),
							}.Build(),
						}.Build(),
					},
					Rows: []*Ydb.Value{
						Ydb.Value_builder{
							Items: []*Ydb.Value{Ydb.Value_builder{
								Uint64Value: proto.Uint64(1),
							}.Build(), Ydb.Value_builder{
								TextValue: proto.String("1"),
							}.Build()},
						}.Build(),
						Ydb.Value_builder{
							Items: []*Ydb.Value{Ydb.Value_builder{
								Uint64Value: proto.Uint64(2),
							}.Build(), Ydb.Value_builder{
								TextValue: proto.String("2"),
							}.Build()},
						}.Build(),
						Ydb.Value_builder{
							Items: []*Ydb.Value{Ydb.Value_builder{
								Uint64Value: proto.Uint64(3),
							}.Build(), Ydb.Value_builder{
								TextValue: proto.String("3"),
							}.Build()},
						}.Build(),
					},
				}.Build(),
			}.Build(), nil)
			stream.EXPECT().Recv().Return(Ydb_Query.ExecuteQueryResponsePart_builder{
				Status:         Ydb.StatusIds_SUCCESS,
				ResultSetIndex: 0,
				ResultSet: Ydb.ResultSet_builder{
					Rows: []*Ydb.Value{
						Ydb.Value_builder{
							Items: []*Ydb.Value{Ydb.Value_builder{
								Uint64Value: proto.Uint64(4),
							}.Build(), Ydb.Value_builder{
								TextValue: proto.String("4"),
							}.Build()},
						}.Build(),
						Ydb.Value_builder{
							Items: []*Ydb.Value{Ydb.Value_builder{
								Uint64Value: proto.Uint64(5),
							}.Build(), Ydb.Value_builder{
								TextValue: proto.String("5"),
							}.Build()},
						}.Build(),
					},
				}.Build(),
			}.Build(), nil)
			stream.EXPECT().Recv().Return(Ydb_Query.ExecuteQueryResponsePart_builder{
				Status:         Ydb.StatusIds_SUCCESS,
				ResultSetIndex: 1,
				ResultSet: Ydb.ResultSet_builder{
					Columns: []*Ydb.Column{
						Ydb.Column_builder{
							Name: "c",
							Type: Ydb.Type_builder{
								TypeId: Ydb.Type_UINT64.Enum(),
							}.Build(),
						}.Build(),
						Ydb.Column_builder{
							Name: "d",
							Type: Ydb.Type_builder{
								TypeId: Ydb.Type_UTF8.Enum(),
							}.Build(),
						}.Build(),
						Ydb.Column_builder{
							Name: "e",
							Type: Ydb.Type_builder{
								TypeId: Ydb.Type_BOOL.Enum(),
							}.Build(),
						}.Build(),
					},
					Rows: []*Ydb.Value{
						Ydb.Value_builder{
							Items: []*Ydb.Value{Ydb.Value_builder{
								Uint64Value: proto.Uint64(1),
							}.Build(), Ydb.Value_builder{
								TextValue: proto.String("1"),
							}.Build(), Ydb.Value_builder{
								BoolValue: proto.Bool(true),
							}.Build()},
						}.Build(),
						Ydb.Value_builder{
							Items: []*Ydb.Value{Ydb.Value_builder{
								Uint64Value: proto.Uint64(2),
							}.Build(), Ydb.Value_builder{
								TextValue: proto.String("2"),
							}.Build(), Ydb.Value_builder{
								BoolValue: proto.Bool(false),
							}.Build()},
						}.Build(),
					},
				}.Build(),
			}.Build(), nil)
			stream.EXPECT().Recv().Return(Ydb_Query.ExecuteQueryResponsePart_builder{
				Status:         Ydb.StatusIds_SUCCESS,
				ResultSetIndex: 1,
				ResultSet: Ydb.ResultSet_builder{
					Rows: []*Ydb.Value{
						Ydb.Value_builder{
							Items: []*Ydb.Value{Ydb.Value_builder{
								Uint64Value: proto.Uint64(3),
							}.Build(), Ydb.Value_builder{
								TextValue: proto.String("3"),
							}.Build(), Ydb.Value_builder{
								BoolValue: proto.Bool(true),
							}.Build()},
						}.Build(),
						Ydb.Value_builder{
							Items: []*Ydb.Value{Ydb.Value_builder{
								Uint64Value: proto.Uint64(4),
							}.Build(), Ydb.Value_builder{
								TextValue: proto.String("4"),
							}.Build(), Ydb.Value_builder{
								BoolValue: proto.Bool(false),
							}.Build()},
						}.Build(),
						Ydb.Value_builder{
							Items: []*Ydb.Value{Ydb.Value_builder{
								Uint64Value: proto.Uint64(5),
							}.Build(), Ydb.Value_builder{
								TextValue: proto.String("5"),
							}.Build(), Ydb.Value_builder{
								BoolValue: proto.Bool(false),
							}.Build()},
						}.Build(),
					},
				}.Build(),
			}.Build(), nil)
			stream.EXPECT().Recv().Return(Ydb_Query.ExecuteQueryResponsePart_builder{
				Status:         Ydb.StatusIds_SUCCESS,
				ResultSetIndex: 2,
				ResultSet: Ydb.ResultSet_builder{
					Columns: []*Ydb.Column{
						Ydb.Column_builder{
							Name: "c",
							Type: Ydb.Type_builder{
								TypeId: Ydb.Type_UINT64.Enum(),
							}.Build(),
						}.Build(),
						Ydb.Column_builder{
							Name: "d",
							Type: Ydb.Type_builder{
								TypeId: Ydb.Type_UTF8.Enum(),
							}.Build(),
						}.Build(),
						Ydb.Column_builder{
							Name: "e",
							Type: Ydb.Type_builder{
								TypeId: Ydb.Type_BOOL.Enum(),
							}.Build(),
						}.Build(),
					},
					Rows: []*Ydb.Value{
						Ydb.Value_builder{
							Items: []*Ydb.Value{Ydb.Value_builder{
								Uint64Value: proto.Uint64(1),
							}.Build(), Ydb.Value_builder{
								TextValue: proto.String("1"),
							}.Build(), Ydb.Value_builder{
								BoolValue: proto.Bool(true),
							}.Build()},
						}.Build(),
						Ydb.Value_builder{
							Items: []*Ydb.Value{Ydb.Value_builder{
								Uint64Value: proto.Uint64(2),
							}.Build(), Ydb.Value_builder{
								TextValue: proto.String("2"),
							}.Build(), Ydb.Value_builder{
								BoolValue: proto.Bool(false),
							}.Build()},
						}.Build(),
					},
				}.Build(),
			}.Build(), nil)
			stream.EXPECT().Recv().Return(Ydb_Query.ExecuteQueryResponsePart_builder{
				Status:         Ydb.StatusIds_SUCCESS,
				ResultSetIndex: 2,
				ResultSet: Ydb.ResultSet_builder{
					Rows: []*Ydb.Value{
						Ydb.Value_builder{
							Items: []*Ydb.Value{Ydb.Value_builder{
								Uint64Value: proto.Uint64(3),
							}.Build(), Ydb.Value_builder{
								TextValue: proto.String("3"),
							}.Build(), Ydb.Value_builder{
								BoolValue: proto.Bool(true),
							}.Build()},
						}.Build(),
						Ydb.Value_builder{
							Items: []*Ydb.Value{Ydb.Value_builder{
								Uint64Value: proto.Uint64(4),
							}.Build(), Ydb.Value_builder{
								TextValue: proto.String("4"),
							}.Build(), Ydb.Value_builder{
								BoolValue: proto.Bool(false),
							}.Build()},
						}.Build(),
						Ydb.Value_builder{
							Items: []*Ydb.Value{Ydb.Value_builder{
								Uint64Value: proto.Uint64(5),
							}.Build(), Ydb.Value_builder{
								TextValue: proto.String("5"),
							}.Build(), Ydb.Value_builder{
								BoolValue: proto.Bool(false),
							}.Build()},
						}.Build(),
					},
				}.Build(),
			}.Build(), nil)
			stream.EXPECT().Recv().Return(Ydb_Query.ExecuteQueryResponsePart_builder{
				Status:         Ydb.StatusIds_SUCCESS,
				ResultSetIndex: 0,
				ExecStats: Ydb_TableStats.QueryStats_builder{
					ProcessCpuTimeUs: 300,
					QueryPlan:        "123",
					QueryAst:         "456",
					TotalDurationUs:  100,
					TotalCpuTimeUs:   200,
				}.Build(),
			}.Build(), nil)
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
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			ctrl := gomock.NewController(t)
			stream := newExecuteQueryStreamMock(ctrl)
			stream.EXPECT().Recv().Return(Ydb_Query.ExecuteQueryResponsePart_builder{
				Status:         Ydb.StatusIds_SUCCESS,
				ResultSetIndex: 0,
				ResultSet: Ydb.ResultSet_builder{
					Columns: []*Ydb.Column{
						Ydb.Column_builder{
							Name: "a",
							Type: Ydb.Type_builder{
								TypeId: Ydb.Type_UINT64.Enum(),
							}.Build(),
						}.Build(),
						Ydb.Column_builder{
							Name: "b",
							Type: Ydb.Type_builder{
								TypeId: Ydb.Type_UTF8.Enum(),
							}.Build(),
						}.Build(),
					},
					Rows: []*Ydb.Value{
						Ydb.Value_builder{
							Items: []*Ydb.Value{Ydb.Value_builder{
								Uint64Value: proto.Uint64(1),
							}.Build(), Ydb.Value_builder{
								TextValue: proto.String("1"),
							}.Build()},
						}.Build(),
						Ydb.Value_builder{
							Items: []*Ydb.Value{Ydb.Value_builder{
								Uint64Value: proto.Uint64(2),
							}.Build(), Ydb.Value_builder{
								TextValue: proto.String("2"),
							}.Build()},
						}.Build(),
						Ydb.Value_builder{
							Items: []*Ydb.Value{Ydb.Value_builder{
								Uint64Value: proto.Uint64(3),
							}.Build(), Ydb.Value_builder{
								TextValue: proto.String("3"),
							}.Build()},
						}.Build(),
					},
				}.Build(),
			}.Build(), nil)
			stream.EXPECT().Recv().Return(Ydb_Query.ExecuteQueryResponsePart_builder{
				Status:         Ydb.StatusIds_SUCCESS,
				ResultSetIndex: 0,
				ResultSet: Ydb.ResultSet_builder{
					Rows: []*Ydb.Value{
						Ydb.Value_builder{
							Items: []*Ydb.Value{Ydb.Value_builder{
								Uint64Value: proto.Uint64(4),
							}.Build(), Ydb.Value_builder{
								TextValue: proto.String("4"),
							}.Build()},
						}.Build(),
						Ydb.Value_builder{
							Items: []*Ydb.Value{Ydb.Value_builder{
								Uint64Value: proto.Uint64(5),
							}.Build(), Ydb.Value_builder{
								TextValue: proto.String("5"),
							}.Build()},
						}.Build(),
					},
				}.Build(),
			}.Build(), nil)
			stream.EXPECT().Recv().Return(Ydb_Query.ExecuteQueryResponsePart_builder{
				Status:         Ydb.StatusIds_SUCCESS,
				ResultSetIndex: 1,
				ResultSet: Ydb.ResultSet_builder{
					Columns: []*Ydb.Column{
						Ydb.Column_builder{
							Name: "c",
							Type: Ydb.Type_builder{
								TypeId: Ydb.Type_UINT64.Enum(),
							}.Build(),
						}.Build(),
						Ydb.Column_builder{
							Name: "d",
							Type: Ydb.Type_builder{
								TypeId: Ydb.Type_UTF8.Enum(),
							}.Build(),
						}.Build(),
						Ydb.Column_builder{
							Name: "e",
							Type: Ydb.Type_builder{
								TypeId: Ydb.Type_BOOL.Enum(),
							}.Build(),
						}.Build(),
					},
					Rows: []*Ydb.Value{
						Ydb.Value_builder{
							Items: []*Ydb.Value{Ydb.Value_builder{
								Uint64Value: proto.Uint64(1),
							}.Build(), Ydb.Value_builder{
								TextValue: proto.String("1"),
							}.Build(), Ydb.Value_builder{
								BoolValue: proto.Bool(true),
							}.Build()},
						}.Build(),
						Ydb.Value_builder{
							Items: []*Ydb.Value{Ydb.Value_builder{
								Uint64Value: proto.Uint64(2),
							}.Build(), Ydb.Value_builder{
								TextValue: proto.String("2"),
							}.Build(), Ydb.Value_builder{
								BoolValue: proto.Bool(false),
							}.Build()},
						}.Build(),
					},
				}.Build(),
			}.Build(), nil)
			stream.EXPECT().Recv().Return(Ydb_Query.ExecuteQueryResponsePart_builder{
				Status:         Ydb.StatusIds_SUCCESS,
				ResultSetIndex: 1,
				ResultSet: Ydb.ResultSet_builder{
					Rows: []*Ydb.Value{
						Ydb.Value_builder{
							Items: []*Ydb.Value{Ydb.Value_builder{
								Uint64Value: proto.Uint64(3),
							}.Build(), Ydb.Value_builder{
								TextValue: proto.String("3"),
							}.Build(), Ydb.Value_builder{
								BoolValue: proto.Bool(true),
							}.Build()},
						}.Build(),
						Ydb.Value_builder{
							Items: []*Ydb.Value{Ydb.Value_builder{
								Uint64Value: proto.Uint64(4),
							}.Build(), Ydb.Value_builder{
								TextValue: proto.String("4"),
							}.Build(), Ydb.Value_builder{
								BoolValue: proto.Bool(false),
							}.Build()},
						}.Build(),
						Ydb.Value_builder{
							Items: []*Ydb.Value{Ydb.Value_builder{
								Uint64Value: proto.Uint64(5),
							}.Build(), Ydb.Value_builder{
								TextValue: proto.String("5"),
							}.Build(), Ydb.Value_builder{
								BoolValue: proto.Bool(false),
							}.Build()},
						}.Build(),
					},
				}.Build(),
			}.Build(), nil)
			stream.EXPECT().Recv().Return(Ydb_Query.ExecuteQueryResponsePart_builder{
				Status:         Ydb.StatusIds_SUCCESS,
				ResultSetIndex: 2,
				ResultSet: Ydb.ResultSet_builder{
					Columns: []*Ydb.Column{
						Ydb.Column_builder{
							Name: "c",
							Type: Ydb.Type_builder{
								TypeId: Ydb.Type_UINT64.Enum(),
							}.Build(),
						}.Build(),
						Ydb.Column_builder{
							Name: "d",
							Type: Ydb.Type_builder{
								TypeId: Ydb.Type_UTF8.Enum(),
							}.Build(),
						}.Build(),
						Ydb.Column_builder{
							Name: "e",
							Type: Ydb.Type_builder{
								TypeId: Ydb.Type_BOOL.Enum(),
							}.Build(),
						}.Build(),
					},
					Rows: []*Ydb.Value{
						Ydb.Value_builder{
							Items: []*Ydb.Value{Ydb.Value_builder{
								Uint64Value: proto.Uint64(1),
							}.Build(), Ydb.Value_builder{
								TextValue: proto.String("1"),
							}.Build(), Ydb.Value_builder{
								BoolValue: proto.Bool(true),
							}.Build()},
						}.Build(),
						Ydb.Value_builder{
							Items: []*Ydb.Value{Ydb.Value_builder{
								Uint64Value: proto.Uint64(2),
							}.Build(), Ydb.Value_builder{
								TextValue: proto.String("2"),
							}.Build(), Ydb.Value_builder{
								BoolValue: proto.Bool(false),
							}.Build()},
						}.Build(),
					},
				}.Build(),
			}.Build(), nil)
			stream.EXPECT().Recv().Return(Ydb_Query.ExecuteQueryResponsePart_builder{
				Status:         Ydb.StatusIds_SUCCESS,
				ResultSetIndex: 2,
				ResultSet: Ydb.ResultSet_builder{
					Rows: []*Ydb.Value{
						Ydb.Value_builder{
							Items: []*Ydb.Value{Ydb.Value_builder{
								Uint64Value: proto.Uint64(3),
							}.Build(), Ydb.Value_builder{
								TextValue: proto.String("3"),
							}.Build(), Ydb.Value_builder{
								BoolValue: proto.Bool(true),
							}.Build()},
						}.Build(),
						Ydb.Value_builder{
							Items: []*Ydb.Value{Ydb.Value_builder{
								Uint64Value: proto.Uint64(4),
							}.Build(), Ydb.Value_builder{
								TextValue: proto.String("4"),
							}.Build(), Ydb.Value_builder{
								BoolValue: proto.Bool(false),
							}.Build()},
						}.Build(),
						Ydb.Value_builder{
							Items: []*Ydb.Value{Ydb.Value_builder{
								Uint64Value: proto.Uint64(5),
							}.Build(), Ydb.Value_builder{
								TextValue: proto.String("5"),
							}.Build(), Ydb.Value_builder{
								BoolValue: proto.Bool(false),
							}.Build()},
						}.Build(),
					},
				}.Build(),
				ExecStats: Ydb_TableStats.QueryStats_builder{
					ProcessCpuTimeUs: 300,
					QueryPlan:        "123",
					QueryAst:         "456",
					TotalDurationUs:  100,
					TotalCpuTimeUs:   200,
				}.Build(),
			}.Build(), nil)
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
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			ctrl := gomock.NewController(t)
			stream := newExecuteQueryStreamMock(ctrl)
			stream.EXPECT().Recv().Return(Ydb_Query.ExecuteQueryResponsePart_builder{
				Status:         Ydb.StatusIds_SUCCESS,
				ResultSetIndex: 0,
				ResultSet: Ydb.ResultSet_builder{
					Columns: []*Ydb.Column{
						Ydb.Column_builder{
							Name: "a",
							Type: Ydb.Type_builder{
								TypeId: Ydb.Type_UINT64.Enum(),
							}.Build(),
						}.Build(),
						Ydb.Column_builder{
							Name: "b",
							Type: Ydb.Type_builder{
								TypeId: Ydb.Type_UTF8.Enum(),
							}.Build(),
						}.Build(),
					},
					Rows: []*Ydb.Value{
						Ydb.Value_builder{
							Items: []*Ydb.Value{Ydb.Value_builder{
								Uint64Value: proto.Uint64(1),
							}.Build(), Ydb.Value_builder{
								TextValue: proto.String("1"),
							}.Build()},
						}.Build(),
						Ydb.Value_builder{
							Items: []*Ydb.Value{Ydb.Value_builder{
								Uint64Value: proto.Uint64(2),
							}.Build(), Ydb.Value_builder{
								TextValue: proto.String("2"),
							}.Build()},
						}.Build(),
						Ydb.Value_builder{
							Items: []*Ydb.Value{Ydb.Value_builder{
								Uint64Value: proto.Uint64(3),
							}.Build(), Ydb.Value_builder{
								TextValue: proto.String("3"),
							}.Build()},
						}.Build(),
					},
				}.Build(),
				ExecStats: Ydb_TableStats.QueryStats_builder{
					ProcessCpuTimeUs: 1,
					QueryPlan:        "sdsd",
					QueryAst:         "sdffds",
					TotalDurationUs:  2,
					TotalCpuTimeUs:   3,
				}.Build(),
			}.Build(), nil)
			stream.EXPECT().Recv().Return(Ydb_Query.ExecuteQueryResponsePart_builder{
				Status:         Ydb.StatusIds_SUCCESS,
				ResultSetIndex: 0,
				ResultSet: Ydb.ResultSet_builder{
					Rows: []*Ydb.Value{
						Ydb.Value_builder{
							Items: []*Ydb.Value{Ydb.Value_builder{
								Uint64Value: proto.Uint64(4),
							}.Build(), Ydb.Value_builder{
								TextValue: proto.String("4"),
							}.Build()},
						}.Build(),
						Ydb.Value_builder{
							Items: []*Ydb.Value{Ydb.Value_builder{
								Uint64Value: proto.Uint64(5),
							}.Build(), Ydb.Value_builder{
								TextValue: proto.String("5"),
							}.Build()},
						}.Build(),
					},
				}.Build(),
				ExecStats: Ydb_TableStats.QueryStats_builder{
					ProcessCpuTimeUs: 332400,
					QueryPlan:        "12wesfse3",
					QueryAst:         "sedfefs",
					TotalDurationUs:  10324320,
					TotalCpuTimeUs:   234,
				}.Build(),
			}.Build(), nil)
			stream.EXPECT().Recv().Return(Ydb_Query.ExecuteQueryResponsePart_builder{
				Status:         Ydb.StatusIds_SUCCESS,
				ResultSetIndex: 1,
				ResultSet: Ydb.ResultSet_builder{
					Columns: []*Ydb.Column{
						Ydb.Column_builder{
							Name: "c",
							Type: Ydb.Type_builder{
								TypeId: Ydb.Type_UINT64.Enum(),
							}.Build(),
						}.Build(),
						Ydb.Column_builder{
							Name: "d",
							Type: Ydb.Type_builder{
								TypeId: Ydb.Type_UTF8.Enum(),
							}.Build(),
						}.Build(),
						Ydb.Column_builder{
							Name: "e",
							Type: Ydb.Type_builder{
								TypeId: Ydb.Type_BOOL.Enum(),
							}.Build(),
						}.Build(),
					},
					Rows: []*Ydb.Value{
						Ydb.Value_builder{
							Items: []*Ydb.Value{Ydb.Value_builder{
								Uint64Value: proto.Uint64(1),
							}.Build(), Ydb.Value_builder{
								TextValue: proto.String("1"),
							}.Build(), Ydb.Value_builder{
								BoolValue: proto.Bool(true),
							}.Build()},
						}.Build(),
						Ydb.Value_builder{
							Items: []*Ydb.Value{Ydb.Value_builder{
								Uint64Value: proto.Uint64(2),
							}.Build(), Ydb.Value_builder{
								TextValue: proto.String("2"),
							}.Build(), Ydb.Value_builder{
								BoolValue: proto.Bool(false),
							}.Build()},
						}.Build(),
					},
				}.Build(),
				ExecStats: Ydb_TableStats.QueryStats_builder{
					ProcessCpuTimeUs: 323400,
					QueryPlan:        "sdfgrdg",
					QueryAst:         "sdgsrg",
					TotalDurationUs:  43554,
					TotalCpuTimeUs:   235643,
				}.Build(),
			}.Build(), nil)
			stream.EXPECT().Recv().Return(Ydb_Query.ExecuteQueryResponsePart_builder{
				Status:         Ydb.StatusIds_SUCCESS,
				ResultSetIndex: 1,
				ResultSet: Ydb.ResultSet_builder{
					Rows: []*Ydb.Value{
						Ydb.Value_builder{
							Items: []*Ydb.Value{Ydb.Value_builder{
								Uint64Value: proto.Uint64(3),
							}.Build(), Ydb.Value_builder{
								TextValue: proto.String("3"),
							}.Build(), Ydb.Value_builder{
								BoolValue: proto.Bool(true),
							}.Build()},
						}.Build(),
						Ydb.Value_builder{
							Items: []*Ydb.Value{Ydb.Value_builder{
								Uint64Value: proto.Uint64(4),
							}.Build(), Ydb.Value_builder{
								TextValue: proto.String("4"),
							}.Build(), Ydb.Value_builder{
								BoolValue: proto.Bool(false),
							}.Build()},
						}.Build(),
						Ydb.Value_builder{
							Items: []*Ydb.Value{Ydb.Value_builder{
								Uint64Value: proto.Uint64(5),
							}.Build(), Ydb.Value_builder{
								TextValue: proto.String("5"),
							}.Build(), Ydb.Value_builder{
								BoolValue: proto.Bool(false),
							}.Build()},
						}.Build(),
					},
				}.Build(),
				ExecStats: Ydb_TableStats.QueryStats_builder{
					ProcessCpuTimeUs: 3034564570,
					QueryPlan:        "sdgsrgsg",
					QueryAst:         "dghytjf",
					TotalDurationUs:  45676,
					TotalCpuTimeUs:   4562342,
				}.Build(),
			}.Build(), nil)
			stream.EXPECT().Recv().Return(Ydb_Query.ExecuteQueryResponsePart_builder{
				Status:         Ydb.StatusIds_SUCCESS,
				ResultSetIndex: 2,
				ResultSet: Ydb.ResultSet_builder{
					Columns: []*Ydb.Column{
						Ydb.Column_builder{
							Name: "c",
							Type: Ydb.Type_builder{
								TypeId: Ydb.Type_UINT64.Enum(),
							}.Build(),
						}.Build(),
						Ydb.Column_builder{
							Name: "d",
							Type: Ydb.Type_builder{
								TypeId: Ydb.Type_UTF8.Enum(),
							}.Build(),
						}.Build(),
						Ydb.Column_builder{
							Name: "e",
							Type: Ydb.Type_builder{
								TypeId: Ydb.Type_BOOL.Enum(),
							}.Build(),
						}.Build(),
					},
					Rows: []*Ydb.Value{
						Ydb.Value_builder{
							Items: []*Ydb.Value{Ydb.Value_builder{
								Uint64Value: proto.Uint64(1),
							}.Build(), Ydb.Value_builder{
								TextValue: proto.String("1"),
							}.Build(), Ydb.Value_builder{
								BoolValue: proto.Bool(true),
							}.Build()},
						}.Build(),
						Ydb.Value_builder{
							Items: []*Ydb.Value{Ydb.Value_builder{
								Uint64Value: proto.Uint64(2),
							}.Build(), Ydb.Value_builder{
								TextValue: proto.String("2"),
							}.Build(), Ydb.Value_builder{
								BoolValue: proto.Bool(false),
							}.Build()},
						}.Build(),
					},
				}.Build(),
				ExecStats: Ydb_TableStats.QueryStats_builder{
					ProcessCpuTimeUs: 300,
					QueryPlan:        "123",
					QueryAst:         "456",
					TotalDurationUs:  100,
					TotalCpuTimeUs:   200,
				}.Build(),
			}.Build(), nil)
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
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			ctrl := gomock.NewController(t)
			stream := newExecuteQueryStreamMock(ctrl)
			stream.EXPECT().Recv().Return(Ydb_Query.ExecuteQueryResponsePart_builder{
				Status:         Ydb.StatusIds_SUCCESS,
				ResultSetIndex: 0,
				ResultSet: Ydb.ResultSet_builder{
					Columns: []*Ydb.Column{
						Ydb.Column_builder{
							Name: "a",
							Type: Ydb.Type_builder{
								TypeId: Ydb.Type_UINT64.Enum(),
							}.Build(),
						}.Build(),
						Ydb.Column_builder{
							Name: "b",
							Type: Ydb.Type_builder{
								TypeId: Ydb.Type_UTF8.Enum(),
							}.Build(),
						}.Build(),
					},
					Rows: []*Ydb.Value{
						Ydb.Value_builder{
							Items: []*Ydb.Value{Ydb.Value_builder{
								Uint64Value: proto.Uint64(1),
							}.Build(), Ydb.Value_builder{
								TextValue: proto.String("1"),
							}.Build()},
						}.Build(),
						Ydb.Value_builder{
							Items: []*Ydb.Value{Ydb.Value_builder{
								Uint64Value: proto.Uint64(2),
							}.Build(), Ydb.Value_builder{
								TextValue: proto.String("2"),
							}.Build()},
						}.Build(),
						Ydb.Value_builder{
							Items: []*Ydb.Value{Ydb.Value_builder{
								Uint64Value: proto.Uint64(3),
							}.Build(), Ydb.Value_builder{
								TextValue: proto.String("3"),
							}.Build()},
						}.Build(),
					},
				}.Build(),
			}.Build(), nil)
			stream.EXPECT().Recv().Return(Ydb_Query.ExecuteQueryResponsePart_builder{
				Status:         Ydb.StatusIds_SUCCESS,
				ResultSetIndex: 0,
				ResultSet: Ydb.ResultSet_builder{
					Rows: []*Ydb.Value{
						Ydb.Value_builder{
							Items: []*Ydb.Value{Ydb.Value_builder{
								Uint64Value: proto.Uint64(4),
							}.Build(), Ydb.Value_builder{
								TextValue: proto.String("4"),
							}.Build()},
						}.Build(),
						Ydb.Value_builder{
							Items: []*Ydb.Value{Ydb.Value_builder{
								Uint64Value: proto.Uint64(5),
							}.Build(), Ydb.Value_builder{
								TextValue: proto.String("5"),
							}.Build()},
						}.Build(),
					},
				}.Build(),
			}.Build(), nil)
			stream.EXPECT().Recv().Return(Ydb_Query.ExecuteQueryResponsePart_builder{
				Status:         Ydb.StatusIds_SUCCESS,
				ResultSetIndex: 1,
				ResultSet: Ydb.ResultSet_builder{
					Columns: []*Ydb.Column{
						Ydb.Column_builder{
							Name: "c",
							Type: Ydb.Type_builder{
								TypeId: Ydb.Type_UINT64.Enum(),
							}.Build(),
						}.Build(),
						Ydb.Column_builder{
							Name: "d",
							Type: Ydb.Type_builder{
								TypeId: Ydb.Type_UTF8.Enum(),
							}.Build(),
						}.Build(),
						Ydb.Column_builder{
							Name: "e",
							Type: Ydb.Type_builder{
								TypeId: Ydb.Type_BOOL.Enum(),
							}.Build(),
						}.Build(),
					},
					Rows: []*Ydb.Value{
						Ydb.Value_builder{
							Items: []*Ydb.Value{Ydb.Value_builder{
								Uint64Value: proto.Uint64(1),
							}.Build(), Ydb.Value_builder{
								TextValue: proto.String("1"),
							}.Build(), Ydb.Value_builder{
								BoolValue: proto.Bool(true),
							}.Build()},
						}.Build(),
						Ydb.Value_builder{
							Items: []*Ydb.Value{Ydb.Value_builder{
								Uint64Value: proto.Uint64(2),
							}.Build(), Ydb.Value_builder{
								TextValue: proto.String("2"),
							}.Build(), Ydb.Value_builder{
								BoolValue: proto.Bool(false),
							}.Build()},
						}.Build(),
					},
				}.Build(),
			}.Build(), nil)
			stream.EXPECT().Recv().Return(Ydb_Query.ExecuteQueryResponsePart_builder{
				Status:         Ydb.StatusIds_SUCCESS,
				ResultSetIndex: 1,
				ResultSet: Ydb.ResultSet_builder{
					Rows: []*Ydb.Value{
						Ydb.Value_builder{
							Items: []*Ydb.Value{Ydb.Value_builder{
								Uint64Value: proto.Uint64(3),
							}.Build(), Ydb.Value_builder{
								TextValue: proto.String("3"),
							}.Build(), Ydb.Value_builder{
								BoolValue: proto.Bool(true),
							}.Build()},
						}.Build(),
						Ydb.Value_builder{
							Items: []*Ydb.Value{Ydb.Value_builder{
								Uint64Value: proto.Uint64(4),
							}.Build(), Ydb.Value_builder{
								TextValue: proto.String("4"),
							}.Build(), Ydb.Value_builder{
								BoolValue: proto.Bool(false),
							}.Build()},
						}.Build(),
						Ydb.Value_builder{
							Items: []*Ydb.Value{Ydb.Value_builder{
								Uint64Value: proto.Uint64(5),
							}.Build(), Ydb.Value_builder{
								TextValue: proto.String("5"),
							}.Build(), Ydb.Value_builder{
								BoolValue: proto.Bool(false),
							}.Build()},
						}.Build(),
					},
				}.Build(),
			}.Build(), nil)
			stream.EXPECT().Recv().Return(Ydb_Query.ExecuteQueryResponsePart_builder{
				Status:         Ydb.StatusIds_SUCCESS,
				ResultSetIndex: 2,
				ResultSet: Ydb.ResultSet_builder{
					Columns: []*Ydb.Column{
						Ydb.Column_builder{
							Name: "c",
							Type: Ydb.Type_builder{
								TypeId: Ydb.Type_UINT64.Enum(),
							}.Build(),
						}.Build(),
						Ydb.Column_builder{
							Name: "d",
							Type: Ydb.Type_builder{
								TypeId: Ydb.Type_UTF8.Enum(),
							}.Build(),
						}.Build(),
						Ydb.Column_builder{
							Name: "e",
							Type: Ydb.Type_builder{
								TypeId: Ydb.Type_BOOL.Enum(),
							}.Build(),
						}.Build(),
					},
					Rows: []*Ydb.Value{
						Ydb.Value_builder{
							Items: []*Ydb.Value{Ydb.Value_builder{
								Uint64Value: proto.Uint64(1),
							}.Build(), Ydb.Value_builder{
								TextValue: proto.String("1"),
							}.Build(), Ydb.Value_builder{
								BoolValue: proto.Bool(true),
							}.Build()},
						}.Build(),
						Ydb.Value_builder{
							Items: []*Ydb.Value{Ydb.Value_builder{
								Uint64Value: proto.Uint64(2),
							}.Build(), Ydb.Value_builder{
								TextValue: proto.String("2"),
							}.Build(), Ydb.Value_builder{
								BoolValue: proto.Bool(false),
							}.Build()},
						}.Build(),
					},
				}.Build(),
			}.Build(), nil)
			stream.EXPECT().Recv().Return(Ydb_Query.ExecuteQueryResponsePart_builder{
				Status:         Ydb.StatusIds_SUCCESS,
				ResultSetIndex: 2,
				ResultSet: Ydb.ResultSet_builder{
					Rows: []*Ydb.Value{
						Ydb.Value_builder{
							Items: []*Ydb.Value{Ydb.Value_builder{
								Uint64Value: proto.Uint64(3),
							}.Build(), Ydb.Value_builder{
								TextValue: proto.String("3"),
							}.Build(), Ydb.Value_builder{
								BoolValue: proto.Bool(true),
							}.Build()},
						}.Build(),
						Ydb.Value_builder{
							Items: []*Ydb.Value{Ydb.Value_builder{
								Uint64Value: proto.Uint64(4),
							}.Build(), Ydb.Value_builder{
								TextValue: proto.String("4"),
							}.Build(), Ydb.Value_builder{
								BoolValue: proto.Bool(false),
							}.Build()},
						}.Build(),
						Ydb.Value_builder{
							Items: []*Ydb.Value{Ydb.Value_builder{
								Uint64Value: proto.Uint64(5),
							}.Build(), Ydb.Value_builder{
								TextValue: proto.String("5"),
							}.Build(), Ydb.Value_builder{
								BoolValue: proto.Bool(false),
							}.Build()},
						}.Build(),
					},
				}.Build(),
			}.Build(), nil)
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
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			ctrl := gomock.NewController(t)
			stream := newExecuteQueryStreamMock(ctrl)
			stream.EXPECT().Recv().Return(Ydb_Query.ExecuteQueryResponsePart_builder{
				Status:         Ydb.StatusIds_SUCCESS,
				ResultSetIndex: 0,
				ResultSet: Ydb.ResultSet_builder{
					Columns: []*Ydb.Column{
						Ydb.Column_builder{
							Name: "a",
							Type: Ydb.Type_builder{
								TypeId: Ydb.Type_UINT64.Enum(),
							}.Build(),
						}.Build(),
						Ydb.Column_builder{
							Name: "b",
							Type: Ydb.Type_builder{
								TypeId: Ydb.Type_UTF8.Enum(),
							}.Build(),
						}.Build(),
					},
					Rows: []*Ydb.Value{
						Ydb.Value_builder{
							Items: []*Ydb.Value{Ydb.Value_builder{
								Uint64Value: proto.Uint64(1),
							}.Build(), Ydb.Value_builder{
								TextValue: proto.String("1"),
							}.Build()},
						}.Build(),
						Ydb.Value_builder{
							Items: []*Ydb.Value{Ydb.Value_builder{
								Uint64Value: proto.Uint64(2),
							}.Build(), Ydb.Value_builder{
								TextValue: proto.String("2"),
							}.Build()},
						}.Build(),
						Ydb.Value_builder{
							Items: []*Ydb.Value{Ydb.Value_builder{
								Uint64Value: proto.Uint64(3),
							}.Build(), Ydb.Value_builder{
								TextValue: proto.String("3"),
							}.Build()},
						}.Build(),
					},
				}.Build(),
			}.Build(), nil)
			stream.EXPECT().Recv().Return(Ydb_Query.ExecuteQueryResponsePart_builder{
				Status:         Ydb.StatusIds_SUCCESS,
				ResultSetIndex: 0,
				ResultSet: Ydb.ResultSet_builder{
					Rows: []*Ydb.Value{
						Ydb.Value_builder{
							Items: []*Ydb.Value{Ydb.Value_builder{
								Uint64Value: proto.Uint64(4),
							}.Build(), Ydb.Value_builder{
								TextValue: proto.String("4"),
							}.Build()},
						}.Build(),
						Ydb.Value_builder{
							Items: []*Ydb.Value{Ydb.Value_builder{
								Uint64Value: proto.Uint64(5),
							}.Build(), Ydb.Value_builder{
								TextValue: proto.String("5"),
							}.Build()},
						}.Build(),
					},
				}.Build(),
			}.Build(), nil)
			stream.EXPECT().Recv().Return(Ydb_Query.ExecuteQueryResponsePart_builder{
				Status:         Ydb.StatusIds_SUCCESS,
				ResultSetIndex: 1,
				ResultSet: Ydb.ResultSet_builder{
					Columns: []*Ydb.Column{
						Ydb.Column_builder{
							Name: "c",
							Type: Ydb.Type_builder{
								TypeId: Ydb.Type_UINT64.Enum(),
							}.Build(),
						}.Build(),
						Ydb.Column_builder{
							Name: "d",
							Type: Ydb.Type_builder{
								TypeId: Ydb.Type_UTF8.Enum(),
							}.Build(),
						}.Build(),
						Ydb.Column_builder{
							Name: "e",
							Type: Ydb.Type_builder{
								TypeId: Ydb.Type_BOOL.Enum(),
							}.Build(),
						}.Build(),
					},
					Rows: []*Ydb.Value{
						Ydb.Value_builder{
							Items: []*Ydb.Value{Ydb.Value_builder{
								Uint64Value: proto.Uint64(1),
							}.Build(), Ydb.Value_builder{
								TextValue: proto.String("1"),
							}.Build(), Ydb.Value_builder{
								BoolValue: proto.Bool(true),
							}.Build()},
						}.Build(),
						Ydb.Value_builder{
							Items: []*Ydb.Value{Ydb.Value_builder{
								Uint64Value: proto.Uint64(2),
							}.Build(), Ydb.Value_builder{
								TextValue: proto.String("2"),
							}.Build(), Ydb.Value_builder{
								BoolValue: proto.Bool(false),
							}.Build()},
						}.Build(),
					},
				}.Build(),
			}.Build(), nil)
			stream.EXPECT().Recv().Return(Ydb_Query.ExecuteQueryResponsePart_builder{
				Status:         Ydb.StatusIds_SUCCESS,
				ResultSetIndex: 1,
				ResultSet: Ydb.ResultSet_builder{
					Rows: []*Ydb.Value{
						Ydb.Value_builder{
							Items: []*Ydb.Value{Ydb.Value_builder{
								Uint64Value: proto.Uint64(3),
							}.Build(), Ydb.Value_builder{
								TextValue: proto.String("3"),
							}.Build(), Ydb.Value_builder{
								BoolValue: proto.Bool(true),
							}.Build()},
						}.Build(),
						Ydb.Value_builder{
							Items: []*Ydb.Value{Ydb.Value_builder{
								Uint64Value: proto.Uint64(4),
							}.Build(), Ydb.Value_builder{
								TextValue: proto.String("4"),
							}.Build(), Ydb.Value_builder{
								BoolValue: proto.Bool(false),
							}.Build()},
						}.Build(),
						Ydb.Value_builder{
							Items: []*Ydb.Value{Ydb.Value_builder{
								Uint64Value: proto.Uint64(5),
							}.Build(), Ydb.Value_builder{
								TextValue: proto.String("5"),
							}.Build(), Ydb.Value_builder{
								BoolValue: proto.Bool(false),
							}.Build()},
						}.Build(),
					},
				}.Build(),
			}.Build(), nil)
			stream.EXPECT().Recv().Return(Ydb_Query.ExecuteQueryResponsePart_builder{
				Status:         Ydb.StatusIds_SUCCESS,
				ResultSetIndex: 2,
				ResultSet: Ydb.ResultSet_builder{
					Columns: []*Ydb.Column{
						Ydb.Column_builder{
							Name: "c",
							Type: Ydb.Type_builder{
								TypeId: Ydb.Type_UINT64.Enum(),
							}.Build(),
						}.Build(),
						Ydb.Column_builder{
							Name: "d",
							Type: Ydb.Type_builder{
								TypeId: Ydb.Type_UTF8.Enum(),
							}.Build(),
						}.Build(),
						Ydb.Column_builder{
							Name: "e",
							Type: Ydb.Type_builder{
								TypeId: Ydb.Type_BOOL.Enum(),
							}.Build(),
						}.Build(),
					},
					Rows: []*Ydb.Value{
						Ydb.Value_builder{
							Items: []*Ydb.Value{Ydb.Value_builder{
								Uint64Value: proto.Uint64(1),
							}.Build(), Ydb.Value_builder{
								TextValue: proto.String("1"),
							}.Build(), Ydb.Value_builder{
								BoolValue: proto.Bool(true),
							}.Build()},
						}.Build(),
						Ydb.Value_builder{
							Items: []*Ydb.Value{Ydb.Value_builder{
								Uint64Value: proto.Uint64(2),
							}.Build(), Ydb.Value_builder{
								TextValue: proto.String("2"),
							}.Build(), Ydb.Value_builder{
								BoolValue: proto.Bool(false),
							}.Build()},
						}.Build(),
					},
				}.Build(),
			}.Build(), nil)
			stream.EXPECT().Recv().Return(Ydb_Query.ExecuteQueryResponsePart_builder{
				Status:         Ydb.StatusIds_SUCCESS,
				ResultSetIndex: 2,
				ResultSet: Ydb.ResultSet_builder{
					Rows: []*Ydb.Value{
						Ydb.Value_builder{
							Items: []*Ydb.Value{Ydb.Value_builder{
								Uint64Value: proto.Uint64(3),
							}.Build(), Ydb.Value_builder{
								TextValue: proto.String("3"),
							}.Build(), Ydb.Value_builder{
								BoolValue: proto.Bool(true),
							}.Build()},
						}.Build(),
						Ydb.Value_builder{
							Items: []*Ydb.Value{Ydb.Value_builder{
								Uint64Value: proto.Uint64(4),
							}.Build(), Ydb.Value_builder{
								TextValue: proto.String("4"),
							}.Build(), Ydb.Value_builder{
								BoolValue: proto.Bool(false),
							}.Build()},
						}.Build(),
						Ydb.Value_builder{
							Items: []*Ydb.Value{Ydb.Value_builder{
								Uint64Value: proto.Uint64(5),
							}.Build(), Ydb.Value_builder{
								TextValue: proto.String("5"),
							}.Build(), Ydb.Value_builder{
								BoolValue: proto.Bool(false),
							}.Build()},
						}.Build(),
					},
				}.Build(),
			}.Build(), nil)
			stream.EXPECT().Recv().Return(Ydb_Query.ExecuteQueryResponsePart_builder{
				Status:         Ydb.StatusIds_SUCCESS,
				ResultSetIndex: 0,
				ExecStats: Ydb_TableStats.QueryStats_builder{
					ProcessCpuTimeUs: 300,
					QueryPlan:        "123",
					QueryAst:         "456",
					TotalDurationUs:  100,
					TotalCpuTimeUs:   200,
				}.Build(),
			}.Build(), nil)
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
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			ctrl := gomock.NewController(t)
			stream := newExecuteQueryStreamMock(ctrl)
			stream.EXPECT().Recv().Return(Ydb_Query.ExecuteQueryResponsePart_builder{
				Status:         Ydb.StatusIds_SUCCESS,
				ResultSetIndex: 0,
				ResultSet: Ydb.ResultSet_builder{
					Columns: []*Ydb.Column{
						Ydb.Column_builder{
							Name: "a",
							Type: Ydb.Type_builder{
								TypeId: Ydb.Type_UINT64.Enum(),
							}.Build(),
						}.Build(),
						Ydb.Column_builder{
							Name: "b",
							Type: Ydb.Type_builder{
								TypeId: Ydb.Type_UTF8.Enum(),
							}.Build(),
						}.Build(),
					},
					Rows: []*Ydb.Value{
						Ydb.Value_builder{
							Items: []*Ydb.Value{Ydb.Value_builder{
								Uint64Value: proto.Uint64(1),
							}.Build(), Ydb.Value_builder{
								TextValue: proto.String("1"),
							}.Build()},
						}.Build(),
						Ydb.Value_builder{
							Items: []*Ydb.Value{Ydb.Value_builder{
								Uint64Value: proto.Uint64(2),
							}.Build(), Ydb.Value_builder{
								TextValue: proto.String("2"),
							}.Build()},
						}.Build(),
						Ydb.Value_builder{
							Items: []*Ydb.Value{Ydb.Value_builder{
								Uint64Value: proto.Uint64(3),
							}.Build(), Ydb.Value_builder{
								TextValue: proto.String("3"),
							}.Build()},
						}.Build(),
					},
				}.Build(),
			}.Build(), nil)
			stream.EXPECT().Recv().Return(Ydb_Query.ExecuteQueryResponsePart_builder{
				Status:         Ydb.StatusIds_SUCCESS,
				ResultSetIndex: 0,
				ResultSet: Ydb.ResultSet_builder{
					Rows: []*Ydb.Value{
						Ydb.Value_builder{
							Items: []*Ydb.Value{Ydb.Value_builder{
								Uint64Value: proto.Uint64(4),
							}.Build(), Ydb.Value_builder{
								TextValue: proto.String("4"),
							}.Build()},
						}.Build(),
						Ydb.Value_builder{
							Items: []*Ydb.Value{Ydb.Value_builder{
								Uint64Value: proto.Uint64(5),
							}.Build(), Ydb.Value_builder{
								TextValue: proto.String("5"),
							}.Build()},
						}.Build(),
					},
				}.Build(),
			}.Build(), nil)
			stream.EXPECT().Recv().Return(Ydb_Query.ExecuteQueryResponsePart_builder{
				Status:         Ydb.StatusIds_SUCCESS,
				ResultSetIndex: 1,
				ResultSet: Ydb.ResultSet_builder{
					Columns: []*Ydb.Column{
						Ydb.Column_builder{
							Name: "c",
							Type: Ydb.Type_builder{
								TypeId: Ydb.Type_UINT64.Enum(),
							}.Build(),
						}.Build(),
						Ydb.Column_builder{
							Name: "d",
							Type: Ydb.Type_builder{
								TypeId: Ydb.Type_UTF8.Enum(),
							}.Build(),
						}.Build(),
						Ydb.Column_builder{
							Name: "e",
							Type: Ydb.Type_builder{
								TypeId: Ydb.Type_BOOL.Enum(),
							}.Build(),
						}.Build(),
					},
					Rows: []*Ydb.Value{
						Ydb.Value_builder{
							Items: []*Ydb.Value{Ydb.Value_builder{
								Uint64Value: proto.Uint64(1),
							}.Build(), Ydb.Value_builder{
								TextValue: proto.String("1"),
							}.Build(), Ydb.Value_builder{
								BoolValue: proto.Bool(true),
							}.Build()},
						}.Build(),
						Ydb.Value_builder{
							Items: []*Ydb.Value{Ydb.Value_builder{
								Uint64Value: proto.Uint64(2),
							}.Build(), Ydb.Value_builder{
								TextValue: proto.String("2"),
							}.Build(), Ydb.Value_builder{
								BoolValue: proto.Bool(false),
							}.Build()},
						}.Build(),
					},
				}.Build(),
			}.Build(), nil)
			stream.EXPECT().Recv().Return(Ydb_Query.ExecuteQueryResponsePart_builder{
				Status:         Ydb.StatusIds_SUCCESS,
				ResultSetIndex: 1,
				ResultSet: Ydb.ResultSet_builder{
					Rows: []*Ydb.Value{
						Ydb.Value_builder{
							Items: []*Ydb.Value{Ydb.Value_builder{
								Uint64Value: proto.Uint64(3),
							}.Build(), Ydb.Value_builder{
								TextValue: proto.String("3"),
							}.Build(), Ydb.Value_builder{
								BoolValue: proto.Bool(true),
							}.Build()},
						}.Build(),
						Ydb.Value_builder{
							Items: []*Ydb.Value{Ydb.Value_builder{
								Uint64Value: proto.Uint64(4),
							}.Build(), Ydb.Value_builder{
								TextValue: proto.String("4"),
							}.Build(), Ydb.Value_builder{
								BoolValue: proto.Bool(false),
							}.Build()},
						}.Build(),
						Ydb.Value_builder{
							Items: []*Ydb.Value{Ydb.Value_builder{
								Uint64Value: proto.Uint64(5),
							}.Build(), Ydb.Value_builder{
								TextValue: proto.String("5"),
							}.Build(), Ydb.Value_builder{
								BoolValue: proto.Bool(false),
							}.Build()},
						}.Build(),
					},
				}.Build(),
			}.Build(), nil)
			stream.EXPECT().Recv().Return(Ydb_Query.ExecuteQueryResponsePart_builder{
				Status:         Ydb.StatusIds_SUCCESS,
				ResultSetIndex: 2,
				ResultSet: Ydb.ResultSet_builder{
					Columns: []*Ydb.Column{
						Ydb.Column_builder{
							Name: "c",
							Type: Ydb.Type_builder{
								TypeId: Ydb.Type_UINT64.Enum(),
							}.Build(),
						}.Build(),
						Ydb.Column_builder{
							Name: "d",
							Type: Ydb.Type_builder{
								TypeId: Ydb.Type_UTF8.Enum(),
							}.Build(),
						}.Build(),
						Ydb.Column_builder{
							Name: "e",
							Type: Ydb.Type_builder{
								TypeId: Ydb.Type_BOOL.Enum(),
							}.Build(),
						}.Build(),
					},
					Rows: []*Ydb.Value{
						Ydb.Value_builder{
							Items: []*Ydb.Value{Ydb.Value_builder{
								Uint64Value: proto.Uint64(1),
							}.Build(), Ydb.Value_builder{
								TextValue: proto.String("1"),
							}.Build(), Ydb.Value_builder{
								BoolValue: proto.Bool(true),
							}.Build()},
						}.Build(),
						Ydb.Value_builder{
							Items: []*Ydb.Value{Ydb.Value_builder{
								Uint64Value: proto.Uint64(2),
							}.Build(), Ydb.Value_builder{
								TextValue: proto.String("2"),
							}.Build(), Ydb.Value_builder{
								BoolValue: proto.Bool(false),
							}.Build()},
						}.Build(),
					},
				}.Build(),
			}.Build(), nil)
			stream.EXPECT().Recv().Return(Ydb_Query.ExecuteQueryResponsePart_builder{
				Status:         Ydb.StatusIds_SUCCESS,
				ResultSetIndex: 2,
				ResultSet: Ydb.ResultSet_builder{
					Rows: []*Ydb.Value{
						Ydb.Value_builder{
							Items: []*Ydb.Value{Ydb.Value_builder{
								Uint64Value: proto.Uint64(3),
							}.Build(), Ydb.Value_builder{
								TextValue: proto.String("3"),
							}.Build(), Ydb.Value_builder{
								BoolValue: proto.Bool(true),
							}.Build()},
						}.Build(),
						Ydb.Value_builder{
							Items: []*Ydb.Value{Ydb.Value_builder{
								Uint64Value: proto.Uint64(4),
							}.Build(), Ydb.Value_builder{
								TextValue: proto.String("4"),
							}.Build(), Ydb.Value_builder{
								BoolValue: proto.Bool(false),
							}.Build()},
						}.Build(),
						Ydb.Value_builder{
							Items: []*Ydb.Value{Ydb.Value_builder{
								Uint64Value: proto.Uint64(5),
							}.Build(), Ydb.Value_builder{
								TextValue: proto.String("5"),
							}.Build(), Ydb.Value_builder{
								BoolValue: proto.Bool(false),
							}.Build()},
						}.Build(),
					},
				}.Build(),
				ExecStats: Ydb_TableStats.QueryStats_builder{
					ProcessCpuTimeUs: 300,
					QueryPlan:        "123",
					QueryAst:         "456",
					TotalDurationUs:  100,
					TotalCpuTimeUs:   200,
				}.Build(),
			}.Build(), nil)
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
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			ctrl := gomock.NewController(t)
			stream := newExecuteQueryStreamMock(ctrl)
			stream.EXPECT().Recv().Return(Ydb_Query.ExecuteQueryResponsePart_builder{
				Status:         Ydb.StatusIds_SUCCESS,
				ResultSetIndex: 0,
				ResultSet: Ydb.ResultSet_builder{
					Columns: []*Ydb.Column{
						Ydb.Column_builder{
							Name: "a",
							Type: Ydb.Type_builder{
								TypeId: Ydb.Type_UINT64.Enum(),
							}.Build(),
						}.Build(),
						Ydb.Column_builder{
							Name: "b",
							Type: Ydb.Type_builder{
								TypeId: Ydb.Type_UTF8.Enum(),
							}.Build(),
						}.Build(),
					},
					Rows: []*Ydb.Value{
						Ydb.Value_builder{
							Items: []*Ydb.Value{Ydb.Value_builder{
								Uint64Value: proto.Uint64(1),
							}.Build(), Ydb.Value_builder{
								TextValue: proto.String("1"),
							}.Build()},
						}.Build(),
						Ydb.Value_builder{
							Items: []*Ydb.Value{Ydb.Value_builder{
								Uint64Value: proto.Uint64(2),
							}.Build(), Ydb.Value_builder{
								TextValue: proto.String("2"),
							}.Build()},
						}.Build(),
						Ydb.Value_builder{
							Items: []*Ydb.Value{Ydb.Value_builder{
								Uint64Value: proto.Uint64(3),
							}.Build(), Ydb.Value_builder{
								TextValue: proto.String("3"),
							}.Build()},
						}.Build(),
					},
				}.Build(),
				ExecStats: Ydb_TableStats.QueryStats_builder{
					ProcessCpuTimeUs: 1,
					QueryPlan:        "sdsd",
					QueryAst:         "sdffds",
					TotalDurationUs:  2,
					TotalCpuTimeUs:   3,
				}.Build(),
			}.Build(), nil)
			stream.EXPECT().Recv().Return(Ydb_Query.ExecuteQueryResponsePart_builder{
				Status:         Ydb.StatusIds_SUCCESS,
				ResultSetIndex: 0,
				ResultSet: Ydb.ResultSet_builder{
					Rows: []*Ydb.Value{
						Ydb.Value_builder{
							Items: []*Ydb.Value{Ydb.Value_builder{
								Uint64Value: proto.Uint64(4),
							}.Build(), Ydb.Value_builder{
								TextValue: proto.String("4"),
							}.Build()},
						}.Build(),
						Ydb.Value_builder{
							Items: []*Ydb.Value{Ydb.Value_builder{
								Uint64Value: proto.Uint64(5),
							}.Build(), Ydb.Value_builder{
								TextValue: proto.String("5"),
							}.Build()},
						}.Build(),
					},
				}.Build(),
				ExecStats: Ydb_TableStats.QueryStats_builder{
					ProcessCpuTimeUs: 332400,
					QueryPlan:        "12wesfse3",
					QueryAst:         "sedfefs",
					TotalDurationUs:  10324320,
					TotalCpuTimeUs:   234,
				}.Build(),
			}.Build(), nil)
			stream.EXPECT().Recv().Return(Ydb_Query.ExecuteQueryResponsePart_builder{
				Status:         Ydb.StatusIds_SUCCESS,
				ResultSetIndex: 1,
				ResultSet: Ydb.ResultSet_builder{
					Columns: []*Ydb.Column{
						Ydb.Column_builder{
							Name: "c",
							Type: Ydb.Type_builder{
								TypeId: Ydb.Type_UINT64.Enum(),
							}.Build(),
						}.Build(),
						Ydb.Column_builder{
							Name: "d",
							Type: Ydb.Type_builder{
								TypeId: Ydb.Type_UTF8.Enum(),
							}.Build(),
						}.Build(),
						Ydb.Column_builder{
							Name: "e",
							Type: Ydb.Type_builder{
								TypeId: Ydb.Type_BOOL.Enum(),
							}.Build(),
						}.Build(),
					},
					Rows: []*Ydb.Value{
						Ydb.Value_builder{
							Items: []*Ydb.Value{Ydb.Value_builder{
								Uint64Value: proto.Uint64(1),
							}.Build(), Ydb.Value_builder{
								TextValue: proto.String("1"),
							}.Build(), Ydb.Value_builder{
								BoolValue: proto.Bool(true),
							}.Build()},
						}.Build(),
						Ydb.Value_builder{
							Items: []*Ydb.Value{Ydb.Value_builder{
								Uint64Value: proto.Uint64(2),
							}.Build(), Ydb.Value_builder{
								TextValue: proto.String("2"),
							}.Build(), Ydb.Value_builder{
								BoolValue: proto.Bool(false),
							}.Build()},
						}.Build(),
					},
				}.Build(),
				ExecStats: Ydb_TableStats.QueryStats_builder{
					ProcessCpuTimeUs: 323400,
					QueryPlan:        "sdfgrdg",
					QueryAst:         "sdgsrg",
					TotalDurationUs:  43554,
					TotalCpuTimeUs:   235643,
				}.Build(),
			}.Build(), nil)
			stream.EXPECT().Recv().Return(Ydb_Query.ExecuteQueryResponsePart_builder{
				Status:         Ydb.StatusIds_SUCCESS,
				ResultSetIndex: 1,
				ResultSet: Ydb.ResultSet_builder{
					Rows: []*Ydb.Value{
						Ydb.Value_builder{
							Items: []*Ydb.Value{Ydb.Value_builder{
								Uint64Value: proto.Uint64(3),
							}.Build(), Ydb.Value_builder{
								TextValue: proto.String("3"),
							}.Build(), Ydb.Value_builder{
								BoolValue: proto.Bool(true),
							}.Build()},
						}.Build(),
						Ydb.Value_builder{
							Items: []*Ydb.Value{Ydb.Value_builder{
								Uint64Value: proto.Uint64(4),
							}.Build(), Ydb.Value_builder{
								TextValue: proto.String("4"),
							}.Build(), Ydb.Value_builder{
								BoolValue: proto.Bool(false),
							}.Build()},
						}.Build(),
						Ydb.Value_builder{
							Items: []*Ydb.Value{Ydb.Value_builder{
								Uint64Value: proto.Uint64(5),
							}.Build(), Ydb.Value_builder{
								TextValue: proto.String("5"),
							}.Build(), Ydb.Value_builder{
								BoolValue: proto.Bool(false),
							}.Build()},
						}.Build(),
					},
				}.Build(),
				ExecStats: Ydb_TableStats.QueryStats_builder{
					ProcessCpuTimeUs: 3034564570,
					QueryPlan:        "sdgsrgsg",
					QueryAst:         "dghytjf",
					TotalDurationUs:  45676,
					TotalCpuTimeUs:   4562342,
				}.Build(),
			}.Build(), nil)
			stream.EXPECT().Recv().Return(Ydb_Query.ExecuteQueryResponsePart_builder{
				Status:         Ydb.StatusIds_SUCCESS,
				ResultSetIndex: 2,
				ResultSet: Ydb.ResultSet_builder{
					Columns: []*Ydb.Column{
						Ydb.Column_builder{
							Name: "c",
							Type: Ydb.Type_builder{
								TypeId: Ydb.Type_UINT64.Enum(),
							}.Build(),
						}.Build(),
						Ydb.Column_builder{
							Name: "d",
							Type: Ydb.Type_builder{
								TypeId: Ydb.Type_UTF8.Enum(),
							}.Build(),
						}.Build(),
						Ydb.Column_builder{
							Name: "e",
							Type: Ydb.Type_builder{
								TypeId: Ydb.Type_BOOL.Enum(),
							}.Build(),
						}.Build(),
					},
					Rows: []*Ydb.Value{
						Ydb.Value_builder{
							Items: []*Ydb.Value{Ydb.Value_builder{
								Uint64Value: proto.Uint64(1),
							}.Build(), Ydb.Value_builder{
								TextValue: proto.String("1"),
							}.Build(), Ydb.Value_builder{
								BoolValue: proto.Bool(true),
							}.Build()},
						}.Build(),
						Ydb.Value_builder{
							Items: []*Ydb.Value{Ydb.Value_builder{
								Uint64Value: proto.Uint64(2),
							}.Build(), Ydb.Value_builder{
								TextValue: proto.String("2"),
							}.Build(), Ydb.Value_builder{
								BoolValue: proto.Bool(false),
							}.Build()},
						}.Build(),
					},
				}.Build(),
				ExecStats: Ydb_TableStats.QueryStats_builder{
					ProcessCpuTimeUs: 300,
					QueryPlan:        "123",
					QueryAst:         "456",
					TotalDurationUs:  100,
					TotalCpuTimeUs:   200,
				}.Build(),
			}.Build(), nil)
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
