//go:build go1.23

package query

import (
	"context"
	"fmt"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Query"
	"go.uber.org/mock/gomock"
	grpcCodes "google.golang.org/grpc/codes"
	grpcStatus "google.golang.org/grpc/status"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/pkg/xtest"
	"google.golang.org/protobuf/proto"
)

func TestResultSetRangeRows(t *testing.T) {
	ctx := xtest.Context(t)
	ctrl := gomock.NewController(t)
	t.Run("EmptyResultSet", func(t *testing.T) {
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
				Rows: []*Ydb.Value{},
			}.Build(),
		}.Build(), nil)
		stream.EXPECT().Recv().Return(nil, io.EOF)
		recv, err := stream.Recv()
		require.NoError(t, err)
		rs := newResultSet(func() (*Ydb_Query.ExecuteQueryResponsePart, error) {
			part, err := stream.Recv()
			if err != nil {
				return nil, xerrors.WithStackTrace(err)
			}

			return part, nil
		}, recv)
		require.EqualValues(t, 0, rs.index)
		count := 0
		for _, err := range rs.Rows(ctx) {
			require.NoError(t, err)
			count++
		}
		require.EqualValues(t, 0, count)
	})
	t.Run("SecondResultSetEmpty", func(t *testing.T) {
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
				Rows: []*Ydb.Value{},
			}.Build(),
		}.Build(), nil)
		stream.EXPECT().Recv().Return(nil, io.EOF)
		recv, err := stream.Recv()
		require.NoError(t, err)
		rs := newResultSet(func() (*Ydb_Query.ExecuteQueryResponsePart, error) {
			part, err := stream.Recv()
			if err != nil {
				return nil, xerrors.WithStackTrace(err)
			}

			return part, nil
		}, recv)
		require.EqualValues(t, 0, rs.index)
		count := 0
		for row, err := range rs.Rows(ctx) {
			require.NoError(t, err)
			require.EqualValues(t, count, rs.rowIndex)
			var (
				a uint64
				b string
			)
			err := row.Scan(&a, &b)
			require.NoError(t, err)
			count++
			require.EqualValues(t, count, a)
			require.EqualValues(t, fmt.Sprintf("%v", count), b)
		}
		require.EqualValues(t, count, 3)
	})
	t.Run("BreakIterate", func(t *testing.T) {
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
		recv, err := stream.Recv()
		require.NoError(t, err)
		rs := newResultSet(func() (*Ydb_Query.ExecuteQueryResponsePart, error) {
			part, err := stream.Recv()
			if err != nil {
				return nil, xerrors.WithStackTrace(err)
			}

			return part, nil
		}, recv)
		require.EqualValues(t, 0, rs.index)
		count := 0
		for _, err := range rs.Rows(ctx) {
			require.NoError(t, err)
			require.EqualValues(t, count, rs.rowIndex)
			if count > 0 {
				break
			}
			count++
		}
		require.EqualValues(t, count, 1)
	})
	t.Run("IntermediateResultSetEmpty", func(t *testing.T) {
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
				Rows: []*Ydb.Value{},
			}.Build(),
		}.Build(), nil)
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
					Ydb.Value_builder{
						Items: []*Ydb.Value{Ydb.Value_builder{
							Uint64Value: proto.Uint64(6),
						}.Build(), Ydb.Value_builder{
							TextValue: proto.String("6"),
						}.Build()},
					}.Build(),
				},
			}.Build(),
		}.Build(), nil)
		stream.EXPECT().Recv().Return(nil, io.EOF)
		recv, err := stream.Recv()
		require.NoError(t, err)
		rs := newResultSet(func() (*Ydb_Query.ExecuteQueryResponsePart, error) {
			part, err := stream.Recv()
			if err != nil {
				return nil, xerrors.WithStackTrace(err)
			}

			return part, nil
		}, recv)
		require.EqualValues(t, 0, rs.index)
		count := 0
		for row, err := range rs.Rows(ctx) {
			require.NoError(t, err)
			var (
				a uint64
				b string
			)
			err := row.Scan(&a, &b)
			require.NoError(t, err)
			count++
			require.EqualValues(t, count, a)
			require.EqualValues(t, fmt.Sprintf("%v", count), b)
		}
		require.EqualValues(t, count, 6)
	})
	t.Run("OverTwoParts", func(t *testing.T) {
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
		stream.EXPECT().Recv().Return(nil, io.EOF)
		recv, err := stream.Recv()
		require.NoError(t, err)
		rs := newResultSet(func() (*Ydb_Query.ExecuteQueryResponsePart, error) {
			part, err := stream.Recv()
			if err != nil {
				return nil, xerrors.WithStackTrace(err)
			}

			return part, nil
		}, recv)
		require.EqualValues(t, 0, rs.index)
		count := 0
		for row, err := range rs.Rows(ctx) {
			require.NoError(t, err)
			var (
				a uint64
				b string
			)
			err := row.Scan(&a, &b)
			require.NoError(t, err)
			count++
			require.EqualValues(t, count, a)
			require.EqualValues(t, fmt.Sprintf("%v", count), b)
		}
		require.EqualValues(t, count, 5)
	})
	t.Run("CanceledContext", func(t *testing.T) {
		childCtx, cancel := context.WithCancel(xtest.Context(t))
		defer cancel()
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
		recv, err := stream.Recv()
		require.NoError(t, err)
		rs := newResultSet(func() (*Ydb_Query.ExecuteQueryResponsePart, error) {
			part, err := stream.Recv()
			if err != nil {
				return nil, xerrors.WithStackTrace(err)
			}

			return part, nil
		}, recv)
		require.EqualValues(t, 0, rs.index)
		var (
			count     = 0
			cancelled = false
		)
		for _, err := range rs.Rows(childCtx) {
			count++
			if !cancelled {
				require.NoError(t, err)
				cancel()
				cancelled = true
			} else {
				require.ErrorIs(t, err, context.Canceled)
			}
		}
		require.EqualValues(t, count, 2)
	})
	t.Run("OperationError", func(t *testing.T) {
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
		stream.EXPECT().Recv().Return(nil, xerrors.Operation(xerrors.WithStatusCode(
			Ydb.StatusIds_OVERLOADED,
		)))
		recv, err := stream.Recv()
		require.NoError(t, err)
		rs := newResultSet(func() (*Ydb_Query.ExecuteQueryResponsePart, error) {
			part, err := nextPart(stream)
			if err != nil {
				return nil, xerrors.WithStackTrace(err)
			}
			if resultSetIndex := part.GetResultSetIndex(); resultSetIndex != 0 {
				return nil, xerrors.WithStackTrace(fmt.Errorf(
					"%w: %d != %d",
					errWrongNextResultSetIndex,
					resultSetIndex, 0,
				))
			}

			return part, nil
		}, recv)
		require.EqualValues(t, 0, rs.index)
		count := 0
		for _, err := range rs.Rows(ctx) {
			if count < 3 {
				require.NoError(t, err)
			} else {
				require.True(t, xerrors.IsOperationError(err, Ydb.StatusIds_OVERLOADED))
			}
			count++
		}
		require.EqualValues(t, count, 4)
	})
	t.Run("TransportError", func(t *testing.T) {
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
		stream.EXPECT().Recv().Return(nil, grpcStatus.Error(grpcCodes.Unavailable, ""))
		recv, err := stream.Recv()
		require.NoError(t, err)
		rs := newResultSet(func() (*Ydb_Query.ExecuteQueryResponsePart, error) {
			part, err := nextPart(stream)
			if err != nil {
				return nil, xerrors.WithStackTrace(err)
			}
			if resultSetIndex := part.GetResultSetIndex(); resultSetIndex != 0 {
				return nil, xerrors.WithStackTrace(fmt.Errorf(
					"%w: %d != %d",
					errWrongNextResultSetIndex,
					resultSetIndex, 0,
				))
			}

			return part, nil
		}, recv)
		require.EqualValues(t, 0, rs.index)
		count := 0
		for _, err := range rs.Rows(ctx) {
			if count < 3 {
				require.NoError(t, err)
			} else {
				require.True(t, xerrors.IsTransportError(err, grpcCodes.Unavailable))
			}
			count++
		}
		require.EqualValues(t, count, 4)
	})
	t.Run("WrongResultSetIndex", func(t *testing.T) {
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
		recv, err := stream.Recv()
		require.NoError(t, err)
		rs := newResultSet(func() (*Ydb_Query.ExecuteQueryResponsePart, error) {
			part, err := nextPart(stream)
			if err != nil {
				return nil, xerrors.WithStackTrace(err)
			}

			return part, nil
		}, recv)
		require.EqualValues(t, 0, rs.index)
		count := 0
		for _, err := range rs.Rows(ctx) {
			if count < 3 {
				require.NoError(t, err)
			} else {
				require.ErrorIs(t, err, errWrongResultSetIndex)
			}
			count++
		}
		require.EqualValues(t, count, 4)
	})
}
