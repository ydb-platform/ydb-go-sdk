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
	"google.golang.org/protobuf/proto"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
)

func TestResultSetNext(t *testing.T) {
	ctx := t.Context()
	ctrl := gomock.NewController(t)
	t.Run("Next", func(t *testing.T) {
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
			require.EqualValues(t, 0, rs.Index())
			{
				_, err := rs.nextRow(ctx)
				require.ErrorIs(t, err, io.EOF)
			}
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
			require.EqualValues(t, 0, rs.Index())
			{
				_, err := rs.nextRow(ctx)
				require.NoError(t, err)
				require.EqualValues(t, 0, rs.rowIndex)
			}
			{
				_, err := rs.nextRow(ctx)
				require.NoError(t, err)
				require.EqualValues(t, 1, rs.rowIndex)
			}
			{
				_, err := rs.nextRow(ctx)
				require.NoError(t, err)
				require.EqualValues(t, 2, rs.rowIndex)
			}
			{
				_, err := rs.nextRow(ctx)
				require.ErrorIs(t, err, io.EOF)
			}
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
			require.EqualValues(t, 0, rs.Index())
			{
				_, err := rs.nextRow(ctx)
				require.NoError(t, err)
				require.EqualValues(t, 0, rs.rowIndex)
			}
			{
				_, err := rs.nextRow(ctx)
				require.NoError(t, err)
				require.EqualValues(t, 1, rs.rowIndex)
			}
			{
				_, err := rs.nextRow(ctx)
				require.NoError(t, err)
				require.EqualValues(t, 2, rs.rowIndex)
			}
			{
				_, err := rs.nextRow(ctx)
				require.NoError(t, err)
				require.EqualValues(t, 0, rs.rowIndex)
			}
			{
				_, err := rs.nextRow(ctx)
				require.NoError(t, err)
				require.EqualValues(t, 1, rs.rowIndex)
			}
			{
				_, err := rs.nextRow(ctx)
				require.NoError(t, err)
				require.EqualValues(t, 2, rs.rowIndex)
			}
			{
				_, err := rs.nextRow(ctx)
				require.ErrorIs(t, err, io.EOF)
			}
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
			require.EqualValues(t, 0, rs.Index())
			{
				_, err := rs.nextRow(ctx)
				require.NoError(t, err)
				require.EqualValues(t, 0, rs.rowIndex)
			}
			{
				_, err := rs.nextRow(ctx)
				require.NoError(t, err)
				require.EqualValues(t, 1, rs.rowIndex)
			}
			{
				_, err := rs.nextRow(ctx)
				require.NoError(t, err)
				require.EqualValues(t, 2, rs.rowIndex)
			}
			{
				_, err := rs.nextRow(ctx)
				require.NoError(t, err)
				require.EqualValues(t, 0, rs.rowIndex)
			}
			{
				_, err := rs.nextRow(ctx)
				require.NoError(t, err)
				require.EqualValues(t, 1, rs.rowIndex)
			}
			{
				_, err := rs.nextRow(ctx)
				require.ErrorIs(t, err, io.EOF)
			}
		})
		t.Run("CanceledContext", func(t *testing.T) {
			childCtx, cancel := context.WithCancel(t.Context())
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
			require.EqualValues(t, 0, rs.Index())
			{
				_, err := rs.nextRow(childCtx)
				require.NoError(t, err)
				require.EqualValues(t, 0, rs.rowIndex)
			}
			cancel()
			{
				_, err := rs.nextRow(childCtx)
				require.ErrorIs(t, err, context.Canceled)
			}
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
						"critical violation of the logic: wrong result set rowIndex: %d != %d",
						resultSetIndex, 0,
					))
				}

				return part, nil
			}, recv)
			require.EqualValues(t, 0, rs.Index())
			{
				_, err := rs.nextRow(ctx)
				require.NoError(t, err)
				require.EqualValues(t, 0, rs.rowIndex)
			}
			{
				_, err := rs.nextRow(ctx)
				require.NoError(t, err)
				require.EqualValues(t, 1, rs.rowIndex)
			}
			{
				_, err := rs.nextRow(ctx)
				require.NoError(t, err)
				require.EqualValues(t, 2, rs.rowIndex)
			}
			{
				_, err := rs.nextRow(ctx)
				require.True(t, xerrors.IsOperationError(err, Ydb.StatusIds_OVERLOADED))
			}
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
						"critical violation of the logic: wrong result set rowIndex: %d != %d",
						resultSetIndex, 0,
					))
				}

				return part, nil
			}, recv)
			require.EqualValues(t, 0, rs.Index())
			{
				_, err := rs.nextRow(ctx)
				require.NoError(t, err)
				require.EqualValues(t, 0, rs.rowIndex)
			}
			{
				_, err := rs.nextRow(ctx)
				require.NoError(t, err)
				require.EqualValues(t, 1, rs.rowIndex)
			}
			{
				_, err := rs.nextRow(ctx)
				require.NoError(t, err)
				require.EqualValues(t, 2, rs.rowIndex)
			}
			{
				_, err := rs.nextRow(ctx)
				require.True(t, xerrors.IsTransportError(err, grpcCodes.Unavailable))
			}
		})
		// Regression test: mustBeLastResultSet must take precedence over io.EOF check.
		// errReadNextResultSet wraps io.EOF (via %w), so xerrors.Is(err, io.EOF) is true for it.
		// nextRow must return a non-EOF error when mustBeLastResultSet is set and the recv
		// function signals that a second result set was encountered.
		t.Run("MustBeLastResultSetWithErrReadNextResultSet", func(t *testing.T) {
			// Simulate the error produced by nextPartFunc when a part for the next result
			// set is received while iterating a single-result-set query.
			recvErr := fmt.Errorf(
				"result set (index=0) receive part (index=1) for next result set: %w (%w)",
				io.EOF, errReadNextResultSet,
			)
			recv := Ydb_Query.ExecuteQueryResponsePart_builder{
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
					},
					Rows: []*Ydb.Value{
						Ydb.Value_builder{
							Items: []*Ydb.Value{Ydb.Value_builder{
								Uint64Value: proto.Uint64(1),
							}.Build()},
						}.Build(),
					},
				}.Build(),
			}.Build()
			rs := newResultSet(func() (*Ydb_Query.ExecuteQueryResponsePart, error) {
				return nil, recvErr
			}, recv)
			rs.mustBeLastResultSet = true

			// Read the first (and only) row — should succeed.
			{
				_, err := rs.nextRow(ctx)
				require.NoError(t, err)
			}
			// After exhausting the rows, recv returns errReadNextResultSet (wrapping io.EOF).
			// The error must NOT be io.EOF — it should signal an unexpected second result set.
			{
				_, err := rs.nextRow(ctx)
				require.Error(t, err)
				require.NotErrorIs(t, err, io.EOF)
			}
		})
		// Regression test: when mustBeLastResultSet is false, errReadNextResultSet (which wraps
		// io.EOF) must still cause nextRow to return io.EOF, not a real error. The current result
		// set is exhausted; the caller is expected to advance to the next result set.
		t.Run("NotMustBeLastResultSetWithErrReadNextResultSet", func(t *testing.T) {
			recvErr := fmt.Errorf(
				"result set (index=0) receive part (index=1) for next result set: %w (%w)",
				io.EOF, errReadNextResultSet,
			)
			recv := Ydb_Query.ExecuteQueryResponsePart_builder{
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
					},
					Rows: []*Ydb.Value{
						Ydb.Value_builder{
							Items: []*Ydb.Value{Ydb.Value_builder{
								Uint64Value: proto.Uint64(1),
							}.Build()},
						}.Build(),
					},
				}.Build(),
			}.Build()
			rs := newResultSet(func() (*Ydb_Query.ExecuteQueryResponsePart, error) {
				return nil, recvErr
			}, recv)
			// mustBeLastResultSet = false (default)

			// Read the first (and only) row — should succeed.
			{
				_, err := rs.nextRow(ctx)
				require.NoError(t, err)
			}
			// After exhausting the rows, recv returns errReadNextResultSet (wrapping io.EOF).
			// The error must be io.EOF — the current result set is simply done.
			{
				_, err := rs.nextRow(ctx)
				require.ErrorIs(t, err, io.EOF)
			}
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
			require.EqualValues(t, 0, rs.Index())
			{
				_, err := rs.nextRow(ctx)
				require.NoError(t, err)
				require.EqualValues(t, 0, rs.rowIndex)
			}
			{
				_, err := rs.nextRow(ctx)
				require.NoError(t, err)
				require.EqualValues(t, 1, rs.rowIndex)
			}
			{
				_, err := rs.nextRow(ctx)
				require.NoError(t, err)
				require.EqualValues(t, 2, rs.rowIndex)
			}
			{
				_, err := rs.nextRow(ctx)
				require.ErrorIs(t, err, errWrongResultSetIndex)
			}
			{
				_, err := rs.nextRow(ctx)
				require.ErrorIs(t, err, io.EOF)
			}
		})
	})
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
	recv, err := stream.Recv()
	require.NoError(t, err)
	rs := newResultSet(func() (*Ydb_Query.ExecuteQueryResponsePart, error) {
		part, err := stream.Recv()
		if err != nil {
			return nil, xerrors.WithStackTrace(err)
		}

		return part, nil
	}, recv)
	require.EqualValues(t, 0, rs.Index())
	t.Run("Columns", func(t *testing.T) {
		require.EqualValues(t, []string{"a", "b"}, rs.Columns())
	})
	t.Run("ColumnNames", func(t *testing.T) {
		var types []string
		for _, tt := range rs.ColumnTypes() {
			types = append(types, tt.Yql())
		}
		require.EqualValues(t, []string{"Uint64", "Utf8"}, types)
	})
}
