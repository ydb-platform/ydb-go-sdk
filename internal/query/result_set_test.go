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
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xtest"
)

func TestResultSetNext(t *testing.T) {
	ctx := xtest.Context(t)
	ctrl := gomock.NewController(t)
	t.Run("Next", func(t *testing.T) {
		t.Run("EmptyResultSet", func(t *testing.T) {
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
					Rows: []*Ydb.Value{},
				},
			}, nil)
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
					Rows: []*Ydb.Value{},
				},
			}, nil)
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
					Rows: []*Ydb.Value{},
				},
			}, nil)
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
			childCtx, cancel := context.WithCancel(xtest.Context(t))
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
		t.Run("WrongResultSetIndex", func(t *testing.T) {
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
			Rows: []*Ydb.Value{},
		},
	}, nil)
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
