package query

import (
	"context"
	"errors"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/Ydb_Query_V1"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Query"
	"go.uber.org/mock/gomock"
	grpcCodes "google.golang.org/grpc/codes"
	grpcStatus "google.golang.org/grpc/status"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/pool"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/query/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xtest"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func TestClient(t *testing.T) {
	ctx := xtest.Context(t)
	t.Run("CreateSession", func(t *testing.T) {
		t.Run("HappyWay", func(t *testing.T) {
			ctrl := gomock.NewController(t)
			attachStream := NewMockQueryService_AttachSessionClient(ctrl)
			attachStream.EXPECT().Recv().Return(&Ydb_Query.SessionState{
				Status: Ydb.StatusIds_SUCCESS,
			}, nil).AnyTimes()
			service := NewMockQueryServiceClient(ctrl)
			service.EXPECT().CreateSession(gomock.Any(), gomock.Any()).Return(&Ydb_Query.CreateSessionResponse{
				Status:    Ydb.StatusIds_SUCCESS,
				SessionId: "test",
			}, nil)
			service.EXPECT().AttachSession(gomock.Any(), gomock.Any()).Return(attachStream, nil)
			service.EXPECT().DeleteSession(gomock.Any(), gomock.Any()).Return(&Ydb_Query.DeleteSessionResponse{
				Status: Ydb.StatusIds_SUCCESS,
			}, nil)
			attached := 0
			s, err := createSession(ctx, service, config.New(config.WithTrace(
				&trace.Query{
					OnSessionAttach: func(info trace.QuerySessionAttachStartInfo) func(info trace.QuerySessionAttachDoneInfo) {
						return func(info trace.QuerySessionAttachDoneInfo) {
							if info.Error == nil {
								attached++
							}
						}
					},
					OnSessionDelete: func(info trace.QuerySessionDeleteStartInfo) func(info trace.QuerySessionDeleteDoneInfo) {
						attached--

						return nil
					},
				},
			)))
			require.NoError(t, err)
			require.EqualValues(t, "test", s.id)
			require.EqualValues(t, 1, attached)
			err = s.Close(ctx)
			require.NoError(t, err)
			require.EqualValues(t, 0, attached)
		})
		t.Run("TransportError", func(t *testing.T) {
			t.Run("OnCall", func(t *testing.T) {
				ctrl := gomock.NewController(t)
				service := NewMockQueryServiceClient(ctrl)
				service.EXPECT().CreateSession(gomock.Any(), gomock.Any()).Return(nil, grpcStatus.Error(grpcCodes.Unavailable, ""))
				_, err := createSession(ctx, service, config.New())
				require.Error(t, err)
				require.True(t, xerrors.IsTransportError(err, grpcCodes.Unavailable))
			})
			t.Run("OnAttach", func(t *testing.T) {
				ctrl := gomock.NewController(t)
				service := NewMockQueryServiceClient(ctrl)
				service.EXPECT().CreateSession(gomock.Any(), gomock.Any()).Return(&Ydb_Query.CreateSessionResponse{
					Status:    Ydb.StatusIds_SUCCESS,
					SessionId: "test",
				}, nil)
				service.EXPECT().AttachSession(gomock.Any(), gomock.Any()).Return(nil, grpcStatus.Error(grpcCodes.Unavailable, ""))
				service.EXPECT().DeleteSession(gomock.Any(), gomock.Any()).Return(nil, grpcStatus.Error(grpcCodes.Unavailable, ""))
				_, err := createSession(ctx, service, config.New())
				require.Error(t, err)
				require.True(t, xerrors.IsTransportError(err, grpcCodes.Unavailable))
			})
			t.Run("OnRecv", func(t *testing.T) {
				ctrl := gomock.NewController(t)
				attachStream := NewMockQueryService_AttachSessionClient(ctrl)
				attachStream.EXPECT().Recv().Return(nil, grpcStatus.Error(grpcCodes.Unavailable, "")).AnyTimes()
				service := NewMockQueryServiceClient(ctrl)
				service.EXPECT().CreateSession(gomock.Any(), gomock.Any()).Return(&Ydb_Query.CreateSessionResponse{
					Status:    Ydb.StatusIds_SUCCESS,
					SessionId: "test",
				}, nil)
				service.EXPECT().AttachSession(gomock.Any(), gomock.Any()).Return(attachStream, nil)
				service.EXPECT().DeleteSession(gomock.Any(), gomock.Any()).Return(&Ydb_Query.DeleteSessionResponse{
					Status: Ydb.StatusIds_SUCCESS,
				}, nil)
				_, err := createSession(ctx, service, config.New())
				require.Error(t, err)
				require.True(t, xerrors.IsTransportError(err, grpcCodes.Unavailable))
			})
		})
		t.Run("OperationError", func(t *testing.T) {
			t.Run("OnCall", func(t *testing.T) {
				ctrl := gomock.NewController(t)
				service := NewMockQueryServiceClient(ctrl)
				service.EXPECT().CreateSession(gomock.Any(), gomock.Any()).Return(nil,
					xerrors.Operation(xerrors.WithStatusCode(Ydb.StatusIds_UNAVAILABLE)),
				)
				_, err := createSession(ctx, service, config.New())
				require.Error(t, err)
				require.True(t, xerrors.IsOperationError(err, Ydb.StatusIds_UNAVAILABLE))
			})
			t.Run("OnRecv", func(t *testing.T) {
				ctrl := gomock.NewController(t)
				attachStream := NewMockQueryService_AttachSessionClient(ctrl)
				attachStream.EXPECT().Recv().Return(nil,
					xerrors.Operation(xerrors.WithStatusCode(Ydb.StatusIds_UNAVAILABLE)),
				)
				service := NewMockQueryServiceClient(ctrl)
				service.EXPECT().CreateSession(gomock.Any(), gomock.Any()).Return(&Ydb_Query.CreateSessionResponse{
					Status:    Ydb.StatusIds_SUCCESS,
					SessionId: "test",
				}, nil)
				service.EXPECT().AttachSession(gomock.Any(), gomock.Any()).Return(attachStream, nil)
				service.EXPECT().DeleteSession(gomock.Any(), gomock.Any()).Return(&Ydb_Query.DeleteSessionResponse{
					Status: Ydb.StatusIds_SUCCESS,
				}, nil)
				_, err := createSession(ctx, service, config.New())
				require.Error(t, err)
				require.True(t, xerrors.IsOperationError(err, Ydb.StatusIds_UNAVAILABLE))
			})
		})
	})
	t.Run("Do", func(t *testing.T) {
		t.Run("HappyWay", func(t *testing.T) {
			var visited bool
			err := do(ctx, testPool(ctx, func(ctx context.Context) (*Session, error) {
				return newTestSession("123"), nil
			}), func(ctx context.Context, s query.Session) error {
				visited = true

				return nil
			})
			require.NoError(t, err)
			require.True(t, visited)
		})
		t.Run("RetryableError", func(t *testing.T) {
			counter := 0
			err := do(ctx, testPool(ctx, func(ctx context.Context) (*Session, error) {
				return newTestSession("123"), nil
			}), func(ctx context.Context, s query.Session) error {
				counter++
				if counter < 10 {
					return xerrors.Retryable(errors.New(""))
				}

				return nil
			})
			require.NoError(t, err)
			require.Equal(t, 10, counter)
		})
	})
	t.Run("DoTx", func(t *testing.T) {
		t.Run("HappyWay", func(t *testing.T) {
			ctrl := gomock.NewController(t)
			client := NewMockQueryServiceClient(ctrl)
			client.EXPECT().BeginTransaction(gomock.Any(), gomock.Any()).Return(&Ydb_Query.BeginTransactionResponse{
				Status: Ydb.StatusIds_SUCCESS,
			}, nil)
			client.EXPECT().CommitTransaction(gomock.Any(), gomock.Any()).Return(&Ydb_Query.CommitTransactionResponse{
				Status: Ydb.StatusIds_SUCCESS,
			}, nil)
			err := doTx(ctx, testPool(ctx, func(ctx context.Context) (*Session, error) {
				return newTestSessionWithClient("123", client), nil
			}), func(ctx context.Context, tx query.TxActor) error {
				return nil
			}, &trace.Query{})
			require.NoError(t, err)
		})
		t.Run("RetryableError", func(t *testing.T) {
			counter := 0
			ctrl := gomock.NewController(t)
			client := NewMockQueryServiceClient(ctrl)
			client.EXPECT().BeginTransaction(gomock.Any(), gomock.Any()).Return(&Ydb_Query.BeginTransactionResponse{
				Status: Ydb.StatusIds_SUCCESS,
			}, nil).AnyTimes()
			client.EXPECT().RollbackTransaction(gomock.Any(), gomock.Any()).Return(&Ydb_Query.RollbackTransactionResponse{
				Status: Ydb.StatusIds_SUCCESS,
			}, nil).AnyTimes()
			client.EXPECT().CommitTransaction(gomock.Any(), gomock.Any()).Return(&Ydb_Query.CommitTransactionResponse{
				Status: Ydb.StatusIds_SUCCESS,
			}, nil).AnyTimes()
			err := doTx(ctx, testPool(ctx, func(ctx context.Context) (*Session, error) {
				return newTestSessionWithClient("123", client), nil
			}), func(ctx context.Context, tx query.TxActor) error {
				counter++
				if counter < 10 {
					return xerrors.Retryable(errors.New(""))
				}

				return nil
			}, &trace.Query{})
			require.NoError(t, err)
			require.Equal(t, 10, counter)
		})
	})
	t.Run("Execute", func(t *testing.T) {
		t.Run("HappyWay", func(t *testing.T) {
			ctrl := gomock.NewController(t)
			r, err := clientExecute(ctx, testPool(ctx, func(ctx context.Context) (*Session, error) {
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
				stream.EXPECT().Recv().Return(nil, io.EOF)
				client := NewMockQueryServiceClient(ctrl)
				client.EXPECT().ExecuteQuery(gomock.Any(), gomock.Any()).Return(stream, nil)

				return newTestSessionWithClient("123", client), nil
			}), "")
			require.NoError(t, err)
			{
				rs, err := r.NextResultSet(ctx)
				require.NoError(t, err)
				r1, err := rs.NextRow(ctx)
				require.NoError(t, err)
				var (
					a uint64
					b string
				)
				err = r1.Scan(&a, &b)
				require.NoError(t, err)
				require.EqualValues(t, 1, a)
				require.EqualValues(t, "1", b)
				r2, err := rs.NextRow(ctx)
				require.NoError(t, err)
				err = r2.Scan(&a, &b)
				require.NoError(t, err)
				require.EqualValues(t, 2, a)
				require.EqualValues(t, "2", b)
				r3, err := rs.NextRow(ctx)
				require.NoError(t, err)
				err = r3.Scan(&a, &b)
				require.NoError(t, err)
				require.EqualValues(t, 3, a)
				require.EqualValues(t, "3", b)
				r4, err := rs.NextRow(ctx)
				require.NoError(t, err)
				err = r4.Scan(&a, &b)
				require.NoError(t, err)
				require.EqualValues(t, 4, a)
				require.EqualValues(t, "4", b)
				r5, err := rs.NextRow(ctx)
				require.NoError(t, err)
				err = r5.Scan(&a, &b)
				require.NoError(t, err)
				require.EqualValues(t, 5, a)
				require.EqualValues(t, "5", b)
				r6, err := rs.NextRow(ctx)
				require.ErrorIs(t, err, io.EOF)
				require.Nil(t, r6)
			}
			{
				rs, err := r.NextResultSet(ctx)
				require.NoError(t, err)
				r1, err := rs.NextRow(ctx)
				require.NoError(t, err)
				var (
					a uint64
					b string
					c bool
				)
				err = r1.Scan(&a, &b, &c)
				require.NoError(t, err)
				require.EqualValues(t, 1, a)
				require.EqualValues(t, "1", b)
				require.EqualValues(t, true, c)
				r2, err := rs.NextRow(ctx)
				require.NoError(t, err)
				err = r2.Scan(&a, &b, &c)
				require.NoError(t, err)
				require.EqualValues(t, 2, a)
				require.EqualValues(t, "2", b)
				require.EqualValues(t, false, c)
				r3, err := rs.NextRow(ctx)
				require.ErrorIs(t, err, io.EOF)
				require.Nil(t, r3)
			}
			require.NoError(t, r.Err())
		})
	})
}

func newTestSession(id string) *Session {
	return &Session{
		id:         id,
		statusCode: statusIdle,
		cfg:        config.New(),
	}
}

func newTestSessionWithClient(id string, client Ydb_Query_V1.QueryServiceClient) *Session {
	return &Session{
		id:         id,
		grpcClient: client,
		statusCode: statusIdle,
		cfg:        config.New(),
	}
}

func testPool(
	ctx context.Context,
	createSession func(ctx context.Context) (*Session, error),
) *pool.Pool[*Session, Session] {
	return pool.New[*Session, Session](ctx,
		pool.WithLimit[*Session, Session](1),
		pool.WithCreateFunc(createSession),
	)
}
