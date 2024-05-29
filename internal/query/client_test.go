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

func TestCreateSession(t *testing.T) {
	t.Run("HappyWay", func(t *testing.T) {
		xtest.TestManyTimes(t, func(t testing.TB) {
			ctx := xtest.Context(t)
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
		}, xtest.StopAfter(time.Second))
	})
	t.Run("TransportError", func(t *testing.T) {
		t.Run("OnCall", func(t *testing.T) {
			xtest.TestManyTimes(t, func(t testing.TB) {
				ctx := xtest.Context(t)
				ctrl := gomock.NewController(t)
				service := NewMockQueryServiceClient(ctrl)
				service.EXPECT().CreateSession(gomock.Any(), gomock.Any()).Return(nil, grpcStatus.Error(grpcCodes.Unavailable, ""))
				_, err := createSession(ctx, service, config.New())
				require.Error(t, err)
				require.True(t, xerrors.IsTransportError(err, grpcCodes.Unavailable))
			}, xtest.StopAfter(time.Second))
		})
		t.Run("OnAttach", func(t *testing.T) {
			xtest.TestManyTimes(t, func(t testing.TB) {
				ctx := xtest.Context(t)
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
			}, xtest.StopAfter(time.Second))
		})
		t.Run("OnRecv", func(t *testing.T) {
			xtest.TestManyTimes(t, func(t testing.TB) {
				ctx := xtest.Context(t)
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
			}, xtest.StopAfter(time.Second))
		})
	})
	t.Run("OperationError", func(t *testing.T) {
		t.Run("OnCall", func(t *testing.T) {
			xtest.TestManyTimes(t, func(t testing.TB) {
				ctx := xtest.Context(t)
				ctrl := gomock.NewController(t)
				service := NewMockQueryServiceClient(ctrl)
				service.EXPECT().CreateSession(gomock.Any(), gomock.Any()).Return(nil,
					xerrors.Operation(xerrors.WithStatusCode(Ydb.StatusIds_UNAVAILABLE)),
				)
				_, err := createSession(ctx, service, config.New())
				require.Error(t, err)
				require.True(t, xerrors.IsOperationError(err, Ydb.StatusIds_UNAVAILABLE))
			}, xtest.StopAfter(time.Second))
		})
		t.Run("OnRecv", func(t *testing.T) {
			xtest.TestManyTimes(t, func(t testing.TB) {
				ctx := xtest.Context(t)
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
			}, xtest.StopAfter(time.Second))
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

func TestDo(t *testing.T) {
	ctx := xtest.Context(t)
	t.Run("HappyWay", func(t *testing.T) {
		attempts, err := do(ctx, testPool(ctx, func(ctx context.Context) (*Session, error) {
			return newTestSession("123"), nil
		}), func(ctx context.Context, s query.Session) error {
			return nil
		}, &trace.Query{})
		require.NoError(t, err)
		require.EqualValues(t, 1, attempts)
	})
	t.Run("RetryableError", func(t *testing.T) {
		counter := 0
		attempts, err := do(ctx, testPool(ctx, func(ctx context.Context) (*Session, error) {
			return newTestSession("123"), nil
		}), func(ctx context.Context, s query.Session) error {
			counter++
			if counter < 10 {
				return xerrors.Retryable(errors.New(""))
			}

			return nil
		}, &trace.Query{})
		require.NoError(t, err)
		require.EqualValues(t, 10, attempts)
		require.Equal(t, 10, counter)
	})
}

func TestDoTx(t *testing.T) {
	ctx := xtest.Context(t)
	t.Run("HappyWay", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		client := NewMockQueryServiceClient(ctrl)
		client.EXPECT().BeginTransaction(gomock.Any(), gomock.Any()).Return(&Ydb_Query.BeginTransactionResponse{
			Status: Ydb.StatusIds_SUCCESS,
		}, nil)
		client.EXPECT().CommitTransaction(gomock.Any(), gomock.Any()).Return(&Ydb_Query.CommitTransactionResponse{
			Status: Ydb.StatusIds_SUCCESS,
		}, nil)
		attempts, err := doTx(ctx, testPool(ctx, func(ctx context.Context) (*Session, error) {
			return newTestSessionWithClient("123", client), nil
		}), func(ctx context.Context, tx query.TxActor) error {
			return nil
		}, &trace.Query{})
		require.NoError(t, err)
		require.EqualValues(t, 1, attempts)
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
		attempts, err := doTx(ctx, testPool(ctx, func(ctx context.Context) (*Session, error) {
			return newTestSessionWithClient("123", client), nil
		}), func(ctx context.Context, tx query.TxActor) error {
			counter++
			if counter < 10 {
				return xerrors.Retryable(errors.New(""))
			}

			return nil
		}, &trace.Query{})
		require.NoError(t, err)
		require.EqualValues(t, 10, attempts)
		require.Equal(t, 10, counter)
	})
}

func TestReadRow(t *testing.T) {
	ctx := xtest.Context(t)
	t.Run("HappyWay", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		row, err := readRow(ctx, testPool(ctx, func(ctx context.Context) (*Session, error) {
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
			client := NewMockQueryServiceClient(ctrl)
			client.EXPECT().ExecuteQuery(gomock.Any(), gomock.Any()).Return(stream, nil)

			return &Session{
				id:         "123",
				statusCode: statusIdle,
				cfg:        config.New(),
				grpcClient: client,
			}, nil
		}), "", nil)
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
		row, err := readRow(ctx, testPool(ctx, func(ctx context.Context) (*Session, error) {
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
			client := NewMockQueryServiceClient(ctrl)
			client.EXPECT().ExecuteQuery(gomock.Any(), gomock.Any()).Return(stream, nil)

			return &Session{
				id:         "123",
				statusCode: statusIdle,
				cfg:        config.New(),
				grpcClient: client,
			}, nil
		}), "", nil)
		require.ErrorIs(t, err, errMoreThanOneRow)
		require.Nil(t, row)
	})
	t.Run("MoreThanOneResultSet", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		row, err := readRow(ctx, testPool(ctx, func(ctx context.Context) (*Session, error) {
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
			client := NewMockQueryServiceClient(ctrl)
			client.EXPECT().ExecuteQuery(gomock.Any(), gomock.Any()).Return(stream, nil)

			return &Session{
				id:         "123",
				statusCode: statusIdle,
				cfg:        config.New(),
				grpcClient: client,
			}, nil
		}), "", nil)
		require.ErrorIs(t, err, errMoreThanOneRow)
		require.Nil(t, row)
	})
}
