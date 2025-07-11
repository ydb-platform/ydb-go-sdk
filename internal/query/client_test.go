package query

import (
	"context"
	"errors"
	"io"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/Ydb_Query_V1"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Operations"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Query"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_TableStats"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
	grpcCodes "google.golang.org/grpc/codes"
	grpcStatus "google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/anypb"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/pool"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/query/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/query/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/tx"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xtest"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func TestClient(t *testing.T) {
	ctx := xtest.Context(t)
	t.Run("createSession", func(t *testing.T) {
		t.Run("HappyWay", func(t *testing.T) {
			ctrl := gomock.NewController(t)
			attachStream := NewMockQueryService_AttachSessionClient(ctrl)
			attachStream.EXPECT().Recv().Return(&Ydb_Query.SessionState{
				Status: Ydb.StatusIds_SUCCESS,
			}, nil).AnyTimes()
			client := NewMockQueryServiceClient(ctrl)
			client.EXPECT().CreateSession(gomock.Any(), gomock.Any()).Return(&Ydb_Query.CreateSessionResponse{
				Status:    Ydb.StatusIds_SUCCESS,
				SessionId: "test",
			}, nil)
			client.EXPECT().AttachSession(gomock.Any(), gomock.Any()).Return(attachStream, nil)
			client.EXPECT().DeleteSession(gomock.Any(), gomock.Any()).Return(&Ydb_Query.DeleteSessionResponse{
				Status: Ydb.StatusIds_SUCCESS,
			}, nil)
			attached := 0
			s, err := createSession(ctx, client, WithTrace(
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
			))
			require.NoError(t, err)
			require.EqualValues(t, "test", s.ID())
			require.EqualValues(t, 1, attached)
			err = s.Close(ctx)
			require.NoError(t, err)
			require.EqualValues(t, 0, attached)
		})
		t.Run("TransportError", func(t *testing.T) {
			t.Run("OnCall", func(t *testing.T) {
				ctrl := gomock.NewController(t)
				client := NewMockQueryServiceClient(ctrl)
				client.EXPECT().CreateSession(gomock.Any(), gomock.Any()).Return(nil, grpcStatus.Error(grpcCodes.Unavailable, ""))
				_, err := createSession(ctx, client)
				require.Error(t, err)
				require.True(t, xerrors.IsTransportError(err, grpcCodes.Unavailable))
			})
			t.Run("OnAttach", func(t *testing.T) {
				ctrl := gomock.NewController(t)
				client := NewMockQueryServiceClient(ctrl)
				client.EXPECT().CreateSession(gomock.Any(), gomock.Any()).Return(&Ydb_Query.CreateSessionResponse{
					Status:    Ydb.StatusIds_SUCCESS,
					SessionId: "test",
				}, nil)
				client.EXPECT().AttachSession(gomock.Any(), gomock.Any()).Return(nil, grpcStatus.Error(grpcCodes.Unavailable, ""))
				client.EXPECT().DeleteSession(gomock.Any(), gomock.Any()).Return(nil, grpcStatus.Error(grpcCodes.Unavailable, ""))
				_, err := createSession(ctx, client)
				require.Error(t, err)
				require.True(t, xerrors.IsTransportError(err, grpcCodes.Unavailable))
			})
			t.Run("OnRecv", func(t *testing.T) {
				ctrl := gomock.NewController(t)
				attachStream := NewMockQueryService_AttachSessionClient(ctrl)
				attachStream.EXPECT().Recv().Return(nil, grpcStatus.Error(grpcCodes.Unavailable, "")).AnyTimes()
				client := NewMockQueryServiceClient(ctrl)
				client.EXPECT().CreateSession(gomock.Any(), gomock.Any()).Return(&Ydb_Query.CreateSessionResponse{
					Status:    Ydb.StatusIds_SUCCESS,
					SessionId: "test",
				}, nil)
				client.EXPECT().AttachSession(gomock.Any(), gomock.Any()).Return(attachStream, nil)
				client.EXPECT().DeleteSession(gomock.Any(), gomock.Any()).Return(&Ydb_Query.DeleteSessionResponse{
					Status: Ydb.StatusIds_SUCCESS,
				}, nil)
				_, err := createSession(ctx, client)
				require.Error(t, err)
				require.True(t, xerrors.IsTransportError(err, grpcCodes.Unavailable))
			})
		})
		t.Run("OperationError", func(t *testing.T) {
			t.Run("OnCall", func(t *testing.T) {
				ctrl := gomock.NewController(t)
				client := NewMockQueryServiceClient(ctrl)
				client.EXPECT().CreateSession(gomock.Any(), gomock.Any()).Return(nil,
					xerrors.Operation(xerrors.WithStatusCode(Ydb.StatusIds_UNAVAILABLE)),
				)
				_, err := createSession(ctx, client)
				require.Error(t, err)
				require.True(t, xerrors.IsOperationError(err, Ydb.StatusIds_UNAVAILABLE))
			})
			t.Run("OnRecv", func(t *testing.T) {
				ctrl := gomock.NewController(t)
				attachStream := NewMockQueryService_AttachSessionClient(ctrl)
				attachStream.EXPECT().Recv().Return(nil,
					xerrors.Operation(xerrors.WithStatusCode(Ydb.StatusIds_UNAVAILABLE)),
				)
				client := NewMockQueryServiceClient(ctrl)
				client.EXPECT().CreateSession(gomock.Any(), gomock.Any()).Return(&Ydb_Query.CreateSessionResponse{
					Status:    Ydb.StatusIds_SUCCESS,
					SessionId: "test",
				}, nil)
				client.EXPECT().AttachSession(gomock.Any(), gomock.Any()).Return(attachStream, nil)
				client.EXPECT().DeleteSession(gomock.Any(), gomock.Any()).Return(&Ydb_Query.DeleteSessionResponse{
					Status: Ydb.StatusIds_SUCCESS,
				}, nil)
				_, err := createSession(ctx, client)
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
			}), func(ctx context.Context, s *Session) error {
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
			}), func(ctx context.Context, s *Session) error {
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
			t.Run("LazyTx", func(t *testing.T) {
				ctrl := gomock.NewController(t)
				err := doTx(ctx, testPool(ctx, func(ctx context.Context) (*Session, error) {
					client := NewMockQueryServiceClient(ctrl)
					stream := NewMockQueryService_ExecuteQueryClient(ctrl)
					stream.EXPECT().Recv().DoAndReturn(func() (*Ydb_Query.ExecuteQueryResponsePart, error) {
						client.EXPECT().CommitTransaction(gomock.Any(), gomock.Any()).Return(&Ydb_Query.CommitTransactionResponse{
							Status: Ydb.StatusIds_SUCCESS,
						}, nil)

						return &Ydb_Query.ExecuteQueryResponsePart{
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
						}, nil
					})
					stream.EXPECT().Recv().Return(nil, io.EOF)
					client.EXPECT().ExecuteQuery(gomock.Any(), gomock.Any()).Return(stream, nil)

					return newTestSessionWithClient("123", client, true), nil
				}), func(ctx context.Context, tx query.TxActor) error {
					defer func() {
						require.Equal(t, "456", tx.ID())
					}()

					return tx.Exec(ctx, "")
				}, tx.NewSettings(tx.WithDefaultTxMode()))
				require.NoError(t, err)
			})
			t.Run("NoLazyTx", func(t *testing.T) {
				ctrl := gomock.NewController(t)
				err := doTx(ctx, testPool(ctx, func(ctx context.Context) (*Session, error) {
					client := NewMockQueryServiceClient(ctrl)
					client.EXPECT().BeginTransaction(gomock.Any(), gomock.Any()).DoAndReturn(
						func(ctx context.Context, request *Ydb_Query.BeginTransactionRequest, option ...grpc.CallOption) (
							*Ydb_Query.BeginTransactionResponse, error,
						) {
							client.EXPECT().ExecuteQuery(gomock.Any(), gomock.Any()).DoAndReturn(
								func(ctx context.Context, request *Ydb_Query.ExecuteQueryRequest, option ...grpc.CallOption) (
									Ydb_Query_V1.QueryService_ExecuteQueryClient, error,
								) {
									stream := NewMockQueryService_ExecuteQueryClient(ctrl)
									stream.EXPECT().Recv().DoAndReturn(func() (*Ydb_Query.ExecuteQueryResponsePart, error) {
										client.EXPECT().CommitTransaction(gomock.Any(), gomock.Any()).Return(&Ydb_Query.CommitTransactionResponse{
											Status: Ydb.StatusIds_SUCCESS,
										}, nil)

										return &Ydb_Query.ExecuteQueryResponsePart{
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
										}, nil
									})
									stream.EXPECT().Recv().Return(nil, io.EOF)

									return stream, nil
								},
							)

							return &Ydb_Query.BeginTransactionResponse{
								Status: Ydb.StatusIds_SUCCESS,
								TxMeta: &Ydb_Query.TransactionMeta{Id: "456"},
							}, nil
						},
					)

					return newTestSessionWithClient("123", client, false), nil
				}), func(ctx context.Context, tx query.TxActor) error {
					defer func() {
						require.Equal(t, "456", tx.ID())
					}()

					return tx.Exec(ctx, "")
				}, tx.NewSettings(tx.WithDefaultTxMode()))
				require.NoError(t, err)
			})
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
				return newTestSessionWithClient("123", client, true), nil
			}), func(ctx context.Context, tx query.TxActor) error {
				counter++
				if counter < 10 {
					return xerrors.Retryable(errors.New(""))
				}

				return nil
			}, tx.NewSettings(tx.WithDefaultTxMode()))
			require.NoError(t, err)
			require.Equal(t, 10, counter)
		})
		t.Run("TxLeak", func(t *testing.T) {
			t.Run("OnExec", func(t *testing.T) {
				t.Run("WithoutCommit", func(t *testing.T) {
					xtest.TestManyTimes(t, func(t testing.TB) {
						txInFlight := 0
						ctrl := gomock.NewController(t)
						err := doTx(ctx, testPool(ctx, func(ctx context.Context) (*Session, error) {
							client := NewMockQueryServiceClient(ctrl)
							client.EXPECT().ExecuteQuery(gomock.Any(), gomock.Any()).DoAndReturn(
								func(ctx context.Context, request *Ydb_Query.ExecuteQueryRequest, option ...grpc.CallOption) (
									Ydb_Query_V1.QueryService_ExecuteQueryClient, error,
								) {
									if rand.Int31n(100) < 50 {
										return nil, xerrors.Operation(xerrors.WithStatusCode(Ydb.StatusIds_BAD_SESSION))
									}

									txInFlight++

									stream := NewMockQueryService_ExecuteQueryClient(ctrl)
									stream.EXPECT().Recv().DoAndReturn(func() (*Ydb_Query.ExecuteQueryResponsePart, error) {
										stream.EXPECT().Recv().Return(nil, io.EOF)
										client.EXPECT().CommitTransaction(gomock.Any(), gomock.Any()).DoAndReturn(
											func(
												ctx context.Context, request *Ydb_Query.CommitTransactionRequest,
												option ...grpc.CallOption,
											) (
												*Ydb_Query.CommitTransactionResponse, error,
											) {
												txInFlight--

												return &Ydb_Query.CommitTransactionResponse{
													Status: Ydb.StatusIds_SUCCESS,
												}, nil
											})

										return &Ydb_Query.ExecuteQueryResponsePart{
											Status: Ydb.StatusIds_SUCCESS,
											TxMeta: &Ydb_Query.TransactionMeta{
												Id: "456",
											},
											ExecStats: &Ydb_TableStats.QueryStats{},
										}, nil
									})

									return stream, nil
								})

							return newTestSessionWithClient("123", client, true), nil
						}), func(ctx context.Context, tx query.TxActor) error {
							return tx.Exec(ctx, "")
						}, tx.NewSettings(tx.WithSerializableReadWrite()))
						require.NoError(t, err)
						require.Zero(t, txInFlight)
					})
				})
				t.Run("WithCommit", func(t *testing.T) {
					xtest.TestManyTimes(t, func(t testing.TB) {
						ctrl := gomock.NewController(t)
						txInFlight := 0
						err := doTx(ctx, testPool(ctx, func(ctx context.Context) (*Session, error) {
							client := NewMockQueryServiceClient(ctrl)
							client.EXPECT().ExecuteQuery(gomock.Any(), gomock.Any()).DoAndReturn(
								func(ctx context.Context, request *Ydb_Query.ExecuteQueryRequest, option ...grpc.CallOption) (
									Ydb_Query_V1.QueryService_ExecuteQueryClient, error,
								) {
									require.True(t, request.GetTxControl().GetCommitTx())

									if rand.Int31n(100) < 50 {
										return nil, xerrors.Operation(xerrors.WithStatusCode(Ydb.StatusIds_BAD_SESSION))
									}

									txInFlight++

									stream := NewMockQueryService_ExecuteQueryClient(ctrl)
									stream.EXPECT().Recv().DoAndReturn(func() (*Ydb_Query.ExecuteQueryResponsePart, error) {
										if rand.Int31n(100) < 50 {
											txInFlight--

											return nil, xerrors.Operation(xerrors.WithStatusCode(Ydb.StatusIds_BAD_SESSION))
										}

										stream.EXPECT().Recv().DoAndReturn(func() (*Ydb_Query.ExecuteQueryResponsePart, error) {
											txInFlight--

											return nil, io.EOF
										})

										return &Ydb_Query.ExecuteQueryResponsePart{
											Status: Ydb.StatusIds_SUCCESS,
											TxMeta: &Ydb_Query.TransactionMeta{
												Id: "456",
											},
											ExecStats: &Ydb_TableStats.QueryStats{},
										}, nil
									})

									return stream, nil
								})

							return newTestSessionWithClient("123", client, true), nil
						}), func(ctx context.Context, tx query.TxActor) error {
							return tx.Exec(ctx, "", options.WithCommit())
						}, tx.NewSettings(tx.WithSerializableReadWrite()))
						require.NoError(t, err)
						require.Zero(t, txInFlight)
					})
				})
			})
			t.Run("OnSecondExec", func(t *testing.T) {
				t.Run("WithoutCommit", func(t *testing.T) {
					xtest.TestManyTimes(t, func(t testing.TB) {
						ctrl := gomock.NewController(t)
						txInFlight := 0
						err := doTx(ctx, testPool(ctx, func(ctx context.Context) (*Session, error) {
							client := NewMockQueryServiceClient(ctrl)
							client.EXPECT().ExecuteQuery(gomock.Any(), gomock.Any()).DoAndReturn(
								func(ctx context.Context, request *Ydb_Query.ExecuteQueryRequest, option ...grpc.CallOption) (
									Ydb_Query_V1.QueryService_ExecuteQueryClient, error,
								) {
									if rand.Int31n(100) < 50 {
										return nil, xerrors.Operation(xerrors.WithStatusCode(Ydb.StatusIds_BAD_SESSION))
									}

									txInFlight++

									firstStream := NewMockQueryService_ExecuteQueryClient(ctrl)
									firstStream.EXPECT().Recv().DoAndReturn(func() (*Ydb_Query.ExecuteQueryResponsePart, error) {
										firstStream.EXPECT().Recv().DoAndReturn(func() (*Ydb_Query.ExecuteQueryResponsePart, error) {
											client.EXPECT().ExecuteQuery(gomock.Any(), gomock.Any()).DoAndReturn(
												func(ctx context.Context, request *Ydb_Query.ExecuteQueryRequest, option ...grpc.CallOption) (
													Ydb_Query_V1.QueryService_ExecuteQueryClient, error,
												) {
													if rand.Int31n(100) < 50 {
														client.EXPECT().RollbackTransaction(gomock.Any(), gomock.Any()).DoAndReturn(
															func(ctx context.Context,
																request *Ydb_Query.RollbackTransactionRequest,
																option ...grpc.CallOption,
															) (*Ydb_Query.RollbackTransactionResponse, error) {
																txInFlight--

																return &Ydb_Query.RollbackTransactionResponse{}, nil
															})

														return nil, xerrors.Operation(xerrors.WithStatusCode(Ydb.StatusIds_BAD_SESSION))
													}

													secondStream := NewMockQueryService_ExecuteQueryClient(ctrl)
													secondStream.EXPECT().Recv().DoAndReturn(func() (*Ydb_Query.ExecuteQueryResponsePart, error) {
														secondStream.EXPECT().Recv().DoAndReturn(func() (*Ydb_Query.ExecuteQueryResponsePart, error) {
															client.EXPECT().CommitTransaction(gomock.Any(), gomock.Any()).DoAndReturn(
																func(ctx context.Context, request *Ydb_Query.CommitTransactionRequest, option ...grpc.CallOption) (
																	*Ydb_Query.CommitTransactionResponse, error,
																) {
																	txInFlight--

																	return &Ydb_Query.CommitTransactionResponse{
																		Status: Ydb.StatusIds_SUCCESS,
																	}, nil
																})

															return nil, io.EOF
														})

														return &Ydb_Query.ExecuteQueryResponsePart{
															Status:    Ydb.StatusIds_SUCCESS,
															TxMeta:    &Ydb_Query.TransactionMeta{},
															ExecStats: &Ydb_TableStats.QueryStats{},
														}, nil
													})

													return secondStream, nil
												})

											return nil, io.EOF
										})

										return &Ydb_Query.ExecuteQueryResponsePart{
											Status: Ydb.StatusIds_SUCCESS,
											TxMeta: &Ydb_Query.TransactionMeta{
												Id: "456",
											},
											ExecStats: &Ydb_TableStats.QueryStats{},
										}, nil
									})

									return firstStream, nil
								})

							return newTestSessionWithClient("123", client, true), nil
						}), func(ctx context.Context, tx query.TxActor) error {
							if err := tx.Exec(ctx, ""); err != nil {
								return err
							}

							return tx.Exec(ctx, "")
						}, tx.NewSettings(tx.WithSerializableReadWrite()))
						require.NoError(t, err)
					})
				})
				t.Run("WithCommit", func(t *testing.T) {
					xtest.TestManyTimes(t, func(t testing.TB) {
						ctrl := gomock.NewController(t)
						txInFlight := 0
						err := doTx(ctx, testPool(ctx, func(ctx context.Context) (*Session, error) {
							client := NewMockQueryServiceClient(ctrl)
							client.EXPECT().ExecuteQuery(gomock.Any(), gomock.Any()).DoAndReturn(
								func(ctx context.Context, request *Ydb_Query.ExecuteQueryRequest, option ...grpc.CallOption) (
									Ydb_Query_V1.QueryService_ExecuteQueryClient, error,
								) {
									if rand.Int31n(100) < 50 {
										return nil, xerrors.Operation(xerrors.WithStatusCode(Ydb.StatusIds_BAD_SESSION))
									}

									txInFlight++

									firstStream := NewMockQueryService_ExecuteQueryClient(ctrl)
									firstStream.EXPECT().Recv().DoAndReturn(func() (*Ydb_Query.ExecuteQueryResponsePart, error) {
										firstStream.EXPECT().Recv().DoAndReturn(func() (*Ydb_Query.ExecuteQueryResponsePart, error) {
											client.EXPECT().ExecuteQuery(gomock.Any(), gomock.Any()).DoAndReturn(
												func(ctx context.Context, request *Ydb_Query.ExecuteQueryRequest, option ...grpc.CallOption) (
													Ydb_Query_V1.QueryService_ExecuteQueryClient, error,
												) {
													require.True(t, request.GetTxControl().GetCommitTx())

													if rand.Int31n(100) < 50 {
														client.EXPECT().RollbackTransaction(gomock.Any(), gomock.Any()).DoAndReturn(
															func(ctx context.Context,
																request *Ydb_Query.RollbackTransactionRequest,
																option ...grpc.CallOption,
															) (*Ydb_Query.RollbackTransactionResponse, error) {
																txInFlight--

																return &Ydb_Query.RollbackTransactionResponse{}, nil
															})

														return nil, xerrors.Operation(xerrors.WithStatusCode(Ydb.StatusIds_BAD_SESSION))
													}

													secondStream := NewMockQueryService_ExecuteQueryClient(ctrl)
													secondStream.EXPECT().Recv().DoAndReturn(func() (*Ydb_Query.ExecuteQueryResponsePart, error) {
														secondStream.EXPECT().Recv().DoAndReturn(func() (*Ydb_Query.ExecuteQueryResponsePart, error) {
															return nil, io.EOF
														})

														return &Ydb_Query.ExecuteQueryResponsePart{
															Status:    Ydb.StatusIds_SUCCESS,
															TxMeta:    &Ydb_Query.TransactionMeta{},
															ExecStats: &Ydb_TableStats.QueryStats{},
														}, nil
													})

													return secondStream, nil
												})

											return nil, io.EOF
										})

										return &Ydb_Query.ExecuteQueryResponsePart{
											Status: Ydb.StatusIds_SUCCESS,
											TxMeta: &Ydb_Query.TransactionMeta{
												Id: "456",
											},
											ExecStats: &Ydb_TableStats.QueryStats{},
										}, nil
									})

									return firstStream, nil
								})

							return newTestSessionWithClient("123", client, true), nil
						}), func(ctx context.Context, tx query.TxActor) error {
							if err := tx.Exec(ctx, ""); err != nil {
								return err
							}

							return tx.Exec(ctx, "", options.WithCommit())
						}, tx.NewSettings(tx.WithSerializableReadWrite()))
						require.NoError(t, err)
					})
				})
			})
		})
	})
	t.Run("Exec", func(t *testing.T) {
		t.Run("HappyWay", func(t *testing.T) {
			ctrl := gomock.NewController(t)
			err := clientExec(ctx, testPool(ctx, func(ctx context.Context) (*Session, error) {
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

				return newTestSessionWithClient("123", client, true), nil
			}), "")
			require.NoError(t, err)
		})

		t.Run("AllowImplicitSessions", func(t *testing.T) {
			err := mockClientForImplicitSessionTest(ctx, t).
				Exec(ctx, "SELECT 1")

			require.NoError(t, err)
		})
	})
	t.Run("Query", func(t *testing.T) {
		t.Run("HappyWay", func(t *testing.T) {
			ctrl := gomock.NewController(t)
			r, err := clientQuery(ctx, testPool(ctx, func(ctx context.Context) (*Session, error) {
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

				return newTestSessionWithClient("123", client, true), nil
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
		})
		t.Run("AllowImplicitSessions", func(t *testing.T) {
			_, err := mockClientForImplicitSessionTest(ctx, t).
				Query(ctx, "SELECT 1")

			require.NoError(t, err)
		})
	})
	t.Run("QueryResultSet", func(t *testing.T) {
		t.Run("HappyWay", func(t *testing.T) {
			ctrl := gomock.NewController(t)
			rs, rowsCount, err := clientQueryResultSet(ctx, testPool(ctx, func(ctx context.Context) (*Session, error) {
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
				stream.EXPECT().Recv().Return(nil, io.EOF)
				client := NewMockQueryServiceClient(ctrl)
				client.EXPECT().ExecuteQuery(gomock.Any(), gomock.Any()).Return(stream, nil)

				return newTestSessionWithClient("123", client, true), nil
			}), "", options.ExecuteSettings())
			require.NoError(t, err)
			require.NotNil(t, rs)
			require.Equal(t, 5, rowsCount)
			{
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
		})
		t.Run("MoreThanOneResultSet", func(t *testing.T) {
			ctrl := gomock.NewController(t)
			rs, rowsCount, err := clientQueryResultSet(ctx, testPool(ctx, func(ctx context.Context) (*Session, error) {
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

				return newTestSessionWithClient("123", client, true), nil
			}), "", options.ExecuteSettings())
			require.ErrorIs(t, err, errMoreThanOneResultSet)
			require.Nil(t, rs)
			require.Equal(t, 0, rowsCount)
		})
		t.Run("AllowImplicitSessions", func(t *testing.T) {
			_, err := mockClientForImplicitSessionTest(ctx, t).
				QueryResultSet(ctx, "SELECT 1")

			require.NoError(t, err)
		})
	})
	t.Run("QueryRow", func(t *testing.T) {
		t.Run("HappyWay", func(t *testing.T) {
			ctrl := gomock.NewController(t)
			row, err := clientQueryRow(ctx, testPool(ctx, func(ctx context.Context) (*Session, error) {
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

				return newTestSessionWithClient("123", client, true), nil
			}), "", options.ExecuteSettings())
			require.NoError(t, err)
			require.NotNil(t, row)
			{
				var (
					a uint64
					b string
				)
				err = row.Scan(&a, &b)
				require.NoError(t, err)
				require.EqualValues(t, 1, a)
				require.EqualValues(t, "1", b)
			}
		})
		t.Run("MoreThanOneRow", func(t *testing.T) {
			ctrl := gomock.NewController(t)
			row, err := clientQueryRow(ctx, testPool(ctx, func(ctx context.Context) (*Session, error) {
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
				stream.EXPECT().Recv().Return(nil, io.EOF)
				client := NewMockQueryServiceClient(ctrl)
				client.EXPECT().ExecuteQuery(gomock.Any(), gomock.Any()).Return(stream, nil)

				return newTestSessionWithClient("123", client, true), nil
			}), "", options.ExecuteSettings())
			require.ErrorIs(t, err, errMoreThanOneRow)
			require.Nil(t, row)
		})

		t.Run("AllowImplicitSessions", func(t *testing.T) {
			_, err := mockClientForImplicitSessionTest(ctx, t).
				QueryRow(ctx, "SELECT 1")

			require.NoError(t, err)
		})
	})

	t.Run("Close", func(t *testing.T) {
		t.Run("AllowImplicitSessions", func(t *testing.T) {
			client := mockClientForImplicitSessionTest(ctx, t)
			_, err := client.QueryRow(ctx, "SELECT 1")
			require.NoError(t, err)

			err = client.Close(context.Background())

			require.NoError(t, err)
		})
	})
}

// mockClientForImplicitSessionTest creates a new Client with a test balancer
// for simulating implicit session scenarios in query client testing. It configures
// the mock in such way that calling `CreateSession` or `AttachSession` will result in an error.
func mockClientForImplicitSessionTest(ctx context.Context, t *testing.T) *Client {
	ctrl := gomock.NewController(t)

	stream := NewMockQueryService_ExecuteQueryClient(ctrl)
	stream.EXPECT().Recv().Return(&Ydb_Query.ExecuteQueryResponsePart{
		ResultSet: &Ydb.ResultSet{Rows: []*Ydb.Value{{}}},
	}, nil)
	stream.EXPECT().Recv().Return(nil, io.EOF)

	queryService := NewMockQueryServiceClient(ctrl)
	queryService.EXPECT().ExecuteQuery(gomock.Any(), gomock.Any()).Return(stream, nil)

	cfg := config.New(config.AllowImplicitSessions())

	return newWithQueryServiceClient(ctx, queryService, nil, cfg)
}

type sessionControllerMock struct {
	id     string
	status Status
	done   chan struct{}
}

func (s *sessionControllerMock) IsAlive() bool {
	return IsAlive(s.status)
}

func (s *sessionControllerMock) Close(ctx context.Context) error {
	return nil
}

func (s *sessionControllerMock) SetStatus(status Status) {
	s.status = status
}

func (s *sessionControllerMock) ID() string {
	return s.id
}

func (s *sessionControllerMock) NodeID() uint32 {
	return 0
}

func (s sessionControllerMock) Status() string {
	return s.status.String()
}

func (s sessionControllerMock) Done() <-chan struct{} {
	return s.done
}

func newTestSession(id string) *Session {
	return &Session{
		Core: &sessionControllerMock{
			id:   id,
			done: make(chan struct{}),
		},
		trace: &trace.Query{},
	}
}

func newTestSessionWithClient(id string, client Ydb_Query_V1.QueryServiceClient, lazyTx bool) *Session {
	return &Session{
		Core: &sessionControllerMock{
			id:   id,
			done: make(chan struct{}),
		},
		client: client,
		trace:  &trace.Query{},
		lazyTx: lazyTx,
	}
}

func testPool(
	ctx context.Context,
	createSession func(ctx context.Context) (*Session, error),
) *pool.Pool[*Session, Session] {
	return pool.New[*Session, Session](ctx,
		pool.WithLimit[*Session, Session](1),
		pool.WithCreateItemFunc(createSession),
	)
}

func TestQueryScript(t *testing.T) {
	ctx := xtest.Context(t)
	t.Run("HappyWay", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		client := NewMockQueryServiceClient(ctrl)
		client.EXPECT().ExecuteScript(gomock.Any(), gomock.Any()).Return(&Ydb_Operations.Operation{
			Id:     "123",
			Ready:  true,
			Status: Ydb.StatusIds_SUCCESS,
			Metadata: xtest.Must(anypb.New(&Ydb_Query.ExecuteScriptMetadata{
				ExecutionId: "123",
				ExecStatus:  Ydb_Query.ExecStatus_EXEC_STATUS_STARTING,
				ScriptContent: &Ydb_Query.QueryContent{
					Syntax: Ydb_Query.Syntax_SYNTAX_YQL_V1,
					Text:   "SELECT 1 AS a, 2 AS b",
				},
				ResultSetsMeta: []*Ydb_Query.ResultSetMeta{
					{
						Columns: []*Ydb.Column{
							{
								Name: "a",
								Type: &Ydb.Type{
									Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_INT32},
								},
							},
							{
								Name: "b",
								Type: &Ydb.Type{
									Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_INT32},
								},
							},
						},
					},
				},
				ExecMode: Ydb_Query.ExecMode_EXEC_MODE_EXECUTE,
				ExecStats: &Ydb_TableStats.QueryStats{
					QueryPhases:      nil,
					Compilation:      nil,
					ProcessCpuTimeUs: 0,
					QueryPlan:        "",
					QueryAst:         "",
					TotalDurationUs:  0,
					TotalCpuTimeUs:   0,
				},
			})),
			CostInfo: nil,
		}, nil)
		client.EXPECT().FetchScriptResults(gomock.Any(), gomock.Any()).Return(&Ydb_Query.FetchScriptResultsResponse{
			Status:         Ydb.StatusIds_SUCCESS,
			ResultSetIndex: 0,
			ResultSet: &Ydb.ResultSet{
				Columns: []*Ydb.Column{
					{
						Name: "a",
						Type: &Ydb.Type{
							Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_INT32},
						},
					},
					{
						Name: "b",
						Type: &Ydb.Type{
							Type: &Ydb.Type_TypeId{TypeId: Ydb.Type_INT32},
						},
					},
				},
				Rows: []*Ydb.Value{
					{
						Items: []*Ydb.Value{
							{
								Value: &Ydb.Value_Int32Value{
									Int32Value: 1,
								},
								VariantIndex: 0,
							},
							{
								Value: &Ydb.Value_Int32Value{
									Int32Value: 2,
								},
								VariantIndex: 0,
							},
						},
					},
				},
				Truncated: false,
			},
			NextFetchToken: "456",
		}, nil)
		op, err := executeScript(ctx, client, &Ydb_Query.ExecuteScriptRequest{})
		require.NoError(t, err)
		require.EqualValues(t, "123", op.ID)
		r, err := fetchScriptResults(ctx, client, op.ID)
		require.NoError(t, err)
		require.EqualValues(t, 0, r.ResultSetIndex)
		require.Equal(t, "456", r.NextToken)
		require.NotNil(t, r.ResultSet)
		row, err := r.ResultSet.NextRow(ctx)
		require.NoError(t, err)
		var (
			a int
			b int
		)
		err = row.Scan(&a, &b)
		require.NoError(t, err)
		require.EqualValues(t, 1, a)
		require.EqualValues(t, 2, b)
	})
	t.Run("Error", func(t *testing.T) {
		t.Run("OnExecute", func(t *testing.T) {
		})
		t.Run("OnFetch", func(t *testing.T) {
		})
	})
}
