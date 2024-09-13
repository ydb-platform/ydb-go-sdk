package query

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/rekby/fixenv"
	"github.com/rekby/fixenv/sf"
	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Query"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
	grpcCodes "google.golang.org/grpc/codes"
	grpcStatus "google.golang.org/grpc/status"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/allocator"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/params"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/query/options"
	internal "github.com/ydb-platform/ydb-go-sdk/v3/internal/query/tx"
	baseTx "github.com/ydb-platform/ydb-go-sdk/v3/internal/tx"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xtest"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
	"github.com/ydb-platform/ydb-go-sdk/v3/retry"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/stats"
)

var _ baseTx.Transaction = &Transaction{}

func TestBegin(t *testing.T) {
	t.Run("HappyWay", func(t *testing.T) {
		ctx := xtest.Context(t)
		ctrl := gomock.NewController(t)
		client := NewMockQueryServiceClient(ctrl)
		client.EXPECT().BeginTransaction(gomock.Any(), gomock.Any()).Return(&Ydb_Query.BeginTransactionResponse{
			Status: Ydb.StatusIds_SUCCESS,
			TxMeta: &Ydb_Query.TransactionMeta{
				Id: "123",
			},
		}, nil)
		t.Log("begin")
		txID, err := begin(ctx, client, "123", query.TxSettings())
		require.NoError(t, err)
		require.Equal(t, "123", txID)
	})
	t.Run("TransportError", func(t *testing.T) {
		ctx := xtest.Context(t)
		ctrl := gomock.NewController(t)
		client := NewMockQueryServiceClient(ctrl)
		client.EXPECT().BeginTransaction(gomock.Any(), gomock.Any()).Return(nil, grpcStatus.Error(grpcCodes.Unavailable, ""))
		t.Log("begin")
		_, err := begin(ctx, client, "123", query.TxSettings())
		require.Error(t, err)
		require.True(t, xerrors.IsTransportError(err, grpcCodes.Unavailable))
	})
	t.Run("OperationError", func(t *testing.T) {
		ctx := xtest.Context(t)
		ctrl := gomock.NewController(t)
		client := NewMockQueryServiceClient(ctrl)
		client.EXPECT().BeginTransaction(gomock.Any(), gomock.Any()).Return(nil,
			xerrors.Operation(xerrors.WithStatusCode(Ydb.StatusIds_UNAVAILABLE)),
		)
		t.Log("begin")
		_, err := begin(ctx, client, "123", query.TxSettings())
		require.Error(t, err)
		require.True(t, xerrors.IsOperationError(err, Ydb.StatusIds_UNAVAILABLE))
	})
}

func TestCommitTx(t *testing.T) {
	t.Run("HappyWay", func(t *testing.T) {
		ctx := xtest.Context(t)
		ctrl := gomock.NewController(t)
		service := NewMockQueryServiceClient(ctrl)
		service.EXPECT().CommitTransaction(gomock.Any(), gomock.Any()).Return(
			&Ydb_Query.CommitTransactionResponse{
				Status: Ydb.StatusIds_SUCCESS,
			}, nil,
		)
		t.Log("commit")
		err := commitTx(ctx, service, "123", "456")
		require.NoError(t, err)
	})
	t.Run("TransportError", func(t *testing.T) {
		ctx := xtest.Context(t)
		ctrl := gomock.NewController(t)
		service := NewMockQueryServiceClient(ctrl)
		service.EXPECT().CommitTransaction(gomock.Any(), gomock.Any()).Return(
			nil, grpcStatus.Error(grpcCodes.Unavailable, ""),
		)
		t.Log("commit")
		err := commitTx(ctx, service, "123", "456")
		require.Error(t, err)
		require.True(t, xerrors.IsTransportError(err, grpcCodes.Unavailable))
	})
	t.Run("OperationError", func(t *testing.T) {
		ctx := xtest.Context(t)
		ctrl := gomock.NewController(t)
		service := NewMockQueryServiceClient(ctrl)
		service.EXPECT().CommitTransaction(gomock.Any(), gomock.Any()).Return(nil,
			xerrors.Operation(xerrors.WithStatusCode(Ydb.StatusIds_UNAVAILABLE)),
		)
		t.Log("commit")
		err := commitTx(ctx, service, "123", "456")
		require.Error(t, err)
		require.True(t, xerrors.IsOperationError(err, Ydb.StatusIds_UNAVAILABLE))
	})
}

func TestTxOnCompleted(t *testing.T) {
	t.Run("OnCommitTxSuccess", func(t *testing.T) {
		e := fixenv.New(t)

		QueryGrpcMock(e).EXPECT().CommitTransaction(gomock.Any(), gomock.Any()).Return(
			&Ydb_Query.CommitTransactionResponse{
				Status: Ydb.StatusIds_SUCCESS,
			}, nil,
		)

		tx := TransactionOverGrpcMock(e)

		var completed []error
		tx.OnCompleted(func(transactionResult error) {
			completed = append(completed, transactionResult)
		})
		err := tx.CommitTx(sf.Context(e))
		require.NoError(t, err)
		require.Equal(t, []error{nil}, completed)
	})
	t.Run("OnCommitTxFailed", func(t *testing.T) {
		e := fixenv.New(t)

		testError := errors.New("test-error")

		QueryGrpcMock(e).EXPECT().CommitTransaction(gomock.Any(), gomock.Any()).Return(nil,
			testError,
		)

		tx := TransactionOverGrpcMock(e)

		var completed []error
		tx.OnCompleted(func(transactionResult error) {
			completed = append(completed, transactionResult)
		})
		err := tx.CommitTx(sf.Context(e))
		require.ErrorIs(t, err, testError)
		require.Len(t, completed, 1)
		require.ErrorIs(t, completed[0], err)
	})
	t.Run("OnRollback", func(t *testing.T) {
		e := fixenv.New(t)

		rollbackCalled := false
		QueryGrpcMock(e).EXPECT().RollbackTransaction(gomock.Any(), gomock.Any()).DoAndReturn(
			func(
				ctx context.Context,
				request *Ydb_Query.RollbackTransactionRequest,
				option ...grpc.CallOption,
			) (
				*Ydb_Query.RollbackTransactionResponse,
				error,
			) {
				rollbackCalled = true

				return &Ydb_Query.RollbackTransactionResponse{
					Status: Ydb.StatusIds_SUCCESS,
				}, nil
			})

		tx := TransactionOverGrpcMock(e)
		var completed error

		tx.OnCompleted(func(transactionResult error) {
			// notification before call to the server
			require.False(t, rollbackCalled)
			completed = transactionResult
		})

		_ = tx.Rollback(sf.Context(e))
		require.ErrorIs(t, completed, ErrTransactionRollingBack)
	})
	t.Run("OnExecWithoutCommitTxSuccess", func(t *testing.T) {
		e := fixenv.New(t)

		responseStream := NewMockQueryService_ExecuteQueryClient(MockController(e))
		responseStream.EXPECT().Recv().Return(&Ydb_Query.ExecuteQueryResponsePart{
			Status: Ydb.StatusIds_SUCCESS,
		}, nil)
		responseStream.EXPECT().Recv().Return(nil, io.EOF)

		QueryGrpcMock(e).EXPECT().ExecuteQuery(gomock.Any(), gomock.Any()).Return(responseStream, nil)

		tx := TransactionOverGrpcMock(e)
		var completed []error

		tx.OnCompleted(func(transactionResult error) {
			completed = append(completed, transactionResult)
		})

		err := tx.Exec(sf.Context(e), "")
		require.NoError(t, err)
		require.Empty(t, completed)
	})
	t.Run("OnQueryWithoutCommitTxSuccess", func(t *testing.T) {
		e := fixenv.New(t)

		responseStream := NewMockQueryService_ExecuteQueryClient(MockController(e))
		responseStream.EXPECT().Recv().Return(&Ydb_Query.ExecuteQueryResponsePart{
			Status: Ydb.StatusIds_SUCCESS,
		}, nil)

		QueryGrpcMock(e).EXPECT().ExecuteQuery(gomock.Any(), gomock.Any()).Return(responseStream, nil)

		tx := TransactionOverGrpcMock(e)
		var completed []error

		tx.OnCompleted(func(transactionResult error) {
			completed = append(completed, transactionResult)
		})

		_, err := tx.Query(sf.Context(e), "")
		require.NoError(t, err)
		require.Empty(t, completed)
	})
	t.Run("OnExecWithoutTxSuccess", func(t *testing.T) {
		xtest.TestManyTimes(t, func(t testing.TB) {
			e := fixenv.New(t)

			responseStream := NewMockQueryService_ExecuteQueryClient(MockController(e))
			responseStream.EXPECT().Recv().Return(&Ydb_Query.ExecuteQueryResponsePart{
				Status: Ydb.StatusIds_SUCCESS,
			}, nil)
			responseStream.EXPECT().Recv().Return(nil, io.EOF)

			QueryGrpcMock(e).EXPECT().ExecuteQuery(gomock.Any(), gomock.Any()).Return(responseStream, nil)

			tx := TransactionOverGrpcMock(e)
			var completedMutex sync.Mutex
			var completed []error

			tx.OnCompleted(func(transactionResult error) {
				completedMutex.Lock()
				completed = append(completed, transactionResult)
				completedMutex.Unlock()
			})

			err := tx.Exec(sf.Context(e), "")
			require.NoError(t, err)
			require.Empty(t, completed)
		})
	})
	t.Run("OnQueryWithoutTxSuccess", func(t *testing.T) {
		xtest.TestManyTimes(t, func(t testing.TB) {
			e := fixenv.New(t)

			responseStream := NewMockQueryService_ExecuteQueryClient(MockController(e))
			responseStream.EXPECT().Recv().Return(&Ydb_Query.ExecuteQueryResponsePart{
				Status: Ydb.StatusIds_SUCCESS,
			}, nil)
			responseStream.EXPECT().Recv().Return(nil, io.EOF)

			QueryGrpcMock(e).EXPECT().ExecuteQuery(gomock.Any(), gomock.Any()).Return(responseStream, nil)

			tx := TransactionOverGrpcMock(e)
			var completedMutex sync.Mutex
			var completed []error

			tx.OnCompleted(func(transactionResult error) {
				completedMutex.Lock()
				completed = append(completed, transactionResult)
				completedMutex.Unlock()
			})

			res, err := tx.Query(sf.Context(e), "")
			require.NoError(t, err)
			_ = res.Close(sf.Context(e))
			time.Sleep(time.Millisecond) // time for reaction for closing channel
			require.Empty(t, completed)
		})
	})
	t.Run("OnExecWithTxSuccess", func(t *testing.T) {
		xtest.TestManyTimes(t, func(t testing.TB) {
			e := fixenv.New(t)

			responseStream := NewMockQueryService_ExecuteQueryClient(MockController(e))
			responseStream.EXPECT().Recv().Return(&Ydb_Query.ExecuteQueryResponsePart{
				Status: Ydb.StatusIds_SUCCESS,
			}, nil)
			responseStream.EXPECT().Recv().Return(nil, io.EOF)

			QueryGrpcMock(e).EXPECT().ExecuteQuery(gomock.Any(), gomock.Any()).Return(responseStream, nil)

			tx := TransactionOverGrpcMock(e)
			var completedMutex sync.Mutex
			var completed []error

			tx.OnCompleted(func(transactionResult error) {
				completedMutex.Lock()
				completed = append(completed, transactionResult)
				completedMutex.Unlock()
			})

			err := tx.Exec(sf.Context(e), "", options.WithCommit())
			require.NoError(t, err)
			xtest.SpinWaitCondition(t, &completedMutex, func() bool {
				return len(completed) != 0
			})
			require.Equal(t, []error{nil}, completed)
		})
	})
	t.Run("OnQueryWithTxSuccess", func(t *testing.T) {
		xtest.TestManyTimes(t, func(t testing.TB) {
			e := fixenv.New(t)

			responseStream := NewMockQueryService_ExecuteQueryClient(MockController(e))
			responseStream.EXPECT().Recv().Return(&Ydb_Query.ExecuteQueryResponsePart{
				Status: Ydb.StatusIds_SUCCESS,
			}, nil)
			responseStream.EXPECT().Recv().Return(nil, io.EOF)

			QueryGrpcMock(e).EXPECT().ExecuteQuery(gomock.Any(), gomock.Any()).Return(responseStream, nil)

			tx := TransactionOverGrpcMock(e)
			var completedMutex sync.Mutex
			var completed []error

			tx.OnCompleted(func(transactionResult error) {
				completedMutex.Lock()
				completed = append(completed, transactionResult)
				completedMutex.Unlock()
			})

			res, err := tx.Query(sf.Context(e), "", options.WithCommit())
			_ = res.Close(sf.Context(e))
			require.NoError(t, err)
			xtest.SpinWaitCondition(t, &completedMutex, func() bool {
				return len(completed) != 0
			})
			require.Equal(t, []error{nil}, completed)
		})
	})
	t.Run("OnQueryWithTxSuccessWithTwoResultSet", func(t *testing.T) {
		xtest.TestManyTimes(t, func(t testing.TB) {
			e := fixenv.New(t)

			responseStream := NewMockQueryService_ExecuteQueryClient(MockController(e))
			responseStream.EXPECT().Recv().Return(&Ydb_Query.ExecuteQueryResponsePart{
				ResultSetIndex: 0,
				ResultSet:      &Ydb.ResultSet{},
			}, nil)
			responseStream.EXPECT().Recv().Return(&Ydb_Query.ExecuteQueryResponsePart{
				Status:         Ydb.StatusIds_SUCCESS,
				ResultSetIndex: 1,
				ResultSet:      &Ydb.ResultSet{},
			}, nil)
			responseStream.EXPECT().Recv().Return(nil, io.EOF)

			QueryGrpcMock(e).EXPECT().ExecuteQuery(gomock.Any(), gomock.Any()).Return(responseStream, nil)

			tx := TransactionOverGrpcMock(e)
			var completedMutex sync.Mutex
			var completed []error

			tx.OnCompleted(func(transactionResult error) {
				completedMutex.Lock()
				completed = append(completed, transactionResult)
				completedMutex.Unlock()
			})

			res, err := tx.Query(sf.Context(e), "", options.WithCommit())
			require.NoError(t, err)

			// time for event happened if is
			time.Sleep(time.Millisecond)
			require.Empty(t, completed)

			_, err = res.NextResultSet(sf.Context(e))
			require.NoError(t, err)
			// time for event happened if is
			time.Sleep(time.Millisecond)
			require.Empty(t, completed)

			_, err = res.NextResultSet(sf.Context(e))
			require.NoError(t, err)

			_ = res.Close(sf.Context(e))
			require.NoError(t, err)
			xtest.SpinWaitCondition(t, &completedMutex, func() bool {
				return len(completed) != 0
			})
			require.Equal(t, []error{nil}, completed)
		})
	})
	t.Run("OnExecuteFailedOnInitResponse", func(t *testing.T) {
		for _, commit := range []bool{true, false} {
			t.Run(fmt.Sprint("commit:", commit), func(t *testing.T) {
				e := fixenv.New(t)

				testErr := errors.New("test")
				responseStream := NewMockQueryService_ExecuteQueryClient(MockController(e))
				responseStream.EXPECT().Recv().Return(nil, testErr)

				QueryGrpcMock(e).EXPECT().ExecuteQuery(gomock.Any(), gomock.Any()).Return(responseStream, nil)

				tx := TransactionOverGrpcMock(e)
				var transactionResult error

				tx.OnCompleted(func(err error) {
					transactionResult = err
				})

				err := tx.Exec(sf.Context(e), "", query.WithCommit())
				require.ErrorIs(t, err, testErr)
				require.Error(t, transactionResult)
				require.ErrorIs(t, transactionResult, testErr)
			})
		}
	})
	t.Run("OnExecuteFailedInResponsePart", func(t *testing.T) {
		for _, commit := range []bool{true, false} {
			t.Run(fmt.Sprint("commit:", commit), func(t *testing.T) {
				xtest.TestManyTimes(t, func(t testing.TB) {
					e := fixenv.New(t)

					errorReturned := false
					responseStream := NewMockQueryService_ExecuteQueryClient(MockController(e))
					responseStream.EXPECT().Recv().DoAndReturn(func() (*Ydb_Query.ExecuteQueryResponsePart, error) {
						errorReturned = true

						return nil, xerrors.Operation(xerrors.WithStatusCode(Ydb.StatusIds_BAD_SESSION))
					})

					QueryGrpcMock(e).EXPECT().ExecuteQuery(gomock.Any(), gomock.Any()).Return(responseStream, nil)

					tx := TransactionOverGrpcMock(e)
					var transactionResult error

					tx.OnCompleted(func(err error) {
						transactionResult = err
					})

					err := tx.Exec(sf.Context(e), "", query.WithCommit())
					require.True(t, errorReturned)
					require.True(t, xerrors.IsOperationError(err, Ydb.StatusIds_BAD_SESSION))
					require.Error(t, transactionResult)
					require.True(t, xerrors.IsOperationError(transactionResult, Ydb.StatusIds_BAD_SESSION))
				})
			})
		}
	})
}

func TestRollback(t *testing.T) {
	t.Run("HappyWay", func(t *testing.T) {
		ctx := xtest.Context(t)
		ctrl := gomock.NewController(t)
		service := NewMockQueryServiceClient(ctrl)
		service.EXPECT().RollbackTransaction(gomock.Any(), gomock.Any()).Return(
			&Ydb_Query.RollbackTransactionResponse{
				Status: Ydb.StatusIds_SUCCESS,
			}, nil,
		)
		t.Log("rollback")
		err := rollback(ctx, service, "123", "456")
		require.NoError(t, err)
	})
	t.Run("TransportError", func(t *testing.T) {
		ctx := xtest.Context(t)
		ctrl := gomock.NewController(t)
		service := NewMockQueryServiceClient(ctrl)
		service.EXPECT().RollbackTransaction(gomock.Any(), gomock.Any()).Return(
			nil, grpcStatus.Error(grpcCodes.Unavailable, ""),
		)
		t.Log("rollback")
		err := rollback(ctx, service, "123", "456")
		require.Error(t, err)
		require.True(t, xerrors.IsTransportError(err, grpcCodes.Unavailable))
	})
	t.Run("OperationError", func(t *testing.T) {
		ctx := xtest.Context(t)
		ctrl := gomock.NewController(t)
		service := NewMockQueryServiceClient(ctrl)
		service.EXPECT().RollbackTransaction(gomock.Any(), gomock.Any()).Return(nil,
			xerrors.Operation(xerrors.WithStatusCode(Ydb.StatusIds_UNAVAILABLE)),
		)
		t.Log("rollback")
		err := rollback(ctx, service, "123", "456")
		require.Error(t, err)
		require.True(t, xerrors.IsOperationError(err, Ydb.StatusIds_UNAVAILABLE))
	})
}

type testExecuteSettings struct {
	execMode    options.ExecMode
	statsMode   options.StatsMode
	txControl   *query.TransactionControl
	syntax      options.Syntax
	params      *params.Parameters
	callOptions []grpc.CallOption
}

func (s testExecuteSettings) RetryOpts() []retry.Option {
	return nil
}

func (s testExecuteSettings) StatsCallback() func(stats stats.QueryStats) {
	return nil
}

func (s testExecuteSettings) ExecMode() options.ExecMode {
	return s.execMode
}

func (s testExecuteSettings) StatsMode() options.StatsMode {
	return s.statsMode
}

func (s testExecuteSettings) TxControl() *query.TransactionControl {
	return s.txControl
}

func (s testExecuteSettings) Syntax() options.Syntax {
	return s.syntax
}

func (s testExecuteSettings) Params() *params.Parameters {
	return s.params
}

func (s testExecuteSettings) CallOptions() []grpc.CallOption {
	return s.callOptions
}

var _ executeSettings = testExecuteSettings{}

type txMock func() *internal.Control

func (tx txMock) txControl() *internal.Control {
	if tx == nil {
		return internal.NewControl(internal.WithTxID(""))
	}

	return tx()
}

func TestExecuteSettings(t *testing.T) {
	for _, tt := range []struct {
		name     string
		tx       txMock
		txOpts   []options.Execute
		settings executeSettings
	}{
		{
			name: "WithTxID",
			tx: func() *internal.Control {
				return internal.NewControl(internal.WithTxID("test"))
			},
			settings: testExecuteSettings{
				execMode:  options.ExecModeExecute,
				statsMode: options.StatsModeNone,
				txControl: query.TxControl(query.WithTxID("test")),
				syntax:    options.SyntaxYQL,
			},
		},
		{
			name: "WithStats",
			txOpts: []options.Execute{
				options.WithStatsMode(options.StatsModeFull, nil),
			},
			settings: testExecuteSettings{
				execMode:  options.ExecModeExecute,
				statsMode: options.StatsModeFull,
				txControl: query.TxControl(query.WithTxID("")),
				syntax:    options.SyntaxYQL,
			},
		},
		{
			name: "WithExecMode",
			txOpts: []options.Execute{
				options.WithExecMode(options.ExecModeExplain),
			},
			settings: testExecuteSettings{
				execMode:  options.ExecModeExplain,
				statsMode: options.StatsModeNone,
				txControl: query.TxControl(query.WithTxID("")),
				syntax:    options.SyntaxYQL,
			},
		},
		{
			name: "WithSyntax",
			txOpts: []options.Execute{
				options.WithSyntax(options.SyntaxPostgreSQL),
			},
			settings: testExecuteSettings{
				execMode:  options.ExecModeExecute,
				statsMode: options.StatsModeNone,
				txControl: query.TxControl(query.WithTxID("")),
				syntax:    options.SyntaxPostgreSQL,
			},
		},
		{
			name: "WithGrpcOptions",
			txOpts: []options.Execute{
				options.WithCallOptions(grpc.CallContentSubtype("test")),
			},
			settings: testExecuteSettings{
				execMode:  options.ExecModeExecute,
				statsMode: options.StatsModeNone,
				txControl: query.TxControl(query.WithTxID("")),
				syntax:    options.SyntaxYQL,
				callOptions: []grpc.CallOption{
					grpc.CallContentSubtype("test"),
				},
			},
		},
		{
			name: "WithParams",
			txOpts: []options.Execute{
				options.WithParameters(
					params.Builder{}.Param("$a").Text("A").Build(),
				),
			},
			settings: testExecuteSettings{
				execMode:  options.ExecModeExecute,
				statsMode: options.StatsModeNone,
				txControl: query.TxControl(query.WithTxID("")),
				syntax:    options.SyntaxYQL,
				params:    params.Builder{}.Param("$a").Text("A").Build(),
			},
		},
		{
			name: "WithCommitTx",
			txOpts: []options.Execute{
				options.WithCommit(),
			},
			settings: testExecuteSettings{
				execMode:  options.ExecModeExecute,
				statsMode: options.StatsModeNone,
				txControl: query.TxControl(query.WithTxID(""), query.CommitTx()),
				syntax:    options.SyntaxYQL,
				params:    nil,
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			a := allocator.New()
			settings := options.ExecuteSettings(
				append(
					[]options.Execute{options.WithTxControl(tt.tx.txControl())},
					tt.txOpts...,
				)...,
			)
			require.Equal(t, tt.settings.Syntax(), settings.Syntax())
			require.Equal(t, tt.settings.ExecMode(), settings.ExecMode())
			require.Equal(t, tt.settings.StatsMode(), settings.StatsMode())
			require.Equal(t, tt.settings.TxControl().ToYDB(a).String(), settings.TxControl().ToYDB(a).String())
			require.Equal(t, tt.settings.Params().ToYDB(a), settings.Params().ToYDB(a))
			require.Equal(t, tt.settings.CallOptions(), settings.CallOptions())
		})
	}
}
