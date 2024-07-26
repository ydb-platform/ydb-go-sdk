package query

import (
	"context"
	"errors"
	"sync"
	"testing"

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
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xtest"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
)

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
		OnTransactionCompleted(tx, func(transactionResult error) {
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
		OnTransactionCompleted(tx, func(transactionResult error) {
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
		var completed []error

		OnTransactionCompleted(tx, func(transactionResult error) {
			// notification before call to the server
			require.False(t, rollbackCalled)
			completed = append(completed, transactionResult)
		})

		_ = tx.Rollback(sf.Context(e))
		require.Len(t, completed, 1)
		require.ErrorIs(t, completed[0], ErrTransactionRollingBack)
	})
	t.Run("OnExecuteWithoutCommitTxSuccess", func(t *testing.T) {
		e := fixenv.New(t)

		responseStream := NewMockQueryService_ExecuteQueryClient(MockController(e))
		responseStream.EXPECT().Recv().Return(&Ydb_Query.ExecuteQueryResponsePart{
			Status: Ydb.StatusIds_SUCCESS,
		}, nil)

		QueryGrpcMock(e).EXPECT().ExecuteQuery(gomock.Any(), gomock.Any()).Return(responseStream, nil)

		tx := TransactionOverGrpcMock(e)
		var completed []error

		OnTransactionCompleted(tx, func(transactionResult error) {
			completed = append(completed, transactionResult)
		})

		_, err := tx.Execute(sf.Context(e), "")
		require.NoError(t, err)
		require.Len(t, completed, 0)
	})
	t.Run("OnExecuteWithTxSuccess", func(t *testing.T) {
		e := fixenv.New(t)

		responseStream := NewMockQueryService_ExecuteQueryClient(MockController(e))
		responseStream.EXPECT().Recv().Return(&Ydb_Query.ExecuteQueryResponsePart{
			Status: Ydb.StatusIds_SUCCESS,
		}, nil)

		QueryGrpcMock(e).EXPECT().ExecuteQuery(gomock.Any(), gomock.Any()).Return(responseStream, nil)

		tx := TransactionOverGrpcMock(e)
		var completedMutex sync.Mutex
		var completed []error

		OnTransactionCompleted(tx, func(transactionResult error) {
			completedMutex.Lock()
			completed = append(completed, transactionResult)
			completedMutex.Unlock()
		})

		res, err := tx.Execute(sf.Context(e), "", options.WithCommit())
		_ = res.Close(sf.Context(e))
		require.NoError(t, err)
		xtest.SpinWaitCondition(t, &completedMutex, func() bool {
			return len(completed) != 0
		})
		require.Len(t, completed, 1)
		require.ErrorIs(t, completed[0], ErrTransactionRollingBack)
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

var _ executeConfig = testExecuteSettings{}

func TestTxExecuteSettings(t *testing.T) {
	for _, tt := range []struct {
		name     string
		txID     string
		txOpts   []options.TxExecuteOption
		settings executeConfig
	}{
		{
			name:   "WithTxID",
			txID:   "test",
			txOpts: nil,
			settings: testExecuteSettings{
				execMode:  options.ExecModeExecute,
				statsMode: options.StatsModeNone,
				txControl: query.TxControl(query.WithTxID("test")),
				syntax:    options.SyntaxYQL,
			},
		},
		{
			name: "WithStats",
			txOpts: []options.TxExecuteOption{
				options.WithStatsMode(options.StatsModeFull),
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
			txOpts: []options.TxExecuteOption{
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
			txOpts: []options.TxExecuteOption{
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
			txOpts: []options.TxExecuteOption{
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
			txOpts: []options.TxExecuteOption{
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
	} {
		t.Run(tt.name, func(t *testing.T) {
			a := allocator.New()
			settings := options.TxExecuteSettings(tt.txID, tt.txOpts...).ExecuteSettings
			require.Equal(t, tt.settings.Syntax(), settings.Syntax())
			require.Equal(t, tt.settings.ExecMode(), settings.ExecMode())
			require.Equal(t, tt.settings.StatsMode(), settings.StatsMode())
			require.Equal(t, tt.settings.TxControl().ToYDB(a).String(), settings.TxControl().ToYDB(a).String())
			require.Equal(t, tt.settings.Params().ToYDB(a), settings.Params().ToYDB(a))
			require.Equal(t, tt.settings.CallOptions(), settings.CallOptions())
		})
	}
}
