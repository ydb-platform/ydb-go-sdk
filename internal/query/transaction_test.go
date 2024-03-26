package query

import (
	"testing"

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
