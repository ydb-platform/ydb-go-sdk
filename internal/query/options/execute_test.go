package options

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/params"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/stats"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/tx"
)

type txMock func() *tx.Control

func (f txMock) txControl() *tx.Control {
	if f == nil {
		return tx.NewControl(tx.WithTxID(""))
	}

	return f()
}

func TestExecuteSettings(t *testing.T) {
	for _, tt := range []struct {
		name     string
		tx       txMock
		txOpts   []Execute
		settings executeSettings
	}{
		{
			name: "WithTxID",
			tx: func() *tx.Control {
				return tx.NewControl(tx.WithTxID("test"))
			},
			settings: executeSettings{
				execMode:  ExecModeExecute,
				statsMode: StatsModeNone,
				txControl: tx.NewControl(tx.WithTxID("test")),
				syntax:    SyntaxYQL,
				params:    &params.Params{},
			},
		},
		{
			name: "WithStats",
			txOpts: []Execute{
				WithStatsMode(StatsModeFull, nil),
			},
			settings: executeSettings{
				execMode:  ExecModeExecute,
				statsMode: StatsModeFull,
				txControl: tx.NewControl(tx.WithTxID("")),
				syntax:    SyntaxYQL,
				params:    &params.Params{},
			},
		},
		{
			name: "WithExecMode",
			txOpts: []Execute{
				WithExecMode(ExecModeExplain),
			},
			settings: executeSettings{
				execMode:  ExecModeExplain,
				statsMode: StatsModeNone,
				txControl: tx.NewControl(tx.WithTxID("")),
				syntax:    SyntaxYQL,
				params:    &params.Params{},
			},
		},
		{
			name: "WithSyntax",
			txOpts: []Execute{
				WithSyntax(SyntaxPostgreSQL),
			},
			settings: executeSettings{
				execMode:  ExecModeExecute,
				statsMode: StatsModeNone,
				txControl: tx.NewControl(tx.WithTxID("")),
				syntax:    SyntaxPostgreSQL,
				params:    &params.Params{},
			},
		},
		{
			name: "WithGrpcOptions",
			txOpts: []Execute{
				WithCallOptions(grpc.CallContentSubtype("test")),
			},
			settings: executeSettings{
				execMode:  ExecModeExecute,
				statsMode: StatsModeNone,
				txControl: tx.NewControl(tx.WithTxID("")),
				syntax:    SyntaxYQL,
				params:    &params.Params{},
				callOptions: []grpc.CallOption{
					grpc.CallContentSubtype("test"),
				},
			},
		},
		{
			name: "WithParams",
			txOpts: []Execute{
				WithParameters(
					params.Builder{}.Param("$a").Text("A").Build(),
				),
			},
			settings: executeSettings{
				execMode:  ExecModeExecute,
				statsMode: StatsModeNone,
				txControl: tx.NewControl(tx.WithTxID("")),
				syntax:    SyntaxYQL,
				params:    params.Builder{}.Param("$a").Text("A").Build(),
			},
		},
		{
			name: "WithCommitTx",
			txOpts: []Execute{
				WithCommit(),
			},
			settings: executeSettings{
				execMode:  ExecModeExecute,
				statsMode: StatsModeNone,
				txControl: tx.NewControl(tx.WithTxID(""), tx.CommitTx()),
				syntax:    SyntaxYQL,
				params:    &params.Params{},
			},
		},
		{
			name: "WithResourcePool",
			txOpts: []Execute{
				WithResourcePool("test-pool-id"),
			},
			settings: executeSettings{
				execMode:     ExecModeExecute,
				statsMode:    StatsModeNone,
				txControl:    tx.NewControl(tx.WithTxID("")),
				syntax:       SyntaxYQL,
				params:       &params.Params{},
				resourcePool: "test-pool-id",
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			settings := ExecuteSettings(
				append(
					[]Execute{WithTxControl(tt.tx.txControl())},
					tt.txOpts...,
				)...,
			)
			require.Equal(t, tt.settings.Syntax(), settings.Syntax())
			require.Equal(t, tt.settings.ExecMode(), settings.ExecMode())
			require.Equal(t, tt.settings.StatsMode(), settings.StatsMode())
			require.Equal(t, tt.settings.ResourcePool(), settings.ResourcePool())
			require.Equal(t,
				tt.settings.TxControl().ToYdbQueryTransactionControl().String(),
				settings.TxControl().ToYdbQueryTransactionControl().String(),
			)
			require.Equal(t, must(tt.settings.Params().ToYDB()), must(settings.Params().ToYDB()))
			require.Equal(t, tt.settings.CallOptions(), settings.CallOptions())
		})
	}
}

func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}

	return v
}

func TestResponsePartLimitSizeBytes(t *testing.T) {
	settings := ExecuteSettings(
		WithTxControl(tx.NewControl(tx.WithTxID(""))),
		WithResponsePartLimitSizeBytes(1024),
	)
	require.Equal(t, int64(1024), settings.ResponsePartLimitSizeBytes())
}

func TestLabel(t *testing.T) {
	settings := defaultExecuteSettings()
	require.Equal(t, "undefined", settings.Label())
}

func TestStatsModeApplyWithCallback(t *testing.T) {
	called := false
	callback := func(stats.QueryStats) {
		called = true
	}
	settings := ExecuteSettings(
		WithTxControl(tx.NewControl(tx.WithTxID(""))),
		WithStatsMode(StatsModeBasic, callback),
	)
	require.Equal(t, StatsModeBasic, settings.StatsMode())
	require.NotNil(t, settings.StatsCallback())
	// Verify callback works
	settings.StatsCallback()(nil)
	require.True(t, called)
}

func TestRetryOpts(t *testing.T) {
	settings := defaultExecuteSettings()
	require.Nil(t, settings.RetryOpts())
}

func TestStatsCallback(t *testing.T) {
	settings := defaultExecuteSettings()
	require.Nil(t, settings.StatsCallback())
}

func TestStatsMode(t *testing.T) {
	t.Run("StatsModeNone", func(t *testing.T) {
		settings := ExecuteSettings(
			WithTxControl(tx.NewControl(tx.WithTxID(""))),
			StatsMode(StatsModeNone),
		)
		require.Equal(t, StatsModeNone, settings.StatsMode())
	})

	t.Run("StatsModeBasic", func(t *testing.T) {
		settings := ExecuteSettings(
			WithTxControl(tx.NewControl(tx.WithTxID(""))),
			StatsMode(StatsModeBasic),
		)
		require.Equal(t, StatsModeBasic, settings.StatsMode())
	})
}

func TestThisOptionIsNotForExecuteOnTx(t *testing.T) {
	txCtrl := (*txControlOption)(tx.NewControl(tx.WithTxID("test")))
	// Should not panic
	txCtrl.thisOptionIsNotForExecuteOnTx()
}
