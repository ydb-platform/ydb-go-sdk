package options

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/allocator"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/params"
	internal "github.com/ydb-platform/ydb-go-sdk/v3/internal/query/tx"
)

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
		txOpts   []Execute
		settings executeSettings
	}{
		{
			name: "WithTxID",
			tx: func() *internal.Control {
				return internal.NewControl(internal.WithTxID("test"))
			},
			settings: executeSettings{
				execMode:  ExecModeExecute,
				statsMode: StatsModeNone,
				txControl: internal.NewControl(internal.WithTxID("test")),
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
				txControl: internal.NewControl(internal.WithTxID("")),
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
				txControl: internal.NewControl(internal.WithTxID("")),
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
				txControl: internal.NewControl(internal.WithTxID("")),
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
				txControl: internal.NewControl(internal.WithTxID("")),
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
				txControl: internal.NewControl(internal.WithTxID("")),
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
				txControl: internal.NewControl(internal.WithTxID(""), internal.CommitTx()),
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
				txControl:    internal.NewControl(internal.WithTxID("")),
				syntax:       SyntaxYQL,
				params:       &params.Params{},
				resourcePool: "test-pool-id",
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			a := allocator.New()
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
			require.Equal(t, tt.settings.TxControl().ToYDB(a).String(), settings.TxControl().ToYDB(a).String())
			require.Equal(t, must(tt.settings.Params().ToYDB(a)), must(settings.Params().ToYDB(a)))
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
