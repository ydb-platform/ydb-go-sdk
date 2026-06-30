//go:build integration
// +build integration

package integration

import (
	"database/sql"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func TestDatabaseSQLDefaultProcessor(st *testing.T) {
	for _, tt := range []struct {
		name         string
		opts         []ydb.ConnectorOption
		expQueryCall bool
		expTableCall bool
	}{
		{
			name:         "ExplicitTableProcessor",
			opts:         []ydb.ConnectorOption{ydb.WithQueryService(false)},
			expQueryCall: false,
			expTableCall: true,
		},
		{
			name:         "ExplicitQueryProcessor",
			opts:         []ydb.ConnectorOption{ydb.WithQueryService(true)},
			expQueryCall: true,
			expTableCall: false,
		},
		{
			name:         "DefaultProcessor",
			opts:         nil,
			expQueryCall: true,
			expTableCall: false,
		},
	} {
		st.Run(tt.name, func(st *testing.T) {
			t := newScope(st)

			var (
				queryCalled bool
				tableCalled bool
			)

			driver := t.Driver(
				ydb.WithTraceQuery(trace.Query{
					OnSessionExec: func(info trace.QuerySessionExecStartInfo) func(info trace.QuerySessionExecDoneInfo) {
						queryCalled = true

						return nil
					},
				}),
				ydb.WithTraceTable(trace.Table{
					OnSessionQueryExecute: func(info trace.TableExecuteDataQueryStartInfo) func(trace.TableExecuteDataQueryDoneInfo) {
						tableCalled = true

						return nil
					},
				}),
			)
			defer driver.Close(st.Context())

			db := sql.OpenDB(ydb.MustConnector(driver, tt.opts...))
			defer db.Close()

			_, err := db.Exec("SELECT 1")
			require.NoError(st, err)
			assert.Equal(st, tt.expQueryCall, queryCalled, "queryCalled mismatch")
			assert.Equal(st, tt.expTableCall, tableCalled, "tableCalled mismatch")
		})
	}
}
