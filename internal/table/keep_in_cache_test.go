package table

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Table"
	"google.golang.org/protobuf/proto"

	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
	"github.com/ydb-platform/ydb-go-sdk/v3/testutil"
)

func TestQueryCachePolicyKeepInCache(t *testing.T) {
	for _, test := range [...]struct {
		name                    string
		params                  *table.QueryParameters
		executeDataQueryOptions []options.ExecuteDataQueryOption
		keepInCache             bool
	}{
		{
			name:                    "no params, no options",
			params:                  table.NewQueryParameters(),
			executeDataQueryOptions: nil,
			keepInCache:             false,
		},
		{
			name:                    "not empty params, no options",
			params:                  table.NewQueryParameters(table.ValueParam("a", types.UTF8Value("b"))),
			executeDataQueryOptions: nil,
			keepInCache:             true,
		},
		{
			name:   "no params, with server cache",
			params: table.NewQueryParameters(),
			executeDataQueryOptions: []options.ExecuteDataQueryOption{
				options.WithKeepInCache(true),
			},
			keepInCache: true,
		},
		{
			name:   "not empty params, with server cache",
			params: table.NewQueryParameters(table.ValueParam("a", types.UTF8Value("b"))),
			executeDataQueryOptions: []options.ExecuteDataQueryOption{
				options.WithKeepInCache(true),
			},
			keepInCache: true,
		},
		{
			name:   "no params, no server cache",
			params: table.NewQueryParameters(),
			executeDataQueryOptions: []options.ExecuteDataQueryOption{
				options.WithKeepInCache(false),
			},
			keepInCache: false,
		},
		{
			name:   "not empty params, no server cache",
			params: table.NewQueryParameters(table.ValueParam("a", types.UTF8Value("b"))),
			executeDataQueryOptions: []options.ExecuteDataQueryOption{
				options.WithKeepInCache(false),
			},
			keepInCache: false,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			b := StubBuilder{
				T: t,
				cc: testutil.NewBalancer(
					testutil.WithInvokeHandlers(
						testutil.InvokeHandlers{
							testutil.TableExecuteDataQuery: func(request interface{}) (proto.Message, error) {
								r, ok := request.(*Ydb_Table.ExecuteDataQueryRequest)
								if !ok {
									t.Fatalf("cannot cast request '%T' to *Ydb_Table.ExecuteDataQueryRequest", request)
								}
								require.Equal(t, test.keepInCache, r.QueryCachePolicy.GetKeepInCache())
								return &Ydb_Table.ExecuteQueryResult{
									TxMeta: &Ydb_Table.TransactionMeta{
										Id: "",
									},
								}, nil
							},
							testutil.TableCreateSession: func(interface{}) (proto.Message, error) {
								return &Ydb_Table.CreateSessionResult{
									SessionId: testutil.SessionID(),
								}, nil
							},
						},
					),
				),
			}
			s, err := b.createSession(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			_, _, err = s.Execute(
				context.Background(), table.TxControl(
					table.BeginTx(
						table.WithOnlineReadOnly(),
					),
					table.CommitTx(),
				),
				"SELECT 1",
				test.params,
				test.executeDataQueryOptions...,
			)
			if err != nil {
				t.Fatal(err)
			}
		})
	}
}
