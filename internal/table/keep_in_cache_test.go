package table

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Table"

	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/testutil"
)

func TestQueryCachePolicyKeepInCache(t *testing.T) {
	for _, test := range [...]struct {
		name                    string
		executeDataQueryOptions []options.ExecuteDataQueryOption
		keepInCache             bool
	}{
		{
			name:                    "no options",
			executeDataQueryOptions: nil,
			keepInCache:             true,
		},
		{
			name: "with server cache",
			executeDataQueryOptions: []options.ExecuteDataQueryOption{
				options.WithKeepInCache(true),
			},
			keepInCache: true,
		},
		{
			name: "no server cache",
			executeDataQueryOptions: []options.ExecuteDataQueryOption{
				options.WithKeepInCache(false),
			},
			keepInCache: false,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			b := StubBuilder{
				T: t,
				cc: testutil.NewRouter(
					testutil.WithInvokeHandlers(
						testutil.InvokeHandlers{
							// nolint:unparam
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
							// nolint:unparam
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
				table.NewQueryParameters(),
				test.executeDataQueryOptions...,
			)
			if err != nil {
				t.Fatal(err)
			}
		})
	}
}
