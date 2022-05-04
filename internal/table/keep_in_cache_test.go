package table

import (
	"context"
	"testing"

	"google.golang.org/protobuf/proto"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Table"

	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/testutil"
)

func TestQueryCachePolicyKeepInCache(t *testing.T) {
	for _, test := range [...]struct {
		name                   string
		queryCachePolicyOption []options.QueryCachePolicyOption
	}{
		{
			name: "with server cache",
			queryCachePolicyOption: []options.QueryCachePolicyOption{
				options.WithQueryCachePolicyKeepInCache(),
			},
		},
		{
			name:                   "no server cache",
			queryCachePolicyOption: []options.QueryCachePolicyOption{},
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
								if len(test.queryCachePolicyOption) > 0 {
									if !r.QueryCachePolicy.GetKeepInCache() {
										t.Fatalf("keep-in-cache policy must be true, got: %v", r.QueryCachePolicy.GetKeepInCache())
									}
								} else {
									if r.QueryCachePolicy.GetKeepInCache() {
										t.Fatalf("keep-in-cache policy must be false, got: %v", r.QueryCachePolicy.GetKeepInCache())
									}
								}
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
				options.WithQueryCachePolicy(test.queryCachePolicyOption...),
			)
			if err != nil {
				t.Fatal(err)
			}
		})
	}
}
