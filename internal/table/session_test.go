package table

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/ydb-platform/ydb-go-genproto/Ydb_Table_V1"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Scheme"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Table"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/operation"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
	"github.com/ydb-platform/ydb-go-sdk/v3/testutil"
)

func TestSessionKeepAlive(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var (
		status Ydb_Table.KeepAliveResult_SessionStatus
		e      error
	)
	b := StubBuilder{
		T: t,
		Cluster: testutil.NewCluster(
			testutil.WithInvokeHandlers(
				testutil.InvokeHandlers{
					// nolint:unparam
					testutil.TableKeepAlive: func(request interface{}) (proto.Message, error) {
						return &Ydb_Table.KeepAliveResult{SessionStatus: status}, e
					},
					// nolint:unparam
					testutil.TableCreateSession: func(request interface{}) (result proto.Message, err error) {
						return &Ydb_Table.CreateSessionResult{
							SessionId: testutil.SessionID(),
						}, nil
					},
				},
			),
		),
	}
	s, err := b.createSession(ctx)
	if err != nil {
		t.Fatal(err)
	}
	e = errors.New("any error")
	err = s.KeepAlive(ctx)
	if err == nil {
		t.Fatal(err)
	}

	status, e = Ydb_Table.KeepAliveResult_SESSION_STATUS_READY, nil
	err = s.KeepAlive(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if s.Status() != options.SessionReady.String() {
		t.Fatalf("Result %v differ from, expectd %v", s.Status(), options.SessionReady.String())
	}

	status, e = Ydb_Table.KeepAliveResult_SESSION_STATUS_BUSY, nil
	err = s.KeepAlive(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if s.Status() != options.SessionBusy.String() {
		t.Fatalf("Result %v differ from, expectd %v", s.Status(), options.SessionBusy.String())
	}
}

func TestSessionDescribeTable(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var (
		result *Ydb_Table.DescribeTableResult
		e      error
	)
	b := StubBuilder{
		T: t,
		Cluster: testutil.NewCluster(
			testutil.WithInvokeHandlers(
				testutil.InvokeHandlers{
					//nolint: unparam
					testutil.TableCreateSession: func(interface{}) (proto.Message, error) {
						return &Ydb_Table.CreateSessionResult{
							SessionId: testutil.SessionID(),
						}, nil
					},
					//nolint: unparam
					testutil.TableDescribeTable: func(interface{}) (proto.Message, error) {
						r := &Ydb_Table.DescribeTableResult{}
						proto.Merge(r, result)
						return r, e
					},
				},
			),
		),
	}
	s, err := b.createSession(ctx)
	if err != nil {
		t.Fatal(err)
	}

	{
		e = errors.New("any error")
		_, err = s.DescribeTable(ctx, "")
		if err == nil {
			t.Fatal(err)
		}
	}
	{
		e = nil
		expect := options.Description{
			Name:       "testName",
			PrimaryKey: []string{"testKey"},
			Columns: []options.Column{
				{
					Name:   "testColumn",
					Type:   types.Void(),
					Family: "testFamily",
				},
			},
			KeyRanges: []options.KeyRange{
				{
					From: nil,
					To:   types.Int64Value(100500),
				},
				{
					From: types.Int64Value(100500),
					To:   nil,
				},
			},
			ColumnFamilies: []options.ColumnFamily{
				{
					Name:         "testFamily",
					Data:         options.StoragePool{},
					Compression:  options.ColumnFamilyCompressionLZ4,
					KeepInMemory: options.FeatureEnabled,
				},
			},
			Attributes: map[string]string{},
			ReadReplicaSettings: options.ReadReplicasSettings{
				Type:  options.ReadReplicasAnyAzReadReplicas,
				Count: 42,
			},
			StorageSettings: options.StorageSettings{
				TableCommitLog0:    options.StoragePool{Media: "m1"},
				TableCommitLog1:    options.StoragePool{Media: "m2"},
				External:           options.StoragePool{Media: "m3"},
				StoreExternalBlobs: options.FeatureEnabled,
			},
			Indexes: []options.IndexDescription{},
		}
		result = &Ydb_Table.DescribeTableResult{
			Self: &Ydb_Scheme.Entry{
				Name:                 expect.Name,
				Owner:                "",
				Type:                 0,
				EffectivePermissions: nil,
				Permissions:          nil,
			},
			Columns: []*Ydb_Table.ColumnMeta{
				{
					Name:   expect.Columns[0].Name,
					Type:   value.TypeToYDB(expect.Columns[0].Type),
					Family: "testFamily",
				},
			},
			PrimaryKey: expect.PrimaryKey,
			ShardKeyBounds: []*Ydb.TypedValue{
				value.ToYDB(expect.KeyRanges[0].To),
			},
			Indexes:    nil,
			TableStats: nil,
			ColumnFamilies: []*Ydb_Table.ColumnFamily{
				{
					Name:         "testFamily",
					Data:         nil,
					Compression:  Ydb_Table.ColumnFamily_COMPRESSION_LZ4,
					KeepInMemory: Ydb.FeatureFlag_ENABLED,
				},
			},
			ReadReplicasSettings: expect.ReadReplicaSettings.ToYDB(),
			StorageSettings:      expect.StorageSettings.ToYDB(),
		}

		d, err := s.DescribeTable(ctx, "")
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(d, expect) {
			t.Fatalf("Result %+v differ from, expectd %+v", d, expect)
		}
	}
}

func TestSessionOperationModeOnExecuteDataQuery(t *testing.T) {
	fromTo := [...]struct {
		srcMode operation.Mode
		dstMode operation.Mode
	}{
		{
			srcMode: operation.ModeUnknown,
			dstMode: operation.ModeSync,
		},
		{
			srcMode: operation.ModeSync,
			dstMode: operation.ModeSync,
		},
		{
			srcMode: operation.ModeAsync,
			dstMode: operation.ModeAsync,
		},
	}
	for _, test := range []struct {
		method testutil.MethodCode
		do     func(t *testing.T, ctx context.Context, c *client)
	}{
		{
			method: testutil.TableExecuteDataQuery,
			do: func(t *testing.T, ctx context.Context, c *client) {
				s := &session{
					tableService: Ydb_Table_V1.NewTableServiceClient(c.cluster),
				}
				_, _, err := s.Execute(ctx, table.TxControl(), "", table.NewQueryParameters())
				testutil.NoError(t, err)
			},
		},
		{
			method: testutil.TableExplainDataQuery,
			do: func(t *testing.T, ctx context.Context, c *client) {
				s := &session{
					tableService: Ydb_Table_V1.NewTableServiceClient(c.cluster),
				}
				_, err := s.Explain(ctx, "")
				testutil.NoError(t, err)
			},
		},
		{
			method: testutil.TablePrepareDataQuery,
			do: func(t *testing.T, ctx context.Context, c *client) {
				s := &session{
					tableService: Ydb_Table_V1.NewTableServiceClient(c.cluster),
				}
				_, err := s.Prepare(ctx, "")
				testutil.NoError(t, err)
			},
		},
		{
			method: testutil.TableCreateSession,
			do: func(t *testing.T, ctx context.Context, c *client) {
				_, err := c.createSession(ctx)
				testutil.NoError(t, err)
			},
		},
		{
			method: testutil.TableDeleteSession,
			do: func(t *testing.T, ctx context.Context, c *client) {
				s := &session{
					tableService: Ydb_Table_V1.NewTableServiceClient(c.cluster),
				}
				testutil.NoError(t, s.Close(ctx))
			},
		},
		{
			method: testutil.TableBeginTransaction,
			do: func(t *testing.T, ctx context.Context, c *client) {
				s := &session{
					tableService: Ydb_Table_V1.NewTableServiceClient(c.cluster),
				}
				_, err := s.BeginTransaction(ctx, table.TxSettings())
				testutil.NoError(t, err)
			},
		},
		{
			method: testutil.TableCommitTransaction,
			do: func(t *testing.T, ctx context.Context, c *client) {
				tx := &Transaction{
					s: &session{
						tableService: Ydb_Table_V1.NewTableServiceClient(c.cluster),
					},
				}
				_, err := tx.CommitTx(ctx)
				testutil.NoError(t, err)
			},
		},
		{
			method: testutil.TableRollbackTransaction,
			do: func(t *testing.T, ctx context.Context, c *client) {
				tx := &Transaction{
					s: &session{
						tableService: Ydb_Table_V1.NewTableServiceClient(c.cluster),
					},
				}
				err := tx.Rollback(ctx)
				testutil.NoError(t, err)
			},
		},
		{
			method: testutil.TableKeepAlive,
			do: func(t *testing.T, ctx context.Context, c *client) {
				s := &session{
					tableService: Ydb_Table_V1.NewTableServiceClient(c.cluster),
				}
				testutil.NoError(t, s.KeepAlive(ctx))
			},
		},
	} {
		t.Run(
			test.method.String(),
			func(t *testing.T) {
				for _, srcDst := range fromTo {
					t.Run(srcDst.srcMode.String()+"->"+srcDst.dstMode.String(), func(t *testing.T) {
						client := newClient(
							context.Background(),
							testutil.NewCluster(
								testutil.WithInvokeHandlers(
									testutil.InvokeHandlers{
										// nolint:unparam
										testutil.TableExecuteDataQuery: func(interface{}) (proto.Message, error) {
											return &Ydb_Table.ExecuteQueryResult{
												TxMeta: &Ydb_Table.TransactionMeta{
													Id: "",
												},
											}, nil
										},
										// nolint:unparam
										testutil.TableBeginTransaction: func(interface{}) (proto.Message, error) {
											return &Ydb_Table.BeginTransactionResult{
												TxMeta: &Ydb_Table.TransactionMeta{
													Id: "",
												},
											}, nil
										},
										// nolint:unparam
										testutil.TableExplainDataQuery: func(request interface{}) (result proto.Message, err error) {
											return &Ydb_Table.ExecuteQueryResult{}, nil
										},
										// nolint:unparam
										testutil.TablePrepareDataQuery: func(request interface{}) (result proto.Message, err error) {
											return &Ydb_Table.PrepareQueryResult{}, nil
										},
										// nolint:unparam
										testutil.TableCreateSession: func(request interface{}) (result proto.Message, err error) {
											return &Ydb_Table.CreateSessionResult{
												SessionId: testutil.SessionID(),
											}, nil
										},
										// nolint:unparam
										testutil.TableDeleteSession: func(request interface{}) (result proto.Message, err error) {
											return &Ydb_Table.DeleteSessionResponse{}, nil
										},
										// nolint:unparam
										testutil.TableCommitTransaction: func(request interface{}) (result proto.Message, err error) {
											return &Ydb_Table.CommitTransactionResponse{}, nil
										},
										// nolint:unparam
										testutil.TableRollbackTransaction: func(request interface{}) (result proto.Message, err error) {
											return &Ydb_Table.RollbackTransactionResponse{}, nil
										},
										// nolint:unparam
										testutil.TableKeepAlive: func(request interface{}) (result proto.Message, err error) {
											return &Ydb_Table.KeepAliveResult{}, nil
										},
									},
								),
							),
							nil,
							config.New(),
						)
						ctx, cancel := context.WithTimeout(
							context.Background(),
							time.Second,
						)
						if srcDst.srcMode != 0 {
							ctx = operation.WithMode(
								ctx,
								srcDst.srcMode,
							)
						}
						defer cancel()
						test.do(t, ctx, client)
					})
				}
			},
		)
	}
}

func TestQueryCachePolicyKeepInCache(t *testing.T) {
	for _, test := range [...]struct {
		name                   string
		queryCachePolicyOption []options.QueryCachePolicyOption
	}{
		{
			name:                   "with server cache",
			queryCachePolicyOption: []options.QueryCachePolicyOption{options.WithQueryCachePolicyKeepInCache()},
		},
		{
			name:                   "no server cache",
			queryCachePolicyOption: []options.QueryCachePolicyOption{},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			b := StubBuilder{
				T: t,
				Cluster: testutil.NewCluster(
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
							testutil.TableCreateSession: func(request interface{}) (result proto.Message, err error) {
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
