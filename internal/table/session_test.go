package table

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"

	"github.com/ydb-platform/ydb-go-genproto/Ydb_Table_V1"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Operations"
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
		cc: ydb_testutil.NewDB(
			ydb_testutil.WithInvokeHandlers(
				ydb_testutil.InvokeHandlers{
					// nolint:unparam
					// nolint:nolintlint
					ydb_testutil.TableKeepAlive: func(interface{}) (proto.Message, error) {
						return &Ydb_Table.KeepAliveResult{SessionStatus: status}, e
					},
					// nolint:unparam
					ydb_testutil.TableCreateSession: func(interface{}) (proto.Message, error) {
						return &Ydb_Table.CreateSessionResult{
							SessionId: ydb_testutil.SessionID(),
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
	if s.Status() != ydb_table_options.SessionReady.String() {
		t.Fatalf("Result %v differ from, expectd %v", s.Status(), ydb_table_options.SessionReady.String())
	}

	status, e = Ydb_Table.KeepAliveResult_SESSION_STATUS_BUSY, nil
	err = s.KeepAlive(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if s.Status() != ydb_table_options.SessionBusy.String() {
		t.Fatalf("Result %v differ from, expectd %v", s.Status(), ydb_table_options.SessionBusy.String())
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
		cc: ydb_testutil.NewDB(
			ydb_testutil.WithInvokeHandlers(
				ydb_testutil.InvokeHandlers{
					// nolint:unparam
					ydb_testutil.TableCreateSession: func(interface{}) (proto.Message, error) {
						return &Ydb_Table.CreateSessionResult{
							SessionId: ydb_testutil.SessionID(),
						}, nil
					},
					ydb_testutil.TableDescribeTable: func(interface{}) (proto.Message, error) {
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
		expect := ydb_table_options.Description{
			Name:       "testName",
			PrimaryKey: []string{"testKey"},
			Columns: []ydb_table_options.Column{
				{
					Name:   "testColumn",
					Type:   ydb_table_types.Void(),
					Family: "testFamily",
				},
			},
			KeyRanges: []ydb_table_options.KeyRange{
				{
					From: nil,
					To:   ydb_table_types.Int64Value(100500),
				},
				{
					From: ydb_table_types.Int64Value(100500),
					To:   nil,
				},
			},
			ColumnFamilies: []ydb_table_options.ColumnFamily{
				{
					Name:         "testFamily",
					Data:         ydb_table_options.StoragePool{},
					Compression:  ydb_table_options.ColumnFamilyCompressionLZ4,
					KeepInMemory: ydb_table_options.FeatureEnabled,
				},
			},
			Attributes: map[string]string{},
			ReadReplicaSettings: ydb_table_options.ReadReplicasSettings{
				Type:  ydb_table_options.ReadReplicasAnyAzReadReplicas,
				Count: 42,
			},
			StorageSettings: ydb_table_options.StorageSettings{
				TableCommitLog0:    ydb_table_options.StoragePool{Media: "m1"},
				TableCommitLog1:    ydb_table_options.StoragePool{Media: "m2"},
				External:           ydb_table_options.StoragePool{Media: "m3"},
				StoreExternalBlobs: ydb_table_options.FeatureEnabled,
			},
			Indexes: []ydb_table_options.IndexDescription{},
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
		method ydb_testutil.MethodCode
		do     func(t *testing.T, ctx context.Context, c *client)
	}{
		{
			method: ydb_testutil.TableExecuteDataQuery,
			do: func(t *testing.T, ctx context.Context, c *client) {
				s := &session{
					tableService: Ydb_Table_V1.NewTableServiceClient(c.cc),
				}
				_, _, err := s.Execute(ctx, ydb_table.TxControl(), "", ydb_table.NewQueryParameters())
				ydb_testutil.NoError(t, err)
			},
		},
		{
			method: ydb_testutil.TableExplainDataQuery,
			do: func(t *testing.T, ctx context.Context, c *client) {
				s := &session{
					tableService: Ydb_Table_V1.NewTableServiceClient(c.cc),
				}
				_, err := s.Explain(ctx, "")
				ydb_testutil.NoError(t, err)
			},
		},
		{
			method: ydb_testutil.TablePrepareDataQuery,
			do: func(t *testing.T, ctx context.Context, c *client) {
				s := &session{
					tableService: Ydb_Table_V1.NewTableServiceClient(c.cc),
				}
				_, err := s.Prepare(ctx, "")
				ydb_testutil.NoError(t, err)
			},
		},
		{
			method: ydb_testutil.TableCreateSession,
			do: func(t *testing.T, ctx context.Context, c *client) {
				_, err := c.createSession(ctx)
				ydb_testutil.NoError(t, err)
			},
		},
		{
			method: ydb_testutil.TableDeleteSession,
			do: func(t *testing.T, ctx context.Context, c *client) {
				s := &session{
					tableService: Ydb_Table_V1.NewTableServiceClient(c.cc),
				}
				ydb_testutil.NoError(t, s.Close(ctx))
			},
		},
		{
			method: ydb_testutil.TableBeginTransaction,
			do: func(t *testing.T, ctx context.Context, c *client) {
				s := &session{
					tableService: Ydb_Table_V1.NewTableServiceClient(c.cc),
				}
				_, err := s.BeginTransaction(ctx, ydb_table.TxSettings())
				ydb_testutil.NoError(t, err)
			},
		},
		{
			method: ydb_testutil.TableCommitTransaction,
			do: func(t *testing.T, ctx context.Context, c *client) {
				tx := &Transaction{
					s: &session{
						tableService: Ydb_Table_V1.NewTableServiceClient(c.cc),
					},
				}
				_, err := tx.CommitTx(ctx)
				ydb_testutil.NoError(t, err)
			},
		},
		{
			method: ydb_testutil.TableRollbackTransaction,
			do: func(t *testing.T, ctx context.Context, c *client) {
				tx := &Transaction{
					s: &session{
						tableService: Ydb_Table_V1.NewTableServiceClient(c.cc),
					},
				}
				err := tx.Rollback(ctx)
				ydb_testutil.NoError(t, err)
			},
		},
		{
			method: ydb_testutil.TableKeepAlive,
			do: func(t *testing.T, ctx context.Context, c *client) {
				s := &session{
					tableService: Ydb_Table_V1.NewTableServiceClient(c.cc),
				}
				ydb_testutil.NoError(t, s.KeepAlive(ctx))
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
							ydb_testutil.NewDB(
								ydb_testutil.WithInvokeHandlers(
									ydb_testutil.InvokeHandlers{
										// nolint:unparam
										ydb_testutil.TableExecuteDataQuery: func(interface{}) (proto.Message, error) {
											return &Ydb_Table.ExecuteQueryResult{
												TxMeta: &Ydb_Table.TransactionMeta{
													Id: "",
												},
											}, nil
										},
										// nolint:unparam
										ydb_testutil.TableBeginTransaction: func(interface{}) (proto.Message, error) {
											return &Ydb_Table.BeginTransactionResult{
												TxMeta: &Ydb_Table.TransactionMeta{
													Id: "",
												},
											}, nil
										},
										ydb_testutil.TableExplainDataQuery: func(request interface{}) (result proto.Message, err error) {
											return &Ydb_Table.ExecuteQueryResult{}, nil
										},
										ydb_testutil.TablePrepareDataQuery: func(request interface{}) (result proto.Message, err error) {
											return &Ydb_Table.PrepareQueryResult{}, nil
										},
										// nolint:unparam
										ydb_testutil.TableCreateSession: func(interface{}) (proto.Message, error) {
											return &Ydb_Table.CreateSessionResult{
												SessionId: ydb_testutil.SessionID(),
											}, nil
										},
										ydb_testutil.TableDeleteSession: func(request interface{}) (result proto.Message, err error) {
											return &Ydb_Table.DeleteSessionResponse{}, nil
										},
										ydb_testutil.TableCommitTransaction: func(request interface{}) (result proto.Message, err error) {
											return &Ydb_Table.CommitTransactionResponse{}, nil
										},
										ydb_testutil.TableRollbackTransaction: func(request interface{}) (result proto.Message, err error) {
											return &Ydb_Table.RollbackTransactionResponse{}, nil
										},
										ydb_testutil.TableKeepAlive: func(request interface{}) (result proto.Message, err error) {
											return &Ydb_Table.KeepAliveResult{}, nil
										},
									},
								),
							),
							nil,
							ydb_table_config.New(),
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
		queryCachePolicyOption []ydb_table_options.QueryCachePolicyOption
	}{
		{
			name: "with server cache",
			queryCachePolicyOption: []ydb_table_options.QueryCachePolicyOption{
				ydb_table_options.WithQueryCachePolicyKeepInCache(),
			},
		},
		{
			name:                   "no server cache",
			queryCachePolicyOption: []ydb_table_options.QueryCachePolicyOption{},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			b := StubBuilder{
				T: t,
				cc: ydb_testutil.NewDB(
					ydb_testutil.WithInvokeHandlers(
						ydb_testutil.InvokeHandlers{
							// nolint:unparam
							ydb_testutil.TableExecuteDataQuery: func(request interface{}) (proto.Message, error) {
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
							ydb_testutil.TableCreateSession: func(interface{}) (proto.Message, error) {
								return &Ydb_Table.CreateSessionResult{
									SessionId: ydb_testutil.SessionID(),
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
				context.Background(), ydb_table.TxControl(
					ydb_table.BeginTx(
						ydb_table.WithOnlineReadOnly(),
					),
					ydb_table.CommitTx(),
				),
				"SELECT 1",
				ydb_table.NewQueryParameters(),
				ydb_table_options.WithQueryCachePolicy(test.queryCachePolicyOption...),
			)
			if err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestTxSkipRollbackForCommitted(t *testing.T) {
	var (
		begin    = 0
		commit   = 0
		rollback = 0
	)
	b := StubBuilder{
		T: t,
		cc: ydb_testutil.NewDB(
			ydb_testutil.WithInvokeHandlers(
				ydb_testutil.InvokeHandlers{
					ydb_testutil.TableBeginTransaction: func(request interface{}) (proto.Message, error) {
						_, ok := request.(*Ydb_Table.BeginTransactionRequest)
						if !ok {
							t.Fatalf("cannot cast request '%T' to *Ydb_Table.BeginTransactionRequest", request)
						}
						result, err := anypb.New(
							&Ydb_Table.BeginTransactionResult{
								TxMeta: &Ydb_Table.TransactionMeta{
									Id: "",
								},
							},
						)
						if err != nil {
							return nil, err
						}
						begin++
						return &Ydb_Table.BeginTransactionResponse{
							Operation: &Ydb_Operations.Operation{
								Ready:  true,
								Status: Ydb.StatusIds_SUCCESS,
								Result: result,
							},
						}, nil
					},
					ydb_testutil.TableCommitTransaction: func(request interface{}) (proto.Message, error) {
						_, ok := request.(*Ydb_Table.CommitTransactionRequest)
						if !ok {
							t.Fatalf("cannot cast request '%T' to *Ydb_Table.CommitTransactionRequest", request)
						}
						result, err := anypb.New(
							&Ydb_Table.CommitTransactionResult{},
						)
						if err != nil {
							return nil, err
						}
						commit++
						return &Ydb_Table.CommitTransactionResponse{
							Operation: &Ydb_Operations.Operation{
								Ready:  true,
								Status: Ydb.StatusIds_SUCCESS,
								Result: result,
							},
						}, nil
					},
					// nolint:unparam
					ydb_testutil.TableRollbackTransaction: func(request interface{}) (proto.Message, error) {
						_, ok := request.(*Ydb_Table.RollbackTransactionRequest)
						if !ok {
							t.Fatalf("cannot cast request '%T' to *Ydb_Table.RollbackTransactionRequest", request)
						}
						rollback++
						return &Ydb_Table.RollbackTransactionResponse{
							Operation: &Ydb_Operations.Operation{
								Ready:  true,
								Status: Ydb.StatusIds_SUCCESS,
							},
						}, nil
					},
					// nolint:unparam
					ydb_testutil.TableCreateSession: func(interface{}) (proto.Message, error) {
						return &Ydb_Table.CreateSessionResult{
							SessionId: ydb_testutil.SessionID(),
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
	{
		x, err := s.BeginTransaction(context.Background(), ydb_table.TxSettings())
		if err != nil {
			t.Fatal(err)
		}
		if begin != 1 {
			t.Fatalf("unexpected begin: %d", begin)
		}
		_, err = x.CommitTx(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if commit != 1 {
			t.Fatalf("unexpected commit: %d", begin)
		}
		_, _ = x.CommitTx(context.Background())
		if commit != 1 {
			t.Fatalf("unexpected commit: %d", begin)
		}
		err = x.Rollback(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if rollback != 0 {
			t.Fatalf("unexpected rollback: %d", begin)
		}
	}
	{
		x, err := s.BeginTransaction(context.Background(), ydb_table.TxSettings())
		if err != nil {
			t.Fatal(err)
		}
		if begin != 2 {
			t.Fatalf("unexpected begin: %d", begin)
		}
		err = x.Rollback(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if rollback != 1 {
			t.Fatalf("unexpected rollback: %d", begin)
		}
		_, err = x.CommitTx(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if commit != 2 {
			t.Fatalf("unexpected commit: %d", begin)
		}
	}
}
