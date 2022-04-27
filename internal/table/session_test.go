package table

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/ydb-platform/ydb-go-genproto/Ydb_Table_V1"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Scheme"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Table"
	"google.golang.org/protobuf/proto"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/operation"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/table/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
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
		cc: testutil.NewDB(
			testutil.WithInvokeHandlers(
				testutil.InvokeHandlers{
					// nolint:unparam
					// nolint:nolintlint
					testutil.TableKeepAlive: func(interface{}) (proto.Message, error) {
						return &Ydb_Table.KeepAliveResult{SessionStatus: status}, e
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
	s, err := b.createSession(ctx)
	if err != nil {
		t.Fatal(err)
	}
	e = fmt.Errorf("any error")
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
		cc: testutil.NewDB(
			testutil.WithInvokeHandlers(
				testutil.InvokeHandlers{
					// nolint:unparam
					testutil.TableCreateSession: func(interface{}) (proto.Message, error) {
						return &Ydb_Table.CreateSessionResult{
							SessionId: testutil.SessionID(),
						}, nil
					},
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
		e = fmt.Errorf("any error")
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
		do     func(t *testing.T, ctx context.Context, c *Client)
	}{
		{
			method: testutil.TableExecuteDataQuery,
			do: func(t *testing.T, ctx context.Context, c *Client) {
				s := &session{
					tableService: Ydb_Table_V1.NewTableServiceClient(c.cc),
					config:       config.New(),
				}
				_, _, err := s.Execute(ctx, table.TxControl(), "", table.NewQueryParameters())
				testutil.NoError(t, err)
			},
		},
		{
			method: testutil.TableExplainDataQuery,
			do: func(t *testing.T, ctx context.Context, c *Client) {
				s := &session{
					tableService: Ydb_Table_V1.NewTableServiceClient(c.cc),
					config:       config.New(),
				}
				_, err := s.Explain(ctx, "")
				testutil.NoError(t, err)
			},
		},
		{
			method: testutil.TablePrepareDataQuery,
			do: func(t *testing.T, ctx context.Context, c *Client) {
				s := &session{
					tableService: Ydb_Table_V1.NewTableServiceClient(c.cc),
					config:       config.New(),
				}
				_, err := s.Prepare(ctx, "")
				testutil.NoError(t, err)
			},
		},
		{
			method: testutil.TableCreateSession,
			do: func(t *testing.T, ctx context.Context, c *Client) {
				_, err := c.createSession(ctx)
				testutil.NoError(t, err)
			},
		},
		{
			method: testutil.TableDeleteSession,
			do: func(t *testing.T, ctx context.Context, c *Client) {
				s := &session{
					tableService: Ydb_Table_V1.NewTableServiceClient(c.cc),
					config:       config.New(),
				}
				testutil.NoError(t, s.Close(ctx))
			},
		},
		{
			method: testutil.TableBeginTransaction,
			do: func(t *testing.T, ctx context.Context, c *Client) {
				s := &session{
					tableService: Ydb_Table_V1.NewTableServiceClient(c.cc),
					config:       config.New(),
				}
				_, err := s.BeginTransaction(ctx, table.TxSettings())
				testutil.NoError(t, err)
			},
		},
		{
			method: testutil.TableCommitTransaction,
			do: func(t *testing.T, ctx context.Context, c *Client) {
				tx := &transaction{
					s: &session{
						tableService: Ydb_Table_V1.NewTableServiceClient(c.cc),
						config:       config.New(),
					},
				}
				_, err := tx.CommitTx(ctx)
				testutil.NoError(t, err)
			},
		},
		{
			method: testutil.TableRollbackTransaction,
			do: func(t *testing.T, ctx context.Context, c *Client) {
				tx := &transaction{
					s: &session{
						tableService: Ydb_Table_V1.NewTableServiceClient(c.cc),
						config:       config.New(),
					},
				}
				err := tx.Rollback(ctx)
				testutil.NoError(t, err)
			},
		},
		{
			method: testutil.TableKeepAlive,
			do: func(t *testing.T, ctx context.Context, c *Client) {
				s := &session{
					tableService: Ydb_Table_V1.NewTableServiceClient(c.cc),
					config:       config.New(),
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
						client := New(
							testutil.NewDB(
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
										testutil.TableExplainDataQuery: func(request interface{}) (result proto.Message, err error) {
											return &Ydb_Table.ExecuteQueryResult{}, nil
										},
										testutil.TablePrepareDataQuery: func(request interface{}) (result proto.Message, err error) {
											return &Ydb_Table.PrepareQueryResult{}, nil
										},
										// nolint:unparam
										testutil.TableCreateSession: func(interface{}) (proto.Message, error) {
											return &Ydb_Table.CreateSessionResult{
												SessionId: testutil.SessionID(),
											}, nil
										},
										testutil.TableDeleteSession: func(request interface{}) (result proto.Message, err error) {
											return &Ydb_Table.DeleteSessionResponse{}, nil
										},
										testutil.TableCommitTransaction: func(request interface{}) (result proto.Message, err error) {
											return &Ydb_Table.CommitTransactionResponse{}, nil
										},
										testutil.TableRollbackTransaction: func(request interface{}) (result proto.Message, err error) {
											return &Ydb_Table.RollbackTransactionResponse{}, nil
										},
										testutil.TableKeepAlive: func(request interface{}) (result proto.Message, err error) {
											return &Ydb_Table.KeepAliveResult{}, nil
										},
									},
								),
							),
							config.New(),
						)
						ctx, cancel := context.WithTimeout(
							context.Background(),
							time.Second,
						)
						defer cancel()
						test.do(t, ctx, client)
					})
				}
			},
		)
	}
}
