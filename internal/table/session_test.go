package table

import (
	"context"
	"errors"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/assert"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value"
	"reflect"
	"testing"
	"time"

	options2 "github.com/ydb-platform/ydb-go-sdk/v3/table/options"

	"github.com/ydb-platform/ydb-go-genproto/Ydb_Table_V1"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Scheme"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Table"
	"google.golang.org/protobuf/proto"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/table/scanner"
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
		Cluster: testutil.NewDB(
			testutil.WithInvokeHandlers(
				testutil.InvokeHandlers{
					testutil.TableKeepAlive: func(request interface{}) (proto.Message, error) {
						return &Ydb_Table.KeepAliveResult{SessionStatus: status}, e
					},
					testutil.TableCreateSession: func(request interface{}) (result proto.Message, err error) {
						return &Ydb_Table.CreateSessionResult{}, nil
					},
				},
			),
		),
	}
	s, err := b.CreateSession(ctx)
	if err != nil {
		t.Fatal(err)
	}

	{
		e = errors.New("any error")
		_, err := s.KeepAlive(ctx)
		if err == nil {
			t.Fatal(err)
		}
	}
	{
		status, e = Ydb_Table.KeepAliveResult_SESSION_STATUS_READY, nil
		info, err := s.KeepAlive(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if info.Status != SessionReady {
			t.Fatalf("Result %v differ from, expectd %v", info.Status, SessionReady)
		}
	}
	{
		status, e = Ydb_Table.KeepAliveResult_SESSION_STATUS_BUSY, nil
		info, err := s.KeepAlive(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if info.Status != SessionBusy {
			t.Fatalf("Result %v differ from, expectd %v", info.Status, SessionBusy)
		}
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
		Cluster: testutil.NewDB(
			testutil.WithInvokeHandlers(
				testutil.InvokeHandlers{
					testutil.TableCreateSession: func(request interface{}) (result proto.Message, err error) {
						return &Ydb_Table.CreateSessionResult{}, nil
					},
					testutil.TableDescribeTable: func(request interface{}) (proto.Message, error) {
						r := &Ydb_Table.DescribeTableResult{}
						proto.Merge(r, result)
						return r, e
					},
				},
			),
		),
	}
	s, err := b.CreateSession(ctx)
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
		expect := Description{
			Name:       "testName",
			PrimaryKey: []string{"testKey"},
			Columns: []options2.Column{
				{
					Name:   "testColumn",
					Type:   types.Void(),
					Family: "testFamily",
				},
			},
			KeyRanges: []KeyRange{
				{
					From: nil,
					To:   types.Int64Value(100500),
				},
				{
					From: types.Int64Value(100500),
					To:   nil,
				},
			},
			ColumnFamilies: []ColumnFamily{
				{
					Name:         "testFamily",
					Data:         StoragePool{},
					Compression:  ColumnFamilyCompressionLZ4,
					KeepInMemory: options2.FeatureEnabled,
				},
			},
			Attributes: map[string]string{},
			ReadReplicaSettings: ReadReplicasSettings{
				Type:  ReadReplicasAnyAzReadReplicas,
				Count: 42,
			},
			StorageSettings: StorageSettings{
				TableCommitLog0:    StoragePool{Media: "m1"},
				TableCommitLog1:    StoragePool{Media: "m2"},
				External:           StoragePool{Media: "m3"},
				StoreExternalBlobs: options2.FeatureEnabled,
			},
			Indexes: []IndexDescription{},
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
				value.ValueToYDB(expect.KeyRanges[0].To),
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
			ReadReplicasSettings: expect.ReadReplicaSettings.toYDB(),
			StorageSettings:      expect.StorageSettings.toYDB(),
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
		srcMode ydb.OperationMode
		dstMode ydb.OperationMode
	}{
		{
			srcMode: ydb.OperationModeUnknown,
			dstMode: ydb.OperationModeSync,
		},
		{
			srcMode: ydb.OperationModeSync,
			dstMode: ydb.OperationModeSync,
		},
		{
			srcMode: ydb.OperationModeAsync,
			dstMode: ydb.OperationModeAsync,
		},
	}
	for _, test := range []struct {
		method testutil.MethodCode
		do     func(t *testing.T, ctx context.Context, c table.client)
	}{
		{
			method: testutil.TableExecuteDataQuery,
			do: func(t *testing.T, ctx context.Context, c table.client) {
				s := &Session{
					c:            c,
					tableService: Ydb_Table_V1.NewTableServiceClient(c.cluster),
				}
				_, _, err := s.Execute(ctx, scanner.TxControl(), "", NewQueryParameters())
				assert.NoError(t, err)
			},
		},
		{
			method: testutil.TableExplainDataQuery,
			do: func(t *testing.T, ctx context.Context, c table.client) {
				s := &Session{
					c:            c,
					tableService: Ydb_Table_V1.NewTableServiceClient(c.cluster),
				}
				_, err := s.Explain(ctx, "")
				assert.NoError(t, err)
			},
		},
		{
			method: testutil.TablePrepareDataQuery,
			do: func(t *testing.T, ctx context.Context, c table.client) {
				s := &Session{
					c:            c,
					tableService: Ydb_Table_V1.NewTableServiceClient(c.cluster),
				}
				_, err := s.Prepare(ctx, "")
				assert.NoError(t, err)
			},
		},
		{
			method: testutil.TableCreateSession,
			do: func(t *testing.T, ctx context.Context, c table.client) {
				_, err := c.CreateSession(ctx)
				assert.NoError(t, err)
			},
		},
		{
			method: testutil.TableDeleteSession,
			do: func(t *testing.T, ctx context.Context, c table.client) {
				s := &Session{
					c:            c,
					tableService: Ydb_Table_V1.NewTableServiceClient(c.cluster),
				}
				assert.NoError(t, s.Close(ctx))
			},
		},
		{
			method: testutil.TableBeginTransaction,
			do: func(t *testing.T, ctx context.Context, c table.client) {
				s := &Session{
					c:            c,
					tableService: Ydb_Table_V1.NewTableServiceClient(c.cluster),
				}
				_, err := s.BeginTransaction(ctx, scanner.TxSettings())
				assert.NoError(t, err)
			},
		},
		{
			method: testutil.TableCommitTransaction,
			do: func(t *testing.T, ctx context.Context, c table.client) {
				tx := &Transaction{
					s: &Session{
						c:            c,
						tableService: Ydb_Table_V1.NewTableServiceClient(c.cluster),
					},
				}
				_, err := tx.CommitTx(ctx)
				assert.NoError(t, err)
			},
		},
		{
			method: testutil.TableRollbackTransaction,
			do: func(t *testing.T, ctx context.Context, c table.client) {
				tx := &Transaction{
					s: &Session{
						c:            c,
						tableService: Ydb_Table_V1.NewTableServiceClient(c.cluster),
					},
				}
				err := tx.Rollback(ctx)
				assert.NoError(t, err)
			},
		},
		{
			method: testutil.TableKeepAlive,
			do: func(t *testing.T, ctx context.Context, c table.client) {
				s := &Session{
					c:            c,
					tableService: Ydb_Table_V1.NewTableServiceClient(c.cluster),
				}
				_, err := s.KeepAlive(ctx)
				assert.NoError(t, err)
			},
		},
	} {
		t.Run(
			test.method.String(),
			func(t *testing.T) {
				for _, srcDst := range fromTo {
					t.Run(srcDst.srcMode.String()+"->"+srcDst.dstMode.String(), func(t *testing.T) {
						client := table.client{
							cluster: testutil.NewDB(
								testutil.WithInvokeHandlers(
									testutil.InvokeHandlers{
										testutil.TableExecuteDataQuery: func(request interface{}) (result proto.Message, err error) {
											return &Ydb_Table.ExecuteQueryResult{
												TxMeta: &Ydb_Table.TransactionMeta{
													Id: "",
												},
											}, nil
										},
										testutil.TableBeginTransaction: func(request interface{}) (result proto.Message, err error) {
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
										testutil.TableCreateSession: func(request interface{}) (result proto.Message, err error) {
											return &Ydb_Table.CreateSessionResult{}, nil
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
						}
						ctx, cancel := context.WithTimeout(
							context.Background(),
							time.Second,
						)
						if srcDst.srcMode != 0 {
							ctx = ydb.WithOperationMode(
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
