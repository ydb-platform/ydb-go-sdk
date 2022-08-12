package table

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/Ydb_Table_V1"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Operations"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Scheme"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Table"
	"google.golang.org/protobuf/proto"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/operation"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/table/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value/allocator"
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
		cc: testutil.NewBalancer(
			testutil.WithInvokeHandlers(
				testutil.InvokeHandlers{
					testutil.TableKeepAlive: func(interface{}) (proto.Message, error) {
						return &Ydb_Table.KeepAliveResult{SessionStatus: status}, e
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
		cc: testutil.NewBalancer(
			testutil.WithInvokeHandlers(
				testutil.InvokeHandlers{
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
		a := allocator.New()
		defer a.Free()
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
					Type:   value.TypeToYDB(expect.Columns[0].Type, a),
					Family: "testFamily",
				},
			},
			PrimaryKey: expect.PrimaryKey,
			ShardKeyBounds: []*Ydb.TypedValue{
				value.ToYDB(expect.KeyRanges[0].To, a),
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
				_, err := c.internalPoolCreateSession(ctx)
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
							testutil.NewBalancer(
								testutil.WithInvokeHandlers(
									testutil.InvokeHandlers{
										testutil.TableExecuteDataQuery: func(interface{}) (proto.Message, error) {
											return &Ydb_Table.ExecuteQueryResult{
												TxMeta: &Ydb_Table.TransactionMeta{
													Id: "",
												},
											}, nil
										},
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

func TestCreateTableRegression(t *testing.T) {
	client := New(
		testutil.NewBalancer(
			testutil.WithInvokeHandlers(
				testutil.InvokeHandlers{
					testutil.TableCreateSession: func(request interface{}) (proto.Message, error) {
						return &Ydb_Table.CreateSessionResult{
							SessionId: "",
						}, nil
					},
					testutil.TableCreateTable: func(act interface{}) (proto.Message, error) {
						exp := &Ydb_Table.CreateTableRequest{
							SessionId: "",
							Path:      "episodes",
							Columns: []*Ydb_Table.ColumnMeta{
								{
									Name: "series_id",
									Type: &Ydb.Type{Type: &Ydb.Type_OptionalType{
										OptionalType: &Ydb.OptionalType{Item: &Ydb.Type{Type: &Ydb.Type_TypeId{
											TypeId: Ydb.Type_UINT64,
										}}},
									}},
								},
								{
									Name: "season_id",
									Type: &Ydb.Type{Type: &Ydb.Type_OptionalType{
										OptionalType: &Ydb.OptionalType{Item: &Ydb.Type{Type: &Ydb.Type_TypeId{
											TypeId: Ydb.Type_UINT64,
										}}},
									}},
								},
								{
									Name: "episode_id",
									Type: &Ydb.Type{Type: &Ydb.Type_OptionalType{
										OptionalType: &Ydb.OptionalType{Item: &Ydb.Type{Type: &Ydb.Type_TypeId{
											TypeId: Ydb.Type_UINT64,
										}}},
									}},
								},
								{
									Name: "title",
									Type: &Ydb.Type{Type: &Ydb.Type_OptionalType{
										OptionalType: &Ydb.OptionalType{Item: &Ydb.Type{Type: &Ydb.Type_TypeId{
											TypeId: Ydb.Type_UTF8,
										}}},
									}},
								},
								{
									Name: "air_date",
									Type: &Ydb.Type{Type: &Ydb.Type_OptionalType{
										OptionalType: &Ydb.OptionalType{Item: &Ydb.Type{Type: &Ydb.Type_TypeId{
											TypeId: Ydb.Type_UINT64,
										}}},
									}},
								},
							},
							PrimaryKey: []string{
								"series_id",
								"season_id",
								"episode_id",
							},
							OperationParams: &Ydb_Operations.OperationParams{
								OperationMode: Ydb_Operations.OperationParams_SYNC,
							},
							Attributes: map[string]string{
								"attr": "attr_value",
							},
						}
						if !proto.Equal(exp, act.(proto.Message)) {
							//nolint:revive
							return nil, fmt.Errorf("proto's not equal: \n\nact: %v\n\nexp: %s\n\n", act, exp)
						}
						return &Ydb_Table.CreateTableResponse{}, nil
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

	err := client.Do(ctx, func(ctx context.Context, s table.Session) error {
		return s.CreateTable(ctx, "episodes",
			options.WithColumn("series_id", types.Optional(types.TypeUint64)),
			options.WithColumn("season_id", types.Optional(types.TypeUint64)),
			options.WithColumn("episode_id", types.Optional(types.TypeUint64)),
			options.WithColumn("title", types.Optional(types.TypeUTF8)),
			options.WithColumn("air_date", types.Optional(types.TypeUint64)),
			options.WithPrimaryKeyColumn("series_id", "season_id", "episode_id"),
			options.WithAttribute("attr", "attr_value"),
		)
	})

	require.NoError(t, err, "")
}

func TestDescribeTableRegression(t *testing.T) {
	client := New(
		testutil.NewBalancer(
			testutil.WithInvokeHandlers(
				testutil.InvokeHandlers{
					testutil.TableCreateSession: func(request interface{}) (proto.Message, error) {
						return &Ydb_Table.CreateSessionResult{
							SessionId: "",
						}, nil
					},
					testutil.TableDescribeTable: func(act interface{}) (proto.Message, error) {
						return &Ydb_Table.DescribeTableResult{
							Self: &Ydb_Scheme.Entry{
								Name: "episodes",
							},
							Columns: []*Ydb_Table.ColumnMeta{
								{
									Name: "series_id",
									Type: &Ydb.Type{Type: &Ydb.Type_OptionalType{
										OptionalType: &Ydb.OptionalType{Item: &Ydb.Type{Type: &Ydb.Type_TypeId{
											TypeId: Ydb.Type_UINT64,
										}}},
									}},
								},
								{
									Name: "season_id",
									Type: &Ydb.Type{Type: &Ydb.Type_OptionalType{
										OptionalType: &Ydb.OptionalType{Item: &Ydb.Type{Type: &Ydb.Type_TypeId{
											TypeId: Ydb.Type_UINT64,
										}}},
									}},
								},
								{
									Name: "episode_id",
									Type: &Ydb.Type{Type: &Ydb.Type_OptionalType{
										OptionalType: &Ydb.OptionalType{Item: &Ydb.Type{Type: &Ydb.Type_TypeId{
											TypeId: Ydb.Type_UINT64,
										}}},
									}},
								},
								{
									Name: "title",
									Type: &Ydb.Type{Type: &Ydb.Type_OptionalType{
										OptionalType: &Ydb.OptionalType{Item: &Ydb.Type{Type: &Ydb.Type_TypeId{
											TypeId: Ydb.Type_UTF8,
										}}},
									}},
								},
								{
									Name: "air_date",
									Type: &Ydb.Type{Type: &Ydb.Type_OptionalType{
										OptionalType: &Ydb.OptionalType{Item: &Ydb.Type{Type: &Ydb.Type_TypeId{
											TypeId: Ydb.Type_UINT64,
										}}},
									}},
								},
							},
							PrimaryKey: []string{
								"series_id",
								"season_id",
								"episode_id",
							},
							Attributes: map[string]string{
								"attr": "attr_value",
							},
						}, nil
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

	var act options.Description

	err := client.Do(ctx, func(ctx context.Context, s table.Session) (err error) {
		act, err = s.DescribeTable(ctx, "episodes")
		return err
	})

	require.NoError(t, err, "")

	exp := options.Description{
		Name: "episodes",
		Columns: []options.Column{
			{
				Name: "series_id",
				Type: types.Optional(types.TypeUint64),
			},
			{
				Name: "season_id",
				Type: types.Optional(types.TypeUint64),
			},
			{
				Name: "episode_id",
				Type: types.Optional(types.TypeUint64),
			},
			{
				Name: "title",
				Type: types.Optional(types.TypeUTF8),
			},
			{
				Name: "air_date",
				Type: types.Optional(types.TypeUint64),
			},
		},
		KeyRanges: []options.KeyRange{
			{},
		},
		PrimaryKey: []string{
			"series_id",
			"season_id",
			"episode_id",
		},
		Attributes: map[string]string{
			"attr": "attr_value",
		},
	}

	if fmt.Sprintf("%+v", act) != fmt.Sprintf("%+v", exp) {
		t.Fatalf("description's not equal: \n\nact: %+v\n\nexp: %+v\n\n", act, exp)
	}
}
