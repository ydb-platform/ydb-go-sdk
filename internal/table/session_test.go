package table

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/Ydb_Table_V1"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Operations"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Scheme"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Table"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/operation"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/table/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/tx"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/types"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/value"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xcontext"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xtest"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/testutil"
)

func TestSessionKeepAlive(t *testing.T) {
	ctx, cancel := xcontext.WithCancel(context.Background())
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
	if s.Status() != table.SessionReady {
		t.Fatalf("Result %v differ from, expectd %v", s.Status(), table.SessionReady)
	}

	status, e = Ydb_Table.KeepAliveResult_SESSION_STATUS_BUSY, nil
	err = s.KeepAlive(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if s.Status() != table.SessionBusy {
		t.Fatalf("Result %v differ from, expectd %v", s.Status(), table.SessionBusy)
	}
}

func TestSessionDescribeTable(t *testing.T) {
	ctx, cancel := xcontext.WithCancel(context.Background())
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
					Type:   types.NewVoid(),
					Family: "testFamily",
				},
			},
			KeyRanges: []options.KeyRange{
				{
					From: nil,
					To:   value.Int64Value(100500),
				},
				{
					From: value.Int64Value(100500),
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
			Indexes:     []options.IndexDescription{},
			Changefeeds: make([]options.ChangefeedDescription, 0),
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
					Type:   types.TypeToYDB(expect.Columns[0].Type),
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
				client := Ydb_Table_V1.NewTableServiceClient(c.cc)
				s := &Session{
					client: client,
					dataQuery: tableClientExecutor{
						client: client,
					},
					config: config.New(),
				}
				_, _, err := s.Execute(ctx, table.TxControl(), "", table.NewQueryParameters())
				require.NoError(t, err)
			},
		},
		{
			method: testutil.TableExplainDataQuery,
			do: func(t *testing.T, ctx context.Context, c *Client) {
				client := Ydb_Table_V1.NewTableServiceClient(c.cc)
				s := &Session{
					client: client,
					dataQuery: tableClientExecutor{
						client: client,
					},
					config: config.New(),
				}
				_, err := s.Explain(ctx, "")
				require.NoError(t, err)
			},
		},
		{
			method: testutil.TablePrepareDataQuery,
			do: func(t *testing.T, ctx context.Context, c *Client) {
				client := Ydb_Table_V1.NewTableServiceClient(c.cc)
				s := &Session{
					client: client,
					dataQuery: tableClientExecutor{
						client: client,
					},
					config: config.New(),
				}
				_, err := s.Prepare(ctx, "")
				require.NoError(t, err)
			},
		},
		{
			method: testutil.TableCreateSession,
			do: func(t *testing.T, ctx context.Context, c *Client) {
				_ = simpleSession(t)
			},
		},
		{
			method: testutil.TableDeleteSession,
			do: func(t *testing.T, ctx context.Context, c *Client) {
				client := Ydb_Table_V1.NewTableServiceClient(c.cc)
				s := &Session{
					client: client,
					dataQuery: tableClientExecutor{
						client: client,
					},
					config: config.New(),
				}
				require.NoError(t, s.Close(ctx))
			},
		},
		{
			method: testutil.TableBeginTransaction,
			do: func(t *testing.T, ctx context.Context, c *Client) {
				client := Ydb_Table_V1.NewTableServiceClient(c.cc)
				s := &Session{
					client: client,
					dataQuery: tableClientExecutor{
						client: client,
					},
					config: config.New(),
				}
				_, err := s.BeginTransaction(ctx, table.TxSettings())
				require.NoError(t, err)
			},
		},
		{
			method: testutil.TableCommitTransaction,
			do: func(t *testing.T, ctx context.Context, c *Client) {
				client := Ydb_Table_V1.NewTableServiceClient(c.cc)
				tx := &transaction{
					Identifier: tx.ID(""),
					s: &Session{
						client: client,
						dataQuery: tableClientExecutor{
							client: client,
						},
						config: config.New(),
					},
				}
				_, err := tx.CommitTx(ctx)
				require.NoError(t, err)
			},
		},
		{
			method: testutil.TableRollbackTransaction,
			do: func(t *testing.T, ctx context.Context, c *Client) {
				client := Ydb_Table_V1.NewTableServiceClient(c.cc)
				tx := &transaction{
					Identifier: tx.ID(""),
					s: &Session{
						client: client,
						dataQuery: tableClientExecutor{
							client: client,
						},
						config: config.New(),
					},
				}
				err := tx.Rollback(ctx)
				require.NoError(t, err)
			},
		},
		{
			method: testutil.TableKeepAlive,
			do: func(t *testing.T, ctx context.Context, c *Client) {
				client := Ydb_Table_V1.NewTableServiceClient(c.cc)
				s := &Session{
					client: client,
					dataQuery: tableClientExecutor{
						client: client,
					},
					config: config.New(),
				}
				require.NoError(t, s.KeepAlive(ctx))
			},
		},
	} {
		t.Run(
			test.method.String(),
			func(t *testing.T) {
				for _, srcDst := range fromTo {
					t.Run(srcDst.srcMode.String()+"->"+srcDst.dstMode.String(), func(t *testing.T) {
						client := New(context.Background(), testutil.NewBalancer(
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
										return &Ydb_Table.ExplainQueryResult{}, nil
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
										return &Ydb_Table.CommitTransactionResult{}, nil
									},
									testutil.TableRollbackTransaction: func(request interface{}) (result proto.Message, err error) {
										return &Ydb_Table.RollbackTransactionResponse{}, nil
									},
									testutil.TableKeepAlive: func(request interface{}) (result proto.Message, err error) {
										return &Ydb_Table.KeepAliveResult{}, nil
									},
								},
							),
						), config.New())
						ctx, cancel := xcontext.WithTimeout(
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
	client := New(context.Background(), testutil.NewBalancer(
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
	), config.New(config.UseQuerySession(false)))

	ctx, cancel := xcontext.WithTimeout(
		context.Background(),
		time.Second,
	)
	defer cancel()

	err := client.Do(ctx, func(ctx context.Context, s table.Session) error {
		return s.CreateTable(ctx, "episodes",
			options.WithColumn("series_id", types.NewOptional(types.Uint64)),
			options.WithColumn("season_id", types.NewOptional(types.Uint64)),
			options.WithColumn("episode_id", types.NewOptional(types.Uint64)),
			options.WithColumn("title", types.NewOptional(types.Text)),
			options.WithColumn("air_date", types.NewOptional(types.Uint64)),
			options.WithPrimaryKeyColumn("series_id", "season_id", "episode_id"),
			options.WithAttribute("attr", "attr_value"),
		)
	})

	require.NoError(t, err, "")
}

func TestDescribeTableRegression(t *testing.T) {
	client := New(context.Background(), testutil.NewBalancer(
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
	), config.New(config.UseQuerySession(false)))

	ctx, cancel := xcontext.WithTimeout(
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
				Type: types.NewOptional(types.Uint64),
			},
			{
				Name: "season_id",
				Type: types.NewOptional(types.Uint64),
			},
			{
				Name: "episode_id",
				Type: types.NewOptional(types.Uint64),
			},
			{
				Name: "title",
				Type: types.NewOptional(types.Text),
			},
			{
				Name: "air_date",
				Type: types.NewOptional(types.Uint64),
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
		ColumnFamilies: []options.ColumnFamily{},
		Attributes: map[string]string{
			"attr": "attr_value",
		},
		Indexes:     []options.IndexDescription{},
		Changefeeds: []options.ChangefeedDescription{},
	}

	assert.Equal(t, exp, act)
}

var errUnexpectedRequest = errors.New("unexpected request")

type copyTablesMock struct {
	*Ydb_Table.CopyTablesRequest
}

func (mock *copyTablesMock) CopyTables(
	_ context.Context, in *Ydb_Table.CopyTablesRequest, opts ...grpc.CallOption,
) (*Ydb_Table.CopyTablesResponse, error) {
	if in.String() == mock.String() {
		return &Ydb_Table.CopyTablesResponse{}, nil
	}

	return nil, fmt.Errorf("%w: %s, exp: %s", errUnexpectedRequest, in, mock.String())
}

func TestCopyTables(t *testing.T) {
	ctx := xtest.Context(t)
	for _, tt := range []struct {
		sessionID            string
		operationTimeout     time.Duration
		operationCancelAfter time.Duration
		service              *copyTablesMock
		opts                 []options.CopyTablesOption
		err                  error
	}{
		{
			sessionID:            "1",
			operationTimeout:     time.Second,
			operationCancelAfter: time.Second,
			service: &copyTablesMock{
				CopyTablesRequest: &Ydb_Table.CopyTablesRequest{
					SessionId: "1",
					Tables: []*Ydb_Table.CopyTableItem{
						{
							SourcePath:      "from",
							DestinationPath: "to",
							OmitIndexes:     true,
						},
					},
					OperationParams: &Ydb_Operations.OperationParams{
						OperationMode:    Ydb_Operations.OperationParams_SYNC,
						OperationTimeout: durationpb.New(time.Second),
						CancelAfter:      durationpb.New(time.Second),
					},
				},
			},
			opts: []options.CopyTablesOption{
				options.CopyTablesItem("from", "to", true),
			},
			err: nil,
		},
		{
			sessionID:            "2",
			operationTimeout:     2 * time.Second,
			operationCancelAfter: 2 * time.Second,
			service: &copyTablesMock{
				CopyTablesRequest: &Ydb_Table.CopyTablesRequest{
					SessionId: "2",
					Tables: []*Ydb_Table.CopyTableItem{
						{
							SourcePath:      "from1",
							DestinationPath: "to1",
							OmitIndexes:     true,
						},
						{
							SourcePath:      "from2",
							DestinationPath: "to2",
							OmitIndexes:     false,
						},
						{
							SourcePath:      "from3",
							DestinationPath: "to3",
							OmitIndexes:     true,
						},
					},
					OperationParams: &Ydb_Operations.OperationParams{
						OperationMode:    Ydb_Operations.OperationParams_SYNC,
						OperationTimeout: durationpb.New(2 * time.Second),
						CancelAfter:      durationpb.New(2 * time.Second),
					},
				},
			},
			opts: []options.CopyTablesOption{
				options.CopyTablesItem("from1", "to1", true),
				options.CopyTablesItem("from2", "to2", false),
				options.CopyTablesItem("from3", "to3", true),
			},
			err: nil,
		},
		{
			sessionID:            "3",
			operationTimeout:     time.Second,
			operationCancelAfter: time.Second,
			service: &copyTablesMock{
				CopyTablesRequest: &Ydb_Table.CopyTablesRequest{
					SessionId: "1",
					Tables: []*Ydb_Table.CopyTableItem{
						{
							SourcePath:      "from",
							DestinationPath: "to",
							OmitIndexes:     true,
						},
					},
					OperationParams: &Ydb_Operations.OperationParams{
						OperationMode:    Ydb_Operations.OperationParams_SYNC,
						OperationTimeout: durationpb.New(time.Second),
						CancelAfter:      durationpb.New(time.Second),
					},
				},
			},
			opts: []options.CopyTablesOption{
				options.CopyTablesItem("from1", "to1", true),
			},
			err: errUnexpectedRequest,
		},
		{
			sessionID:            "4",
			operationTimeout:     time.Second,
			operationCancelAfter: time.Second,
			service:              &copyTablesMock{},
			opts:                 nil,
			err:                  errParamsRequired,
		},
	} {
		t.Run("", func(t *testing.T) {
			err := copyTables(ctx, tt.sessionID, tt.operationTimeout, tt.operationCancelAfter, tt.service, tt.opts...)
			if tt.err != nil {
				require.ErrorIs(t, err, tt.err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

type renameTablesMock struct {
	*Ydb_Table.RenameTablesRequest
}

func (mock *renameTablesMock) RenameTables(
	_ context.Context, in *Ydb_Table.RenameTablesRequest, opts ...grpc.CallOption,
) (*Ydb_Table.RenameTablesResponse, error) {
	if in.String() == mock.String() {
		return &Ydb_Table.RenameTablesResponse{}, nil
	}

	return nil, fmt.Errorf("%w: %s, exp: %s", errUnexpectedRequest, in, mock.String())
}

func TestRenameTables(t *testing.T) {
	ctx := xtest.Context(t)
	for _, tt := range []struct {
		sessionID            string
		operationTimeout     time.Duration
		operationCancelAfter time.Duration
		service              *renameTablesMock
		opts                 []options.RenameTablesOption
		err                  error
	}{
		{
			sessionID:            "1",
			operationTimeout:     time.Second,
			operationCancelAfter: time.Second,
			service: &renameTablesMock{
				RenameTablesRequest: &Ydb_Table.RenameTablesRequest{
					SessionId: "1",
					Tables: []*Ydb_Table.RenameTableItem{
						{
							SourcePath:         "from",
							DestinationPath:    "to",
							ReplaceDestination: true,
						},
					},
					OperationParams: &Ydb_Operations.OperationParams{
						OperationMode:    Ydb_Operations.OperationParams_SYNC,
						OperationTimeout: durationpb.New(time.Second),
						CancelAfter:      durationpb.New(time.Second),
					},
				},
			},
			opts: []options.RenameTablesOption{
				options.RenameTablesItem("from", "to", true),
			},
			err: nil,
		},
		{
			sessionID:            "2",
			operationTimeout:     2 * time.Second,
			operationCancelAfter: 2 * time.Second,
			service: &renameTablesMock{
				RenameTablesRequest: &Ydb_Table.RenameTablesRequest{
					SessionId: "2",
					Tables: []*Ydb_Table.RenameTableItem{
						{
							SourcePath:         "from1",
							DestinationPath:    "to1",
							ReplaceDestination: true,
						},
						{
							SourcePath:         "from2",
							DestinationPath:    "to2",
							ReplaceDestination: false,
						},
						{
							SourcePath:         "from3",
							DestinationPath:    "to3",
							ReplaceDestination: true,
						},
					},
					OperationParams: &Ydb_Operations.OperationParams{
						OperationMode:    Ydb_Operations.OperationParams_SYNC,
						OperationTimeout: durationpb.New(2 * time.Second),
						CancelAfter:      durationpb.New(2 * time.Second),
					},
				},
			},
			opts: []options.RenameTablesOption{
				options.RenameTablesItem("from1", "to1", true),
				options.RenameTablesItem("from2", "to2", false),
				options.RenameTablesItem("from3", "to3", true),
			},
			err: nil,
		},
		{
			sessionID:            "3",
			operationTimeout:     time.Second,
			operationCancelAfter: time.Second,
			service: &renameTablesMock{
				RenameTablesRequest: &Ydb_Table.RenameTablesRequest{
					SessionId: "1",
					Tables: []*Ydb_Table.RenameTableItem{
						{
							SourcePath:         "from",
							DestinationPath:    "to",
							ReplaceDestination: true,
						},
					},
					OperationParams: &Ydb_Operations.OperationParams{
						OperationMode:    Ydb_Operations.OperationParams_SYNC,
						OperationTimeout: durationpb.New(time.Second),
						CancelAfter:      durationpb.New(time.Second),
					},
				},
			},
			opts: []options.RenameTablesOption{
				options.RenameTablesItem("from1", "to1", true),
			},
			err: errUnexpectedRequest,
		},
		{
			sessionID:            "4",
			operationTimeout:     time.Second,
			operationCancelAfter: time.Second,
			service:              &renameTablesMock{},
			opts:                 nil,
			err:                  errParamsRequired,
		},
	} {
		t.Run("", func(t *testing.T) {
			err := renameTables(ctx, tt.sessionID, tt.operationTimeout, tt.operationCancelAfter, tt.service, tt.opts...)
			if tt.err != nil {
				require.ErrorIs(t, err, tt.err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
