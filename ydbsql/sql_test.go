package ydbsql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"io"
	"log"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Table"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/cmp"
	internal "github.com/ydb-platform/ydb-go-sdk/v3/internal/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/testutil"
)

// Interface checks.
var (
	c sqlConn

	_ driver.Conn               = &c
	_ driver.ExecerContext      = &c
	_ driver.QueryerContext     = &c
	_ driver.Pinger             = &c
	_ driver.SessionResetter    = &c
	_ driver.ConnPrepareContext = &c
	_ driver.ConnBeginTx        = &c
	_ driver.NamedValueChecker  = &c
)

func TestIsolationMapping(t *testing.T) {
	for _, test := range []struct {
		name   string
		opts   driver.TxOptions
		txExp  table.TxOption
		txcExp []table.TxControlOption
		err    bool
	}{
		{
			name: "default",
			opts: driver.TxOptions{
				Isolation: driver.IsolationLevel(sql.LevelDefault),
				ReadOnly:  false,
			},
			txExp: table.WithSerializableReadWrite(),
		},
		{
			name: "serializable",
			opts: driver.TxOptions{
				Isolation: driver.IsolationLevel(sql.LevelSerializable),
				ReadOnly:  false,
			},
			txExp: table.WithSerializableReadWrite(),
		},
		{
			name: "linearizable",
			opts: driver.TxOptions{
				Isolation: driver.IsolationLevel(sql.LevelLinearizable),
				ReadOnly:  false,
			},
			txExp: table.WithSerializableReadWrite(),
		},
		{
			name: "default ro",
			opts: driver.TxOptions{
				Isolation: driver.IsolationLevel(sql.LevelDefault),
				ReadOnly:  true,
			},
			txExp: table.WithSerializableReadWrite(),
		},
		{
			name: "serializable ro",
			opts: driver.TxOptions{
				Isolation: driver.IsolationLevel(sql.LevelSerializable),
				ReadOnly:  true,
			},
			txExp: table.WithSerializableReadWrite(),
		},
		{
			name: "linearizable ro",
			opts: driver.TxOptions{
				Isolation: driver.IsolationLevel(sql.LevelLinearizable),
				ReadOnly:  true,
			},
			txExp: table.WithSerializableReadWrite(),
		},
		{
			name: "read uncommitted",
			opts: driver.TxOptions{
				Isolation: driver.IsolationLevel(sql.LevelReadUncommitted),
				ReadOnly:  true,
			},
			txcExp: []table.TxControlOption{
				table.BeginTx(
					table.WithOnlineReadOnly(
						table.WithInconsistentReads(),
					),
				),
				table.CommitTx(),
			},
		},
		{
			name: "read committed",
			opts: driver.TxOptions{
				Isolation: driver.IsolationLevel(sql.LevelReadCommitted),
				ReadOnly:  true,
			},
			txcExp: []table.TxControlOption{
				table.BeginTx(
					table.WithOnlineReadOnly(),
				),
				table.CommitTx(),
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			txAct, txcAct, err := txIsolationOrControl(test.opts)
			if !test.err && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if test.err && err == nil {
				t.Fatalf("expected error; got nil")
			}

			var sAct, sExp *table.TransactionSettings
			if txAct != nil {
				sAct = table.TxSettings(txAct)
			}
			if test.txExp != nil {
				sExp = table.TxSettings(test.txExp)
			}

			cmp.Equal(t, sAct, sExp)

			var cAct, cExp *table.TransactionControl
			if txcAct != nil {
				cAct = table.TxControl(txcAct...)
			}
			if test.txcExp != nil {
				cExp = table.TxControl(test.txcExp...)
			}
			cmp.Equal(t, cAct, cExp)
		})
	}
}

func TestQuery(t *testing.T) {
	c, err := Connector(
		withClient(
			internal.New(
				context.Background(),
				testutil.NewCluster(
					testutil.WithInvokeHandlers(
						testutil.InvokeHandlers{
							// nolint:unparam
							testutil.TableCreateSession: func(request interface{}) (result proto.Message, err error) {
								return &Ydb_Table.CreateSessionResult{}, err
							},
							// nolint:unparam
							testutil.TableExecuteDataQuery: func(_ interface{}) (result proto.Message, err error) {
								return &Ydb_Table.ExecuteQueryResult{
									TxMeta: &Ydb_Table.TransactionMeta{
										Id: "",
									},
								}, err
							},
							// nolint:unparam
							testutil.TableStreamExecuteScanQuery: func(_ interface{}) (result proto.Message, err error) {
								return &Ydb_Table.ExecuteSchemeQueryResponse{}, err
							},
							// nolint:unparam
							testutil.TablePrepareDataQuery: func(request interface{}) (result proto.Message, err error) {
								return &Ydb_Table.PrepareQueryResult{}, nil
							},
						},
					),
					testutil.WithNewStreamHandlers(
						testutil.NewStreamHandlers{
							// nolint:unparam
							testutil.TableStreamExecuteScanQuery: func(_ *grpc.StreamDesc) (grpc.ClientStream, error) {
								return &testutil.ClientStream{
									OnRecvMsg: func(m interface{}) error {
										return io.EOF
									},
									OnSendMsg: func(m interface{}) error {
										return nil
									},
									OnCloseSend: func() error {
										return nil
									},
								}, nil
							},
						},
					),
				),
			),
		),
		WithDefaultExecDataQueryOption(),
	)
	if err != nil {
		log.Fatal(err)
	}
	for _, test := range [...]struct {
		subName       string
		scanQueryMode bool
	}{
		{
			subName:       "Legacy",
			scanQueryMode: false,
		},
		{
			subName:       "WithScanQuery",
			scanQueryMode: true,
		},
	} {
		t.Run("QueryContext/Conn/"+test.subName, func(t *testing.T) {
			db := sql.OpenDB(c)
			ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
			defer cancel()
			if test.scanQueryMode {
				ctx = WithScanQuery(ctx)
			}
			rows, err := db.QueryContext(ctx, "SELECT 1")
			cmp.NoError(t, err)
			cmp.NotNil(t, rows)
		})
		t.Run("QueryContext/STMT/"+test.subName, func(t *testing.T) {
			db := sql.OpenDB(c)
			ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
			defer cancel()
			stmt, err := db.PrepareContext(ctx, "SELECT 1")
			cmp.NoError(t, err)
			defer func() { _ = stmt.Close() }()
			if test.scanQueryMode {
				ctx = WithScanQuery(ctx)
			}
			rows, err := stmt.QueryContext(ctx)
			cmp.NoError(t, err)
			cmp.NotNil(t, rows)
		})
		t.Run("ExecContext/Conn/"+test.subName, func(t *testing.T) {
			db := sql.OpenDB(c)
			ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
			defer cancel()
			if test.scanQueryMode {
				ctx = WithScanQuery(ctx)
			}
			rows, err := db.ExecContext(ctx, "SELECT 1")
			cmp.NoError(t, err)
			cmp.NotNil(t, rows)
		})
		t.Run("ExecContext/STMT/"+test.subName, func(t *testing.T) {
			db := sql.OpenDB(c)
			ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
			defer cancel()
			stmt, err := db.PrepareContext(ctx, "SELECT 1")
			cmp.NoError(t, err)
			defer func() { _ = stmt.Close() }()
			if test.scanQueryMode {
				ctx = WithScanQuery(ctx)
			}
			rows, err := stmt.ExecContext(ctx)
			cmp.NoError(t, err)
			cmp.NotNil(t, rows)
		})
	}
}
