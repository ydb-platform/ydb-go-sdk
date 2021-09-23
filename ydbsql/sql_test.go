package ydbsql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"io"
	"log"
	"os"
	"testing"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/assert"

	table "github.com/ydb-platform/ydb-go-sdk/v3/table"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/meta/credentials"
	internal "github.com/ydb-platform/ydb-go-sdk/v3/internal/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Table"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"

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

			assert.Equal(t, sAct, sExp)

			var cAct, cExp *table.TransactionControl
			if txcAct != nil {
				cAct = table.TxControl(txcAct...)
			}
			if test.txcExp != nil {
				cExp = table.TxControl(test.txcExp...)
			}
			assert.Equal(t, cAct, cExp)
		})
	}
}

func openDB(ctx context.Context) (*sql.DB, error) {
	var (
		dtrace trace.Driver
		ttrace trace.Table
	)
	trace.Stub(&dtrace, func(name string, args ...interface{}) {
		log.Printf("[driver] %s: %+v", name, trace.ClearContext(args))
	})
	trace.Stub(&ttrace, func(name string, args ...interface{}) {
		log.Printf("[table] %s: %+v", name, trace.ClearContext(args))
	})

	connectParams := ydb.MustConnectionString(os.Getenv("YDB"))
	db := sql.OpenDB(Connector(
		WithConnectParams(connectParams),
		WithCredentials(credentials.AuthTokenCredentials{
			AuthToken: os.Getenv("YDB_ACCESS_TOKEN_CREDENTIALS"),
		}),
		WithTraceDriver(dtrace),
		WithTraceTable(ttrace),
	))

	return db, db.PingContext(ctx)
}

func TestQuery(t *testing.T) {
	c := Connector(
		withClient(
			internal.NewClientAsPool(
				testutil.NewDB(
					testutil.WithInvokeHandlers(
						testutil.InvokeHandlers{
							testutil.TableCreateSession: func(request interface{}) (result proto.Message, err error) {
								return &Ydb_Table.CreateSessionResult{}, err
							},
							testutil.TableExecuteDataQuery: func(_ interface{}) (result proto.Message, err error) {
								return &Ydb_Table.ExecuteQueryResult{
									TxMeta: &Ydb_Table.TransactionMeta{
										Id: "",
									},
								}, err
							},
							testutil.TablePrepareDataQuery: func(request interface{}) (result proto.Message, err error) {
								return &Ydb_Table.PrepareQueryResult{}, nil
							},
						},
					),
					testutil.WithNewStreamHandlers(
						testutil.NewStreamHandlers{
							testutil.TableStreamExecuteScanQuery: func(_ *grpc.StreamDesc) (c grpc.ClientStream, err error) {
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
								}, err
							},
						},
					),
				),
				internal.DefaultConfig(),
			),
		),
		WithDefaultExecDataQueryOption(),
	)

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
			assert.NoError(t, err)
			assert.NotNil(t, rows)
		})
		t.Run("QueryContext/STMT/"+test.subName, func(t *testing.T) {
			db := sql.OpenDB(c)
			ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
			defer cancel()
			stmt, err := db.PrepareContext(ctx, "SELECT 1")
			assert.NoError(t, err)
			defer func() { _ = stmt.Close() }()
			if test.scanQueryMode {
				ctx = WithScanQuery(ctx)
			}
			rows, err := stmt.QueryContext(ctx)
			assert.NoError(t, err)
			assert.NotNil(t, rows)
		})
		t.Run("ExecContext/Conn/"+test.subName, func(t *testing.T) {
			db := sql.OpenDB(c)
			ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
			defer cancel()
			if test.scanQueryMode {
				ctx = WithScanQuery(ctx)
			}
			rows, err := db.ExecContext(ctx, "SELECT 1")
			assert.NoError(t, err)
			assert.NotNil(t, rows)
		})
		t.Run("ExecContext/STMT/"+test.subName, func(t *testing.T) {
			db := sql.OpenDB(c)
			ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
			defer cancel()
			stmt, err := db.PrepareContext(ctx, "SELECT 1")
			assert.NoError(t, err)
			defer func() { _ = stmt.Close() }()
			if test.scanQueryMode {
				ctx = WithScanQuery(ctx)
			}
			rows, err := stmt.ExecContext(ctx)
			assert.NoError(t, err)
			assert.NotNil(t, rows)
		})
	}
}

func TestDatabaseSelect(t *testing.T) {
	t.Skip("need to be tested with docker")

	for _, test := range []struct {
		query  string
		params []interface{}
	}{
		{
			query: "DECLARE $a AS INT64; SELECT $a",
			params: []interface{}{
				sql.Named("a", int64(1)),
			},
		},
	} {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		t.Run("exec", func(t *testing.T) {
			db, err := openDB(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = db.Close() }()
			res, err := db.ExecContext(ctx, test.query, test.params...)
			if err != nil {
				t.Fatal(err)
			}
			log.Printf("result=%v", res)
		})
		t.Run("query", func(t *testing.T) {
			db, err := openDB(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = db.Close() }()
			rows, err := db.QueryContext(ctx, test.query, test.params...)
			if err != nil {
				t.Fatal(err)
			}
			log.Printf("rows=%v", rows)
		})
	}
}

func TestStatement(t *testing.T) {
	t.Skip("need to be tested with docker")

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	db, err := openDB(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	stmt, err := conn.PrepareContext(ctx, "SELECT NULL;")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = stmt.Close() }()

	_, _ = stmt.Exec()
	_, _ = stmt.Exec()

	_, _ = conn.QueryContext(ctx, "SELECT 42;")
}

func TestTx(t *testing.T) {
	t.Skip("need to be tested with docker")

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	db, err := openDB(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	stmt, err := tx.PrepareContext(ctx, "SELECT NULL;")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = stmt.Close() }()

	_, _ = stmt.Exec()
	_ = tx.Commit()

	time.Sleep(5 * time.Second)

	{
		rows, err := db.QueryContext(context.Background(), "SELECT 42")
		if err != nil {
			t.Fatal(err)
		}
		_ = rows.Close()

		time.Sleep(5 * time.Second)
	}
	time.Sleep(5 * time.Second)
}

func TestDriver(t *testing.T) {
	t.Skip("need to be tested with docker")

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	db, err := openDB(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	rows, err := db.QueryContext(ctx, `
		DECLARE $seriesData AS "List<Struct<
			series_id: Uint64,
			title: Utf8,
			series_info: Utf8,
			release_date: Date>>";

		SELECT
			series_id,
			title,
			series_info,
			release_date
		FROM AS_TABLE($seriesData);
	`,
		sql.Named("seriesData", getSeriesData()),
	)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var (
			seriesID    uint64
			title       string
			seriesInfo  string
			releaseDate time.Time
		)
		err := rows.Scan(
			&seriesID,
			&title,
			&seriesInfo,
			&releaseDate,
		)
		if err != nil {
			t.Fatal(err)
		}
		log.Printf("test: #%d %q %q %s", seriesID, title, seriesInfo, time.Time(releaseDate))
	}
	log.Println("rows err", rows.Err())

	row := db.QueryRowContext(ctx, `
		DECLARE $dt AS Datetime;
		SELECT NULL, $dt;
	`,
		sql.Named("dt", types.DateValueFromTime(time.Now())),
	)
	var a, b sql.NullTime
	err = row.Scan(&a, &b)
	if err != nil {
		t.Fatal(err)
	}
	log.Println("date now:", a.Time, b.Time)
}

func getSeriesData() types.Value {
	return types.ListValue(
		seriesData(1, days("2006-02-03"), "IT Crowd", ""+
			"The IT Crowd is a British sitcom produced by Channel 4, written by Graham Linehan, produced by "+
			"Ash Atalla and starring Chris O'Dowd, Richard Ayoade, Katherine Parkinson, and Matt Berry."),
		seriesData(2, days("2014-04-06"), "Silicon Valley", ""+
			"Silicon Valley is an American comedy television series created by Mike Judge, John Altschuler and "+
			"Dave Krinsky. The series focuses on five young men who founded a startup company in Silicon Valley."),
	)
}

func seriesData(id uint64, released time.Time, title, info string) types.Value {
	return types.StructValue(
		types.StructFieldValue("series_id", types.Uint64Value(id)),
		types.StructFieldValue("release_date", types.DateValueFromTime(released)),
		types.StructFieldValue("title", types.UTF8Value(title)),
		types.StructFieldValue("series_info", types.UTF8Value(info)),
	)
}

func days(date string) time.Time {
	const ISO8601 = "2006-01-02"
	t, err := time.Parse(ISO8601, date)
	if err != nil {
		panic(err)
	}
	return t
}
