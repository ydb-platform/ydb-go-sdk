package ydbsql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"log"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/yandex-cloud/ydb-go-sdk"
	"github.com/yandex-cloud/ydb-go-sdk/internal/traceutil"
	"github.com/yandex-cloud/ydb-go-sdk/table"
)

// Interface checks.
var (
	c conn

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
		name string
		opts driver.TxOptions
		exp  table.TxOption
		err  bool
	}{
		{
			name: "default",
			opts: driver.TxOptions{
				Isolation: driver.IsolationLevel(sql.LevelDefault),
				ReadOnly:  false,
			},
			exp: table.WithSerializableReadWrite(),
		},
		{
			name: "serializable",
			opts: driver.TxOptions{
				Isolation: driver.IsolationLevel(sql.LevelSerializable),
				ReadOnly:  false,
			},
			exp: table.WithSerializableReadWrite(),
		},
		{
			name: "linearizable",
			opts: driver.TxOptions{
				Isolation: driver.IsolationLevel(sql.LevelLinearizable),
				ReadOnly:  false,
			},
			exp: table.WithSerializableReadWrite(),
		},
		{
			name: "default ro",
			opts: driver.TxOptions{
				Isolation: driver.IsolationLevel(sql.LevelDefault),
				ReadOnly:  true,
			},
			exp: table.WithSerializableReadWrite(),
		},
		{
			name: "serializable ro",
			opts: driver.TxOptions{
				Isolation: driver.IsolationLevel(sql.LevelSerializable),
				ReadOnly:  true,
			},
			exp: table.WithSerializableReadWrite(),
		},
		{
			name: "linearizable ro",
			opts: driver.TxOptions{
				Isolation: driver.IsolationLevel(sql.LevelLinearizable),
				ReadOnly:  true,
			},
			exp: table.WithSerializableReadWrite(),
		},
		{
			name: "read uncommitted",
			opts: driver.TxOptions{
				Isolation: driver.IsolationLevel(sql.LevelReadUncommitted),
				ReadOnly:  true,
			},
			exp: table.WithOnlineReadOnly(
				table.WithInconsistentReads(),
			),
		},
		{
			name: "read committed",
			opts: driver.TxOptions{
				Isolation: driver.IsolationLevel(sql.LevelReadCommitted),
				ReadOnly:  true,
			},
			exp: table.WithOnlineReadOnly(),
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			act, err := txIsolation(test.opts)
			if !test.err && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if test.err && err == nil {
				t.Fatalf("expected error; got nil")
			}

			sAct := table.TxSettings(act)
			sExp := table.TxSettings(test.exp)
			if !reflect.DeepEqual(sAct, sExp) {
				t.Fatalf("unexpected settings: %+v; want %+v", sAct, sExp)
			}
		})
	}
}

func openDB(ctx context.Context) (*sql.DB, error) {
	var (
		dtrace ydb.DriverTrace
		ctrace table.ClientTrace
		strace table.SessionPoolTrace
	)
	traceutil.Stub(&dtrace, func(name string, args ...interface{}) {
		log.Printf("[driver] %s: %+v", name, traceutil.ClearContext(args))
	})
	traceutil.Stub(&ctrace, func(name string, args ...interface{}) {
		log.Printf("[client] %s: %+v", name, traceutil.ClearContext(args))
	})
	traceutil.Stub(&strace, func(name string, args ...interface{}) {
		log.Printf("[session] %s: %+v", name, traceutil.ClearContext(args))
	})

	db := sql.OpenDB(Connector(
		WithEndpoint("ydb-ru.yandex.net:2135"),
		WithDatabase("/ru/home/kamardin/mydb"),
		WithCredentials(ydb.AuthTokenCredentials{
			AuthToken: os.Getenv("YDB_TOKEN"),
		}),
		WithDriverTrace(dtrace),
		WithClientTrace(ctrace),
		WithSessionPoolTrace(strace),
		WithSessionPoolIdleThreshold(time.Second),
	))

	return db, db.PingContext(ctx)
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
			defer db.Close()
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
			defer db.Close()
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
	defer db.Close()

	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	stmt, err := conn.PrepareContext(ctx, "SELECT NULL;")
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Close()

	stmt.Exec()
	stmt.Exec()

	conn.QueryContext(ctx, "SELECT 42;")
}

func TestTx(t *testing.T) {
	t.Skip("need to be tested with docker")

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	db, err := openDB(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	stmt, err := tx.PrepareContext(ctx, "SELECT NULL;")
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Close()

	stmt.Exec()
	tx.Commit()

	time.Sleep(5 * time.Second)

	{
		rows, err := db.QueryContext(context.Background(), "SELECT 42")
		if err != nil {
			t.Fatal(err)
		}
		rows.Close()

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
	defer db.Close()

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
			releaseDate Date
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
		sql.Named("dt", Datetime(time.Now())),
	)
	var a, b time.Time
	if err := row.Scan(
		Nullable((*Datetime)(&a)),
		(*Datetime)(&b),
	); err != nil {
		t.Fatal(err)
	}
	log.Println("date now:", a, b)
}

func getSeriesData() ydb.Value {
	return ydb.ListValue(
		seriesData(1, days("2006-02-03"), "IT Crowd", ""+
			"The IT Crowd is a British sitcom produced by Channel 4, written by Graham Linehan, produced by "+
			"Ash Atalla and starring Chris O'Dowd, Richard Ayoade, Katherine Parkinson, and Matt Berry."),
		seriesData(2, days("2014-04-06"), "Silicon Valley", ""+
			"Silicon Valley is an American comedy television series created by Mike Judge, John Altschuler and "+
			"Dave Krinsky. The series focuses on five young men who founded a startup company in Silicon Valley."),
	)
}

func seriesData(id uint64, released uint32, title, info string) ydb.Value {
	return ydb.StructValue(
		ydb.StructFieldValue("series_id", ydb.Uint64Value(id)),
		ydb.StructFieldValue("release_date", ydb.DateValue(released)),
		ydb.StructFieldValue("title", ydb.UTF8Value(title)),
		ydb.StructFieldValue("series_info", ydb.UTF8Value(info)),
	)
}

func days(date string) uint32 {
	const ISO8601 = "2006-01-02"
	t, err := time.Parse(ISO8601, date)
	if err != nil {
		panic(err)
	}
	return ydb.Time(t).Date()
}
