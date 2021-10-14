// +build integration

package test

import (
	"context"
	"database/sql"
	"log"
	"os"
	"testing"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
	_ "github.com/ydb-platform/ydb-go-sdk/v3/ydbsql"
)

func TestLegacyDriverOpen(t *testing.T) {
	t.Skip("run under docker")
	connection := os.Getenv("YDB_CONNECTION_STRING")
	t.Logf("test connection to '%s'", connection)
	db, err := sql.Open("ydb", connection)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Ping(); err != nil {
		t.Fatal(err)
	}
}

func TestDatabaseSelect(t *testing.T) {
	t.Skip("run under docker")
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
			db, err := openSql(ctx)
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
			db, err := openSql(ctx)
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
	t.Skip("run under docker")
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	db, err := openSql(ctx)
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
	t.Skip("run under docker")
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	db, err := openSql(ctx)
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
	t.Skip("run under docker")
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	db, err := openSql(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	rows, err := db.QueryContext(ctx, `
		DECLARE $seriesData AS List<Struct<
			series_id: Uint64,
			release_date: Uint64,
			title: Utf8,
			series_info: Utf8,
			comment: Optional<Utf8>>>;

		SELECT
			series_id,
			release_date,
			title,
			series_info,
		    comment
		FROM AS_TABLE($seriesData);
	`,
		sql.Named("seriesData", getSeriesData()),
	)
	if err != nil {
		t.Fatal(err)
	}
	var (
		seriesID    uint64
		releaseDate uint64
		title       string
		seriesInfo  string
		comment     *string
	)
	for rows.Next() {
		err = rows.Scan(
			&seriesID,
			&releaseDate,
			&title,
			&seriesInfo,
			&comment,
		)
		if err != nil {
			t.Fatal(err)
		}
		log.Printf("test: #%d %q %q %s %v", seriesID, title, seriesInfo, time.Unix(int64(releaseDate), 0), comment)
	}
	log.Println("rows err", rows.Err())

	row := db.QueryRowContext(ctx, `
		DECLARE $dt AS Datetime;
		SELECT NULL, $dt;
	`,
		sql.Named("dt", types.DatetimeValueFromTime(time.Now())),
	)
	var a, b sql.NullTime
	err = row.Scan(&a, &b)
	if err != nil {
		t.Fatal(err)
	}
	log.Println("date now:", a.Time, b.Time)
}
