package main

import (
	"context"
	"log"
	"path"
	"time"

	"github.com/yandex-cloud/ydb-go-sdk"
	"github.com/yandex-cloud/ydb-go-sdk/opt"
	"github.com/yandex-cloud/ydb-go-sdk/table"
)

var (
	roTX = table.TxControl(
		table.BeginTx(table.WithOnlineReadOnly()),
		table.CommitTx(),
	)
	rwTX = table.TxControl(
		table.BeginTx(table.WithSerializableReadWrite()),
		table.CommitTx(),
	)
)

func run(ctx context.Context, endpoint, prefix string, config *ydb.DriverConfig) error {
	driver, err := (&ydb.Dialer{
		DriverConfig: config,
	}).Dial(ctx, endpoint)
	if err != nil {
		return err
	}

	c := table.Client{Driver: driver}
	session, err := c.CreateSession(ctx)
	if err != nil {
		return err
	}
	defer session.Close(context.Background())

	prefix = path.Join(config.Database, prefix)

	err = session.CreateTable(ctx, path.Join(prefix, "users"),
		table.WithColumn("id", ydb.Optional(ydb.TypeUint64)),
		table.WithColumn("username", ydb.Optional(ydb.TypeUTF8)),
		table.WithColumn("mode", ydb.Optional(ydb.TypeUint64)),
		table.WithColumn("magic", ydb.Optional(ydb.TypeUint32)),
		table.WithColumn("score", ydb.Optional(ydb.TypeInt64)),
		table.WithColumn("updated", ydb.Optional(ydb.TypeTimestamp)),
		table.WithPrimaryKeyColumn("id"),
	)
	if err != nil {
		return err
	}
	{
		const fill = `
			DECLARE $users AS "List<Struct<
				id: Uint64?,
				username: Utf8?,
				mode: Uint64?,
				magic: Uint32?,
				score: Int64?,
				updated: Timestamp?>>";
			
			REPLACE INTO users
			SELECT
				id,
				username,
				mode,
				magic,
				score,
				updated
			FROM AS_TABLE($users);`

		users := Users{
			{ID: 0, Username: "Randy", Mode: 042},
			{ID: 1, Username: "Leo", Score: opt.OInt64(42)},
		}
		stmt, err := session.Prepare(ctx, withPragma(prefix, fill))
		if err != nil {
			return err
		}
		_, _, err = stmt.Execute(ctx, rwTX, table.NewQueryParameters(
			table.ValueParam("$users", users.ListValue()),
		))
		if err != nil {
			return err
		}
	}
	{
		const insert = `
			DECLARE $id AS "Uint64?";
			DECLARE $username AS "Utf8?";
			DECLARE $mode AS "Uint64?";
			DECLARE $magic AS "Uint32?";
			DECLARE $score AS "Int64?";
			DECLARE $updated AS "Timestamp?";
			
			UPSERT INTO users 
				(id, username, mode, magic, score, updated)
			VALUES
				($id, $username, $mode, $magic, $score, $updated);`

		user := User{
			ID:       3,
			Username: "Elliot",
			Score:    opt.OInt64(43),
			Updated:  time.Now(),
		}
		stmt, err := session.Prepare(ctx, withPragma(prefix, insert))
		if err != nil {
			return err
		}
		_, _, err = stmt.Execute(ctx, rwTX, user.QueryParameters())
		if err != nil {
			return err
		}
	}
	{
		const query = `SELECT * FROM users;`

		stmt, err := session.Prepare(ctx, withPragma(prefix, query))
		if err != nil {
			return err
		}
		_, res, err := stmt.Execute(ctx, roTX, nil)
		if err != nil {
			return err
		}

		res.NextSet()

		var users Users
		if err := (&users).Scan(res); err != nil {
			return err
		}
		for _, user := range users {
			log.Printf("> user %+v", user)
		}
	}

	return nil
}

func withPragma(prefix, query string) string {
	return `PRAGMA TablePathPrefix("` + prefix + `");` +
		query
}
