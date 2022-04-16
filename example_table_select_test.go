package ydb_test

import (
	"context"
	"log"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result/named"
)

func Example_tableSelect() {
	ctx := context.Background()
	db, err := ydb.Open(ctx,
		"grpcs://localhost:2135/?database=/local",
		ydb.WithAnonymousCredentials(),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		_ = db.Close(ctx)
	}()
	var (
		query = `SELECT 42 as id, "my string" as myStr`
		id    int32  // required value
		myStr string // optional value
	)
	err = db.Table().Do( // Do retry operation on errors with best effort
		ctx, // context manage exiting from Do
		func(ctx context.Context, s table.Session) (err error) { // retry operation
			_, res, err := s.Execute(ctx, table.DefaultTxControl(), query, nil)
			if err != nil {
				return err // for driver retry
			}
			defer func() {
				_ = res.Close() // must close always
			}()
			if err = res.NextResultSetErr(ctx); err != nil { // check single result set and switch to it
				return err // for driver retry
			}
			for res.NextRow() { // iterate over rows
				err = res.ScanNamed(
					named.Required("id", &id),
					named.OptionalWithDefault("myStr", &myStr),
				)
				if err != nil {
					return err
				}
				log.Printf("id=%v, myStr='%s'\n", id, myStr)
			}
			return res.Err() // for driver retry if not nil
		},
	)
	if err != nil {
		log.Printf("unexpected error: %v", err)
	}
}
