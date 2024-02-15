package meta_test

import (
	"context"
	"log"

	"google.golang.org/grpc/metadata"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/meta"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result/named"
)

func Example_consumedUnitsCount() {
	ctx := context.TODO()
	db, err := ydb.Open(ctx, "grpc://localhost:2136/local")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close(ctx) // cleanup resources
	var (
		query              = `SELECT 42 as id, "my string" as myStr`
		id                 int32  // required value
		myStr              string // optional value
		totalConsumedUnits uint64
	)
	err = db.Table().Do( // Do retry operation on errors with best effort
		meta.WithTrailerCallback(ctx, func(md metadata.MD) {
			totalConsumedUnits += meta.ConsumedUnits(md)
		}),
		func(ctx context.Context, s table.Session) (err error) { // retry operation
			_, res, err := s.Execute(ctx, table.DefaultTxControl(), query, nil)
			if err != nil {
				return err // for auto-retry with driver
			}
			defer res.Close()                                // cleanup resources
			if err = res.NextResultSetErr(ctx); err != nil { // check single result set and switch to it
				return err // for auto-retry with driver
			}
			for res.NextRow() { // iterate over rows
				err = res.ScanNamed(
					named.Required("id", &id),
					named.OptionalWithDefault("myStr", &myStr),
				)
				if err != nil {
					return err // generally scan error not retryable, return it for driver check error
				}
				log.Printf("id=%v, myStr='%s'\n", id, myStr)
			}

			return res.Err() // return finally result error for auto-retry with driver
		},
		table.WithIdempotent(),
	)
	if err != nil {
		log.Printf("unexpected error: %v", err)
	}
	log.Println("total consumed units:", totalConsumedUnits)
}
