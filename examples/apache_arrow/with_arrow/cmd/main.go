package main

import (
	"context"
	"fmt"
	"os"

	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
)

func main() {
	ctx := context.Background()
	dsn := os.Getenv("YDB_CONNECTION_STRING")
	if dsn == "" {
		dsn = "grpc://localhost:2136/local"
	}
	db, err := ydb.Open(ctx, dsn, ydb.WithAnonymousCredentials())
	if err != nil {
		panic(err)
	}
	defer db.Close(ctx)

	decoder := query.NewArrowDecoder(ipc.NewReader)
	err = db.Query().Do(ctx, func(ctx context.Context, s query.Session) error {
		result, err := s.Query(ctx, `SELECT 42 AS id, "my string"u AS name;
SELECT 24 AS id, "WOW"u AS name;`, query.WithArrow(decoder))
		if err != nil {
			return err
		}
		defer result.Close(ctx)

		for rs, err := range result.ResultSets(ctx) {
			if err != nil {
				return err
			}
			for row, err := range rs.Rows(ctx) {
				if err != nil {
					return err
				}
				var id int32
				var name string
				if err := row.ScanNamed(query.Named("id", &id), query.Named("name", &name)); err != nil {
					return err
				}
				fmt.Printf("id=%d name=%q\n", id, name)
			}
		}

		return nil
	}, query.WithIdempotent())
	if err != nil {
		panic(err)
	}
}
