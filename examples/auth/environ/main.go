package main

import (
	"context"
	"fmt"
	"os"

	environ "github.com/ydb-platform/ydb-go-sdk-auth-environ"

	ydb "github.com/ydb-platform/ydb-go-sdk/v3"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	db, err := ydb.Open(ctx,
		os.Getenv("YDB_CONNECTION_STRING"),
		environ.WithEnvironCredentials(ctx),
	)
	if err != nil {
		panic(err)
	}
	defer func() { _ = db.Close(ctx) }()

	whoAmI, err := db.Discovery().WhoAmI(ctx)
	if err != nil {
		panic(err)
	}

	fmt.Println(whoAmI.String())
}
