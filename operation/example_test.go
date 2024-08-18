package operation_test

import (
	"context"
	"fmt"

	"github.com/ydb-platform/ydb-go-sdk/v3"
)

func Example_listOperations() {
	ctx := context.TODO()
	db, err := ydb.Open(ctx, "grpc://localhost:2136/local")
	if err != nil {
		panic(err)
	}
	defer db.Close(ctx) // cleanup resources
	operations, err := db.Operation().List(ctx)
	if err != nil {
		panic(err)
	}
	fmt.Println("operations:")
	for _, op := range operations {
		fmt.Println(" -", op)
	}
}
