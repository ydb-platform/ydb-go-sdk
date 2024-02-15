package scheme_test

import (
	"context"
	"fmt"

	"github.com/ydb-platform/ydb-go-sdk/v3"
)

func Example() {
	ctx := context.TODO()
	db, err := ydb.Open(ctx, "grpc://localhost:2136/local")
	if err != nil {
		fmt.Printf("failed to connect: %v", err)

		return
	}
	defer db.Close(ctx) // cleanup resources
	err = db.Scheme().MakeDirectory(ctx, "/local/test")
	if err != nil {
		fmt.Printf("failed to make directory: %v", err)
	}
	d, err := db.Scheme().ListDirectory(ctx, "/local/test")
	if err != nil {
		fmt.Printf("failed to list directory: %v", err)
	}
	fmt.Printf("list directory: %+v\n", d)
}
