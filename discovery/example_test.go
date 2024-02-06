package discovery_test

import (
	"context"
	"fmt"

	"github.com/ydb-platform/ydb-go-sdk/v3"
)

func Example_discoverCluster() {
	ctx := context.TODO()
	db, err := ydb.Open(ctx, "grpc://localhost:2136/local")
	if err != nil {
		fmt.Printf("failed to connect: %v", err)

		return
	}
	defer db.Close(ctx) // cleanup resources
	endpoints, err := db.Discovery().Discover(ctx)
	if err != nil {
		fmt.Printf("discover failed: %v", err)

		return
	}
	fmt.Printf("%s endpoints:\n", db.Name())
	for i, e := range endpoints {
		fmt.Printf("%d) %s\n", i, e.String())
	}
}

func Example_whoAmI() {
	ctx := context.TODO()
	db, err := ydb.Open(ctx, "grpc://localhost:2136/local")
	if err != nil {
		fmt.Printf("failed to connect: %v", err)

		return
	}
	defer db.Close(ctx) // cleanup resources
	whoAmI, err := db.Discovery().WhoAmI(ctx)
	if err != nil {
		fmt.Printf("discover failed: %v", err)

		return
	}
	fmt.Printf("%s whoAmI: %s\n", db.Name(), whoAmI.String())
}
