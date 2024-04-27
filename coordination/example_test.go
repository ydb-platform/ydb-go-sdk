package coordination_test

import (
	"context"
	"fmt"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/coordination"
	"github.com/ydb-platform/ydb-go-sdk/v3/coordination/options"
)

//nolint:errcheck
func Example_createDropNode() {
	ctx := context.TODO()
	db, err := ydb.Open(ctx, "grpc://localhost:2136/local")
	if err != nil {
		fmt.Printf("failed to connect: %v", err)

		return
	}
	defer db.Close(ctx) // cleanup resources
	// create node
	err = db.Coordination().CreateNode(ctx, "/local/test", coordination.NodeConfig{
		Path:                     "",
		SelfCheckPeriodMillis:    1000,
		SessionGracePeriodMillis: 1000,
		ReadConsistencyMode:      coordination.ConsistencyModeRelaxed,
		AttachConsistencyMode:    coordination.ConsistencyModeRelaxed,
		RatelimiterCountersMode:  coordination.RatelimiterCountersModeDetailed,
	})
	if err != nil {
		fmt.Printf("failed to create node: %v", err)

		return
	}
	defer db.Coordination().DropNode(ctx, "/local/test")
	e, c, err := db.Coordination().DescribeNode(ctx, "/local/test")
	if err != nil {
		fmt.Printf("failed to describe node: %v", err)

		return
	}
	fmt.Printf("node description: %+v\nnode config: %+v\n", e, c)
}

func Example_semaphore() {
	ctx := context.TODO()
	db, err := ydb.Open(ctx, "grpc://localhost:2136/local")
	if err != nil {
		fmt.Printf("failed to connect: %v", err)

		return
	}
	defer db.Close(ctx) // cleanup resources
	// create node
	err = db.Coordination().CreateNode(ctx, "/local/test", coordination.NodeConfig{
		Path:                     "",
		SelfCheckPeriodMillis:    1000,
		SessionGracePeriodMillis: 1000,
		ReadConsistencyMode:      coordination.ConsistencyModeStrict,
		AttachConsistencyMode:    coordination.ConsistencyModeStrict,
		RatelimiterCountersMode:  coordination.RatelimiterCountersModeDetailed,
	})
	if err != nil {
		fmt.Printf("failed to create node: %v", err)

		return
	}
	defer func() {
		dropNodeErr := db.Coordination().DropNode(ctx, "/local/test")
		if dropNodeErr != nil {
			fmt.Printf("failed to drop node: %v\n", dropNodeErr)
		}
	}()

	e, c, err := db.Coordination().DescribeNode(ctx, "/local/test")
	if err != nil {
		fmt.Printf("failed to describe node: %v\n", err)

		return
	}
	fmt.Printf("node description: %+v\nnode config: %+v\n", e, c)

	s, err := db.Coordination().Session(ctx, "/local/test")
	if err != nil {
		fmt.Printf("failed to create session: %v\n", err)

		return
	}
	defer s.Close(ctx)
	fmt.Printf("session 1 created, id: %d\n", s.SessionID())

	err = s.CreateSemaphore(ctx, "my-semaphore", 20, options.WithCreateData([]byte{1, 2, 3}))
	if err != nil {
		fmt.Printf("failed to create semaphore: %v", err)

		return
	}
	fmt.Printf("semaphore my-semaphore created\n")

	lease, err := s.AcquireSemaphore(ctx, "my-semaphore", 10)
	if err != nil {
		fmt.Printf("failed to acquire semaphore: %v", err)

		return
	}
	defer func() {
		releaseErr := lease.Release()
		if releaseErr != nil {
			fmt.Printf("failed to release lease: %v", releaseErr)
		}
	}()

	fmt.Printf("session 1 acquired semaphore 10\n")

	s.Reconnect()
	fmt.Printf("session 1 reconnected\n")

	desc, err := s.DescribeSemaphore(
		ctx,
		"my-semaphore",
		options.WithDescribeOwners(true),
		options.WithDescribeWaiters(true),
	)
	if err != nil {
		fmt.Printf("failed to describe semaphore: %v", err)

		return
	}
	fmt.Printf("session 1 described semaphore %v\n", desc)

	err = lease.Release()
	if err != nil {
		fmt.Printf("failed to release semaphore: %v", err)

		return
	}
	fmt.Printf("session 1 released semaphore my-semaphore\n")

	err = s.DeleteSemaphore(ctx, "my-semaphore", options.WithForceDelete(true))
	if err != nil {
		fmt.Printf("failed to delete semaphore: %v", err)

		return
	}
	fmt.Printf("deleted semaphore my-semaphore\n")
}
