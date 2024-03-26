//go:build integration
// +build integration

package integration

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/coordination"
	"github.com/ydb-platform/ydb-go-sdk/v3/coordination/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/log"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

//nolint:errcheck
func TestCoordinationSemaphore(t *testing.T) {
	ctx := context.TODO()
	db, err := ydb.Open(ctx,
		os.Getenv("YDB_CONNECTION_STRING"),
		ydb.WithAccessTokenCredentials(os.Getenv("YDB_ACCESS_TOKEN_CREDENTIALS")),
		ydb.WithLogger(
			log.Default(os.Stderr, log.WithMinLevel(log.TRACE)),
			trace.MatchDetails(`ydb\.(coordination).*`),
		),
	)
	if err != nil {
		t.Fatalf("failed to connect: %v", err)
	}
	defer db.Close(ctx) // cleanup resources

	const nodePath = "/local/coordination/node/test"

	// create node
	err = db.Coordination().CreateNode(ctx, nodePath, coordination.NodeConfig{
		Path:                     "",
		SelfCheckPeriodMillis:    1000,
		SessionGracePeriodMillis: 1000,
		ReadConsistencyMode:      coordination.ConsistencyModeStrict,
		AttachConsistencyMode:    coordination.ConsistencyModeStrict,
		RatelimiterCountersMode:  coordination.RatelimiterCountersModeDetailed,
	})
	if err != nil {
		t.Fatalf("failed to create node: %v", err)
	}
	defer db.Coordination().DropNode(ctx, nodePath)
	e, c, err := db.Coordination().DescribeNode(ctx, nodePath)
	if err != nil {
		t.Fatalf("failed to describe node: %v\n", err)
	}
	fmt.Printf("node description: %+v\nnode config: %+v\n", e, c)

	s, err := db.Coordination().Session(ctx, nodePath)
	if err != nil {
		t.Fatalf("failed to create session: %v\n", err)
	}
	defer s.Close(ctx)
	fmt.Printf("session 1 created, id: %d\n", s.SessionID())

	err = s.CreateSemaphore(ctx, "my-semaphore", 20, options.WithCreateData([]byte{1, 2, 3}))
	if err != nil {
		t.Fatalf("failed to create semaphore: %v", err)
	}
	fmt.Printf("semaphore my-semaphore created\n")

	lease, err := s.AcquireSemaphore(ctx, "my-semaphore", 10)
	if err != nil {
		t.Fatalf("failed to acquire semaphore: %v", err)
	}
	defer lease.Release()
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
		t.Fatalf("failed to describe semaphore: %v", err)
	}
	fmt.Printf("session 1 described semaphore %v\n", desc)

	err = lease.Release()
	if err != nil {
		t.Fatalf("failed to release semaphore: %v", err)
	}
	fmt.Printf("session 1 released semaphore my-semaphore\n")

	err = s.DeleteSemaphore(ctx, "my-semaphore", options.WithForceDelete(true))
	if err != nil {
		t.Fatalf("failed to delete semaphore: %v", err)
	}

	fmt.Printf("deleted semaphore my-semaphore\n")
}
