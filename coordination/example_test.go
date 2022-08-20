package coordination_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/coordination"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/coordination/config"
)

// nolint: errcheck
func Test_Example(t *testing.T) {
	ctx := context.TODO()
	db, err := ydb.Open(
		ctx,
		"grpcs://localhost:2135/?database=/local",
		ydb.WithCertificatesFromFile("~ydb-docker/ydb_certs/ca.pem"),
	)
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
	fmt.Printf("node description: %+vnode config: %+v\n", e, c)

	var (
		name  = "mySem"
		name2 = "mySem2"
		path  = "/local/test"
	)

	session, err := db.Coordination().SessionStart(ctx, path, config.WithSessionTimeoutMillis(500000))

	//fmt.Printf("Session ID: %v\n", id)

	if err != nil {
		fmt.Printf("SessionStart failed %v", err)
		return
	}

	err = session.CreateSemaphore(ctx, name, path, 123)
	if err != nil {
		fmt.Printf("failed CreateSemaphore: %v", err)
		return
	}

	err = session.CreateSemaphore(ctx, name2, path, 123)
	if err != nil {
		fmt.Printf("failed CreateSemaphore 2: %v", err)
		return
	}

	res, err := session.DescribeSemaphore(ctx, name, path, 999)
	if err != nil {
		fmt.Printf("failed DescribeSemaphore: %v", err)
		return
	}

	fmt.Printf("DescribeSemaphore: %v\n", res)

	locker := session.CreateLocker(name, path)

	err = locker.Lock(ctx, config.WithLockCount(53))
	if err != nil {
		fmt.Printf("locker.Lock failed: %v", err)
		return
	}

	locker2 := session.CreateLocker(name2, path)
	err = locker2.Lock(ctx, config.WithLockCount(44))
	if err != nil {
		fmt.Printf("locker.Lock failed: %v", err)
		return
	}

	res, err = session.DescribeSemaphore(ctx, name, path, 999)
	if err != nil {
		fmt.Printf("failed DescribeSemaphore 2: %v", err)
		return
	}

	fmt.Printf("DescribeSemaphore 2: %v\n", res)

	res, err = session.DescribeSemaphore(ctx, name2, path, 999)
	if err != nil {
		fmt.Printf("failed DescribeSemaphore 3: %v", err)
		return
	}

	fmt.Printf("DescribeSemaphore 3: %v\n", res)

	//locker := db.Coordination().CreateLocker(name, path)

	//data := []byte("hello, sem data 1")
	//err = locker.Lock(ctx, config.WithLockCount(100), config.WithLockData(data))
	//if err != nil {
	//	fmt.Printf("failed 1 lock: %v", err)
	//	return
	//}

	//err = locker.Unlock(ctx)
	//if err != nil {
	//	fmt.Printf("failed 1 unlock: %v", err)
	//	return
	//}

	//res, err := db.Coordination().DescribeSemaphore(ctx, name, path, 0)
	//
	//if err != nil {
	//	fmt.Printf("failed DescribeSemaphore: %v", err)
	//	return
	//}

	//locker2 := db.Coordination().CreateLocker(name, path)

	//data = []byte("hello, sem data 2")
	//err = locker.Lock(ctx, config.WithLockCount(95), config.WithLockData(data))
	//if err != nil {
	//	fmt.Printf("failed 2 lock: %v", err)
	//	return
	//}

	//res, err := db.Coordination().DescribeSemaphore(ctx, name, path, 0)
	//
	//if err != nil {
	//	fmt.Printf("failed DescribeSemaphore: %v", err)
	//	return
	//}

	//fmt.Printf("DescribeSemaphore: %v\n", res)
	//
	//data, err = locker.LoadData(ctx)
	//if err != nil {
	//	fmt.Printf("LoadData failed %v\n", err)
	//	return
	//}
	//
	//fmt.Printf("OPA DATA: %v\n", string(data))
}
