//go:build !fast
// +build !fast

package test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/config"
	cfg "github.com/ydb-platform/ydb-go-sdk/v3/coordination"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/coordination"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/errors"
	internal "github.com/ydb-platform/ydb-go-sdk/v3/internal/ratelimiter"
	public "github.com/ydb-platform/ydb-go-sdk/v3/ratelimiter"
)

const (
	testCoordinationNodePath = "/local/test"
	testResource             = "test_res"
)

func TestRateLimiter(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*2)
	defer cancel()

	db, err := ydb.New(
		ctx,
		ydb.WithConnectionString(os.Getenv("YDB_CONNECTION_STRING")),
		ydb.WithAnonymousCredentials(),
		ydb.With(
			config.WithRequestTimeout(time.Second*2),
			config.WithStreamTimeout(time.Second*2),
			config.WithOperationTimeout(time.Second*2),
			config.WithOperationCancelAfter(time.Second*2),
		),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = db.Close(ctx)
	}()

	client := internal.New(db)
	coordinationClient := coordination.New(db)

	err = coordinationClient.DropNode(ctx, testCoordinationNodePath)
	if err != nil {
		if d := ydb.TransportErrorDescription(err); d != nil && d.Code() == int32(errors.TransportErrorUnimplemented) {
			return
		}
		if d := ydb.OperationErrorDescription(err); d != nil && d.Code() != int32(errors.StatusSchemeError) {
			t.Fatal(err)
		}
	}
	err = coordinationClient.CreateNode(ctx, testCoordinationNodePath, cfg.Config{
		Path:                     "",
		SelfCheckPeriodMillis:    1000,
		SessionGracePeriodMillis: 1000,
		ReadConsistencyMode:      cfg.ConsistencyModeRelaxed,
		AttachConsistencyMode:    cfg.ConsistencyModeRelaxed,
		RateLimiterCountersMode:  cfg.RateLimiterCountersModeDetailed,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err = coordinationClient.DropNode(ctx, testCoordinationNodePath)
		if err != nil {
			t.Fatal(err)
		}
	}()

	err = client.CreateResource(ctx, testCoordinationNodePath, public.Resource{
		ResourcePath: testResource,
		HierarchicalDrr: public.HierarchicalDrrSettings{
			MaxUnitsPerSecond:       1,
			MaxBurstSizeCoefficient: 2,
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		err = client.DropResource(ctx, testCoordinationNodePath, testResource)
		if err != nil {
			t.Fatal("Cannot drop resource")
		}
	}()

	described, err := client.DescribeResource(ctx, testCoordinationNodePath, testResource)
	if err != nil {
		t.Fatal(err)
	}
	if described == nil ||
		described.ResourcePath != testResource ||
		described.HierarchicalDrr.MaxUnitsPerSecond != 1.0 ||
		described.HierarchicalDrr.MaxBurstSizeCoefficient != 2.0 {
		t.Fatal("Resource invalid")
	}

	err = client.AlterResource(ctx, testCoordinationNodePath, public.Resource{
		ResourcePath: testResource,
		HierarchicalDrr: public.HierarchicalDrrSettings{
			MaxUnitsPerSecond:       3,
			MaxBurstSizeCoefficient: 4,
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	described, err = client.DescribeResource(ctx, testCoordinationNodePath, testResource)
	if err != nil {
		t.Fatal(err)
	}
	if described == nil ||
		described.ResourcePath != testResource ||
		described.HierarchicalDrr.MaxUnitsPerSecond != 3.0 ||
		described.HierarchicalDrr.MaxBurstSizeCoefficient != 4.0 {
		t.Fatal("Resource invalid")
	}

	list, err := client.ListResource(ctx, testCoordinationNodePath, testResource, true)
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != 1 || list[0] != testResource {
		t.Fatal("ListResource error")
	}

	err = client.AcquireResource(ctx, testCoordinationNodePath, testResource, 1, false)
	if err != nil {
		t.Fatal(err)
	}
	err = client.AcquireResource(ctx, testCoordinationNodePath, testResource, 10000, true)
	if err != nil {
		t.Fatal(err)
	}
	err = client.AcquireResource(ctx, testCoordinationNodePath, testResource, 10000, false)
	if err == nil {
		t.Fatal("Resource must not be acquired")
	}
}
