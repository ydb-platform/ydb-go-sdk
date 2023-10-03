package metrics_test

import (
	"context"
	"os"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/metrics"
)

func ExampleConfig() {
	var registryConfig metrics.Config // registryConfig is an implementation registry.Config
	db, err := ydb.Open(
		context.TODO(),
		os.Getenv("YDB_CONNECTION_STRING"),
		metrics.WithTraces(registryConfig),
	)
	if err != nil {
		panic(err)
	}
	defer func() {
		_ = db.Close(context.TODO())
	}()
	// work with db
}
