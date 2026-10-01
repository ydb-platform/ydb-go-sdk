package transactionalwriterbenchmark

import (
	"context"
	"fmt"

	ydb "github.com/ydb-platform/ydb-go-sdk/v3"
)

type topicTopology struct {
	ActivePartitions int
}

func openDatabase(ctx context.Context, cfg config, metrics *instrumentation) (*ydb.Driver, error) {
	options := make([]ydb.Option, 0, 2)
	if metrics != nil {
		options = append(options, ydb.WithTraceTopic(metrics.topicTrace()))
	}
	options = append(options, ydb.WithAnonymousCredentials())

	db, err := ydb.Open(ctx, cfg.DSN, options...)
	if err != nil {
		return nil, fmt.Errorf("open YDB driver: %w", err)
	}

	return db, nil
}
