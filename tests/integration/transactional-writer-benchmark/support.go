package transactionalwriterbenchmark

import (
	"context"
	"fmt"

	"google.golang.org/grpc"

	ydb "github.com/ydb-platform/ydb-go-sdk/v3"
	sdkconfig "github.com/ydb-platform/ydb-go-sdk/v3/config"
)

type topicTopology struct {
	ActivePartitionIDs []int64
	TotalPartitions    int
}

func openDatabase(ctx context.Context, cfg config, metrics *instrumentation) (*ydb.Driver, error) {
	options := make([]ydb.Option, 0, 3)
	if metrics != nil {
		options = append(
			options,
			ydb.With(sdkconfig.WithGrpcOptions(
				grpc.WithChainUnaryInterceptor(metrics.unaryClientInterceptor),
				grpc.WithChainStreamInterceptor(metrics.streamClientInterceptor),
			)),
			ydb.WithTraceTopic(metrics.topicTrace()),
		)
	}
	options = append(options, ydb.WithAnonymousCredentials())

	db, err := ydb.Open(ctx, cfg.DSN, options...)
	if err != nil {
		return nil, fmt.Errorf("open YDB driver: %w", err)
	}

	return db, nil
}
