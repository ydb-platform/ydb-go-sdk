package main

import (
	"context"
	"errors"
	"fmt"
	"log"

	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
)

func wrap(err error, explanation string) error {
	if err != nil {
		return fmt.Errorf("%s: %w", explanation, err)
	}
	return err
}

func testUniformPartitions(ctx context.Context, c table.Client, tablePath string) error {
	log.Printf("Create uniform partitions table: %v\n", tablePath)

	err := c.Do(ctx,
		func(ctx context.Context, session table.Session) error {
			err := session.CreateTable(ctx, tablePath,
				options.WithColumn("key", types.Optional(types.TypeUint64)),
				options.WithColumn("value", types.Optional(types.TypeJSON)),
				options.WithPrimaryKeyColumn("key"),

				options.WithProfile(
					options.WithPartitioningPolicy(
						options.WithPartitioningPolicyMode(options.PartitioningAutoSplitMerge),
						options.WithPartitioningPolicyUniformPartitions(4),
					),
				),
			)
			if err != nil {
				return wrap(err, "failed to create table")
			}

			desc, err := session.DescribeTable(ctx, tablePath, options.WithShardKeyBounds())
			if err != nil {
				return wrap(err, "failed to get table description")
			}
			if len(desc.KeyRanges) != 4 {
				return errors.New("key ranges len is not as expected")
			}

			return nil
		},
	)
	return wrap(err, "failed to execute operation")
}

func testExplicitPartitions(ctx context.Context, c table.Client, tablePath string) error {
	log.Printf("Create explicit partitions table: %v\n", tablePath)

	err := c.Do(ctx,
		func(ctx context.Context, session table.Session) error {
			err := session.CreateTable(ctx, tablePath,
				options.WithColumn("key", types.Optional(types.TypeUint64)),
				options.WithColumn("value", types.Optional(types.TypeJSON)),
				options.WithPrimaryKeyColumn("key"),

				options.WithProfile(
					options.WithPartitioningPolicy(
						options.WithPartitioningPolicyExplicitPartitions(
							types.TupleValue(types.OptionalValue(types.Uint64Value(100))),
							types.TupleValue(types.OptionalValue(types.Uint64Value(300))),
							types.TupleValue(types.OptionalValue(types.Uint64Value(400))),
						),
					),
				),
			)
			if err != nil {
				return wrap(err, "failed to create table")
			}

			desc, err := session.DescribeTable(ctx, tablePath, options.WithShardKeyBounds())
			if err != nil {
				return wrap(err, "failed to get table description")
			}
			if len(desc.KeyRanges) != 4 {
				return errors.New("key ranges len is not as expected")
			}

			return nil
		},
	)
	return wrap(err, "failed to execute operation")
}
