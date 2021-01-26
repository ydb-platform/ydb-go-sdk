package main

import (
	"context"
	"crypto/tls"
	"errors"
	"flag"
	"fmt"
	"log"
	"path"
	"time"

	"github.com/yandex-cloud/ydb-go-sdk/example/internal/ydbutil"

	"github.com/yandex-cloud/ydb-go-sdk"
	"github.com/yandex-cloud/ydb-go-sdk/example/internal/cli"
	table "github.com/yandex-cloud/ydb-go-sdk/table"
)

type Command struct {
	config func(cli.Parameters) *ydb.DriverConfig
	tls    func() *tls.Config
	table  string
}

func wrap(err error, explanation string) error {
	if err != nil {
		return fmt.Errorf("%s: %w", explanation, err)
	}
	return err
}

func (cmd *Command) testUniformPartitions(ctx context.Context, sp table.SessionProvider) error {
	log.Printf("Create table: %v\n", cmd.table)

	return wrap(table.Retry(ctx, sp, table.OperationFunc(func(ctx context.Context, session *table.Session) error {
		err := session.CreateTable(ctx, cmd.table,
			table.WithColumn("key", ydb.Optional(ydb.TypeUint64)),
			table.WithColumn("value", ydb.Optional(ydb.TypeJSON)),
			table.WithPrimaryKeyColumn("key"),

			table.WithProfile(
				table.WithPartitioningPolicy(
					table.WithPartitioningPolicyMode(table.PartitioningAutoSplitMerge),
					table.WithPartitioningPolicyUniformPartitions(4),
				),
			),
		)
		if err != nil {
			return wrap(err, "failed to create table")
		}

		desc, err := session.DescribeTable(ctx, cmd.table, table.WithShardKeyBounds())
		if err != nil {
			return wrap(err, "failed to get table description")
		}
		if len(desc.KeyRanges) != 4 {
			return errors.New("key ranges len is not as expected")
		}

		return nil
	})), "failed to execute operation")
}

func (cmd *Command) testExplicitPartitions(ctx context.Context, sp table.SessionProvider) error {
	log.Printf("Create table: %v\n", cmd.table)

	return wrap(table.Retry(ctx, sp, table.OperationFunc(func(ctx context.Context, session *table.Session) error {
		err := session.CreateTable(ctx, cmd.table,
			table.WithColumn("key", ydb.Optional(ydb.TypeUint64)),
			table.WithColumn("value", ydb.Optional(ydb.TypeJSON)),
			table.WithPrimaryKeyColumn("key"),

			table.WithProfile(
				table.WithPartitioningPolicy(
					table.WithPartitioningPolicyExplicitPartitions(
						ydb.TupleValue(ydb.OptionalValue(ydb.Uint64Value(100))),
						ydb.TupleValue(ydb.OptionalValue(ydb.Uint64Value(300))),
						ydb.TupleValue(ydb.OptionalValue(ydb.Uint64Value(400))),
					),
				),
			),
		)
		if err != nil {
			return wrap(err, "failed to create table")
		}

		desc, err := session.DescribeTable(ctx, cmd.table, table.WithShardKeyBounds())
		if err != nil {
			return wrap(err, "failed to get table description")
		}
		if len(desc.KeyRanges) != 4 {
			return errors.New("key ranges len is not as expected")
		}

		return nil
	})), "failed to execute operation")
}

func (cmd *Command) Run(ctx context.Context, params cli.Parameters) error {
	dialer := &ydb.Dialer{
		DriverConfig: cmd.config(params),
		TLSConfig:    cmd.tls(),
		Timeout:      time.Second,
	}
	driver, err := dialer.Dial(ctx, params.Endpoint)
	prefix := path.Join(params.Database, params.Path)
	tableName := cmd.table
	cmd.table = path.Join(prefix, cmd.table)

	if err != nil {
		return err // handle error
	}
	tc := table.Client{
		Driver: driver,
	}
	sp := &table.SessionPool{
		IdleThreshold: time.Second,
		Builder:       &tc,
	}
	defer sp.Close(ctx)

	err = ydbutil.CleanupDatabase(ctx, driver, sp, params.Database, tableName)
	if err != nil {
		return err
	}
	err = ydbutil.EnsurePathExists(ctx, driver, params.Database, params.Path)
	if err != nil {
		return err
	}

	if err := cmd.testUniformPartitions(ctx, sp); err != nil {
		return wrap(err, "failed to test uniform partitions")
	}

	err = ydbutil.CleanupDatabase(ctx, driver, sp, params.Database, tableName)
	if err != nil {
		return err
	}

	if err := cmd.testExplicitPartitions(ctx, sp); err != nil {
		return wrap(err, "failed to test explicit partitions")
	}

	return nil
}

func (cmd *Command) ExportFlags(ctx context.Context, flag *flag.FlagSet) {
	cmd.config = cli.ExportDriverConfig(ctx, flag)
	cmd.tls = cli.ExportTLSConfig(flag)
	flag.StringVar(&cmd.table, "table", "explicit_partitions_example", "Path for table")
}
