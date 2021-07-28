package main

import (
	"context"
	"crypto/tls"
	"flag"
	"fmt"
	"path"
	"time"

	"github.com/yandex-cloud/ydb-go-sdk"
	"github.com/yandex-cloud/ydb-go-sdk/example/internal/cli"
	"github.com/yandex-cloud/ydb-go-sdk/table"
)

var (
	simpleCreateQuery = `
PRAGMA TablePathPrefix("%s");
CREATE TABLE small_table (
    a Uint64,
    b Uint64,
    c Utf8,
	d Date,
    PRIMARY KEY (a, b)
);
`
	familyCreateQuery = `
PRAGMA TablePathPrefix("%s");
CREATE TABLE small_table2 (
	a Uint64,
    b Uint64,
    c Utf8 FAMILY family_large,
	d Date,
    PRIMARY KEY (a, b),
    FAMILY family_large (
        COMPRESSION = "lz4"
    )
);
`
	settingsCreateQuery = `
PRAGMA TablePathPrefix("%s");
CREATE TABLE small_table3 (
	a Uint64,
    b Uint64,
    c Utf8,
	d Date,
    PRIMARY KEY (a, b)
)
WITH (
    AUTO_PARTITIONING_BY_SIZE = ENABLED, --Automatic positioning mode by the size of the partition
    AUTO_PARTITIONING_PARTITION_SIZE_MB = 512, --Preferred size of each partition in megabytes
	AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 32 --The minimum number of partitions when the automatic merging of partitions stops working
);
`
	dropQuery = `
PRAGMA TablePathPrefix("%s");
DROP  TABLE small_table;
DROP  TABLE small_table2;
DROP  TABLE small_table3;
`
	alterQuery = `
PRAGMA TablePathPrefix("%s");
ALTER TABLE small_table ADD COLUMN e Uint64, DROP COLUMN c;
`
	alterSettingsQuery = `
PRAGMA TablePathPrefix("%s");
ALTER TABLE small_table2 SET (AUTO_PARTITIONING_BY_SIZE = DISABLED);
`
	alterTTLQuery = `
PRAGMA TablePathPrefix("%s");
ALTER TABLE small_table3 SET (TTL = Interval("PT3H") ON d);
`
)

type Command struct {
	config func(cli.Parameters) *ydb.DriverConfig
	tls    func() *tls.Config
}

func (cmd *Command) ExportFlags(ctx context.Context, flag *flag.FlagSet) {
	cmd.config = cli.ExportDriverConfig(ctx, flag)
	cmd.tls = cli.ExportTLSConfig(flag)
}

func executeQuery(ctx context.Context, sp *table.SessionPool, prefix string, query string) (err error) {

	err = table.Retry(ctx, sp,
		table.OperationFunc(func(ctx context.Context, s *table.Session) error {
			err := s.ExecuteSchemeQuery(ctx, fmt.Sprintf(query, prefix))
			return err
		}),
	)
	if err != nil {
		return err
	}
	return nil
}

func (cmd *Command) Run(ctx context.Context, params cli.Parameters) error {

	dialer := &ydb.Dialer{
		DriverConfig: cmd.config(params),
		TLSConfig:    cmd.tls(),
		Timeout:      time.Second,
	}
	driver, err := dialer.Dial(ctx, params.Endpoint)
	if err != nil {
		return fmt.Errorf("dial error: %w", err)
	}
	defer driver.Close()
	tableClient := table.Client{
		Driver:            driver,
		MaxQueryCacheSize: -1,
	}
	sp := table.SessionPool{
		IdleThreshold: time.Second,
		Builder:       &tableClient,
	}
	defer sp.Close(ctx)
	prefix := path.Join(params.Database, params.Path)

	//simple creation with composite primary key
	err = executeQuery(ctx, &sp, prefix, simpleCreateQuery)
	if err != nil {
		return err
	}

	//creation with column family
	err = executeQuery(ctx, &sp, prefix, familyCreateQuery)
	if err != nil {
		return err
	}

	//creation with table settings
	err = executeQuery(ctx, &sp, prefix, settingsCreateQuery)
	if err != nil {
		return err
	}

	//add column and drop column.
	err = executeQuery(ctx, &sp, prefix, alterQuery)
	if err != nil {
		return err
	}

	//change AUTO_PARTITIONING_BY_SIZE setting.
	err = executeQuery(ctx, &sp, prefix, alterSettingsQuery)
	if err != nil {
		return err
	}

	//add TTL. Clear the old data after the three-hour interval has expired.
	err = executeQuery(ctx, &sp, prefix, alterTTLQuery)
	if err != nil {
		return err
	}

	//drop tables small_table,small_table2,small_table3.
	err = executeQuery(ctx, &sp, prefix, dropQuery)
	if err != nil {
		return err
	}

	return nil
}
