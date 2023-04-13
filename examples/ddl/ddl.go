package main

import (
	"context"
	"fmt"

	"github.com/ydb-platform/ydb-go-sdk/v3/table"
)

//nolint:lll
var (
	simpleCreateQuery = `
PRAGMA TablePathPrefix("%s");
CREATE TABLE small_table (
    a Uint64,
    b Uint64,
    c Text,
	d Date,
    PRIMARY KEY (a, b)
);
`
	familyCreateQuery = `
PRAGMA TablePathPrefix("%s");
CREATE TABLE small_table2 (
	a Uint64,
    b Uint64,
    c Text FAMILY family_large,
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
    c Text,
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

func executeQuery(ctx context.Context, c table.Client, prefix string, query string) (err error) {
	err = c.Do(ctx,
		func(ctx context.Context, s table.Session) error {
			err = s.ExecuteSchemeQuery(ctx, fmt.Sprintf(query, prefix))
			return err
		},
	)
	if err != nil {
		return err
	}
	return nil
}
