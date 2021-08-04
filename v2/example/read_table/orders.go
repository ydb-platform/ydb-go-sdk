package main

import (
	"github.com/yandex-cloud/ydb-go-sdk/v2"
	"github.com/yandex-cloud/ydb-go-sdk/v2/example/internal/cli"
	"github.com/yandex-cloud/ydb-go-sdk/v2/table"
	"context"
	"crypto/tls"
	"flag"
	"fmt"
	"log"
	"path"
)

type Command struct {
	config func(cli.Parameters) *ydb.DriverConfig
	tls    func() *tls.Config
}

func (cmd *Command) ExportFlags(ctx context.Context, flag *flag.FlagSet) {
	cmd.config = cli.ExportDriverConfig(ctx, flag)
	cmd.tls = cli.ExportTLSConfig(flag)
}

func (cmd *Command) Run(ctx context.Context, params cli.Parameters) error {

	driver, sp, err := cmd.prepareTest(ctx, params)
	if err != nil {
		return err
	}
	defer driver.Close()
	defer sp.Close(ctx)

	prefix := path.Join(params.Database, params.Path)
	tableName := "orders"
	fmt.Println("Read whole table, unsorted:")
	err = readTable(ctx, sp, path.Join(
		prefix, tableName,
	))
	if err != nil {
		return fmt.Errorf("read table error: %w", err)
	}

	fmt.Println("Sorted by composite primary key:")
	err = readTable(ctx, sp, path.Join(
		prefix, tableName,
	), table.ReadOrdered())
	if err != nil {
		return fmt.Errorf("read table error: %w", err)
	}

	fmt.Println("Any five rows:")
	err = readTable(ctx, sp, path.Join(
		prefix, tableName,
	), table.ReadRowLimit(5))
	if err != nil {
		return fmt.Errorf("read table error: %w", err)
	}

	fmt.Println("First five rows by PK (ascending) with subset of columns:")
	err = readTable(ctx, sp, path.Join(
		prefix, tableName,
	), table.ReadRowLimit(5), table.ReadColumn("customer_id"),
		table.ReadColumn("order_id"),
		table.ReadColumn("order_date"), table.ReadOrdered())
	if err != nil {
		return fmt.Errorf("read table error: %w", err)
	}

	fmt.Println("Read all rows with first PK component (customer_id,) greater or equal than 2 and less then 3:")
	keyRange := table.KeyRange{
		From: ydb.TupleValue(ydb.OptionalValue(ydb.Uint64Value(2))),
		To:   ydb.TupleValue(ydb.OptionalValue(ydb.Uint64Value(3))),
	}
	err = readTable(ctx, sp, path.Join(
		prefix, tableName,
	), table.ReadKeyRange(keyRange))
	if err != nil {
		return fmt.Errorf("read table error: %w", err)
	}

	fmt.Println("Read all rows with composite PK lexicographically less or equal than (1,4):")
	err = readTable(ctx, sp, path.Join(
		prefix, tableName,
	), table.ReadLessOrEqual(ydb.TupleValue(ydb.OptionalValue(ydb.Uint64Value(1)), ydb.OptionalValue(ydb.Uint64Value(4)))))
	if err != nil {
		return fmt.Errorf("read table error: %w", err)
	}

	fmt.Println("Read all rows with composite PK lexicographically greater or equal than (1,2) and less than (3,4):")
	keyRange = table.KeyRange{
		From: ydb.TupleValue(ydb.OptionalValue(ydb.Uint64Value(1)), ydb.OptionalValue(ydb.Uint64Value(2))),
		To:   ydb.TupleValue(ydb.OptionalValue(ydb.Uint64Value(3)), ydb.OptionalValue(ydb.Uint64Value(1))),
	}
	err = readTable(ctx, sp, path.Join(
		prefix, tableName,
	), table.ReadKeyRange(keyRange))
	if err != nil {
		return fmt.Errorf("read table error: %w", err)
	}

	return nil
}

func readTable(ctx context.Context, sp *table.SessionPool, path string, opts ...table.ReadTableOption) (err error) {
	var res *table.Result

	err = table.Retry(ctx, sp,
		table.OperationFunc(func(ctx context.Context, s *table.Session) (err error) {
			res, err = s.StreamReadTable(ctx, path, opts...)
			return err
		}),
	)
	if err != nil {
		return err
	}

	for res.NextStreamSet(ctx) {
		for res.NextRow() {

			res.SeekItem("customer_id")
			id := res.OUint64()

			res.SeekItem("order_id")
			orderId := res.OUint64()

			res.SeekItem("order_date")
			date := res.ODate()

			if res.ColumnCount() == 4 {
				res.SeekItem("description")
				description := res.OUTF8()
				log.Printf("#  Order, CustomerId: %d, OrderId: %d, Description: %s, Order date: %s", id, orderId, description, intToStringDate(date))
			} else {
				log.Printf("#  Order, CustomerId: %d, OrderId: %d, Order date: %s", id, orderId, intToStringDate(date))
			}
		}
	}
	if err := res.Err(); err != nil {
		return err
	}
	return nil
}
