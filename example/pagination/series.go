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
	"github.com/yandex-cloud/ydb-go-sdk/example/internal/ydbutil"
	"github.com/yandex-cloud/ydb-go-sdk/table"
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
	dialer := &ydb.Dialer{
		DriverConfig: cmd.config(params),
		TLSConfig:    cmd.tls(),
		Timeout:      time.Second,
	}
	driver, err := dialer.Dial(ctx, params.Endpoint)
	if err != nil {
		return fmt.Errorf("dial error: %v", err)
	}

	tableClient := table.Client{
		Driver: driver,
	}
	sp := table.SessionPool{
		IdleThreshold: time.Second,
		Builder:       &tableClient,
	}
	defer sp.Close(ctx)

	err = ydbutil.CleanupDatabase(ctx, driver, &sp, params.Database, "schools")
	if err != nil {
		return err
	}
	err = ydbutil.EnsurePathExists(ctx, driver, params.Database, params.Path)
	if err != nil {
		return err
	}

	prefix := path.Join(params.Database, params.Path)

	err = createTable(ctx, &sp, path.Join(prefix, "schools"))
	if err != nil {
		return fmt.Errorf("create tables error: %v", err)
	}

	err = fillTableWithData(ctx, &sp, prefix)
	if err != nil {
		return fmt.Errorf("fill tables with data error: %v", err)
	}

	lastNum := 0
	lastCity := ""
	limit := 3
	maxPages := 10
	for i, empty := 0, false; i < maxPages && !empty; i++ {
		fmt.Printf("> Page %v:\n", i+1)
		empty, err = selectPaging(ctx, &sp, prefix, limit, &lastNum, &lastCity)
		if err != nil {
			return fmt.Errorf("get page %v error: %v", i, err)
		}
	}

	return nil
}

func selectPaging(
	ctx context.Context, sp *table.SessionPool, prefix string, limit int, lastNum *int, lastCity *string) (
	empty bool, err error) {

	var query = fmt.Sprintf(`
		PRAGMA TablePathPrefix("%v");

		DECLARE $limit AS Uint64;
		DECLARE $lastCity AS Utf8;
		DECLARE $lastNumber AS Uint32;

		$Data = (
			SELECT * FROM schools
			WHERE city = $lastCity AND number > $lastNumber
			ORDER BY city, number LIMIT $limit

			UNION ALL

			SELECT * FROM schools
			WHERE city > $lastCity
			ORDER BY city, number LIMIT $limit
		);
		SELECT * FROM $Data ORDER BY city, number LIMIT $limit;`, prefix)

	readTx := table.TxControl(table.BeginTx(table.WithOnlineReadOnly()), table.CommitTx())

	var res *table.Result
	err = table.Retry(ctx, sp,
		table.OperationFunc(func(ctx context.Context, s *table.Session) (err error) {
			_, res, err = s.Execute(ctx, readTx, query,
				table.NewQueryParameters(
					table.ValueParam("$limit", ydb.Uint64Value(uint64(limit))),
					table.ValueParam("$lastCity", ydb.UTF8Value(*lastCity)),
					table.ValueParam("$lastNumber", ydb.Uint32Value(uint32(*lastNum))),
				),
			)
			return
		}),
	)
	if err != nil {
		return
	}
	if err = res.Err(); err != nil {
		return
	}
	if !res.NextSet() || !res.HasNextRow() {
		empty = true
		return
	}

	for res.NextRow() {
		res.SeekItem("city")
		*lastCity = res.OUTF8()

		res.SeekItem("number")
		*lastNum = int(res.OUint32())

		res.SeekItem("address")
		addr := res.OUTF8()

		fmt.Printf("\t%v, School #%v, Address: %v\n", *lastCity, *lastNum, addr)
	}
	return
}

func fillTableWithData(ctx context.Context, sp *table.SessionPool, prefix string) (err error) {
	var query = fmt.Sprintf(`
		PRAGMA TablePathPrefix("%v");

		DECLARE $schoolsData AS "List<Struct<
			city: Utf8,
			number: Uint32,
			address: Utf8>>";

		REPLACE INTO schools
		SELECT
			city,
			number,
			address
		FROM AS_TABLE($schoolsData);`, prefix)

	writeTx := table.TxControl(table.BeginTx(table.WithSerializableReadWrite()), table.CommitTx())

	return table.Retry(ctx, sp,
		table.OperationFunc(func(ctx context.Context, s *table.Session) (err error) {
			_, _, err = s.Execute(ctx, writeTx, query, table.NewQueryParameters(
				table.ValueParam("$schoolsData", getSchoolData()),
			))
			return err
		}),
	)
}

func createTable(ctx context.Context, sp *table.SessionPool, path string) (err error) {
	err = table.Retry(ctx, sp,
		table.OperationFunc(func(ctx context.Context, s *table.Session) error {
			return s.CreateTable(ctx, path,
				table.WithColumn("city", ydb.Optional(ydb.TypeUTF8)),
				table.WithColumn("number", ydb.Optional(ydb.TypeUint32)),
				table.WithColumn("address", ydb.Optional(ydb.TypeUTF8)),
				table.WithPrimaryKeyColumn("city", "number"),
			)
		}),
	)
	if err != nil {
		return err
	}

	return nil
}
