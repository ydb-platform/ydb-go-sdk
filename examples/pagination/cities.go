package main

import (
	"context"
	"fmt"

	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result/named"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
)

func selectPaging(
	ctx context.Context,
	c table.Client,
	prefix string,
	limit int,
	lastNum *uint,
	lastCity *string,
) (
	empty bool,
	err error,
) {

	var query = fmt.Sprintf(`
		PRAGMA TablePathPrefix("%v");

		DECLARE $limit AS Uint64;
		DECLARE $lastCity AS Text;
		DECLARE $lastNumber AS Uint32;
		
		$part1 = (
			SELECT * FROM schools
			WHERE city = $lastCity AND number > $lastNumber
			ORDER BY city, number LIMIT $limit
		);
		
		$part2 = (
			SELECT * FROM schools
			WHERE city > $lastCity
			ORDER BY city, number LIMIT $limit
		);
		
		$union = (
			SELECT * FROM $part1
			UNION ALL
			SELECT * FROM $part2
		);
		
		SELECT * FROM $union
		ORDER BY city, number LIMIT $limit;
		`, prefix)

	readTx := table.TxControl(table.BeginTx(table.WithOnlineReadOnly()), table.CommitTx())

	err = c.Do(ctx,
		func(ctx context.Context, s table.Session) (err error) {
			_, res, err := s.Execute(ctx, readTx, query,
				table.NewQueryParameters(
					table.ValueParam("$limit", types.Uint64Value(uint64(limit))),
					table.ValueParam("$lastCity", types.TextValue(*lastCity)),
					table.ValueParam("$lastNumber", types.Uint32Value(uint32(*lastNum))),
				),
			)
			if err != nil {
				return err
			}
			defer func() {
				_ = res.Close()
			}()
			if !res.NextResultSet(ctx) || !res.HasNextRow() {
				empty = true
				return
			}
			var addr string
			for res.NextRow() {
				err = res.ScanNamed(
					named.Optional("city", &lastCity),
					named.Optional("number", &lastNum),
					named.OptionalWithDefault("address", &addr),
				)
				if err != nil {
					return err
				}
				fmt.Printf("\t%v, School #%v, Address: %v\n", *lastCity, *lastNum, addr)
			}
			return res.Err()
		},
	)
	if err != nil {
		return
	}
	return empty, nil
}

func fillTableWithData(ctx context.Context, c table.Client, prefix string) (err error) {
	var query = fmt.Sprintf(`
		PRAGMA TablePathPrefix("%v");

		DECLARE $schoolsData AS List<Struct<
			city: Text,
			number: Uint32,
			address: Text>>;

		REPLACE INTO schools
		SELECT
			city,
			number,
			address
		FROM AS_TABLE($schoolsData);`, prefix)

	writeTx := table.TxControl(table.BeginTx(table.WithSerializableReadWrite()), table.CommitTx())

	err = c.Do(ctx,
		func(ctx context.Context, s table.Session) (err error) {
			_, _, err = s.Execute(ctx, writeTx, query, table.NewQueryParameters(
				table.ValueParam("$schoolsData", getSchoolData()),
			))
			return err
		})
	return err
}

func createTable(ctx context.Context, c table.Client, path string) (err error) {
	err = c.Do(ctx,
		func(ctx context.Context, s table.Session) error {
			return s.CreateTable(ctx, path,
				options.WithColumn("city", types.Optional(types.TypeUTF8)),
				options.WithColumn("number", types.Optional(types.TypeUint32)),
				options.WithColumn("address", types.Optional(types.TypeUTF8)),
				options.WithPrimaryKeyColumn("city", "number"),
			)
		},
	)
	if err != nil {
		return err
	}

	return nil
}
