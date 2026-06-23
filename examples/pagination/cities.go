package main

import (
	"context"
	"fmt"

	ydb "github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
)

func selectPaging(
	ctx context.Context,
	c query.Client,
	prefix string,
	limit int,
	lastNum *uint,
	lastCity *string,
) (
	empty bool,
	err error,
) {
	sql := fmt.Sprintf(`
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

	err = c.Do(ctx,
		func(ctx context.Context, s query.Session) (err error) {
			rs, err := s.QueryResultSet(ctx, sql,
				query.WithTxControl(query.OnlineReadOnlyTxControl()),
				query.WithParameters(
					ydb.ParamsBuilder().
						Param("$limit").Any(types.Uint64Value(uint64(limit))).
						Param("$lastCity").Any(types.TextValue(*lastCity)).
						Param("$lastNumber").Any(types.Uint32Value(uint32(*lastNum))).
						Build(),
				),
			)
			if err != nil {
				return err
			}
			defer func() {
				_ = rs.Close(ctx)
			}()

			hasRows := false
			for row, err := range rs.Rows(ctx) {
				if err != nil {
					return err
				}
				hasRows = true
				var (
					city    *string
					number  *uint32
					address string
				)
				err = row.ScanNamed(
					query.Named("city", &city),
					query.Named("number", &number),
					query.Named("address", &address),
				)
				if err != nil {
					return err
				}
				*lastCity = *city
				*lastNum = uint(*number)
				fmt.Printf("\t%v, School #%v, Address: %v\n", *lastCity, *lastNum, address)
			}

			if !hasRows {
				empty = true
			}

			return nil
		},
	)

	return empty, err
}

func fillTableWithData(ctx context.Context, c query.Client, prefix string) (err error) {
	sql := fmt.Sprintf(`
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

	err = c.Do(ctx,
		func(ctx context.Context, s query.Session) (err error) {
			return s.Exec(ctx, sql,
				query.WithTxControl(query.SerializableReadWriteTxControl(query.CommitTx())),
				query.WithParameters(
					ydb.ParamsBuilder().
						Param("$schoolsData").Any(getSchoolData()).
						Build(),
				),
			)
		})

	return err
}

func createTable(ctx context.Context, c query.Client, tablePath string) (err error) {
	return c.Exec(ctx,
		fmt.Sprintf(`
			CREATE TABLE IF NOT EXISTS `+"`%s`"+` (
				city Text,
				number Uint32,
				address Text,
				PRIMARY KEY (city, number)
			)`, tablePath),
		query.WithTxControl(query.ImplicitTxControl()),
	)
}
