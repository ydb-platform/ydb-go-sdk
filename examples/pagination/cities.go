package main

import (
	"context"
	"fmt"

	ydb "github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
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

	var messages []string
	err = c.Do(ctx,
		func(ctx context.Context, s query.Session) (err error) {
			res, err := s.Query(ctx, sql, query.WithParameters(ydb.ParamsBuilder().
				Param("$limit").Uint64(uint64(limit)).
				Param("$lastCity").Text(*lastCity).
				Param("$lastNumber").Uint32(uint32(*lastNum)).
				Build()))
			if err != nil {
				return err
			}
			defer func() {
				_ = res.Close(ctx)
			}()
			attemptEmpty := true
			attemptLastNum := *lastNum
			attemptLastCity := *lastCity
			var attemptMessages []string
			for resultSet, err := range res.ResultSets(ctx) {
				if err != nil {
					return err
				}
				for row, err := range resultSet.Rows(ctx) {
					if err != nil {
						return err
					}
					var (
						city    string
						number  uint32
						address string
					)
					if err = row.ScanNamed(
						query.Named("city", &city),
						query.Named("number", &number),
						query.Named("address", &address),
					); err != nil {
						return err
					}
					attemptEmpty = false
					attemptLastCity = city
					attemptLastNum = uint(number)
					attemptMessages = append(attemptMessages, fmt.Sprintf(
						"\t%v, School #%v, Address: %v\n", city, number, address,
					))
				}
			}
			empty = attemptEmpty
			*lastCity = attemptLastCity
			*lastNum = attemptLastNum
			messages = attemptMessages

			return nil
		},
		query.WithIdempotent(),
	)
	for _, message := range messages {
		fmt.Print(message)
	}

	return empty, err
}

func fillTableWithData(ctx context.Context, c query.Client, prefix string) error {
	sql := fmt.Sprintf(`
		PRAGMA TablePathPrefix("%v");

		REPLACE INTO schools
		SELECT
			city,
			number,
			address
		FROM AS_TABLE($schoolsData);`, prefix)

	return c.Exec(ctx, sql, query.WithParameters(ydb.ParamsBuilder().
		Param("$schoolsData").Any(getSchoolData()).
		Build()), query.WithIdempotent())
}

func createTable(ctx context.Context, c query.Client, tablePath string) error {
	return c.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			city Text,
			number Uint32,
			address Text,
			PRIMARY KEY (city, number)
		)`, "`"+tablePath+"`"), query.WithIdempotent())
}
