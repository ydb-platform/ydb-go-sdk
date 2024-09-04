package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"path"
	"time"

	ydb "github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
)

func read(ctx context.Context, c query.Client, prefix string) error {
	return c.Do(ctx,
		func(ctx context.Context, s query.Session) (err error) {
			result, err := s.Query(ctx, fmt.Sprintf(`
					SELECT
						series_id,
						title,
						release_date
					FROM
						%s
				`, "`"+path.Join(prefix, "series")+"`"),
				query.WithTxControl(query.TxControl(query.BeginTx(query.WithSnapshotReadOnly()))),
			)
			if err != nil {
				return err
			}

			defer func() {
				_ = result.Close(ctx)
			}()

			for {
				resultSet, err := result.NextResultSet(ctx)
				if err != nil {
					if errors.Is(err, io.EOF) {
						break
					}

					return err
				}
				for {
					row, err := resultSet.NextRow(ctx)
					if err != nil {
						if errors.Is(err, io.EOF) {
							break
						}

						return err
					}

					var info struct {
						SeriesID    string    `sql:"series_id"`
						Title       string    `sql:"title"`
						ReleaseDate time.Time `sql:"release_date"`
					}
					err = row.ScanStruct(&info)
					if err != nil {
						return err
					}
					log.Printf("%+v", info)
				}
			}

			return nil
		},
	)
}

func fillTablesWithData(ctx context.Context, c query.Client, prefix string) error {
	series, seasons, episodes := getData()

	err := c.Exec(ctx, fmt.Sprintf(`
		DECLARE $seriesData AS List<Struct<
			series_id: Bytes,
			title: Text,
			series_info: Text,
			release_date: Date,
			comment: Optional<Text>>>;
		
		REPLACE INTO %s
		SELECT
			series_id,
			title,
			series_info,
			release_date,
			comment
		FROM AS_TABLE($seriesData);`,
		"`"+path.Join(prefix, "series")+"`"),
		query.WithParameters(ydb.ParamsBuilder().
			Param("$seriesData").
			BeginList().AddItems(series...).EndList().
			Build(),
		),
	)
	if err != nil {
		return err
	}

	err = c.Exec(ctx, fmt.Sprintf(`
		DECLARE $seasonsData AS List<Struct<
			series_id: Bytes,
			season_id: Bytes,
			title: Text,
			first_aired: Date,
			last_aired: Date>>;

		REPLACE INTO %s
		SELECT
			series_id,
			season_id,
			title,
			first_aired,
			last_aired
		FROM AS_TABLE($seasonsData);`,
		"`"+path.Join(prefix, "seasons")+"`"),
		query.WithParameters(ydb.ParamsBuilder().
			Param("$seasonsData").
			BeginList().AddItems(seasons...).EndList().
			Build(),
		),
	)
	if err != nil {
		return err
	}

	err = c.Exec(ctx, fmt.Sprintf(`
		DECLARE $episodesData AS List<Struct<
			series_id: Bytes,
			season_id: Bytes,
			episode_id: Bytes,
			title: Text,
			air_date: Date>>;

		REPLACE INTO %s
		SELECT
			series_id,
			season_id,
			episode_id,
			title,
			air_date
		FROM AS_TABLE($episodesData);`,
		"`"+path.Join(prefix, "episodes")+"`"),
		query.WithParameters(ydb.ParamsBuilder().
			Param("$episodesData").
			BeginList().AddItems(episodes...).EndList().
			Build(),
		),
	)
	if err != nil {
		return err
	}

	return nil
}

func createTables(ctx context.Context, c query.Client, prefix string) error {
	err := c.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			series_id Bytes,
			title Text,
			series_info Text,
			release_date Date,
			comment Text,
			
			PRIMARY KEY(series_id)
		)`, "`"+path.Join(prefix, "series")+"`"),
		query.WithTxControl(query.NoTx()),
	)
	if err != nil {
		return err
	}

	err = c.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			series_id Bytes,
			season_id Bytes,
			title Text,
			first_aired Date,
			last_aired Date,
			
			PRIMARY KEY(series_id,season_id)
		)`, "`"+path.Join(prefix, "seasons")+"`"),
		query.WithTxControl(query.NoTx()),
	)
	if err != nil {
		return err
	}

	err = c.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS  %s (
			series_id Bytes,
			season_id Bytes,
			episode_id Bytes,
			title Text,
			air_date Date,
			
			PRIMARY KEY(series_id,season_id,episode_id)
		)`, "`"+path.Join(prefix, "episodes")+"`"),
		query.WithTxControl(query.NoTx()),
	)
	if err != nil {
		return err
	}

	return nil
}
