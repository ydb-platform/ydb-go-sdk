package main

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"path"
	"text/template"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/retry"
	"github.com/ydb-platform/ydb-go-sdk/v3/sugar"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
)

func render(t *template.Template, data interface{}) string {
	var buf bytes.Buffer
	err := t.Execute(&buf, data)
	if err != nil {
		panic(err)
	}
	return buf.String()
}

func sliceToInterfaces[T any](v []T) []interface{} {
	ii := make([]interface{}, len(v))
	for i, vv := range v {
		ii[i] = vv
	}
	return ii
}

func selectDefault(ctx context.Context, db *sql.DB, prefix string) (err error) {
	// explain of query
	err = retry.Do(ctx, db, func(ctx context.Context, cc *sql.Conn) (err error) {
		row := cc.QueryRowContext(ydb.WithQueryMode(ctx, ydb.ExplainQueryMode),
			render(
				template.Must(template.New("").Parse(`
					PRAGMA TablePathPrefix("{{ .TablePathPrefix }}");

					SELECT series_id, title, release_date FROM series;
				`)), struct {
					TablePathPrefix string
				}{
					TablePathPrefix: prefix,
				},
			),
		)
		var (
			ast  string
			plan string
		)
		if err = row.Scan(&ast, &plan); err != nil {
			return err
		}
		//log.Printf("AST = %s\n\nPlan = %s", ast, plan)
		return nil
	}, retry.WithDoRetryOptions(retry.WithIdempotent(true)))
	if err != nil {
		return fmt.Errorf("explain query failed: %w", err)
	}
	err = retry.Do(ydb.WithTxControl(ctx, table.OnlineReadOnlyTxControl()), db, func(ctx context.Context, cc *sql.Conn) (err error) {
		rows, err := cc.QueryContext(ctx,
			render(
				template.Must(template.New("").Parse(`
					PRAGMA TablePathPrefix("{{ .TablePathPrefix }}");

					SELECT series_id, title, release_date FROM series;
				`)), struct {
					TablePathPrefix string
				}{
					TablePathPrefix: prefix,
				},
			),
		)
		if err != nil {
			return err
		}
		defer func() {
			_ = rows.Close()
		}()
		var (
			id          *string
			title       *string
			releaseDate *time.Time
		)
		log.Println("> select of all known series:")
		for rows.Next() {
			if err = rows.Scan(&id, &title, &releaseDate); err != nil {
				return err
			}
			log.Printf(
				"> [%s] %s (%s)",
				*id, *title, releaseDate.Format("2006-01-02"),
			)
		}
		return rows.Err()
	}, retry.WithDoRetryOptions(retry.WithIdempotent(true)))
	if err != nil {
		return fmt.Errorf("execute data query failed: %w", err)
	}
	return nil
}

func selectScan(ctx context.Context, db *sql.DB, prefix string) (err error) {
	// scan query
	err = retry.Do(ydb.WithTxControl(ctx, table.StaleReadOnlyTxControl()), db, func(ctx context.Context, cc *sql.Conn) (err error) {
		var (
			id        string
			seriesIDs []types.Value
			seasonIDs []types.Value
		)
		// getting series ID's
		row := cc.QueryRowContext(ydb.WithQueryMode(ctx, ydb.ScanQueryMode),
			render(
				template.Must(template.New("").Parse(`
					PRAGMA TablePathPrefix("{{ .TablePathPrefix }}");

					DECLARE $seriesTitle AS Text;
	
					SELECT 			series_id 		
					FROM 			series
					WHERE 			title LIKE $seriesTitle;
				`)), struct {
					TablePathPrefix string
				}{
					TablePathPrefix: prefix,
				},
			),
			table.NewQueryParameters( // supports native ydb-go-sdk query parameters as arg
				table.ValueParam("$seriesTitle", types.TextValue("%IT Crowd%")),
			),
		)
		if err = row.Scan(&id); err != nil {
			return err
		}
		seriesIDs = append(seriesIDs, types.BytesValueFromString(id))
		if err = row.Err(); err != nil {
			return err
		}

		// getting season ID's
		rows, err := cc.QueryContext(ydb.WithQueryMode(ctx, ydb.ScanQueryMode),
			render(
				template.Must(template.New("").Parse(`
					PRAGMA TablePathPrefix("{{ .TablePathPrefix }}");

					DECLARE $seasonTitle AS Text;
			
					SELECT 			season_id 		
					FROM 			seasons
					WHERE 			title LIKE $seasonTitle
				`)), struct {
					TablePathPrefix string
				}{
					TablePathPrefix: prefix,
				},
			),
			sql.Named("seasonTitle", "%Season 1%"),
		)
		if err != nil {
			return err
		}
		for rows.Next() {
			if err = rows.Scan(&id); err != nil {
				return err
			}
			seasonIDs = append(seasonIDs, types.BytesValueFromString(id))
		}
		if err = rows.Err(); err != nil {
			return err
		}
		_ = rows.Close()

		// getting final query result
		params := table.NewQueryParameters(
			table.ValueParam("seriesIDs", types.ListValue(seriesIDs...)),
			table.ValueParam("seasonIDs", types.ListValue(seasonIDs...)),
			table.ValueParam("from", types.DateValueFromTime(date("2006-01-01"))),
			table.ValueParam("to", types.DateValueFromTime(date("2006-12-31"))),
		)
		declares, err := sugar.GenerateDeclareSection(params)
		if err != nil {
			return err
		}
		rows, err = cc.QueryContext(ydb.WithQueryMode(ctx, ydb.ScanQueryMode),
			render(
				template.Must(template.New("").Parse(`
					PRAGMA TablePathPrefix("{{ .TablePathPrefix }}");
					PRAGMA AnsiInForEmptyOrNullableItemsCollections;

					{{ .Declares }}

					SELECT 
						episode_id, title, air_date FROM episodes
					WHERE 	
						series_id IN $seriesIDs 
						AND season_id IN $seasonIDs 
						AND air_date BETWEEN $from AND $to;
				`)), struct {
					TablePathPrefix string
					Declares        string
				}{
					TablePathPrefix: prefix,
					Declares:        declares,
				},
			),
			params,
		)
		if err != nil {
			return err
		}
		defer func() {
			_ = rows.Close()
		}()
		var (
			episodeID  string
			title      string
			firstAired time.Time
		)
		log.Println("> scan select of episodes of `Season 1` of `IT Crowd` between 2006-01-01 and 2006-12-31:")
		for rows.Next() {
			if err = rows.Scan(&episodeID, &title, &firstAired); err != nil {
				return err
			}
			log.Printf(
				"> [%s] %s (%s)",
				episodeID, title, firstAired.Format("2006-01-02"),
			)
		}
		return rows.Err()
	}, retry.WithDoRetryOptions(retry.WithIdempotent(true)))
	if err != nil {
		return fmt.Errorf("scan query failed: %w", err)
	}
	return nil
}

func fillTablesWithData(ctx context.Context, db *sql.DB, prefix string) (err error) {
	series, seasonsData, episodesData := getData()
	args := []sql.NamedArg{
		sql.Named("seriesData", types.ListValue(series...)),
		sql.Named("seasonsData", types.ListValue(seasonsData...)),
		sql.Named("episodesData", types.ListValue(episodesData...)),
	}
	declares, err := sugar.GenerateDeclareSection(args)
	if err != nil {
		return err
	}
	err = retry.DoTx(ctx, db, func(ctx context.Context, tx *sql.Tx) error {
		if _, err = tx.ExecContext(ctx,
			render(
				template.Must(template.New("").Parse(`
					PRAGMA TablePathPrefix("{{ .TablePathPrefix }}");

					{{ .Declares }}
				
					REPLACE INTO series
					SELECT
						series_id,
						title,
						series_info,
						release_date,
						comment
					FROM AS_TABLE($seriesData);
						
					REPLACE INTO seasons
					SELECT
						series_id,
						season_id,
						title,
						first_aired,
						last_aired
					FROM AS_TABLE($seasonsData);
						
					REPLACE INTO episodes
					SELECT
						series_id,
						season_id,
						episode_id,
						title,
						air_date
					FROM AS_TABLE($episodesData);
				`)), struct {
					TablePathPrefix string
					Declares        string
				}{
					TablePathPrefix: prefix,
					Declares:        declares,
				},
			),
			sliceToInterfaces(args)...,
		); err != nil {
			return err
		}
		return nil
	}, retry.WithDoTxRetryOptions(retry.WithIdempotent(true)))
	if err != nil {
		return fmt.Errorf("upsert query failed: %w", err)
	}
	return nil
}

func prepareSchema(ctx context.Context, db *sql.DB, prefix string) (err error) {
	err = retry.Do(ctx, db, func(ctx context.Context, cc *sql.Conn) error {
		_, err = cc.ExecContext(
			ydb.WithQueryMode(ctx, ydb.SchemeQueryMode),
			fmt.Sprintf("DROP TABLE `%s`", path.Join(prefix, "series")),
		)
		if err != nil {
			_, _ = fmt.Fprintf(os.Stdout, "warn: drop series table failed: %v", err)
		}
		_, err = cc.ExecContext(
			ydb.WithQueryMode(ctx, ydb.SchemeQueryMode),
			render(
				template.Must(template.New("").Parse(`
					PRAGMA TablePathPrefix("{{ .TablePathPrefix }}");

					CREATE TABLE series (
						series_id Bytes,
						title Text,
						series_info Text,
						release_date Date,
						comment Text,
						INDEX index_series_title GLOBAL ASYNC ON ( title ),
						PRIMARY KEY (
							series_id
						)
					) WITH (
						AUTO_PARTITIONING_BY_LOAD = ENABLED
					);
				`)), struct {
					TablePathPrefix string
				}{
					TablePathPrefix: prefix,
				},
			),
		)
		if err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "create series table failed: %v", err)
			return err
		}
		return nil
	}, retry.WithDoRetryOptions(retry.WithIdempotent(true)))
	if err != nil {
		return fmt.Errorf("create table failed: %w", err)
	}
	err = retry.Do(ctx, db, func(ctx context.Context, cc *sql.Conn) error {
		_, err = cc.ExecContext(
			ydb.WithQueryMode(ctx, ydb.SchemeQueryMode),
			fmt.Sprintf("DROP TABLE `%s`", path.Join(prefix, "seasons")),
		)
		if err != nil {
			_, _ = fmt.Fprintf(os.Stdout, "warn: drop seasons table failed: %v", err)
		}
		_, err = cc.ExecContext(
			ydb.WithQueryMode(ctx, ydb.SchemeQueryMode),
			render(
				template.Must(template.New("").Parse(`
					PRAGMA TablePathPrefix("{{ .TablePathPrefix }}");
			
					CREATE TABLE seasons (
						series_id Bytes,
						season_id Bytes,
						title Text,
						first_aired Date,
						last_aired Date,
						INDEX index_seasons_title GLOBAL ASYNC ON ( title ),
						INDEX index_seasons_first_aired GLOBAL ASYNC ON ( first_aired ),
						PRIMARY KEY (
							series_id,
							season_id
						)
					) WITH (
						AUTO_PARTITIONING_BY_LOAD = ENABLED
					);
				`)), struct {
					TablePathPrefix string
				}{
					TablePathPrefix: prefix,
				},
			),
		)
		if err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "create seasons table failed: %v", err)
			return err
		}
		return nil
	}, retry.WithDoRetryOptions(retry.WithIdempotent(true)))
	if err != nil {
		return fmt.Errorf("create table failed: %w", err)
	}
	err = retry.Do(ctx, db, func(ctx context.Context, cc *sql.Conn) error {
		_, err = cc.ExecContext(
			ydb.WithQueryMode(ctx, ydb.SchemeQueryMode),
			fmt.Sprintf("DROP TABLE `%s`", path.Join(prefix, "episodes")),
		)
		if err != nil {
			_, _ = fmt.Fprintf(os.Stdout, "warn: drop episodes table failed: %v", err)
		}
		_, err = cc.ExecContext(
			ydb.WithQueryMode(ctx, ydb.SchemeQueryMode),
			render(
				template.Must(template.New("").Parse(`
					PRAGMA TablePathPrefix("{{ .TablePathPrefix }}");
			
					CREATE TABLE episodes (
						series_id Bytes,
						season_id Bytes,
						episode_id Bytes,
						title Text,
						air_date Date,
						views Uint64,
						INDEX index_episodes_air_date GLOBAL ASYNC ON ( air_date ),
						PRIMARY KEY (
							series_id,
							season_id,
							episode_id
						)
					) WITH (
						AUTO_PARTITIONING_BY_LOAD = ENABLED
					);
				`)), struct {
					TablePathPrefix string
				}{
					TablePathPrefix: prefix,
				},
			),
		)
		if err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "create episodes table failed: %v", err)
			return err
		}
		return nil
	}, retry.WithDoRetryOptions(retry.WithIdempotent(true)))
	if err != nil {
		return fmt.Errorf("create table failed: %w", err)
	}
	return nil
}
