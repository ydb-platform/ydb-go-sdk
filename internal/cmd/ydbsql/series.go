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

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
)

type templateConfig struct {
	TablePathPrefix string
}

var fill = template.Must(template.New("fill database").Parse(`
PRAGMA TablePathPrefix("{{ .TablePathPrefix }}");

DECLARE $seriesData AS List<Struct<
	series_id: Uint64,
	title: Utf8,
	series_info: Utf8,
	release_date: Date,
	comment: Optional<Utf8>>>;

DECLARE $seasonsData AS List<Struct<
	series_id: Uint64,
	season_id: Uint64,
	title: Utf8,
	first_aired: Date,
	last_aired: Date>>;

DECLARE $episodesData AS List<Struct<
	series_id: Uint64,
	season_id: Uint64,
	episode_id: Uint64,
	title: Utf8,
	air_date: Date>>;

REPLACE INTO series
SELECT
	series_id,
	title,
	series_info,
	CAST(release_date AS Uint64) AS release_date,
	comment
FROM AS_TABLE($seriesData);

REPLACE INTO seasons
SELECT
	series_id,
	season_id,
	title,
	CAST(first_aired AS Uint64) AS first_aired,
	CAST(last_aired AS Uint64) AS last_aired
FROM AS_TABLE($seasonsData);

REPLACE INTO episodes
SELECT
	series_id,
	season_id,
	episode_id,
	title,
	CAST(air_date AS Uint64) AS air_date
FROM AS_TABLE($episodesData);
`))

func cleanupDatabase(ctx context.Context, c table.Client, prefix string, names ...string) (err error) {
	session, err := c.CreateSession(ctx)
	defer func() { _ = session.Close(ctx) }()
	if err != nil {
		return err
	}
	for _, name := range names {
		fullPath := path.Join(prefix, name)
		err = session.DropTable(ctx, fullPath)
		if err != nil {
			return err
		}
	}
	return err
}

func ensurePathExists(ctx context.Context, db *sql.DB) error {
	// TODO: fix it
	return nil
}

func readTable(ctx context.Context, db *sql.DB, path string) error {
	query := fmt.Sprintf("PRAGMA TablePathPrefix(\"%s\"); select series_id,title,release_date from series order by series_id", path)
	/*stmt, err := db.PrepareContext(ctx, query)
	if err != nil {
		return err
	}*/
	res, err := db.QueryContext(ctx, query)
	if err != nil {
		return err
	}
	log.Printf("\n> read_table:")
	var (
		id    *uint64
		title *string
		date  *uint64
	)

	for res.Next() {
		err = res.Scan(&id, &title, &date)
		if err != nil {
			return err
		}
		log.Printf("#  %d %s %d", *id, *title, *date)
	}

	if err = res.Err(); err != nil {
		return err
	}
	return nil
}

func describeTableOptions(ctx context.Context, c table.Client) error {
	var desc options.TableOptionsDescription
	err, issues := c.Retry(
		ctx,
		false,
		func(ctx context.Context, s *table.Session) (err error) {
			desc, err = s.DescribeTableOptions(ctx)
			return
		},
	)
	if err != nil {
		log.SetOutput(os.Stderr)
		log.Printf("\n> describeTableOptions issues:\n")
		for _, e := range issues {
			log.Printf("\t> %v\n", e)
		}
		return err
	}
	log.Println("\n> describe_table_options:")

	for i, p := range desc.TableProfilePresets {
		log.Printf("TableProfilePresets: %d/%d: %+v", i+1, len(desc.TableProfilePresets), p)
	}
	for i, p := range desc.StoragePolicyPresets {
		log.Printf("StoragePolicyPresets: %d/%d: %+v", i+1, len(desc.StoragePolicyPresets), p)
	}
	for i, p := range desc.CompactionPolicyPresets {
		log.Printf("CompactionPolicyPresets: %d/%d: %+v", i+1, len(desc.CompactionPolicyPresets), p)
	}
	for i, p := range desc.PartitioningPolicyPresets {
		log.Printf("PartitioningPolicyPresets: %d/%d: %+v", i+1, len(desc.PartitioningPolicyPresets), p)
	}
	for i, p := range desc.ExecutionPolicyPresets {
		log.Printf("ExecutionPolicyPresets: %d/%d: %+v", i+1, len(desc.ExecutionPolicyPresets), p)
	}
	for i, p := range desc.ReplicationPolicyPresets {
		log.Printf("ReplicationPolicyPresets: %d/%d: %+v", i+1, len(desc.ReplicationPolicyPresets), p)
	}
	for i, p := range desc.CachingPolicyPresets {
		log.Printf("CachingPolicyPresets: %d/%d: %+v", i+1, len(desc.CachingPolicyPresets), p)
	}

	return nil
}

func selectSimple(ctx context.Context, db *sql.DB, prefix string) error {
	query := render(
		template.Must(template.New("").Parse(`
			PRAGMA TablePathPrefix("{{ .TablePathPrefix }}");
			DECLARE $seriesID AS Uint64;
			$format = DateTime::Format("%Y-%m-%d");
			SELECT
				series_id,
				title,
				$format(DateTime::FromSeconds(CAST(DateTime::ToSeconds(DateTime::IntervalFromDays(CAST(release_date AS Int16))) AS Uint32))) AS release_date
			FROM
				series
			WHERE
				series_id = $seriesID;
		`)),
		templateConfig{
			TablePathPrefix: prefix,
		},
	)

	tx, err := db.Begin()
	if err != nil {
		return err
	}
	stmt, err := tx.PrepareContext(ctx, query)
	if err != nil {
		return err
	}
	res, err := stmt.QueryContext(ctx, sql.Named("seriesID", uint64(1)))
	if err != nil {
		return err
	}

	var (
		id    *uint64
		title *string
		date  *[]byte
	)
	for res.Next() {
		err = res.Scan(&id, &title, &date)
		if err != nil {
			return err
		}
		log.Printf(
			"\n> select_simple_transaction: %d %s %s",
			*id, *title, *date,
		)
	}
	if err = res.Err(); err != nil {
		return err
	}
	return tx.Commit()
}

func scanQuerySelect(ctx context.Context, db *sql.DB, prefix string) error {
	query := render(
		template.Must(template.New("").Parse(`
			PRAGMA TablePathPrefix("{{ .TablePathPrefix }}");

			DECLARE $series AS List<UInt64>;

			SELECT series_id, season_id, title, CAST(CAST(first_aired AS Date) AS String) AS first_aired
			FROM seasons
			WHERE series_id IN $series;

			SELECT series_id, season_id, title, CAST(CAST(first_aired AS Date) AS String) AS first_aired
			FROM seasons
		`)),
		templateConfig{
			TablePathPrefix: prefix,
		},
	)
	stmt, err := db.PrepareContext(ctx, query)
	if err != nil {
		return err
	}
	res, err := stmt.QueryContext(ctx, sql.Named("series", types.ListValue(
		types.Uint64Value(1),
		types.Uint64Value(10),
	)))

	if err != nil {
		return err
	}
	var (
		seriesID uint64
		seasonID uint64
		title    string
		date     string // due to cast in select query
	)
	log.Print("\n> scan_query_select:")
	for res.Next() {
		err = res.Scan(&seriesID, &seasonID, &title, &date)
		if err != nil {
			return err
		}
		log.Printf("#  Season, SeriesId: %d, SeasonId: %d, Title: %s, Air date: %s", seriesID, seasonID, title, date)
	}
	res.NextResultSet()
	log.Print("\n> all rows in table:")
	for res.Next() {
		err = res.Scan(&seriesID, &seasonID, &title, &date)
		if err != nil {
			return err
		}
		log.Printf("#  Season, SeriesId: %d, SeasonId: %d, Title: %s, Air date: %s", seriesID, seasonID, title, date)
	}
	if err = res.Err(); err != nil {
		return err
	}
	return nil
}

func fillTablesWithData(ctx context.Context, db *sql.DB, prefix string) error {
	query := render(fill, templateConfig{
		TablePathPrefix: prefix,
	})
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	stmt, err := tx.PrepareContext(ctx, query)
	if err != nil {
		return err
	}
	_, err = stmt.ExecContext(ctx, sql.Named("seriesData", getSeriesData()),
		sql.Named("seasonsData", getSeasonsData()),
		sql.Named("episodesData", getEpisodesData()))
	if err != nil {
		return err
	}
	return tx.Commit()
}

func createTables(ctx context.Context, c table.Client, prefix string) error {
	err, issues := c.Retry(
		ctx,
		false,
		func(ctx context.Context, s *table.Session) (err error) {
			return s.CreateTable(ctx, path.Join(prefix, "series"),
				options.WithColumn("series_id", types.Optional(types.TypeUint64)),
				options.WithColumn("title", types.Optional(types.TypeUTF8)),
				options.WithColumn("series_info", types.Optional(types.TypeUTF8)),
				options.WithColumn("release_date", types.Optional(types.TypeUint64)),
				options.WithColumn("comment", types.Optional(types.TypeUTF8)),
				options.WithPrimaryKeyColumn("series_id"),
			)
		},
	)
	if err != nil {
		log.SetOutput(os.Stderr)
		log.Printf("\n> createTables issues:\n")
		for _, e := range issues {
			log.Printf("\t> %v\n", e)
		}
		return err
	}

	err, issues = c.Retry(
		ctx,
		false,
		func(ctx context.Context, s *table.Session) (err error) {
			return s.CreateTable(ctx, path.Join(prefix, "seasons"),
				options.WithColumn("series_id", types.Optional(types.TypeUint64)),
				options.WithColumn("season_id", types.Optional(types.TypeUint64)),
				options.WithColumn("title", types.Optional(types.TypeUTF8)),
				options.WithColumn("first_aired", types.Optional(types.TypeUint64)),
				options.WithColumn("last_aired", types.Optional(types.TypeUint64)),
				options.WithPrimaryKeyColumn("series_id", "season_id"),
			)
		},
	)
	if err != nil {
		log.SetOutput(os.Stderr)
		log.Printf("\n> createTables issues:\n")
		for _, e := range issues {
			log.Printf("\t> %v\n", e)
		}
		return err
	}

	err, issues = c.Retry(
		ctx,
		false,
		func(ctx context.Context, s *table.Session) (err error) {
			return s.CreateTable(ctx, path.Join(prefix, "episodes"),
				options.WithColumn("series_id", types.Optional(types.TypeUint64)),
				options.WithColumn("season_id", types.Optional(types.TypeUint64)),
				options.WithColumn("episode_id", types.Optional(types.TypeUint64)),
				options.WithColumn("title", types.Optional(types.TypeUTF8)),
				options.WithColumn("air_date", types.Optional(types.TypeUint64)),
				options.WithPrimaryKeyColumn("series_id", "season_id", "episode_id"),
			)
		},
	)
	if err != nil {
		log.SetOutput(os.Stderr)
		log.Printf("\n> createTables issues:\n")
		for _, e := range issues {
			log.Printf("\t> %v\n", e)
		}
	}
	return err
}

func describeTable(ctx context.Context, c table.Client, path string) (err error) {
	err, issues := c.Retry(
		ctx,
		false,
		func(ctx context.Context, s *table.Session) (err error) {
			desc, err := s.DescribeTable(ctx, path)
			if err != nil {
				return
			}
			log.Printf("\n> describe table: %s", path)
			for _, c := range desc.Columns {
				log.Printf("column, name: %s, %s", c.Type, c.Name)
			}
			return
		},
	)
	if err != nil {
		log.SetOutput(os.Stderr)
		log.Printf("\n> describeTable issues:\n")
		for _, e := range issues {
			log.Printf("\t> %v\n", e)
		}
	}
	return err
}

func render(t *template.Template, data interface{}) string {
	var buf bytes.Buffer
	err := t.Execute(&buf, data)
	if err != nil {
		panic(err)
	}
	return buf.String()
}
