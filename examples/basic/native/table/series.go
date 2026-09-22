package main

import (
	"bytes"
	"context"
	"log"
	"path"
	"text/template"

	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result/named"
	"github.com/ydb-platform/ydb-go-sdk/v3/types"
)

type templateConfig struct {
	TablePathPrefix string
}

var fill = template.Must(template.New("fill database").Parse(`
PRAGMA TablePathPrefix("{{ .TablePathPrefix }}");

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

func readTable(ctx context.Context, c table.Client, path string) error {
	return c.Do(ctx,
		func(ctx context.Context, s table.Session) error {
			res, err := s.StreamReadTable(ctx, path,
				options.ReadOrdered(),
				options.ReadColumn("series_id"),
				options.ReadColumn("title"),
				options.ReadColumn("release_date"),
			)
			if err != nil {
				return err
			}

			defer func() {
				_ = res.Close()
			}()

			log.Printf("> read_table:")

			var (
				id    *uint64
				title *string
				date  *uint64
			)

			for res.NextResultSet(ctx) {
				for res.NextRow() {
					err = res.ScanNamed(
						named.Optional("series_id", &id),
						named.Optional("title", &title),
						named.Optional("release_date", &date),
					)
					if err != nil {
						return err
					}
					log.Printf("#  %d %s %d", *id, *title, *date)
				}
			}
			if err := res.Err(); err != nil {
				return err
			}
			if stats := res.Stats(); stats != nil {
				for i := 0; ; i++ {
					phase, ok := stats.NextPhase()
					if !ok {
						break
					}
					log.Printf(
						"# phase #%d: took %s",
						i, phase.Duration(),
					)
					for {
						tbl, ok := phase.NextTableAccess()
						if !ok {
							break
						}
						log.Printf(
							"#  accessed %s: read=(%drows, %dbytes)",
							tbl.Name, tbl.Reads.Rows, tbl.Reads.Bytes,
						)
					}
				}
			}

			return res.Err()
		},
	)
}

func describeTableOptions(ctx context.Context, c table.Client) error {
	return c.Do(ctx,
		func(ctx context.Context, s table.Session) (err error) {
			desc, err := s.DescribeTableOptions(ctx)
			if err != nil {
				return err
			}

			log.Println("> describe_table_options:")

			for i := range desc.TableProfilePresets {
				log.Printf("TableProfilePresets: %d/%d: %+v", i+1,
					len(desc.TableProfilePresets), desc.TableProfilePresets[i],
				)
			}
			for i := range desc.StoragePolicyPresets {
				log.Printf("StoragePolicyPresets: %d/%d: %+v", i+1,
					len(desc.StoragePolicyPresets), desc.StoragePolicyPresets[i],
				)
			}
			for i := range desc.CompactionPolicyPresets {
				log.Printf("CompactionPolicyPresets: %d/%d: %+v", i+1,
					len(desc.CompactionPolicyPresets), desc.CompactionPolicyPresets[i],
				)
			}
			for i := range desc.PartitioningPolicyPresets {
				log.Printf("PartitioningPolicyPresets: %d/%d: %+v", i+1,
					len(desc.PartitioningPolicyPresets), desc.PartitioningPolicyPresets[i],
				)
			}
			for i := range desc.ExecutionPolicyPresets {
				log.Printf("ExecutionPolicyPresets: %d/%d: %+v", i+1,
					len(desc.ExecutionPolicyPresets), desc.ExecutionPolicyPresets[i],
				)
			}
			for i := range desc.ReplicationPolicyPresets {
				log.Printf("ReplicationPolicyPresets: %d/%d: %+v", i+1,
					len(desc.ReplicationPolicyPresets), desc.ReplicationPolicyPresets[i],
				)
			}
			for i := range desc.CachingPolicyPresets {
				log.Printf("CachingPolicyPresets: %d/%d: %+v", i+1,
					len(desc.CachingPolicyPresets), desc.CachingPolicyPresets[i],
				)
			}

			return nil
		},
	)
}

func selectSimple(ctx context.Context, c table.Client, prefix string) error {
	query := render(
		template.Must(template.New("").Parse(`
			PRAGMA TablePathPrefix("{{ .TablePathPrefix }}");
			$format = DateTime::Format("%Y-%m-%d");
			SELECT
				series_id,
				title,
				$format(
					DateTime::FromSeconds(
						CAST(DateTime::ToSeconds(DateTime::IntervalFromDays(CAST(release_date AS Int16))) AS Uint32)
					)
				) AS release_date
			FROM
				series
			WHERE
				series_id = $seriesID;
		`)),
		templateConfig{
			TablePathPrefix: prefix,
		},
	)
	readTx := table.TxControl(
		table.BeginTx(
			table.WithOnlineReadOnly(),
		),
		table.CommitTx(),
	)

	return c.Do(ctx,
		func(ctx context.Context, s table.Session) error {
			_, res, err := s.Execute(ctx, readTx, query,
				table.NewQueryParameters(
					table.ValueParam("$seriesID", types.Uint64Value(1)),
				),
				options.WithCollectStatsModeBasic(),
			)
			if err != nil {
				return err
			}

			defer func() {
				_ = res.Close()
			}()

			var (
				id    *uint64
				title *string
				date  *[]byte
			)
			for res.NextResultSet(ctx) {
				for res.NextRow() {
					err = res.ScanNamed(
						named.Optional("series_id", &id),
						named.Optional("title", &title),
						named.Optional("release_date", &date),
					)
					if err != nil {
						return err
					}
					log.Printf(
						"> select_simple_transaction: %d %s %s",
						*id, *title, *date,
					)
				}
			}

			return res.Err()
		},
	)
}

func fillTablesWithData(ctx context.Context, c table.Client, prefix string) error {
	writeTx := table.TxControl(
		table.BeginTx(
			table.WithSerializableReadWrite(),
		),
		table.CommitTx(),
	)

	return c.Do(ctx,
		func(ctx context.Context, s table.Session) (err error) {
			_, _, err = s.Execute(ctx, writeTx, render(fill, templateConfig{
				TablePathPrefix: prefix,
			}), table.NewQueryParameters(
				table.ValueParam("$seriesData", getSeriesData()),
				table.ValueParam("$seasonsData", getSeasonsData()),
				table.ValueParam("$episodesData", getEpisodesData()),
			))

			return err
		},
	)
}

func createTables(ctx context.Context, c table.Client, prefix string) error {
	return c.Do(ctx,
		func(ctx context.Context, s table.Session) error {
			err := s.CreateTable(ctx, path.Join(prefix, "series"),
				options.WithColumn("series_id", types.Optional(types.TypeUint64)),
				options.WithColumn("title", types.Optional(types.TypeUTF8)),
				options.WithColumn("series_info", types.Optional(types.TypeUTF8)),
				options.WithColumn("release_date", types.Optional(types.TypeUint64)),
				options.WithColumn("comment", types.Optional(types.TypeUTF8)),
				options.WithPrimaryKeyColumn("series_id"),
			)
			if err != nil {
				return err
			}

			err = s.CreateTable(ctx, path.Join(prefix, "seasons"),
				options.WithColumn("series_id", types.Optional(types.TypeUint64)),
				options.WithColumn("season_id", types.Optional(types.TypeUint64)),
				options.WithColumn("title", types.Optional(types.TypeUTF8)),
				options.WithColumn("first_aired", types.Optional(types.TypeUint64)),
				options.WithColumn("last_aired", types.Optional(types.TypeUint64)),
				options.WithPrimaryKeyColumn("series_id", "season_id"),
			)
			if err != nil {
				return err
			}

			err = s.CreateTable(ctx, path.Join(prefix, "episodes"),
				options.WithColumn("series_id", types.Optional(types.TypeUint64)),
				options.WithColumn("season_id", types.Optional(types.TypeUint64)),
				options.WithColumn("episode_id", types.Optional(types.TypeUint64)),
				options.WithColumn("title", types.Optional(types.TypeUTF8)),
				options.WithColumn("air_date", types.Optional(types.TypeUint64)),
				options.WithPrimaryKeyColumn("series_id", "season_id", "episode_id"),
			)
			if err != nil {
				return err
			}

			return nil
		},
	)
}

func describeTable(ctx context.Context, c table.Client, path string) error {
	desc, err := c.DescribeTable(ctx, path)
	if err != nil {
		return err
	}
	log.Printf("> describe table: %s", path)
	for i := range desc.Columns {
		log.Printf("column, name: %s, %s", desc.Columns[i].Type, desc.Columns[i].Name)
	}

	return nil
}

func render(t *template.Template, data any) string {
	var buf bytes.Buffer
	if err := t.Execute(&buf, data); err != nil {
		panic(err)
	}

	return buf.String()
}
