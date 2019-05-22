package main

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"net"
	"path"
	"strings"
	"text/template"
	"time"

	"github.com/yandex-cloud/ydb-go-sdk"
	"github.com/yandex-cloud/ydb-go-sdk/scheme"
	"github.com/yandex-cloud/ydb-go-sdk/table"
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

func run(ctx context.Context, endpoint, prefix string, config *ydb.DriverConfig) error {
	driver, err := (&ydb.Dialer{
		DriverConfig: config,
		NetDial: func(ctx context.Context, addr string) (net.Conn, error) {
			var d net.Dialer
			conn, err := d.DialContext(ctx, "tcp", addr)
			if err == nil {
				return conn, nil
			}
			log.Printf("want to dial %q", addr)
			h, _, _ := net.SplitHostPort(addr)
			log.Printf("stripping %q host", h)
			return d.DialContext(ctx, "tcp", "localhost:22135")
		},
	}).Dial(ctx, endpoint)
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

	err = cleanupDatabase(ctx, driver, &sp, config.Database)
	if err != nil {
		return err
	}

	err = ensurePathExists(ctx, driver, config.Database, prefix)
	if err != nil {
		return err
	}

	err = describeTableOptions(ctx, &sp)
	if err != nil {
		return fmt.Errorf("describe table options error: %v", err)
	}

	err = createTables(ctx, &sp, path.Join(
		config.Database,
		prefix,
	))
	if err != nil {
		return fmt.Errorf("create tables error: %v", err)
	}

	err = describeTable(ctx, &sp, path.Join(
		config.Database,
		prefix, "series",
	))
	if err != nil {
		return fmt.Errorf("describe table error: %v", err)
	}

	err = fillTablesWithData(ctx, &sp, path.Join(
		config.Database,
		prefix,
	))
	if err != nil {
		return fmt.Errorf("fill tables with data error: %v", err)
	}

	err = selectSimple(ctx, &sp, path.Join(
		config.Database,
		prefix,
	))
	if err != nil {
		return fmt.Errorf("select simple error: %v", err)
	}

	err = readTable(ctx, &sp, path.Join(
		config.Database,
		prefix, "series",
	))
	if err != nil {
		return fmt.Errorf("read table error: %v", err)
	}

	return nil
}

func readTable(ctx context.Context, sp *table.SessionPool, path string) (err error) {
	var res *table.Result
	err = table.Retry(ctx, sp,
		table.OperationFunc(func(ctx context.Context, s *table.Session) (err error) {
			res, err = s.StreamReadTable(ctx, path,
				table.ReadOrdered(),
				table.ReadColumn("series_id"),
				table.ReadColumn("title"),
				table.ReadColumn("release_date"),
			)
			return
		}),
	)
	if err != nil {
		return err
	}
	// TODO(kamardin): truncated flag.
	for res.NextSet() {
		for res.NextRow() {
			res.NextItem()
			id := res.Uint64()

			res.NextItem()
			title := res.UTF8()

			res.NextItem()
			date := res.String()

			log.Printf("\n> read_table: %d %s %d", id, title, date)
		}
	}
	if err := res.Err(); err != nil {
		return err
	}
	return nil
}

func describeTableOptions(ctx context.Context, sp *table.SessionPool) (err error) {
	var desc table.TableOptionsDescription
	err = table.Retry(ctx, sp,
		table.OperationFunc(func(ctx context.Context, s *table.Session) (err error) {
			desc, err = s.DescribeTableOptions(ctx)
			return
		}),
	)
	if err != nil {
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

func selectSimple(ctx context.Context, sp *table.SessionPool, prefix string) (err error) {
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
	readTx := table.TxControl(
		table.BeginTx(
			table.WithOnlineReadOnly(),
		),
		table.CommitTx(),
	)
	var res *table.Result
	err = table.Retry(ctx, sp,
		table.OperationFunc(func(ctx context.Context, s *table.Session) (err error) {
			_, res, err = s.ExecuteDataQuery(ctx, readTx,
				table.TextDataQuery(query),
				table.NewQueryParameters(
					table.ValueParam("$seriesID", ydb.Uint64Value(1)),
				),
				table.WithExecuteDataQueryOperationParams(
					table.WithOperationModeSync(),
				),
				table.WithQueryCachePolicy(
					table.WithQueryCachePolicyKeepInCache(),
				),
				table.WithCollectStatsModeBasic(),
			)
			return
		}),
	)
	if err != nil {
		return err
	}
	// TODO(kamardin): truncated flag.
	for res.NextSet() {
		for res.NextRow() {
			res.SeekItem("series_id")
			id := res.OUint64()

			res.NextItem()
			title := res.OUTF8()

			res.NextItem()
			date := res.OString()

			log.Printf(
				"\n> select_simple_transaction: %d %s %s",
				id, title, date,
			)
		}
	}
	if err := res.Err(); err != nil {
		return err
	}
	return nil
}

func fillTablesWithData(ctx context.Context, sp *table.SessionPool, prefix string) (err error) {
	// Prepare write transaction.
	writeTx := table.TxControl(
		table.BeginTx(
			table.WithSerializableReadWrite(),
		),
		table.CommitTx(),
	)
	return table.Retry(ctx, sp,
		table.OperationFunc(func(ctx context.Context, s *table.Session) (err error) {
			query, err := s.PrepareDataQuery(ctx, render(fill, templateConfig{
				TablePathPrefix: prefix,
			}))
			if err != nil {
				return err
			}
			_, _, err = s.ExecuteDataQuery(ctx, writeTx, query, table.NewQueryParameters(
				table.ValueParam("$seriesData", getSeriesData()),
				table.ValueParam("$seasonsData", getSeasonsData()),
				table.ValueParam("$episodesData", getEpisodesData()),
			))
			return err
		}),
	)
}

func createTables(ctx context.Context, sp *table.SessionPool, prefix string) (err error) {
	err = table.Retry(ctx, sp,
		table.OperationFunc(func(ctx context.Context, s *table.Session) error {
			return s.CreateTable(ctx, path.Join(prefix, "series"),
				table.WithColumn("series_id", ydb.Optional(ydb.TypeUint64)),
				table.WithColumn("title", ydb.Optional(ydb.TypeUTF8)),
				table.WithColumn("series_info", ydb.Optional(ydb.TypeUTF8)),
				table.WithColumn("release_date", ydb.Optional(ydb.TypeUint64)),
				table.WithColumn("comment", ydb.Optional(ydb.TypeUTF8)),
				table.WithPrimaryKeyColumn("series_id"),
			)
		}),
	)
	if err != nil {
		return err
	}

	err = table.Retry(ctx, sp,
		table.OperationFunc(func(ctx context.Context, s *table.Session) error {
			return s.CreateTable(ctx, path.Join(prefix, "seasons"),
				table.WithColumn("series_id", ydb.Optional(ydb.TypeUint64)),
				table.WithColumn("season_id", ydb.Optional(ydb.TypeUint64)),
				table.WithColumn("title", ydb.Optional(ydb.TypeUTF8)),
				table.WithColumn("first_aired", ydb.Optional(ydb.TypeUint64)),
				table.WithColumn("last_aired", ydb.Optional(ydb.TypeUint64)),
				table.WithPrimaryKeyColumn("series_id", "season_id"),
			)
		}),
	)
	if err != nil {
		return err
	}

	err = table.Retry(ctx, sp,
		table.OperationFunc(func(ctx context.Context, s *table.Session) error {
			return s.CreateTable(ctx, path.Join(prefix, "episodes"),
				table.WithColumn("series_id", ydb.Optional(ydb.TypeUint64)),
				table.WithColumn("season_id", ydb.Optional(ydb.TypeUint64)),
				table.WithColumn("episode_id", ydb.Optional(ydb.TypeUint64)),
				table.WithColumn("title", ydb.Optional(ydb.TypeUTF8)),
				table.WithColumn("air_date", ydb.Optional(ydb.TypeUint64)),
				table.WithPrimaryKeyColumn("series_id", "season_id", "episode_id"),
			)
		}),
	)
	if err != nil {
		return err
	}

	return nil
}

func describeTable(ctx context.Context, sp *table.SessionPool, path string) (err error) {
	err = table.Retry(ctx, sp,
		table.OperationFunc(func(ctx context.Context, s *table.Session) error {
			desc, err := s.DescribeTable(ctx, path)
			if err != nil {
				return err
			}
			log.Printf("\n> describe table: %s", path)
			for _, c := range desc.Columns {
				log.Printf("column, name: %s, %s", c.Type, c.Name)
			}
			return nil
		}),
	)
	return nil
}

func cleanupDatabase(ctx context.Context, d ydb.Driver, sp *table.SessionPool, database string) error {
	s := scheme.Client{
		Driver: d,
	}
	var list func(int, string) error
	list = func(i int, p string) error {
		dir, err := s.ListDirectory(ctx, p)
		if err != nil {
			return err
		}
		log.Println(strings.Repeat(" ", i*2), "inspecting", dir.Name, dir.Type)
		for _, c := range dir.Children {
			pt := path.Join(p, c.Name)
			switch c.Type {
			case scheme.EntryDirectory:
				if err := list(i+1, pt); err != nil {
					return err
				}
				log.Println(strings.Repeat(" ", i*2), "removing", c.Type, pt)
				if err := s.RemoveDirectory(ctx, pt); err != nil {
					return err
				}

			case scheme.EntryTable:
				s, err := sp.Get(ctx)
				if err != nil {
					return err
				}
				defer sp.Put(ctx, s)
				log.Println(strings.Repeat(" ", i*2), "dropping", c.Type, pt)
				if err := s.DropTable(ctx, pt); err != nil {
					return err
				}

			default:
				log.Println(strings.Repeat(" ", i*2), "skipping", c.Type, pt)
			}
		}
		return nil
	}
	return list(0, database)
}

func ensurePathExists(ctx context.Context, d ydb.Driver, database, path string) error {
	s := scheme.Client{
		Driver: d,
	}

	database = strings.TrimSuffix(database, "/")
	path = strings.Trim(path, "/")
	full := database + "/" + path

	for i := 0; i < len(path); i++ {
		x := strings.IndexByte(path[i:], '/')
		if x == -1 {
			x = len(path[i:])
		}
		i += x
		sub := full[:len(database)+1+i]
		info, err := s.DescribePath(ctx, sub)
		operr, ok := err.(*ydb.OpError)
		if ok && operr.Reason == ydb.StatusSchemeError {
			log.Printf("creating %q", sub)
			err = s.MakeDirectory(ctx, sub)
		}
		if err != nil {
			return err
		}
		if ok {
			continue
		}
		log.Printf("exists %q", sub)
		switch info.Type {
		case
			scheme.EntryDatabase,
			scheme.EntryDirectory:
			// OK
		default:
			return fmt.Errorf(
				"entry %q exists but it is a %s",
				sub, info.Type,
			)
		}
	}

	return nil
}

func render(t *template.Template, data interface{}) string {
	var buf bytes.Buffer
	err := t.Execute(&buf, data)
	if err != nil {
		panic(err)
	}
	return buf.String()
}
