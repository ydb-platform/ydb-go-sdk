//go:build !fast
// +build !fast

package ydb_test

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"os"
	"path"
	"testing"
	"text/template"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/retry"
	"github.com/ydb-platform/ydb-go-sdk/v3/sugar"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
)

const (
	folder = "database_sql_test"
)

func TestDatabaseSql(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 42*time.Second)
	defer cancel()

	db, err := sql.Open("ydb", os.Getenv("YDB_CONNECTION_STRING"))
	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		// cleanup
		_ = db.Close()
	}()

	if err = db.PingContext(ctx); err != nil {
		t.Fatalf("driver not initialized: %+v", err)
	}

	cc, err := ydb.Unwrap(db)
	if err != nil {
		t.Fatal(err)
	}

	// prepare scheme
	err = sugar.RemoveRecursive(ctx, cc, folder)
	if err != nil {
		t.Fatal(err)
	}
	err = sugar.MakeRecursive(ctx, cc, folder)
	if err != nil {
		t.Fatal(err)
	}
	err = createTables(ctx, t, db, path.Join(cc.Name(), folder))
	if err != nil {
		t.Fatal(err)
	}

	// fill data
	if err = fill(ctx, t, db, path.Join(cc.Name(), folder)); err != nil {
		t.Fatalf("fill failed: %v\n", err)
	}

	// getting explain of query
	row := db.QueryRowContext(
		ydb.WithQueryMode(ctx, ydb.ExplainQueryMode),
		render(
			querySelect,
			templateConfig{
				TablePathPrefix: path.Join(cc.Name(), folder),
			},
		),
		sql.Named("seriesID", uint64(1)),
		sql.Named("seasonID", uint64(1)),
		sql.Named("episodeID", uint64(1)),
	)
	var (
		ast  string
		plan string
	)
	if err = row.Scan(&ast, &plan); err != nil {
		t.Fatalf("cannot explain: %v", err)
	}
	t.Logf("ast = %v", ast)
	t.Logf("plan = %v", plan)

	err = retry.DoTx(ctx, db, func(ctx context.Context, tx *sql.Tx) (err error) {
		var stmt *sql.Stmt
		stmt, err = tx.PrepareContext(ctx, render(
			querySelect,
			templateConfig{
				TablePathPrefix: path.Join(cc.Name(), folder),
			},
		))
		if err != nil {
			return fmt.Errorf("cannot prepare query: %w", err)
		}

		row = stmt.QueryRowContext(ctx,
			sql.Named("seriesID", uint64(1)),
			sql.Named("seasonID", uint64(1)),
			sql.Named("episodeID", uint64(1)),
		)
		var views sql.NullFloat64
		if err = row.Scan(&views); err != nil {
			return fmt.Errorf("cannot scan views: %w", err)
		}
		if views.Valid {
			return fmt.Errorf("unexpected valid views: %v", views.Float64)
		}
		t.Logf("views = %v", views)
		// increment `views`
		_, err = tx.ExecContext(ctx,
			render(
				queryUpsert,
				templateConfig{
					TablePathPrefix: path.Join(cc.Name(), folder),
				},
			),
			sql.Named("seriesID", uint64(1)),
			sql.Named("seasonID", uint64(1)),
			sql.Named("episodeID", uint64(1)),
			sql.Named("views", uint64(views.Float64+1)), // increment views
		)
		if err != nil {
			return fmt.Errorf("cannot upsert views: %w", err)
		}
		return nil
	}, retry.Idempotent(true))
	if err != nil {
		t.Fatalf("begin tx failed: %v\n", err)
	}
	err = retry.DoTx(ctx, db, func(ctx context.Context, tx *sql.Tx) error {
		row := tx.QueryRowContext(ctx,
			render(
				querySelect,
				templateConfig{
					TablePathPrefix: path.Join(cc.Name(), folder),
				},
			),
			sql.Named("seriesID", uint64(1)),
			sql.Named("seasonID", uint64(1)),
			sql.Named("episodeID", uint64(1)),
		)
		var views sql.NullFloat64
		if err = row.Scan(&views); err != nil {
			return fmt.Errorf("cannot select current views: %w", err)
		}
		if !views.Valid {
			return fmt.Errorf("unexpected invalid views: %v", views)
		}
		t.Logf("views = %v", views)
		if views.Float64 != 1 {
			return fmt.Errorf("unexpected views value: %v", views)
		}
		return nil
	}, retry.Idempotent(true))
	if err != nil {
		t.Fatalf("begin tx failed: %v\n", err)
	}
}

func seriesData(id uint64, released time.Time, title, info, comment string) types.Value {
	var commentv types.Value
	if comment == "" {
		commentv = types.NullValue(types.TypeUTF8)
	} else {
		commentv = types.OptionalValue(types.UTF8Value(comment))
	}
	return types.StructValue(
		types.StructFieldValue("series_id", types.OptionalValue(types.Uint64Value(id))),
		types.StructFieldValue("release_date", types.OptionalValue(types.DateValueFromTime(released))),
		types.StructFieldValue("title", types.OptionalValue(types.UTF8Value(title))),
		types.StructFieldValue("series_info", types.OptionalValue(types.UTF8Value(info))),
		types.StructFieldValue("comment", commentv),
	)
}

func seasonData(seriesID, seasonID uint64, title string, first, last time.Time) types.Value {
	return types.StructValue(
		types.StructFieldValue("series_id", types.OptionalValue(types.Uint64Value(seriesID))),
		types.StructFieldValue("season_id", types.OptionalValue(types.Uint64Value(seasonID))),
		types.StructFieldValue("title", types.OptionalValue(types.UTF8Value(title))),
		types.StructFieldValue("first_aired", types.OptionalValue(types.DateValueFromTime(first))),
		types.StructFieldValue("last_aired", types.OptionalValue(types.DateValueFromTime(last))),
	)
}

func episodeData(seriesID, seasonID, episodeID uint64, title string, date time.Time) types.Value {
	return types.StructValue(
		types.StructFieldValue("series_id", types.OptionalValue(types.Uint64Value(seriesID))),
		types.StructFieldValue("season_id", types.OptionalValue(types.Uint64Value(seasonID))),
		types.StructFieldValue("episode_id", types.OptionalValue(types.Uint64Value(episodeID))),
		types.StructFieldValue("title", types.OptionalValue(types.UTF8Value(title))),
		types.StructFieldValue("air_date", types.OptionalValue(types.DateValueFromTime(date))),
	)
}

func getSeriesData() types.Value {
	return types.ListValue(
		seriesData(
			1, days("2006-02-03"), "IT Crowd", ""+
				"The IT Crowd is a British sitcom produced by Channel 4, written by Graham Linehan, produced by "+
				"Ash Atalla and starring Chris O'Dowd, Richard Ayoade, Katherine Parkinson, and Matt Berry.",
			"", // NULL comment.
		),
		seriesData(
			2, days("2014-04-06"), "Silicon Valley", ""+
				"Silicon Valley is an American comedy television series created by Mike Judge, John Altschuler and "+
				"Dave Krinsky. The series focuses on five young men who founded a startup company in Silicon Valley.",
			"Some comment here",
		),
	)
}

func getSeasonsData() types.Value {
	return types.ListValue(
		seasonData(1, 1, "Season 1", days("2006-02-03"), days("2006-03-03")),
		seasonData(1, 2, "Season 2", days("2007-08-24"), days("2007-09-28")),
		seasonData(1, 3, "Season 3", days("2008-11-21"), days("2008-12-26")),
		seasonData(1, 4, "Season 4", days("2010-06-25"), days("2010-07-30")),
		seasonData(2, 1, "Season 1", days("2014-04-06"), days("2014-06-01")),
		seasonData(2, 2, "Season 2", days("2015-04-12"), days("2015-06-14")),
		seasonData(2, 3, "Season 3", days("2016-04-24"), days("2016-06-26")),
		seasonData(2, 4, "Season 4", days("2017-04-23"), days("2017-06-25")),
		seasonData(2, 5, "Season 5", days("2018-03-25"), days("2018-05-13")),
	)
}

func getEpisodesData() types.Value {
	return types.ListValue(
		episodeData(1, 1, 1, "Yesterday's Jam", days("2006-02-03")),
		episodeData(1, 1, 2, "Calamity Jen", days("2006-02-03")),
		episodeData(1, 1, 3, "Fifty-Fifty", days("2006-02-10")),
		episodeData(1, 1, 4, "The Red Door", days("2006-02-17")),
		episodeData(1, 1, 5, "The Haunting of Bill Crouse", days("2006-02-24")),
		episodeData(1, 1, 6, "Aunt Irma Visits", days("2006-03-03")),
		episodeData(1, 2, 1, "The Work Outing", days("2006-08-24")),
		episodeData(1, 2, 2, "Return of the Golden Child", days("2007-08-31")),
		episodeData(1, 2, 3, "Moss and the German", days("2007-09-07")),
		episodeData(1, 2, 4, "The Dinner Party", days("2007-09-14")),
		episodeData(1, 2, 5, "Smoke and Mirrors", days("2007-09-21")),
		episodeData(1, 2, 6, "Men Without Women", days("2007-09-28")),
		episodeData(1, 3, 1, "From Hell", days("2008-11-21")),
		episodeData(1, 3, 2, "Are We Not Men?", days("2008-11-28")),
		episodeData(1, 3, 3, "Tramps Like Us", days("2008-12-05")),
		episodeData(1, 3, 4, "The Speech", days("2008-12-12")),
		episodeData(1, 3, 5, "Friendface", days("2008-12-19")),
		episodeData(1, 3, 6, "Calendar Geeks", days("2008-12-26")),
		episodeData(1, 4, 1, "Jen The Fredo", days("2010-06-25")),
		episodeData(1, 4, 2, "The Final Countdown", days("2010-07-02")),
		episodeData(1, 4, 3, "Something Happened", days("2010-07-09")),
		episodeData(1, 4, 4, "Italian For Beginners", days("2010-07-16")),
		episodeData(1, 4, 5, "Bad Boys", days("2010-07-23")),
		episodeData(1, 4, 6, "Reynholm vs Reynholm", days("2010-07-30")),
		episodeData(2, 1, 1, "Minimum Viable Product", days("2014-04-06")),
		episodeData(2, 1, 2, "The Cap Table", days("2014-04-13")),
		episodeData(2, 1, 3, "Articles of Incorporation", days("2014-04-20")),
		episodeData(2, 1, 4, "Fiduciary Duties", days("2014-04-27")),
		episodeData(2, 1, 5, "Signaling Risk", days("2014-05-04")),
		episodeData(2, 1, 6, "Third Party Insourcing", days("2014-05-11")),
		episodeData(2, 1, 7, "Proof of Concept", days("2014-05-18")),
		episodeData(2, 1, 8, "Optimal Tip-to-Tip Efficiency", days("2014-06-01")),
		episodeData(2, 2, 1, "Sand Hill Shuffle", days("2015-04-12")),
		episodeData(2, 2, 2, "Runaway Devaluation", days("2015-04-19")),
		episodeData(2, 2, 3, "Bad Money", days("2015-04-26")),
		episodeData(2, 2, 4, "The Lady", days("2015-05-03")),
		episodeData(2, 2, 5, "Server Space", days("2015-05-10")),
		episodeData(2, 2, 6, "Homicide", days("2015-05-17")),
		episodeData(2, 2, 7, "Adult Content", days("2015-05-24")),
		episodeData(2, 2, 8, "White Hat/Black Hat", days("2015-05-31")),
		episodeData(2, 2, 9, "Binding Arbitration", days("2015-06-07")),
		episodeData(2, 2, 10, "Two Days of the Condor", days("2015-06-14")),
		episodeData(2, 3, 1, "Founder Friendly", days("2016-04-24")),
		episodeData(2, 3, 2, "Two in the Box", days("2016-05-01")),
		episodeData(2, 3, 3, "Meinertzhagen's Haversack", days("2016-05-08")),
		episodeData(2, 3, 4, "Maleant Data Systems Solutions", days("2016-05-15")),
		episodeData(2, 3, 5, "The Empty Chair", days("2016-05-22")),
		episodeData(2, 3, 6, "Bachmanity Insanity", days("2016-05-29")),
		episodeData(2, 3, 7, "To Build a Better Beta", days("2016-06-05")),
		episodeData(2, 3, 8, "Bachman's Earnings Over-Ride", days("2016-06-12")),
		episodeData(2, 3, 9, "Daily Active Users", days("2016-06-19")),
		episodeData(2, 3, 10, "The Uptick", days("2016-06-26")),
		episodeData(2, 4, 1, "Success Failure", days("2017-04-23")),
		episodeData(2, 4, 2, "Terms of Service", days("2017-04-30")),
		episodeData(2, 4, 3, "Intellectual Property", days("2017-05-07")),
		episodeData(2, 4, 4, "Teambuilding Exercise", days("2017-05-14")),
		episodeData(2, 4, 5, "The Blood Boy", days("2017-05-21")),
		episodeData(2, 4, 6, "Customer Service", days("2017-05-28")),
		episodeData(2, 4, 7, "The Patent Troll", days("2017-06-04")),
		episodeData(2, 4, 8, "The Keenan Vortex", days("2017-06-11")),
		episodeData(2, 4, 9, "Hooli-Con", days("2017-06-18")),
		episodeData(2, 4, 10, "Server Error", days("2017-06-25")),
		episodeData(2, 5, 1, "Grow Fast or Die Slow", days("2018-03-25")),
		episodeData(2, 5, 2, "Reorientation", days("2018-04-01")),
		episodeData(2, 5, 3, "Chief Operating Officer", days("2018-04-08")),
		episodeData(2, 5, 4, "Tech Evangelist", days("2018-04-15")),
		episodeData(2, 5, 5, "Facial Recognition", days("2018-04-22")),
		episodeData(2, 5, 6, "Artificial Emotional Intelligence", days("2018-04-29")),
		episodeData(2, 5, 7, "Initial Coin Offering", days("2018-05-06")),
		episodeData(2, 5, 8, "Fifty-One Percent", days("2018-05-13")),
	)
}

const dateISO8601 = "2006-01-02"

func days(date string) time.Time {
	t, err := time.Parse(dateISO8601, date)
	if err != nil {
		panic(err)
	}
	return t
}

type templateConfig struct {
	TablePathPrefix string
}

var (
	fillQuery = template.Must(template.New("fillQuery database").Parse(`
		PRAGMA TablePathPrefix("{{ .TablePathPrefix }}");
		
		DECLARE $seriesData AS List<Struct<
			series_id: Optional<Uint64>,
			title: Optional<Utf8>,
			series_info: Optional<Utf8>,
			release_date: Optional<Date>,
			comment: Optional<Utf8>>>;
		
		DECLARE $seasonsData AS List<Struct<
			series_id: Optional<Uint64>,
			season_id: Optional<Uint64>,
			title: Optional<Utf8>,
			first_aired: Optional<Date>,
			last_aired: Optional<Date>>>;
		
		DECLARE $episodesData AS List<Struct<
			series_id: Optional<Uint64>,
			season_id: Optional<Uint64>,
			episode_id: Optional<Uint64>,
			title: Optional<Utf8>,
			air_date: Optional<Date>>>;
		
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
	`))
	querySelect = template.Must(template.New("").Parse(`
		PRAGMA TablePathPrefix("{{ .TablePathPrefix }}");
		DECLARE $seriesID AS Uint64;
		DECLARE $seasonID AS Uint64;
		DECLARE $episodeID AS Uint64;
		SELECT
			views
		FROM
			episodes
		WHERE
			series_id = $seriesID AND season_id = $seasonID AND episode_id = $episodeID;
	`))
	queryUpsert = template.Must(template.New("").Parse(`
		PRAGMA TablePathPrefix("{{ .TablePathPrefix }}");
		DECLARE $seriesID AS Uint64;
		DECLARE $seasonID AS Uint64;
		DECLARE $episodeID AS Uint64;
		DECLARE $views AS Uint64;
		UPSERT INTO episodes ( series_id, season_id, episode_id, views )
		VALUES ( $seriesID, $seasonID, $episodeID, $views );
	`))
)

func fill(ctx context.Context, t *testing.T, db *sql.DB, folder string) error {
	t.Logf("> filling tables\n")
	defer func() {
		t.Logf("> filling tables done\n")
	}()
	stmt, err := db.PrepareContext(ctx, render(fillQuery, templateConfig{
		TablePathPrefix: folder,
	}))
	if err != nil {
		t.Errorf("failed to prepare query: %v", err)
		return err
	}
	_, err = stmt.ExecContext(ctx,
		sql.Named("seriesData", getSeriesData()),
		sql.Named("seasonsData", getSeasonsData()),
		sql.Named("episodesData", getEpisodesData()),
	)
	if err != nil {
		t.Errorf("failed to execute statement: %v", err)
		return err
	}
	return nil
}

func createTables(ctx context.Context, t *testing.T, db *sql.DB, folder string) error {
	_, err := db.ExecContext(
		ydb.WithQueryMode(ctx, ydb.SchemeQueryMode),
		fmt.Sprintf("DROP TABLE `%s`", path.Join(folder, "series")),
	)
	if err != nil {
		t.Logf("warn: drop series table failed: %v", err)
	}
	_, err = db.ExecContext(
		ydb.WithQueryMode(ctx, ydb.SchemeQueryMode),
		fmt.Sprintf(
			`CREATE TABLE `+"`"+path.Join(folder, "series")+"`"+` (
				series_id Uint64,
				title UTF8,
				series_info UTF8,
				release_date Date,
				comment UTF8,
				PRIMARY KEY (
					series_id
				)
			)`,
		),
	)
	if err != nil {
		t.Fatalf("create series table failed: %v", err)
		return err
	}

	_, err = db.ExecContext(
		ydb.WithQueryMode(ctx, ydb.SchemeQueryMode),
		fmt.Sprintf("DROP TABLE `%s`", path.Join(folder, "seasons")),
	)
	if err != nil {
		t.Logf("warn: drop seasons table failed: %v", err)
	}
	_, err = db.ExecContext(
		ydb.WithQueryMode(ctx, ydb.SchemeQueryMode),
		fmt.Sprintf(
			`CREATE TABLE `+"`"+path.Join(folder, "seasons")+"`"+` (
				series_id Uint64,
				season_id Uint64,
				title UTF8,
				first_aired Date,
				last_aired Date,
				PRIMARY KEY (
					series_id,
					season_id
				)
			)`,
		),
	)
	if err != nil {
		t.Fatalf("create seasons table failed: %v", err)
		return err
	}

	_, err = db.ExecContext(
		ydb.WithQueryMode(ctx, ydb.SchemeQueryMode),
		fmt.Sprintf("DROP TABLE `%s`", path.Join(folder, "episodes")),
	)
	if err != nil {
		t.Logf("warn: drop episodes table failed: %v", err)
	}
	_, err = db.ExecContext(
		ydb.WithQueryMode(ctx, ydb.SchemeQueryMode),
		fmt.Sprintf(
			`CREATE TABLE `+"`"+path.Join(folder, "episodes")+"`"+` (
				series_id Uint64,
				season_id Uint64,
				episode_id Uint64,
				title UTF8,
				air_date Date,
				views Uint64,
				PRIMARY KEY (
					series_id,
					season_id,
					episode_id
				)
			)`,
		),
	)
	if err != nil {
		t.Errorf("create episodes table failed: %v", err)
		return err
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
