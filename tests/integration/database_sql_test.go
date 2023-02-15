//go:build !fast
// +build !fast

package integration

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"os"
	"path"
	"sync/atomic"
	"testing"
	"text/template"
	"time"

	"google.golang.org/grpc/metadata"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/meta"
	"github.com/ydb-platform/ydb-go-sdk/v3/retry"
	"github.com/ydb-platform/ydb-go-sdk/v3/sugar"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
)

type databaseSQLScope struct {
	folder string
}

func TestDatabaseSql(t *testing.T) {
	scope := databaseSQLScope{
		folder: "database_sql_test",
	}
	ctx, cancel := context.WithTimeout(context.Background(), 42*time.Second)
	defer cancel()

	var totalConsumedUnits uint64
	defer func() {
		t.Logf("total consumed units: %d", atomic.LoadUint64(&totalConsumedUnits))
	}()

	ctx = meta.WithTrailerCallback(ctx, func(md metadata.MD) {
		atomic.AddUint64(&totalConsumedUnits, meta.ConsumedUnits(md))
	})

	t.Run("sql.Open", func(t *testing.T) {
		db, err := sql.Open("ydb", os.Getenv("YDB_CONNECTION_STRING"))
		if err != nil {
			t.Fatal(err)
		}

		if err = db.PingContext(ctx); err != nil {
			t.Fatalf("driver not initialized: %+v", err)
		}

		_, err = ydb.Unwrap(db)
		if err != nil {
			t.Fatal(err)
		}

		if err = db.Close(); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("sql.OpenDB", func(t *testing.T) {
		cc, err := ydb.Open(ctx, os.Getenv("YDB_CONNECTION_STRING"))
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			// cleanup
			_ = cc.Close(ctx)
		}()

		c, err := ydb.Connector(cc)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			// cleanup
			_ = c.Close()
		}()

		db := sql.OpenDB(c)
		defer func() {
			// cleanup
			_ = db.Close()
		}()

		if err = db.PingContext(ctx); err != nil {
			t.Fatalf("driver not initialized: %+v", err)
		}

		// prepare scheme
		err = sugar.RemoveRecursive(ctx, cc, scope.folder)
		if err != nil {
			t.Fatal(err)
		}
		err = sugar.MakeRecursive(ctx, cc, scope.folder)
		if err != nil {
			t.Fatal(err)
		}
		err = scope.createTables(ctx, t, db, path.Join(cc.Name(), scope.folder))
		if err != nil {
			t.Fatal(err)
		}

		// fill data
		if err = scope.fill(ctx, t, db, path.Join(cc.Name(), scope.folder)); err != nil {
			t.Fatalf("fill failed: %v\n", err)
		}

		// getting explain of query
		row := db.QueryRowContext(
			ydb.WithQueryMode(ctx, ydb.ExplainQueryMode),
			scope.render(
				template.Must(template.New("").Parse(`
					PRAGMA TablePathPrefix("{{ .TablePathPrefix }}");
					DECLARE $seriesID AS Uint64;
					DECLARE $seasonID AS Uint64;
					DECLARE $episodeID AS Uint64;
					SELECT views FROM episodes WHERE series_id = $seriesID AND season_id = $seasonID AND episode_id = $episodeID;
				`)),
				struct {
					TablePathPrefix string
				}{
					TablePathPrefix: path.Join(cc.Name(), scope.folder),
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
			stmt, err = tx.PrepareContext(ctx, scope.render(
				template.Must(template.New("").Parse(`
					PRAGMA TablePathPrefix("{{ .TablePathPrefix }}");
					DECLARE $seriesID AS Uint64;
					DECLARE $seasonID AS Uint64;
					DECLARE $episodeID AS Uint64;
					SELECT views FROM episodes WHERE series_id = $seriesID AND season_id = $seasonID AND episode_id = $episodeID;
				`)),
				struct {
					TablePathPrefix string
				}{
					TablePathPrefix: path.Join(cc.Name(), scope.folder),
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
				scope.render(
					template.Must(template.New("").Parse(`
						PRAGMA TablePathPrefix("{{ .TablePathPrefix }}");
						DECLARE $seriesID AS Uint64;
						DECLARE $seasonID AS Uint64;
						DECLARE $episodeID AS Uint64;
						DECLARE $views AS Uint64;
						UPSERT INTO episodes ( series_id, season_id, episode_id, views )
						VALUES ( $seriesID, $seasonID, $episodeID, $views );
					`)),
					struct {
						TablePathPrefix string
					}{
						TablePathPrefix: path.Join(cc.Name(), scope.folder),
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
		}, retry.WithDoTxRetryOptions(retry.WithIdempotent(true)))
		if err != nil {
			t.Fatalf("do tx failed: %v\n", err)
		}
		err = retry.DoTx(ctx, db,
			func(ctx context.Context, tx *sql.Tx) error {
				row := tx.QueryRowContext(ctx,
					scope.render(
						template.Must(template.New("").Parse(`
							PRAGMA TablePathPrefix("{{ .TablePathPrefix }}");
							DECLARE $seriesID AS Uint64;
							DECLARE $seasonID AS Uint64;
							DECLARE $episodeID AS Uint64;
							SELECT views FROM episodes WHERE series_id = $seriesID AND season_id = $seasonID AND episode_id = $episodeID;
						`)),
						struct {
							TablePathPrefix string
						}{
							TablePathPrefix: path.Join(cc.Name(), scope.folder),
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
			},
			retry.WithDoTxRetryOptions(retry.WithIdempotent(true)),
			retry.WithTxOptions(&sql.TxOptions{
				Isolation: sql.LevelSnapshot,
				ReadOnly:  true,
			}),
		)
		if err != nil {
			t.Fatalf("do tx failed: %v\n", err)
		}
	})
}

func (scope *databaseSQLScope) seriesData(id uint64, released time.Time, title, info, comment string) types.Value {
	var commenValue types.Value
	if comment == "" {
		commenValue = types.NullValue(types.TypeText)
	} else {
		commenValue = types.OptionalValue(types.TextValue(comment))
	}
	return types.StructValue(
		types.StructFieldValue("series_id", types.OptionalValue(types.Uint64Value(id))),
		types.StructFieldValue("release_date", types.OptionalValue(types.DateValueFromTime(released))),
		types.StructFieldValue("title", types.OptionalValue(types.TextValue(title))),
		types.StructFieldValue("series_info", types.OptionalValue(types.TextValue(info))),
		types.StructFieldValue("comment", commenValue),
	)
}

func (scope *databaseSQLScope) seasonData(seriesID, seasonID uint64, title string, first, last time.Time) types.Value {
	return types.StructValue(
		types.StructFieldValue("series_id", types.OptionalValue(types.Uint64Value(seriesID))),
		types.StructFieldValue("season_id", types.OptionalValue(types.Uint64Value(seasonID))),
		types.StructFieldValue("title", types.OptionalValue(types.TextValue(title))),
		types.StructFieldValue("first_aired", types.OptionalValue(types.DateValueFromTime(first))),
		types.StructFieldValue("last_aired", types.OptionalValue(types.DateValueFromTime(last))),
	)
}

func (scope *databaseSQLScope) episodeData(
	seriesID, seasonID, episodeID uint64, title string, date time.Time,
) types.Value {
	return types.StructValue(
		types.StructFieldValue("series_id", types.OptionalValue(types.Uint64Value(seriesID))),
		types.StructFieldValue("season_id", types.OptionalValue(types.Uint64Value(seasonID))),
		types.StructFieldValue("episode_id", types.OptionalValue(types.Uint64Value(episodeID))),
		types.StructFieldValue("title", types.OptionalValue(types.TextValue(title))),
		types.StructFieldValue("air_date", types.OptionalValue(types.DateValueFromTime(date))),
	)
}

func (scope *databaseSQLScope) getSeriesData() types.Value {
	return types.ListValue(
		scope.seriesData(
			1, scope.days("2006-02-03"), "IT Crowd", ""+
				"The IT Crowd is a British sitcom produced by Channel 4, written by Graham Linehan, produced by "+
				"Ash Atalla and starring Chris O'Dowd, Richard Ayoade, Katherine Parkinson, and Matt Berry.",
			"", // NULL comment.
		),
		scope.seriesData(
			2, scope.days("2014-04-06"), "Silicon Valley", ""+
				"Silicon Valley is an American comedy television series created by Mike Judge, John Altschuler and "+
				"Dave Krinsky. The series focuses on five young men who founded a startup company in Silicon Valley.",
			"Some comment here",
		),
	)
}

func (scope *databaseSQLScope) getSeasonsData() types.Value {
	return types.ListValue(
		scope.seasonData(1, 1, "Season 1", scope.days("2006-02-03"), scope.days("2006-03-03")),
		scope.seasonData(1, 2, "Season 2", scope.days("2007-08-24"), scope.days("2007-09-28")),
		scope.seasonData(1, 3, "Season 3", scope.days("2008-11-21"), scope.days("2008-12-26")),
		scope.seasonData(1, 4, "Season 4", scope.days("2010-06-25"), scope.days("2010-07-30")),
		scope.seasonData(2, 1, "Season 1", scope.days("2014-04-06"), scope.days("2014-06-01")),
		scope.seasonData(2, 2, "Season 2", scope.days("2015-04-12"), scope.days("2015-06-14")),
		scope.seasonData(2, 3, "Season 3", scope.days("2016-04-24"), scope.days("2016-06-26")),
		scope.seasonData(2, 4, "Season 4", scope.days("2017-04-23"), scope.days("2017-06-25")),
		scope.seasonData(2, 5, "Season 5", scope.days("2018-03-25"), scope.days("2018-05-13")),
	)
}

func (scope *databaseSQLScope) getEpisodesData() types.Value {
	return types.ListValue(
		scope.episodeData(1, 1, 1, "Yesterday's Jam", scope.days("2006-02-03")),
		scope.episodeData(1, 1, 2, "Calamity Jen", scope.days("2006-02-03")),
		scope.episodeData(1, 1, 3, "Fifty-Fifty", scope.days("2006-02-10")),
		scope.episodeData(1, 1, 4, "The Red Door", scope.days("2006-02-17")),
		scope.episodeData(1, 1, 5, "The Haunting of Bill Crouse", scope.days("2006-02-24")),
		scope.episodeData(1, 1, 6, "Aunt Irma Visits", scope.days("2006-03-03")),
		scope.episodeData(1, 2, 1, "The Work Outing", scope.days("2006-08-24")),
		scope.episodeData(1, 2, 2, "Return of the Golden Child", scope.days("2007-08-31")),
		scope.episodeData(1, 2, 3, "Moss and the German", scope.days("2007-09-07")),
		scope.episodeData(1, 2, 4, "The Dinner Party", scope.days("2007-09-14")),
		scope.episodeData(1, 2, 5, "Smoke and Mirrors", scope.days("2007-09-21")),
		scope.episodeData(1, 2, 6, "Men Without Women", scope.days("2007-09-28")),
		scope.episodeData(1, 3, 1, "From Hell", scope.days("2008-11-21")),
		scope.episodeData(1, 3, 2, "Are We Not Men?", scope.days("2008-11-28")),
		scope.episodeData(1, 3, 3, "Tramps Like Us", scope.days("2008-12-05")),
		scope.episodeData(1, 3, 4, "The Speech", scope.days("2008-12-12")),
		scope.episodeData(1, 3, 5, "Friendface", scope.days("2008-12-19")),
		scope.episodeData(1, 3, 6, "Calendar Geeks", scope.days("2008-12-26")),
		scope.episodeData(1, 4, 1, "Jen The Fredo", scope.days("2010-06-25")),
		scope.episodeData(1, 4, 2, "The Final Countdown", scope.days("2010-07-02")),
		scope.episodeData(1, 4, 3, "Something Happened", scope.days("2010-07-09")),
		scope.episodeData(1, 4, 4, "Italian For Beginners", scope.days("2010-07-16")),
		scope.episodeData(1, 4, 5, "Bad Boys", scope.days("2010-07-23")),
		scope.episodeData(1, 4, 6, "Reynholm vs Reynholm", scope.days("2010-07-30")),
		scope.episodeData(2, 1, 1, "Minimum Viable Product", scope.days("2014-04-06")),
		scope.episodeData(2, 1, 2, "The Cap Table", scope.days("2014-04-13")),
		scope.episodeData(2, 1, 3, "Articles of Incorporation", scope.days("2014-04-20")),
		scope.episodeData(2, 1, 4, "Fiduciary Duties", scope.days("2014-04-27")),
		scope.episodeData(2, 1, 5, "Signaling Risk", scope.days("2014-05-04")),
		scope.episodeData(2, 1, 6, "Third Party Insourcing", scope.days("2014-05-11")),
		scope.episodeData(2, 1, 7, "Proof of Concept", scope.days("2014-05-18")),
		scope.episodeData(2, 1, 8, "Optimal Tip-to-Tip Efficiency", scope.days("2014-06-01")),
		scope.episodeData(2, 2, 1, "Sand Hill Shuffle", scope.days("2015-04-12")),
		scope.episodeData(2, 2, 2, "Runaway Devaluation", scope.days("2015-04-19")),
		scope.episodeData(2, 2, 3, "Bad Money", scope.days("2015-04-26")),
		scope.episodeData(2, 2, 4, "The Lady", scope.days("2015-05-03")),
		scope.episodeData(2, 2, 5, "Server Space", scope.days("2015-05-10")),
		scope.episodeData(2, 2, 6, "Homicide", scope.days("2015-05-17")),
		scope.episodeData(2, 2, 7, "Adult Content", scope.days("2015-05-24")),
		scope.episodeData(2, 2, 8, "White Hat/Black Hat", scope.days("2015-05-31")),
		scope.episodeData(2, 2, 9, "Binding Arbitration", scope.days("2015-06-07")),
		scope.episodeData(2, 2, 10, "Two Days of the Condor", scope.days("2015-06-14")),
		scope.episodeData(2, 3, 1, "Founder Friendly", scope.days("2016-04-24")),
		scope.episodeData(2, 3, 2, "Two in the Box", scope.days("2016-05-01")),
		scope.episodeData(2, 3, 3, "Meinertzhagen's Haversack", scope.days("2016-05-08")),
		scope.episodeData(2, 3, 4, "Maleant Data Systems Solutions", scope.days("2016-05-15")),
		scope.episodeData(2, 3, 5, "The Empty Chair", scope.days("2016-05-22")),
		scope.episodeData(2, 3, 6, "Bachmanity Insanity", scope.days("2016-05-29")),
		scope.episodeData(2, 3, 7, "To Build a Better Beta", scope.days("2016-06-05")),
		scope.episodeData(2, 3, 8, "Bachman's Earnings Over-Ride", scope.days("2016-06-12")),
		scope.episodeData(2, 3, 9, "Daily Active Users", scope.days("2016-06-19")),
		scope.episodeData(2, 3, 10, "The Uptick", scope.days("2016-06-26")),
		scope.episodeData(2, 4, 1, "Success Failure", scope.days("2017-04-23")),
		scope.episodeData(2, 4, 2, "Terms of Service", scope.days("2017-04-30")),
		scope.episodeData(2, 4, 3, "Intellectual Property", scope.days("2017-05-07")),
		scope.episodeData(2, 4, 4, "Teambuilding Exercise", scope.days("2017-05-14")),
		scope.episodeData(2, 4, 5, "The Blood Boy", scope.days("2017-05-21")),
		scope.episodeData(2, 4, 6, "Customer Service", scope.days("2017-05-28")),
		scope.episodeData(2, 4, 7, "The Patent Troll", scope.days("2017-06-04")),
		scope.episodeData(2, 4, 8, "The Keenan Vortex", scope.days("2017-06-11")),
		scope.episodeData(2, 4, 9, "Hooli-Con", scope.days("2017-06-18")),
		scope.episodeData(2, 4, 10, "Server Error", scope.days("2017-06-25")),
		scope.episodeData(2, 5, 1, "Grow Fast or Die Slow", scope.days("2018-03-25")),
		scope.episodeData(2, 5, 2, "Reorientation", scope.days("2018-04-01")),
		scope.episodeData(2, 5, 3, "Chief Operating Officer", scope.days("2018-04-08")),
		scope.episodeData(2, 5, 4, "Tech Evangelist", scope.days("2018-04-15")),
		scope.episodeData(2, 5, 5, "Facial Recognition", scope.days("2018-04-22")),
		scope.episodeData(2, 5, 6, "Artificial Emotional Intelligence", scope.days("2018-04-29")),
		scope.episodeData(2, 5, 7, "Initial Coin Offering", scope.days("2018-05-06")),
		scope.episodeData(2, 5, 8, "Fifty-One Percent", scope.days("2018-05-13")),
	)
}

func (scope *databaseSQLScope) days(date string) time.Time {
	const dateISO8601 = "2006-01-02"
	t, err := time.Parse(dateISO8601, date)
	if err != nil {
		panic(err)
	}
	return t
}

func (scope *databaseSQLScope) fill(ctx context.Context, t *testing.T, db *sql.DB, prefix string) error {
	t.Logf("> filling tables\n")
	defer func() {
		t.Logf("> filling tables done\n")
	}()
	stmt, err := db.PrepareContext(ctx, scope.render(template.Must(template.New("fillQuery database").Parse(`
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
		
		REPLACE INTO series SELECT * FROM AS_TABLE($seriesData);
		
		REPLACE INTO seasons SELECT * FROM AS_TABLE($seasonsData);
		
		REPLACE INTO episodes SELECT * FROM AS_TABLE($episodesData);
	`)), struct {
		TablePathPrefix string
	}{
		TablePathPrefix: prefix,
	}))
	if err != nil {
		t.Errorf("failed to prepare query: %v", err)
		return err
	}
	_, err = stmt.ExecContext(ctx,
		sql.Named("seriesData", scope.getSeriesData()),
		sql.Named("seasonsData", scope.getSeasonsData()),
		sql.Named("episodesData", scope.getEpisodesData()),
	)
	if err != nil {
		t.Errorf("failed to execute statement: %v", err)
		return err
	}
	return nil
}

func (scope *databaseSQLScope) createTables(ctx context.Context, t *testing.T, db *sql.DB, prefix string) error {
	_, err := db.ExecContext(
		ydb.WithQueryMode(ctx, ydb.SchemeQueryMode),
		fmt.Sprintf("DROP TABLE `%s`", path.Join(prefix, "series")),
	)
	if err != nil {
		t.Logf("warn: drop series table failed: %v", err)
	}
	_, err = db.ExecContext(
		ydb.WithQueryMode(ctx, ydb.SchemeQueryMode),
		fmt.Sprintf(
			`CREATE TABLE `+"`"+path.Join(prefix, "series")+"`"+` (
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
		fmt.Sprintf("DROP TABLE `%s`", path.Join(prefix, "seasons")),
	)
	if err != nil {
		t.Logf("warn: drop seasons table failed: %v", err)
	}
	_, err = db.ExecContext(
		ydb.WithQueryMode(ctx, ydb.SchemeQueryMode),
		fmt.Sprintf(
			`CREATE TABLE `+"`"+path.Join(prefix, "seasons")+"`"+` (
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
		fmt.Sprintf("DROP TABLE `%s`", path.Join(prefix, "episodes")),
	)
	if err != nil {
		t.Logf("warn: drop episodes table failed: %v", err)
	}
	_, err = db.ExecContext(
		ydb.WithQueryMode(ctx, ydb.SchemeQueryMode),
		fmt.Sprintf(
			`CREATE TABLE `+"`"+path.Join(prefix, "episodes")+"`"+` (
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

func (scope *databaseSQLScope) render(t *template.Template, data interface{}) string {
	var buf bytes.Buffer
	err := t.Execute(&buf, data)
	if err != nil {
		panic(err)
	}
	return buf.String()
}
