//go:build !fast
// +build !fast

package integration

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/retry"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
)

type sqlPositionalArgsScope struct {
	folder string
}

func TestDatabaseSqlPositionalArgs(t *testing.T) {
	scope := sqlPositionalArgsScope{
		folder: t.Name(),
	}
	ctx, cancel := context.WithTimeout(context.Background(), 42*time.Second)
	defer cancel()

	t.Run("sql.Open", func(t *testing.T) {
		uri, err := url.Parse(os.Getenv("YDB_CONNECTION_STRING"))
		require.NoError(t, err)

		values := uri.Query()
		values.Add("bind_params", "1")
		values.Add("table_path_prefix", scope.folder)
		uri.RawQuery = values.Encode()

		db, err := sql.Open("ydb", uri.String())
		require.NoError(t, err)

		defer func() {
			// cleanup
			_ = db.Close()
		}()

		err = db.PingContext(ctx)
		require.NoError(t, err)

		// prepare scheme
		err = scope.createTables(ctx, t, db)
		require.NoError(t, err)

		// fill data
		err = scope.fill(ctx, t, db)
		require.NoError(t, err)

		// getting explain of query
		row := db.QueryRowContext(
			ydb.WithQueryMode(ctx, ydb.ExplainQueryMode),
			`SELECT views FROM episodes WHERE series_id = ? AND season_id = ? AND episode_id = ?`,
			uint64(1), uint64(1), uint64(1),
		)
		var (
			ast  string
			plan string
		)

		err = row.Scan(&ast, &plan)
		require.NoError(t, err)

		t.Logf("ast = %v", ast)
		t.Logf("plan = %v", plan)

		err = retry.DoTx(ctx, db, func(ctx context.Context, tx *sql.Tx) (err error) {
			row = tx.QueryRowContext(ctx,
				`SELECT views FROM episodes WHERE series_id = ? AND season_id = ? AND episode_id = ?`,
				uint64(1), uint64(1), uint64(1),
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
				`UPSERT INTO episodes ( series_id, season_id, episode_id, views ) VALUES ( ?, ?, ?, ? )`,
				uint64(1), uint64(1), uint64(1), uint64(views.Float64+1), // increment views
			)
			if err != nil {
				return fmt.Errorf("cannot upsert views: %w", err)
			}
			return nil
		}, retry.WithDoTxRetryOptions(retry.WithIdempotent(true)))
		require.NoError(t, err)

		err = retry.DoTx(ctx, db,
			func(ctx context.Context, tx *sql.Tx) error {
				row := tx.QueryRowContext(ctx,
					`SELECT views FROM episodes WHERE series_id = ? AND season_id = ? AND episode_id = ?`,
					uint64(1),
					uint64(1),
					uint64(1),
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
		require.NoError(t, err)
	})
}

func (scope *sqlPositionalArgsScope) seriesData(
	id uint64, released time.Time, title, info, comment string,
) types.Value {
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

func (scope *sqlPositionalArgsScope) seasonData(
	seriesID, seasonID uint64, title string, first, last time.Time,
) types.Value {
	return types.StructValue(
		types.StructFieldValue("series_id", types.OptionalValue(types.Uint64Value(seriesID))),
		types.StructFieldValue("season_id", types.OptionalValue(types.Uint64Value(seasonID))),
		types.StructFieldValue("title", types.OptionalValue(types.TextValue(title))),
		types.StructFieldValue("first_aired", types.OptionalValue(types.DateValueFromTime(first))),
		types.StructFieldValue("last_aired", types.OptionalValue(types.DateValueFromTime(last))),
	)
}

func (scope *sqlPositionalArgsScope) episodeData(
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

func (scope *sqlPositionalArgsScope) getSeriesData() types.Value {
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

func (scope *sqlPositionalArgsScope) getSeasonsData() types.Value {
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

func (scope *sqlPositionalArgsScope) getEpisodesData() types.Value {
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

func (scope *sqlPositionalArgsScope) days(date string) time.Time {
	const dateISO8601 = "2006-01-02"
	t, err := time.Parse(dateISO8601, date)
	if err != nil {
		panic(err)
	}
	return t
}

func (scope *sqlPositionalArgsScope) fill(ctx context.Context, t *testing.T, db *sql.DB) error {
	t.Logf("> filling tables\n")
	defer func() {
		t.Logf("> filling tables done\n")
	}()
	_, err := db.ExecContext(ctx,
		`REPLACE INTO series SELECT * FROM AS_TABLE(?);
		REPLACE INTO seasons SELECT * FROM AS_TABLE(?);
		REPLACE INTO episodes SELECT * FROM AS_TABLE(?);
		`,
		scope.getSeriesData(),
		scope.getSeasonsData(),
		scope.getEpisodesData(),
	)
	if err != nil {
		t.Errorf("failed to execute query: %v", err)
		return err
	}
	return nil
}

func (scope *sqlPositionalArgsScope) createTables(ctx context.Context, t *testing.T, db *sql.DB) error {
	_, err := db.ExecContext(
		ydb.WithQueryMode(ctx, ydb.SchemeQueryMode),
		"DROP TABLE series",
	)
	if err != nil {
		t.Logf("warn: drop series table failed: %v", err)
	}
	_, err = db.ExecContext(
		ydb.WithQueryMode(ctx, ydb.SchemeQueryMode),
		`CREATE TABLE series (
			series_id Uint64,
			title UTF8,
			series_info UTF8,
			release_date Date,
			comment UTF8,
			PRIMARY KEY (
				series_id
			)
		)`,
	)
	if err != nil {
		t.Fatalf("create series table failed: %v", err)
		return err
	}

	_, err = db.ExecContext(
		ydb.WithQueryMode(ctx, ydb.SchemeQueryMode),
		"DROP TABLE seasons",
	)
	if err != nil {
		t.Logf("warn: drop seasons table failed: %v", err)
	}
	_, err = db.ExecContext(
		ydb.WithQueryMode(ctx, ydb.SchemeQueryMode),
		`CREATE TABLE seasons (
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
	)
	if err != nil {
		t.Fatalf("create seasons table failed: %v", err)
		return err
	}

	_, err = db.ExecContext(
		ydb.WithQueryMode(ctx, ydb.SchemeQueryMode),
		"DROP TABLE episodes",
	)
	if err != nil {
		t.Logf("warn: drop episodes table failed: %v", err)
	}
	_, err = db.ExecContext(
		ydb.WithQueryMode(ctx, ydb.SchemeQueryMode),
		`CREATE TABLE episodes (
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
	)
	if err != nil {
		t.Errorf("create episodes table failed: %v", err)
		return err
	}

	return nil
}
