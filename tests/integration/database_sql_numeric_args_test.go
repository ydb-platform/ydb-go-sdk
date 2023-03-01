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

type sqlNumericArgsScope struct {
	folder string
	db     *sql.DB
}

func TestDatabaseSqlNumericArgs(t *testing.T) {
	t.Parallel()

	scope := &sqlNumericArgsScope{
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

		scope.db, err = sql.Open("ydb", uri.String())
		require.NoError(t, err)

		err = scope.db.PingContext(ctx)
		require.NoError(t, err)
	})

	t.Run("tables", func(t *testing.T) {
		t.Run("create", func(t *testing.T) {
			err := scope.createTables(ctx)
			require.NoError(t, err)
		})
		t.Run("fill", func(t *testing.T) {
			err := scope.fill(ctx)
			require.NoError(t, err)
		})
	})

	t.Run("query", func(t *testing.T) {
		t.Run("explain", func(t *testing.T) {
			row := scope.db.QueryRowContext(
				ydb.WithQueryMode(ctx, ydb.ExplainQueryMode),
				`SELECT views FROM episodes WHERE series_id = $1 AND season_id = $2 AND episode_id = $3`,
				uint64(1), uint64(1), uint64(1),
			)
			var (
				ast  string
				plan string
			)
			err := row.Scan(&ast, &plan)
			require.NoError(t, err)
			t.Log(ast, plan)
		})
		t.Run("increment", func(t *testing.T) {
			t.Run("views", func(t *testing.T) {
				err := retry.DoTx(ctx, scope.db, func(ctx context.Context, tx *sql.Tx) (err error) {
					row := tx.QueryRowContext(ctx,
						`SELECT views FROM episodes WHERE series_id = $1 AND season_id = $2 AND episode_id = $3`,
						uint64(1), uint64(1), uint64(1),
					)
					var views sql.NullFloat64
					if err = row.Scan(&views); err != nil {
						return fmt.Errorf("cannot scan views: %w", err)
					}
					if views.Valid {
						return fmt.Errorf("unexpected valid views: %v", views.Float64)
					}
					// increment `views`
					_, err = tx.ExecContext(ctx,
						`UPSERT INTO episodes ( series_id, season_id, episode_id, views ) VALUES ( $1, $2, $3, $4 )`,
						uint64(1), uint64(1), uint64(1), uint64(views.Float64+1), // increment views
					)
					if err != nil {
						return fmt.Errorf("cannot upsert views: %w", err)
					}
					return nil
				}, retry.WithDoTxRetryOptions(retry.WithIdempotent(true)))
				require.NoError(t, err)
			})
		})
		t.Run("lookup", func(t *testing.T) {
			err := retry.DoTx(ctx, scope.db,
				func(ctx context.Context, tx *sql.Tx) error {
					row := tx.QueryRowContext(ctx,
						`SELECT views FROM episodes WHERE series_id = $1 AND season_id = $2 AND episode_id = $3`,
						uint64(1),
						uint64(1),
						uint64(1),
					)
					var views sql.NullFloat64
					if err := row.Scan(&views); err != nil {
						return fmt.Errorf("cannot select current views: %w", err)
					}
					if !views.Valid {
						return fmt.Errorf("unexpected invalid views: %v", views)
					}
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
	})
}

func (s *sqlNumericArgsScope) seriesData(
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

func (s *sqlNumericArgsScope) seasonData(
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

func (s *sqlNumericArgsScope) episodeData(
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

func (s *sqlNumericArgsScope) getSeriesData() types.Value {
	return types.ListValue(
		s.seriesData(
			1, s.days("2006-02-03"), "IT Crowd", ""+
				"The IT Crowd is a British sitcom produced by Channel 4, written by Graham Linehan, produced by "+
				"Ash Atalla and starring Chris O'Dowd, Richard Ayoade, Katherine Parkinson, and Matt Berry.",
			"", // NULL comment.
		),
		s.seriesData(
			2, s.days("2014-04-06"), "Silicon Valley", ""+
				"Silicon Valley is an American comedy television series created by Mike Judge, John Altschuler and "+
				"Dave Krinsky. The series focuses on five young men who founded a startup company in Silicon Valley.",
			"Some comment here",
		),
	)
}

func (s *sqlNumericArgsScope) getSeasonsData() types.Value {
	return types.ListValue(
		s.seasonData(1, 1, "Season 1", s.days("2006-02-03"), s.days("2006-03-03")),
		s.seasonData(1, 2, "Season 2", s.days("2007-08-24"), s.days("2007-09-28")),
		s.seasonData(1, 3, "Season 3", s.days("2008-11-21"), s.days("2008-12-26")),
		s.seasonData(1, 4, "Season 4", s.days("2010-06-25"), s.days("2010-07-30")),
		s.seasonData(2, 1, "Season 1", s.days("2014-04-06"), s.days("2014-06-01")),
		s.seasonData(2, 2, "Season 2", s.days("2015-04-12"), s.days("2015-06-14")),
		s.seasonData(2, 3, "Season 3", s.days("2016-04-24"), s.days("2016-06-26")),
		s.seasonData(2, 4, "Season 4", s.days("2017-04-23"), s.days("2017-06-25")),
		s.seasonData(2, 5, "Season 5", s.days("2018-03-25"), s.days("2018-05-13")),
	)
}

func (s *sqlNumericArgsScope) getEpisodesData() types.Value {
	return types.ListValue(
		s.episodeData(1, 1, 1, "Yesterday's Jam", s.days("2006-02-03")),
		s.episodeData(1, 1, 2, "Calamity Jen", s.days("2006-02-03")),
		s.episodeData(1, 1, 3, "Fifty-Fifty", s.days("2006-02-10")),
		s.episodeData(1, 1, 4, "The Red Door", s.days("2006-02-17")),
		s.episodeData(1, 1, 5, "The Haunting of Bill Crouse", s.days("2006-02-24")),
		s.episodeData(1, 1, 6, "Aunt Irma Visits", s.days("2006-03-03")),
		s.episodeData(1, 2, 1, "The Work Outing", s.days("2006-08-24")),
		s.episodeData(1, 2, 2, "Return of the Golden Child", s.days("2007-08-31")),
		s.episodeData(1, 2, 3, "Moss and the German", s.days("2007-09-07")),
		s.episodeData(1, 2, 4, "The Dinner Party", s.days("2007-09-14")),
		s.episodeData(1, 2, 5, "Smoke and Mirrors", s.days("2007-09-21")),
		s.episodeData(1, 2, 6, "Men Without Women", s.days("2007-09-28")),
		s.episodeData(1, 3, 1, "From Hell", s.days("2008-11-21")),
		s.episodeData(1, 3, 2, "Are We Not Men?", s.days("2008-11-28")),
		s.episodeData(1, 3, 3, "Tramps Like Us", s.days("2008-12-05")),
		s.episodeData(1, 3, 4, "The Speech", s.days("2008-12-12")),
		s.episodeData(1, 3, 5, "Friendface", s.days("2008-12-19")),
		s.episodeData(1, 3, 6, "Calendar Geeks", s.days("2008-12-26")),
		s.episodeData(1, 4, 1, "Jen The Fredo", s.days("2010-06-25")),
		s.episodeData(1, 4, 2, "The Final Countdown", s.days("2010-07-02")),
		s.episodeData(1, 4, 3, "Something Happened", s.days("2010-07-09")),
		s.episodeData(1, 4, 4, "Italian For Beginners", s.days("2010-07-16")),
		s.episodeData(1, 4, 5, "Bad Boys", s.days("2010-07-23")),
		s.episodeData(1, 4, 6, "Reynholm vs Reynholm", s.days("2010-07-30")),
		s.episodeData(2, 1, 1, "Minimum Viable Product", s.days("2014-04-06")),
		s.episodeData(2, 1, 2, "The Cap Table", s.days("2014-04-13")),
		s.episodeData(2, 1, 3, "Articles of Incorporation", s.days("2014-04-20")),
		s.episodeData(2, 1, 4, "Fiduciary Duties", s.days("2014-04-27")),
		s.episodeData(2, 1, 5, "Signaling Risk", s.days("2014-05-04")),
		s.episodeData(2, 1, 6, "Third Party Insourcing", s.days("2014-05-11")),
		s.episodeData(2, 1, 7, "Proof of Concept", s.days("2014-05-18")),
		s.episodeData(2, 1, 8, "Optimal Tip-to-Tip Efficiency", s.days("2014-06-01")),
		s.episodeData(2, 2, 1, "Sand Hill Shuffle", s.days("2015-04-12")),
		s.episodeData(2, 2, 2, "Runaway Devaluation", s.days("2015-04-19")),
		s.episodeData(2, 2, 3, "Bad Money", s.days("2015-04-26")),
		s.episodeData(2, 2, 4, "The Lady", s.days("2015-05-03")),
		s.episodeData(2, 2, 5, "Server Space", s.days("2015-05-10")),
		s.episodeData(2, 2, 6, "Homicide", s.days("2015-05-17")),
		s.episodeData(2, 2, 7, "Adult Content", s.days("2015-05-24")),
		s.episodeData(2, 2, 8, "White Hat/Black Hat", s.days("2015-05-31")),
		s.episodeData(2, 2, 9, "Binding Arbitration", s.days("2015-06-07")),
		s.episodeData(2, 2, 10, "Two Days of the Condor", s.days("2015-06-14")),
		s.episodeData(2, 3, 1, "Founder Friendly", s.days("2016-04-24")),
		s.episodeData(2, 3, 2, "Two in the Box", s.days("2016-05-01")),
		s.episodeData(2, 3, 3, "Meinertzhagen's Haversack", s.days("2016-05-08")),
		s.episodeData(2, 3, 4, "Maleant Data Systems Solutions", s.days("2016-05-15")),
		s.episodeData(2, 3, 5, "The Empty Chair", s.days("2016-05-22")),
		s.episodeData(2, 3, 6, "Bachmanity Insanity", s.days("2016-05-29")),
		s.episodeData(2, 3, 7, "To Build a Better Beta", s.days("2016-06-05")),
		s.episodeData(2, 3, 8, "Bachman's Earnings Over-Ride", s.days("2016-06-12")),
		s.episodeData(2, 3, 9, "Daily Active Users", s.days("2016-06-19")),
		s.episodeData(2, 3, 10, "The Uptick", s.days("2016-06-26")),
		s.episodeData(2, 4, 1, "Success Failure", s.days("2017-04-23")),
		s.episodeData(2, 4, 2, "Terms of Service", s.days("2017-04-30")),
		s.episodeData(2, 4, 3, "Intellectual Property", s.days("2017-05-07")),
		s.episodeData(2, 4, 4, "Teambuilding Exercise", s.days("2017-05-14")),
		s.episodeData(2, 4, 5, "The Blood Boy", s.days("2017-05-21")),
		s.episodeData(2, 4, 6, "Customer Service", s.days("2017-05-28")),
		s.episodeData(2, 4, 7, "The Patent Troll", s.days("2017-06-04")),
		s.episodeData(2, 4, 8, "The Keenan Vortex", s.days("2017-06-11")),
		s.episodeData(2, 4, 9, "Hooli-Con", s.days("2017-06-18")),
		s.episodeData(2, 4, 10, "Server Error", s.days("2017-06-25")),
		s.episodeData(2, 5, 1, "Grow Fast or Die Slow", s.days("2018-03-25")),
		s.episodeData(2, 5, 2, "Reorientation", s.days("2018-04-01")),
		s.episodeData(2, 5, 3, "Chief Operating Officer", s.days("2018-04-08")),
		s.episodeData(2, 5, 4, "Tech Evangelist", s.days("2018-04-15")),
		s.episodeData(2, 5, 5, "Facial Recognition", s.days("2018-04-22")),
		s.episodeData(2, 5, 6, "Artificial Emotional Intelligence", s.days("2018-04-29")),
		s.episodeData(2, 5, 7, "Initial Coin Offering", s.days("2018-05-06")),
		s.episodeData(2, 5, 8, "Fifty-One Percent", s.days("2018-05-13")),
	)
}

func (s *sqlNumericArgsScope) days(date string) time.Time {
	const dateISO8601 = "2006-01-02"
	t, err := time.Parse(dateISO8601, date)
	if err != nil {
		panic(err)
	}
	return t
}

func (s *sqlNumericArgsScope) fill(ctx context.Context) error {
	_, err := s.db.ExecContext(ctx,
		`REPLACE INTO series SELECT * FROM AS_TABLE($1);
		REPLACE INTO seasons SELECT * FROM AS_TABLE($2);
		REPLACE INTO episodes SELECT * FROM AS_TABLE($3);
		`,
		s.getSeriesData(),
		s.getSeasonsData(),
		s.getEpisodesData(),
	)
	return err
}

func (s *sqlNumericArgsScope) createTables(ctx context.Context) error {
	_, _ = s.db.ExecContext(
		ydb.WithQueryMode(ctx, ydb.SchemeQueryMode),
		"DROP TABLE series",
	)

	_, err := s.db.ExecContext(
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
		return fmt.Errorf("create tables series failed: %w", err)
	}

	_, _ = s.db.ExecContext(
		ydb.WithQueryMode(ctx, ydb.SchemeQueryMode),
		"DROP TABLE seasons",
	)

	_, err = s.db.ExecContext(
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
		return fmt.Errorf("create tables seasons failed: %w", err)
	}

	_, _ = s.db.ExecContext(
		ydb.WithQueryMode(ctx, ydb.SchemeQueryMode),
		"DROP TABLE episodes",
	)

	_, err = s.db.ExecContext(
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
		return fmt.Errorf("create tables episodes failed: %w", err)
	}

	return nil
}
