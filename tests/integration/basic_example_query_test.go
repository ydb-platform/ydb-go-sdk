//go:build integration && go1.23
// +build integration,go1.23

package integration

import (
	"context"
	"net/http"
	"os"
	"path"
	"runtime/debug"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	grpcCodes "google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/balancers"
	"github.com/ydb-platform/ydb-go-sdk/v3/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/version"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xtest"
	"github.com/ydb-platform/ydb-go-sdk/v3/log"
	"github.com/ydb-platform/ydb-go-sdk/v3/meta"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
	"github.com/ydb-platform/ydb-go-sdk/v3/sugar"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func TestBasicExampleQuery(sourceTest *testing.T) { //nolint:gocyclo
	if os.Getenv("YDB_VERSION") != "nightly" && version.Lt(os.Getenv("YDB_VERSION"), "24.3") {
		sourceTest.Skip("query service has been production ready since 24.3")
	}

	t := xtest.MakeSyncedTest(sourceTest)
	folder := t.Name()

	ctx, cancel := context.WithTimeout(context.Background(), 42*time.Second)
	defer cancel()

	var totalConsumedUnits atomic.Uint64
	defer func() {
		t.Logf("total consumed units: %d", totalConsumedUnits.Load())
	}()

	ctx = meta.WithTrailerCallback(ctx, func(md metadata.MD) {
		totalConsumedUnits.Add(meta.ConsumedUnits(md))
	})

	var (
		limit      = 50
		shutdowned atomic.Bool
	)

	db, err := ydb.Open(ctx,
		os.Getenv("YDB_CONNECTION_STRING"),
		ydb.WithAccessTokenCredentials(os.Getenv("YDB_ACCESS_TOKEN_CREDENTIALS")),
		ydb.WithApplicationName("table/e2e"),
		withMetrics(t, trace.DetailsAll, time.Second),
		ydb.With(
			config.WithOperationTimeout(time.Second*5),
			config.WithOperationCancelAfter(time.Second*5),
			config.ExcludeGRPCCodesForPessimization(grpcCodes.DeadlineExceeded),
			config.WithGrpcOptions(
				grpc.WithUnaryInterceptor(func(
					ctx context.Context,
					method string,
					req, reply interface{},
					cc *grpc.ClientConn,
					invoker grpc.UnaryInvoker,
					opts ...grpc.CallOption,
				) error {
					return invoker(ctx, method, req, reply, cc, opts...)
				}),
				grpc.WithStreamInterceptor(func(
					ctx context.Context,
					desc *grpc.StreamDesc,
					cc *grpc.ClientConn,
					method string,
					streamer grpc.Streamer,
					opts ...grpc.CallOption,
				) (grpc.ClientStream, error) {
					return streamer(ctx, desc, cc, method, opts...)
				}),
			),
		),
		ydb.WithBalancer(balancers.RandomChoice()),
		ydb.WithDialTimeout(5*time.Second),
		ydb.WithSessionPoolSizeLimit(limit),
		ydb.WithConnectionTTL(5*time.Second),
		ydb.WithLogger(
			newLoggerWithMinLevel(t, log.FromString(os.Getenv("YDB_LOG_SEVERITY_LEVEL"))),
			trace.MatchDetails(`ydb\.(driver|table|discovery|retry|scheme).*`),
		),
		ydb.WithPanicCallback(func(e interface{}) {
			t.Fatalf("panic recovered:%v:\n%s", e, debug.Stack())
		}),
	)
	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		// cleanup
		_ = db.Close(ctx)
	}()

	if err = db.Query().Do(ctx, func(ctx context.Context, _ query.Session) error {
		// hack for wait pool initializing
		return nil
	}); err != nil {
		t.Fatalf("pool not initialized: %+v", err)
	}

	// prepare scheme
	err = sugar.RemoveRecursive(ctx, db, folder)
	if err != nil {
		t.Fatal(err)
	}
	err = sugar.MakeRecursive(ctx, db, folder)
	if err != nil {
		t.Fatal(err)
	}

	t.Run("prepare", func(t *testing.T) {
		t.Run("scheme", func(t *testing.T) {
			t.Run("series", func(t *testing.T) {
				err := db.Query().Exec(ctx, `
					PRAGMA TablePathPrefix("`+path.Join(db.Name(), folder)+`");

					DROP TABLE IF EXISTS series; 

					CREATE TABLE series (
						series_id Uint64,
						title Text,
						series_info Text,
						release_date Date,
						comment Text,
						
						PRIMARY KEY(series_id)
					)`,
				)
				require.NoError(t, err)
			})
			t.Run("seasons", func(t *testing.T) {
				err := db.Query().Exec(ctx, `
					PRAGMA TablePathPrefix("`+path.Join(db.Name(), folder)+`");

					DROP TABLE IF EXISTS seasons; 

					CREATE TABLE seasons (
						series_id Uint64,
						season_id Uint64,
						title Text,
						first_aired Date,
						last_aired Date,
						
						PRIMARY KEY(series_id,season_id)
					)`,
				)
				require.NoError(t, err)
			})
			t.Run("episodes", func(t *testing.T) {
				err := db.Query().Exec(ctx, `
					PRAGMA TablePathPrefix("`+path.Join(db.Name(), folder)+`");

					DROP TABLE IF EXISTS episodes; 

					CREATE TABLE episodes (
						series_id Uint64,
						season_id Uint64,
						episode_id Uint64,
						title Text,
						air_date Date,
						views Uint64,
						
						PRIMARY KEY(series_id,season_id,episode_id)
					)`,
				)
				require.NoError(t, err)
			})
		})
	})

	t.Run("upsert", func(t *testing.T) {
		t.Run("data", func(t *testing.T) {
			err := db.Query().Exec(ctx, `
				PRAGMA TablePathPrefix("`+path.Join(db.Name(), folder)+`");

				DECLARE $seriesData AS List<Struct<
					series_id: Uint64,
					title: Text,
					series_info: Text,
					release_date: Date,
					comment: Optional<Text>>>;

				DECLARE $seasonsData AS List<Struct<
					series_id: Uint64,
					season_id: Uint64,
					title: Text,
					first_aired: Date,
					last_aired: Date>>;

				DECLARE $episodesData AS List<Struct<
					series_id: Uint64,
					season_id: Uint64,
					episode_id: Uint64,
					title: Text,
					air_date: Date>>;

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
				FROM AS_TABLE($episodesData);`,
				query.WithParameters(ydb.ParamsBuilder().
					Param("$seriesData").Any(getSeriesData()).
					Param("$seasonsData").Any(getSeasonsData()).
					Param("$episodesData").Any(getEpisodesData()).
					Build(),
				),
				query.WithIdempotent(),
			)
			require.NoError(t, err)
		})
	})

	t.Run("increment", func(t *testing.T) {
		t.Run("views", func(t *testing.T) {
			err := db.Query().DoTx(ctx,
				func(ctx context.Context, tx query.TxActor) (err error) {
					// select current value of `views`
					row, err := tx.QueryRow(ctx, `
						PRAGMA TablePathPrefix("`+path.Join(db.Name(), folder)+`");

						DECLARE $seriesID AS Uint64;
						DECLARE $seasonID AS Uint64;
						DECLARE $episodeID AS Uint64;

						SELECT
							views
						FROM
							episodes
						WHERE
							series_id = $seriesID AND 
							season_id = $seasonID AND 
							episode_id = $episodeID;`,
						query.WithParameters(ydb.ParamsBuilder().
							Param("$seriesID").Uint64(1).
							Param("$seasonID").Uint64(1).
							Param("$episodeID").Uint64(1).
							Build(),
						),
					)
					if err != nil {
						return err
					}
					var views uint64
					if err = row.ScanNamed(query.Named("views", &views)); err != nil {
						return err
					}
					// increment `views`
					err = tx.Exec(ctx, `
						PRAGMA TablePathPrefix("`+path.Join(db.Name(), folder)+`");

						DECLARE $seriesID AS Uint64;
						DECLARE $seasonID AS Uint64;
						DECLARE $episodeID AS Uint64;
						DECLARE $views AS Uint64;

						UPSERT INTO episodes ( series_id, season_id, episode_id, views )
						VALUES ( $seriesID, $seasonID, $episodeID, $views );`,
						query.WithParameters(ydb.ParamsBuilder().
							Param("$seriesID").Uint64(1).
							Param("$seasonID").Uint64(1).
							Param("$episodeID").Uint64(1).
							Param("$views").Uint64(views+1).
							Build(),
						),
					)
					if err != nil {
						return err
					}
					return nil
				},
				query.WithIdempotent(),
			)
			require.NoError(t, err)
		})
	})

	t.Run("lookup", func(t *testing.T) {
		t.Run("views", func(t *testing.T) {
			row, err := db.Query().QueryRow(ctx, `
				PRAGMA TablePathPrefix("`+path.Join(db.Name(), folder)+`");

				DECLARE $seriesID AS Uint64;
				DECLARE $seasonID AS Uint64;
				DECLARE $episodeID AS Uint64;

				SELECT
					views
				FROM
					episodes
				WHERE
					series_id = $seriesID AND 
					season_id = $seasonID AND 
					episode_id = $episodeID;`,
				query.WithParameters(ydb.ParamsBuilder().
					Param("$seriesID").Uint64(1).
					Param("$seasonID").Uint64(1).
					Param("$episodeID").Uint64(1).
					Build(),
				),
			)
			require.NoError(t, err)
			var views uint64
			require.NoError(t, row.Scan(&views))
			require.EqualValues(t, 1, views)
		})
	})

	t.Run("sessions", func(t *testing.T) {
		t.Run("shutdown", func(t *testing.T) {
			urls := os.Getenv("YDB_SESSIONS_SHUTDOWN_URLS")
			if len(urls) > 0 {
				for _, url := range strings.Split(urls, ",") {
					//nolint:gosec
					_, err = http.Get(url)
					require.NoError(t, err)
				}
				shutdowned.Store(true)
			}
		})
	})

	t.Run("ExecuteDataQuery", func(t *testing.T) {
		row, err := db.Query().QueryRow(ctx, `
			PRAGMA TablePathPrefix("`+path.Join(db.Name(), folder)+`");

			DECLARE $seriesID AS Uint64;

			SELECT
				series_id,
				title,
				release_date
			FROM
				series
			WHERE
				series_id = $seriesID;`,
			query.WithParameters(ydb.ParamsBuilder().Param("$seriesID").Uint64(1).Build()),
			query.WithTxControl(query.SnapshotReadOnlyTxControl()),
		)
		require.NoError(t, err)
		var (
			id    *uint64
			title *string
			date  *time.Time
		)
		require.NoError(t, row.Scan(&id, &title, &date))
		t.Logf(
			"  > %d %s %s\n",
			*id, *title, *date,
		)
	})

	t.Run("ScanQuery", func(t *testing.T) {
		err := db.Query().Do(ctx, func(ctx context.Context, s query.Session) (err error) {
			res, err := s.Query(ctx, `
				PRAGMA TablePathPrefix("`+path.Join(db.Name(), folder)+`");

				DECLARE $series AS List<UInt64>;
	
				SELECT series_id, season_id, title, first_aired
				FROM seasons
				WHERE series_id IN $series;`,
				query.WithParameters(ydb.ParamsBuilder().
					Param("$series").BeginList().
					Add().Uint64(1).
					Add().Uint64(10).
					EndList().
					Build(),
				),
			)
			if err != nil {
				return err
			}
			defer func() {
				_ = res.Close(ctx)
			}()
			t.Logf("> scan_query_select:\n")
			for rs, err := range res.ResultSets(ctx) {
				if err != nil {
					return err
				}
				for row, err := range rs.Rows(ctx) {
					if err != nil {
						return err
					}
					var v struct {
						SeriesID uint64    `sql:"series_id"`
						SeasonID uint64    `sql:"season_id"`
						Title    string    `sql:"title"`
						Date     time.Time `sql:"first_aired"`
					}
					err = row.ScanStruct(&v)
					if err != nil {
						return err
					}
					t.Logf("  >  %v\n", v)
				}
			}

			return nil
		}, query.WithIdempotent())
		require.NoError(t, err)
	})
}
