//go:build integration
// +build integration

package integration

import (
	"context"
	"fmt"
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
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xtest"
	"github.com/ydb-platform/ydb-go-sdk/v3/log"
	"github.com/ydb-platform/ydb-go-sdk/v3/meta"
	"github.com/ydb-platform/ydb-go-sdk/v3/sugar"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result/named"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func TestBasicExampleNative(sourceTest *testing.T) { //nolint:gocyclo
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

	if err = db.Table().Do(ctx, func(ctx context.Context, _ table.Session) error {
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
				err := db.Table().Do(ctx,
					func(ctx context.Context, session table.Session) (err error) {
						if _, err = session.DescribeTable(ctx, path.Join(db.Name(), folder, "series")); err == nil {
							_ = session.DropTable(ctx, path.Join(db.Name(), folder, "series"))
						}
						return session.CreateTable(ctx, path.Join(db.Name(), folder, "series"),
							options.WithColumn("series_id", types.Optional(types.TypeUint64)),
							options.WithColumn("title", types.Optional(types.TypeText)),
							options.WithColumn("series_info", types.Optional(types.TypeText)),
							options.WithColumn("release_date", types.Optional(types.TypeDate)),
							options.WithColumn("comment", types.Optional(types.TypeText)),
							options.WithPrimaryKeyColumn("series_id"),
						)
					},
					table.WithIdempotent(),
				)
				require.NoError(t, err)
			})
			t.Run("seasons", func(t *testing.T) {
				err := db.Table().Do(ctx,
					func(ctx context.Context, session table.Session) (err error) {
						if _, err = session.DescribeTable(ctx, path.Join(db.Name(), folder, "seasons")); err == nil {
							_ = session.DropTable(ctx, path.Join(db.Name(), folder, "seasons"))
						}
						return session.CreateTable(ctx, path.Join(db.Name(), folder, "seasons"),
							options.WithColumn("series_id", types.Optional(types.TypeUint64)),
							options.WithColumn("season_id", types.Optional(types.TypeUint64)),
							options.WithColumn("title", types.Optional(types.TypeText)),
							options.WithColumn("first_aired", types.Optional(types.TypeDate)),
							options.WithColumn("last_aired", types.Optional(types.TypeDate)),
							options.WithPrimaryKeyColumn("series_id", "season_id"),
						)
					},
					table.WithIdempotent(),
				)
				require.NoError(t, err)
			})
			t.Run("episodes", func(t *testing.T) {
				err := db.Table().Do(ctx,
					func(ctx context.Context, session table.Session) (err error) {
						if _, err = session.DescribeTable(ctx, path.Join(db.Name(), folder, "episodes")); err == nil {
							_ = session.DropTable(ctx, path.Join(db.Name(), folder, "episodes"))
						}
						return session.CreateTable(ctx, path.Join(db.Name(), folder, "episodes"),
							options.WithColumn("series_id", types.Optional(types.TypeUint64)),
							options.WithColumn("season_id", types.Optional(types.TypeUint64)),
							options.WithColumn("episode_id", types.Optional(types.TypeUint64)),
							options.WithColumn("title", types.Optional(types.TypeText)),
							options.WithColumn("air_date", types.Optional(types.TypeDate)),
							options.WithColumn("views", types.Optional(types.TypeUint64)),
							options.WithPrimaryKeyColumn("series_id", "season_id", "episode_id"),
						)
					},
					table.WithIdempotent(),
				)
				require.NoError(t, err)
			})
		})
	})

	t.Run("describe", func(t *testing.T) {
		t.Run("table", func(t *testing.T) {
			t.Run("series", func(t *testing.T) {
				err := db.Table().Do(ctx,
					func(ctx context.Context, session table.Session) (err error) {
						_, err = session.DescribeTable(ctx, path.Join(db.Name(), folder, "series"))
						if err != nil {
							return
						}
						return err
					},
					table.WithIdempotent(),
				)
				require.NoError(t, err)
			})
			t.Run("seasons", func(t *testing.T) {
				err := db.Table().Do(ctx,
					func(ctx context.Context, session table.Session) (err error) {
						_, err = session.DescribeTable(ctx, path.Join(db.Name(), folder, "seasons"))
						if err != nil {
							return
						}
						return err
					},
					table.WithIdempotent(),
				)
				require.NoError(t, err)
			})
			t.Run("episodes", func(t *testing.T) {
				err := db.Table().Do(ctx,
					func(ctx context.Context, session table.Session) (err error) {
						_, err = session.DescribeTable(ctx, path.Join(db.Name(), folder, "episodes"))
						if err != nil {
							return
						}
						return err
					},
					table.WithIdempotent(),
				)
				require.NoError(t, err)
			})
		})
	})

	t.Run("upsert", func(t *testing.T) {
		t.Run("data", func(t *testing.T) {
			writeTx := table.TxControl(
				table.BeginTx(
					table.WithSerializableReadWrite(),
				),
				table.CommitTx(),
			)
			err := db.Table().Do(ctx,
				func(ctx context.Context, session table.Session) (err error) {
					stmt, err := session.Prepare(ctx, `
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
					)
					if err != nil {
						return err
					}

					_, _, err = stmt.Execute(ctx, writeTx, table.NewQueryParameters(
						table.ValueParam("$seriesData", getSeriesData()),
						table.ValueParam("$seasonsData", getSeasonsData()),
						table.ValueParam("$episodesData", getEpisodesData()),
					))
					return err
				},
				table.WithIdempotent(),
			)
			require.NoError(t, err)
		})
	})

	t.Run("increment", func(t *testing.T) {
		t.Run("views", func(t *testing.T) {
			err := db.Table().DoTx(ctx,
				func(ctx context.Context, tx table.TransactionActor) (err error) {
					var (
						res   result.Result
						views uint64
					)
					// select current value of `views`
					res, err = tx.Execute(ctx, `
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
						table.NewQueryParameters(
							table.ValueParam("$seriesID", types.Uint64Value(1)),
							table.ValueParam("$seasonID", types.Uint64Value(1)),
							table.ValueParam("$episodeID", types.Uint64Value(1)),
						),
					)
					if err != nil {
						return err
					}
					if err = res.NextResultSetErr(ctx); err != nil {
						return err
					}
					if !res.NextRow() {
						return fmt.Errorf("nothing rows")
					}
					if err = res.ScanNamed(
						named.OptionalWithDefault("views", &views),
					); err != nil {
						return err
					}
					if err = res.Err(); err != nil {
						return err
					}
					if err = res.Close(); err != nil {
						return err
					}
					// increment `views`
					res, err = tx.Execute(ctx, `
						PRAGMA TablePathPrefix("`+path.Join(db.Name(), folder)+`");

						DECLARE $seriesID AS Uint64;
						DECLARE $seasonID AS Uint64;
						DECLARE $episodeID AS Uint64;
						DECLARE $views AS Uint64;

						UPSERT INTO episodes ( series_id, season_id, episode_id, views )
						VALUES ( $seriesID, $seasonID, $episodeID, $views );`,
						table.NewQueryParameters(
							table.ValueParam("$seriesID", types.Uint64Value(1)),
							table.ValueParam("$seasonID", types.Uint64Value(1)),
							table.ValueParam("$episodeID", types.Uint64Value(1)),
							table.ValueParam("$views", types.Uint64Value(views+1)), // increment views
						),
					)
					if err != nil {
						return err
					}
					if err = res.Err(); err != nil {
						return err
					}
					return res.Close()
				},
				table.WithIdempotent(),
			)
			require.NoError(t, err)
		})
	})

	t.Run("lookup", func(t *testing.T) {
		t.Run("views", func(t *testing.T) {
			err = db.Table().Do(ctx,
				func(ctx context.Context, s table.Session) (err error) {
					var (
						res   result.Result
						views uint64
					)
					// select current value of `views`
					_, res, err = s.Execute(ctx,
						table.TxControl(
							table.BeginTx(
								table.WithOnlineReadOnly(),
							),
							table.CommitTx(),
						), `
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
						table.NewQueryParameters(
							table.ValueParam("$seriesID", types.Uint64Value(1)),
							table.ValueParam("$seasonID", types.Uint64Value(1)),
							table.ValueParam("$episodeID", types.Uint64Value(1)),
						),
					)
					if err != nil {
						return err
					}
					if !res.NextResultSet(ctx, "views") {
						return fmt.Errorf("nothing result sets")
					}
					if !res.NextRow() {
						return fmt.Errorf("nothing result rows")
					}
					if err = res.ScanWithDefaults(&views); err != nil {
						return err
					}
					if err = res.Err(); err != nil {
						return err
					}
					if err = res.Close(); err != nil {
						return err
					}
					if views != 1 {
						return fmt.Errorf("unexpected views value: %d", views)
					}
					return nil
				},
				table.WithIdempotent(),
			)
			require.NoError(t, err)
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
		var (
			query = `
					PRAGMA TablePathPrefix("` + path.Join(db.Name(), folder) + `");

					DECLARE $seriesID AS Uint64;

					SELECT
						series_id,
						title,
						release_date
					FROM
						series
					WHERE
						series_id = $seriesID;`
			readTx = table.TxControl(
				table.BeginTx(
					table.WithOnlineReadOnly(),
				),
				table.CommitTx(),
			)
		)
		err := db.Table().Do(ctx,
			func(ctx context.Context, s table.Session) (err error) {
				var (
					res   result.Result
					id    *uint64
					title *string
					date  *time.Time
				)
				_, res, err = s.Execute(ctx, readTx, query,
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
				t.Logf("> select_simple_transaction:\n")
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
						t.Logf(
							"  > %d %s %s\n",
							*id, *title, *date,
						)
					}
				}
				return res.Err()
			},
			table.WithIdempotent(),
		)
		if err != nil && !ydb.IsTimeoutError(err) {
			require.NoError(t, err)
		}
	})

	t.Run("StreamExecuteScanQuery", func(t *testing.T) {
		query := `
			PRAGMA TablePathPrefix("` + path.Join(db.Name(), folder) + `");

			DECLARE $series AS List<UInt64>;

			SELECT series_id, season_id, title, first_aired
			FROM seasons
			WHERE series_id IN $series;`
		err := db.Table().Do(ctx,
			func(ctx context.Context, s table.Session) (err error) {
				var (
					res      result.StreamResult
					seriesID uint64
					seasonID uint64
					title    string
					date     time.Time
				)
				res, err = s.StreamExecuteScanQuery(ctx, query,
					table.NewQueryParameters(
						table.ValueParam("$series",
							types.ListValue(
								types.Uint64Value(1),
								types.Uint64Value(10),
							),
						),
					),
				)
				if err != nil {
					return err
				}
				defer func() {
					_ = res.Close()
				}()
				t.Logf("> scan_query_select:\n")
				for res.NextResultSet(ctx) {
					for res.NextRow() {
						err = res.ScanWithDefaults(&seriesID, &seasonID, &title, &date)
						if err != nil {
							return err
						}
						t.Logf("  > SeriesId: %d, SeasonId: %d, Title: %s, Air date: %s\n", seriesID, seasonID, title, date)
					}
				}
				return res.Err()
			},
			table.WithIdempotent(),
		)
		require.NoError(t, err)
	})

	t.Run("StreamReadTable", func(t *testing.T) {
		err := db.Table().Do(ctx,
			func(ctx context.Context, s table.Session) (err error) {
				var (
					res   result.StreamResult
					id    *uint64
					title *string
					date  *time.Time
				)
				res, err = s.StreamReadTable(ctx, path.Join(db.Name(), folder, "series"),
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
				for res.NextResultSet(ctx, "series_id", "title", "release_date") {
					for res.NextRow() {
						err = res.Scan(&id, &title, &date)
						if err != nil {
							return err
						}
						// t.Logf("  > %d %s %s\n", *id, *title, date.String())
					}
				}
				if err = res.Err(); err != nil {
					return err
				}

				if stats := res.Stats(); stats != nil {
					for i := 0; ; i++ {
						phase, ok := stats.NextPhase()
						if !ok {
							break
						}
						for {
							tbl, ok := phase.NextTableAccess()
							if !ok {
								break
							}
							t.Logf(
								"#  accessed %s: read=(%drows, %dbytes)\n",
								tbl.Name, tbl.Reads.Rows, tbl.Reads.Bytes,
							)
						}
					}
				}

				return res.Err()
			},
			table.WithIdempotent(),
		)
		require.NoError(t, err)
	})
}
