//go:build integration
// +build integration

package integration

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xtest"
	"github.com/ydb-platform/ydb-go-sdk/v3/meta"
	"github.com/ydb-platform/ydb-go-sdk/v3/retry"
	"github.com/ydb-platform/ydb-go-sdk/v3/sugar"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func TestBasicExampleDatabaseSql(t *testing.T) {
	folder := t.Name()

	ctx, cancel := context.WithTimeout(xtest.Context(t), 42*time.Second)
	defer cancel()

	var totalConsumedUnits atomic.Uint64
	defer func() {
		t.Logf("total consumed units: %d", totalConsumedUnits.Load())
	}()

	ctx = meta.WithTrailerCallback(ctx, func(md metadata.MD) {
		totalConsumedUnits.Add(meta.ConsumedUnits(md))
	})

	t.Run("sql.Open", func(t *testing.T) {
		db, err := sql.Open("ydb", os.Getenv("YDB_CONNECTION_STRING"))
		require.NoError(t, err)

		err = db.PingContext(ctx)
		require.NoError(t, err)

		_, err = ydb.Unwrap(db)
		require.NoError(t, err)

		err = db.Close()
		require.NoError(t, err)
	})

	t.Run("sql.OpenDB", func(t *testing.T) {
		nativeDriver, err := ydb.Open(ctx, os.Getenv("YDB_CONNECTION_STRING"),
			withMetrics(t, trace.DetailsAll, 0),
			ydb.WithDiscoveryInterval(time.Second),
		)
		require.NoError(t, err)

		defer func() {
			// cleanup
			_ = nativeDriver.Close(ctx)
		}()

		c, err := ydb.Connector(nativeDriver)
		require.NoError(t, err)

		defer func() {
			// cleanup
			_ = c.Close()
		}()

		db := sql.OpenDB(c)
		defer func() {
			// cleanup
			_ = db.Close()
		}()

		err = db.PingContext(ctx)
		require.NoError(t, err)

		db.SetMaxOpenConns(50)
		db.SetMaxIdleConns(50)

		t.Run("prepare", func(t *testing.T) {
			t.Run("scheme", func(t *testing.T) {
				err = sugar.RemoveRecursive(ctx, nativeDriver, folder)
				require.NoError(t, err)

				err = sugar.MakeRecursive(ctx, nativeDriver, folder)
				require.NoError(t, err)

				t.Run("series", func(t *testing.T) {
					var (
						ctx       = ydb.WithQueryMode(ctx, ydb.SchemeQueryMode)
						exists    bool
						tablePath = path.Join(nativeDriver.Name(), folder, "series")
					)

					exists, err = sugar.IsTableExists(ctx, nativeDriver.Scheme(), tablePath)
					require.NoError(t, err)

					if exists {
						_, err = db.ExecContext(ctx, `DROP TABLE `+"`"+tablePath+"`"+`;`)
						require.NoError(t, err)
					}

					_, err = db.ExecContext(ctx, `
						CREATE TABLE `+"`"+tablePath+"`"+` (
							series_id Uint64,
							title UTF8,
							series_info UTF8,
							release_date Date,
							comment UTF8,
							PRIMARY KEY (
								series_id
							)
						);
					`)
					require.NoError(t, err)
				})
				t.Run("seasons", func(t *testing.T) {
					var (
						ctx       = ydb.WithQueryMode(ctx, ydb.SchemeQueryMode)
						exists    bool
						tablePath = path.Join(nativeDriver.Name(), folder, "seasons")
					)

					exists, err = sugar.IsTableExists(ctx, nativeDriver.Scheme(), tablePath)
					require.NoError(t, err)

					if exists {
						_, err = db.ExecContext(ctx, `DROP TABLE `+"`"+tablePath+"`"+`;`)
						require.NoError(t, err)
					}

					_, err = db.ExecContext(ctx, `
						CREATE TABLE `+"`"+tablePath+"`"+` (
							series_id Uint64,
							season_id Uint64,
							title UTF8,
							first_aired Date,
							last_aired Date,
							PRIMARY KEY (
								series_id,
								season_id
							)
						);
					`)
					require.NoError(t, err)
				})
				t.Run("episodes", func(t *testing.T) {
					var (
						ctx       = ydb.WithQueryMode(ctx, ydb.SchemeQueryMode)
						exists    bool
						tablePath = path.Join(nativeDriver.Name(), folder, "episodes")
					)

					exists, err = sugar.IsTableExists(ctx, nativeDriver.Scheme(), tablePath)
					require.NoError(t, err)

					if exists {
						_, err = db.ExecContext(ctx, `DROP TABLE `+"`"+tablePath+"`"+`;`)
						require.NoError(t, err)
					}

					_, err = db.ExecContext(ctx, `
						CREATE TABLE `+"`"+tablePath+"`"+` (
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
						);
					`)
					require.NoError(t, err)
				})
			})
		})

		t.Run("batch", func(t *testing.T) {
			t.Run("upsert", func(t *testing.T) {
				err = retry.Do(ctx, db, func(ctx context.Context, cc *sql.Conn) error {
					stmt, err := cc.PrepareContext(ctx, `
						PRAGMA TablePathPrefix("`+path.Join(nativeDriver.Name(), folder)+`");
						
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
						
						REPLACE INTO series SELECT * FROM AS_TABLE($seriesData);

						REPLACE INTO seasons SELECT * FROM AS_TABLE($seasonsData);

						REPLACE INTO episodes SELECT * FROM AS_TABLE($episodesData);
					`)
					if err != nil {
						return fmt.Errorf("failed to prepare query: %w", err)
					}
					_, err = stmt.ExecContext(ctx,
						sql.Named("seriesData", getSeriesData()),
						sql.Named("seasonsData", getSeasonsData()),
						sql.Named("episodesData", getEpisodesData()),
					)
					if err != nil {
						return fmt.Errorf("failed to execute statement: %w", err)
					}
					return nil
				}, retry.WithIdempotent(true))
				require.NoError(t, err)
			})
		})

		t.Run("query", func(t *testing.T) {
			query := `
				PRAGMA TablePathPrefix("` + path.Join(nativeDriver.Name(), folder) + `");

				DECLARE $seriesID AS Uint64;
				DECLARE $seasonID AS Uint64;
				DECLARE $episodeID AS Uint64;

				SELECT views 
				FROM episodes 
				WHERE 
					series_id = $seriesID AND 
					season_id = $seasonID AND 
					episode_id = $episodeID;`
			t.Run("explain", func(t *testing.T) {
				row := db.QueryRowContext(
					ydb.WithQueryMode(ctx, ydb.ExplainQueryMode), query,
					sql.Named("seriesID", uint64(1)),
					sql.Named("seasonID", uint64(1)),
					sql.Named("episodeID", uint64(1)),
				)
				var (
					ast  string
					plan string
				)

				err = row.Scan(&ast, &plan)
				require.NoError(t, err)

				t.Logf("ast = %v", ast)
				t.Logf("plan = %v", plan)
			})
			t.Run("increment", func(t *testing.T) {
				t.Run("views", func(t *testing.T) {
					err = retry.DoTx(ctx, db, func(ctx context.Context, tx *sql.Tx) (err error) {
						var stmt *sql.Stmt
						stmt, err = tx.PrepareContext(ctx, query)
						if err != nil {
							return fmt.Errorf("cannot prepare query: %w", err)
						}

						row := stmt.QueryRowContext(ctx,
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
						// increment `views`
						_, err = tx.ExecContext(ctx, `
								PRAGMA TablePathPrefix("`+path.Join(nativeDriver.Name(), folder)+`");
				
								DECLARE $seriesID AS Uint64;
								DECLARE $seasonID AS Uint64;
								DECLARE $episodeID AS Uint64;
								DECLARE $views AS Uint64;
				
								UPSERT INTO episodes ( series_id, season_id, episode_id, views )
								VALUES ( $seriesID, $seasonID, $episodeID, $views );
							`,
							sql.Named("seriesID", uint64(1)),
							sql.Named("seasonID", uint64(1)),
							sql.Named("episodeID", uint64(1)),
							sql.Named("views", uint64(views.Float64+1)), // increment views
						)
						if err != nil {
							return fmt.Errorf("cannot upsert views: %w", err)
						}
						return nil
					}, retry.WithIdempotent(true))
					require.NoError(t, err)
				})
			})
			t.Run("select", func(t *testing.T) {
				t.Run("isolation", func(t *testing.T) {
					t.Run("snapshot", func(t *testing.T) {
						query := `
							PRAGMA TablePathPrefix("` + path.Join(nativeDriver.Name(), folder) + `");
				
							DECLARE $seriesID AS Uint64;
							DECLARE $seasonID AS Uint64;
							DECLARE $episodeID AS Uint64;
			
							SELECT views FROM episodes 
							WHERE 
								series_id = $seriesID AND 
								season_id = $seasonID AND 
								episode_id = $episodeID;
						`
						err = retry.DoTx(ctx, db,
							func(ctx context.Context, tx *sql.Tx) error {
								row := tx.QueryRowContext(ctx, query,
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
								if views.Float64 != 1 {
									return fmt.Errorf("unexpected views value: %v", views)
								}
								return nil
							},
							retry.WithIdempotent(true),
							retry.WithTxOptions(&sql.TxOptions{
								Isolation: sql.LevelSnapshot,
								ReadOnly:  true,
							}),
						)
						if !errors.Is(err, context.DeadlineExceeded) {
							require.NoError(t, err)
						}
					})
				})
				t.Run("scan", func(t *testing.T) {
					t.Run("query", func(t *testing.T) {
						var (
							seriesID  *uint64
							seasonID  *uint64
							episodeID *uint64
							title     *string
							airDate   *time.Time
							views     sql.NullFloat64
							query     = `
								PRAGMA TablePathPrefix("` + path.Join(nativeDriver.Name(), folder) + `");
					
								DECLARE $seriesID AS Optional<Uint64>;
								DECLARE $seasonID AS Optional<Uint64>;
								DECLARE $episodeID AS Optional<Uint64>;
				
								SELECT 
									series_id,
									season_id,
									episode_id,
									title,
									air_date,
									views
								FROM episodes
								WHERE 
									(series_id >= $seriesID OR $seriesID IS NULL) AND
									(season_id >= $seasonID OR $seasonID IS NULL) AND
									(episode_id >= $episodeID OR $episodeID IS NULL) 
								ORDER BY 
									series_id, season_id, episode_id;
							`
						)
						err := retry.DoTx(ctx, db,
							func(ctx context.Context, tx *sql.Tx) error {
								rows, err := tx.QueryContext(ctx, query,
									sql.Named("seriesID", seriesID),
									sql.Named("seasonID", seasonID),
									sql.Named("episodeID", episodeID),
								)
								if err != nil {
									return err
								}
								defer func() {
									_ = rows.Close()
								}()
								for rows.NextResultSet() {
									for rows.Next() {
										if err = rows.Scan(&seriesID, &seasonID, &episodeID, &title, &airDate, &views); err != nil {
											return fmt.Errorf("cannot select current views: %w", err)
										}
										t.Logf("[%d][%d][%d] - %s %q (%d views)",
											*seriesID, *seasonID, *episodeID, airDate.Format("2006-01-02"),
											*title, uint64(views.Float64),
										)
									}
								}
								return rows.Err()
							},
							retry.WithIdempotent(true),
							retry.WithTxOptions(&sql.TxOptions{Isolation: sql.LevelSnapshot, ReadOnly: true}),
						)
						if !errors.Is(err, context.DeadlineExceeded) {
							require.NoError(t, err)
						}
					})
				})
			})
		})
	})
}
