//go:build !fast
// +build !fast

package test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"path"
	"sync"
	"testing"
	"text/template"
	"time"

	"google.golang.org/grpc"

	ydb "github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/balancer"
	"github.com/ydb-platform/ydb-go-sdk/v3/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/sugar"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result/named"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

type stats struct {
	sync.Mutex

	keepAliveMinSize int
	inFlight         int
	balance          int
	limit            int

	waited int
}

func (s *stats) print(t *testing.T) {
	s.Lock()
	defer s.Unlock()
	t.Log("stats:")
	t.Log(" - keepAliveMinSize      :", s.keepAliveMinSize)
	t.Log(" - in_flight:", s.keepAliveMinSize)
	t.Log(" - balance  :", s.keepAliveMinSize)
	t.Log(" - limit      :", s.limit)
	t.Log(" - waited   :", s.waited)
}

func (s *stats) check(t *testing.T) {
	s.Lock()
	defer s.Unlock()
	if s.keepAliveMinSize > s.inFlight {
		t.Fatalf("keepAliveMinSize > in_flight (%d > %d)", s.keepAliveMinSize, s.inFlight)
	}
	if s.inFlight > s.balance {
		t.Fatalf("in_flight > balance (%d > %d)", s.inFlight, s.balance)
	}
	if s.balance > s.limit {
		t.Fatalf("balance > limit (%d > %d)", s.balance, s.limit)
	}
}

func (s *stats) min() int {
	s.Lock()
	defer s.Unlock()
	return s.keepAliveMinSize
}

func (s *stats) max() int {
	s.Lock()
	defer s.Unlock()
	return s.limit
}

func (s *stats) addBalance(t *testing.T, delta int) {
	defer s.check(t)
	s.Lock()
	s.balance += delta
	s.Unlock()
}

func (s *stats) addInFlight(t *testing.T, delta int) {
	defer s.check(t)
	s.Lock()
	s.inFlight += delta
	s.Unlock()
}

// nolint:gocyclo
func TestTable(t *testing.T) {
	folder := "pool_health"

	ctx, cancel := context.WithTimeout(context.Background(), 55*time.Second)
	defer cancel()

	s := &stats{
		keepAliveMinSize: math.MinInt32,
		limit:            math.MaxInt32,
	}
	defer t.Run("check stats", func(t *testing.T) {
		s.Lock()
		defer s.Unlock()
		if s.inFlight != 0 {
			t.Fatalf("inFlight not a zero after closing pool: %d", s.inFlight)
		}
		if s.balance != 0 {
			t.Fatalf("balance not a zero after closing pool: %d", s.balance)
		}
		if s.waited != 0 {
			t.Fatalf("waited not a zero after closing pool: %d", s.waited)
		}
	})

	var (
		err   error
		db    ydb.Connection
		limit = 50
	)
	t.Run("connect", func(t *testing.T) {
		db, err = ydb.New(
			ctx,
			ydb.WithConnectionString(os.Getenv("YDB_CONNECTION_STRING")),
			ydb.WithAnonymousCredentials(),
			ydb.WithUserAgent("tx"),
			ydb.With(
				config.WithRequestTimeout(time.Second*5),
				config.WithStreamTimeout(time.Second*5),
				config.WithOperationTimeout(time.Second*5),
				config.WithOperationCancelAfter(time.Second*5),
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
			ydb.WithBalancer(balancer.PreferLocalDCWithFallBack( // for max tests coverage
				balancer.PreferLocationsWithFallback( // for max tests coverage
					balancer.RoundRobin(),
					"MAN",
				),
			)),
			ydb.WithDialTimeout(5*time.Second),
			ydb.WithSessionPoolIdleThreshold(time.Second*5),
			ydb.WithSessionPoolSizeLimit(limit),
			ydb.WithSessionPoolKeepAliveMinSize(-1),
			ydb.WithDiscoveryInterval(5*time.Second),
			ydb.WithLogger(
				trace.DetailsAll,
				ydb.WithNamespace("ydb"),
				ydb.WithOutWriter(os.Stdout),
				ydb.WithErrWriter(os.Stderr),
				ydb.WithMinLevel(ydb.INFO),
			),
			ydb.WithTraceTable(trace.Table{
				OnSessionNew: func(info trace.SessionNewStartInfo) func(trace.SessionNewDoneInfo) {
					return func(info trace.SessionNewDoneInfo) {
						if info.Error == nil {
							s.addBalance(t, 1)
						}
					}
				},
				OnSessionDelete: func(info trace.SessionDeleteStartInfo) func(trace.SessionDeleteDoneInfo) {
					return func(info trace.SessionDeleteDoneInfo) {
						s.addBalance(t, -1)
					}
				},
				OnPoolInit: func(info trace.PoolInitStartInfo) func(trace.PoolInitDoneInfo) {
					return func(info trace.PoolInitDoneInfo) {
						s.Lock()
						s.keepAliveMinSize = info.KeepAliveMinSize
						s.limit = info.Limit
						s.Unlock()
					}
				},
				OnPoolGet: func(info trace.PoolGetStartInfo) func(trace.PoolGetDoneInfo) {
					return func(info trace.PoolGetDoneInfo) {
						if info.Error == nil {
							s.addInFlight(t, 1)
						}
					}
				},
				OnPoolPut: func(info trace.PoolPutStartInfo) func(trace.PoolPutDoneInfo) {
					s.addInFlight(t, -1)
					return nil
				},
				OnPoolSessionClose: func(info trace.PoolSessionCloseStartInfo) func(trace.PoolSessionCloseDoneInfo) {
					return func(info trace.PoolSessionCloseDoneInfo) {
						s.addInFlight(t, -1)
					}
				},
			}),
		)
		if err != nil {
			t.Fatal(err)
		}
	})
	defer t.Run("cleanup", func(t *testing.T) {
		if e := db.Close(ctx); e != nil {
			t.Fatalf("db close failed: %+v", e)
		}
	})
	t.Run("ping", func(t *testing.T) {
		if err = db.Table().Do(ctx, func(ctx context.Context, _ table.Session) error {
			// hack for wait pool initializing
			return nil
		}); err != nil {
			t.Fatalf("pool not initialized: %+v", err)
		}
	})
	t.Run("pool init check", func(t *testing.T) {
		if s.min() < 0 || s.max() != limit {
			t.Fatalf("pool sizes not applied: %+v", s)
		}
	})
	t.Run("prepare scheme", func(t *testing.T) {
		err := sugar.RemoveRecursive(ctx, db, folder)
		if err != nil {
			t.Fatal(err)
		}
		err = sugar.MakeRecursive(ctx, db, folder)
		if err != nil {
			t.Fatal(err)
		}
		err = describeTableOptions(ctx, db.Table())
		if err != nil {
			t.Fatal(err)
		}

		err = createTables(ctx, db.Table(), path.Join(db.Name(), folder))
		if err != nil {
			t.Fatal(err)
		}

		err = describeTable(ctx, db.Table(), path.Join(db.Name(), folder, "series"))
		if err != nil {
			t.Fatal(err)
		}

		err = describeTable(ctx, db.Table(), path.Join(db.Name(), folder, "seasons"))
		if err != nil {
			t.Fatal(err)
		}

		err = describeTable(ctx, db.Table(), path.Join(db.Name(), folder, "episodes"))
		if err != nil {
			t.Fatal(err)
		}
	})
	t.Run("fill data", func(t *testing.T) {
		if err := fill(ctx, db, folder); err != nil {
			t.Fatalf("fillQuery failed: %v\n", err)
		}
	})
	t.Run("upsert with tx", func(t *testing.T) {
		if err := db.Table().DoTx(
			ctx,
			func(ctx context.Context, tx table.TransactionActor) (err error) {
				var (
					res   result.Result
					views uint64
				)
				// select current value of `views`
				res, err = tx.Execute(
					ctx,
					render(
						querySelect,
						templateConfig{
							TablePathPrefix: path.Join(db.Name(), folder),
						},
					),
					table.NewQueryParameters(
						table.ValueParam("$seriesID", types.Uint64Value(1)),
						table.ValueParam("$seasonID", types.Uint64Value(1)),
						table.ValueParam("$episodeID", types.Uint64Value(1)),
					),
					options.WithQueryCachePolicy(
						options.WithQueryCachePolicyKeepInCache(),
					),
				)
				if err != nil {
					return err
				}
				if err = res.NextResultSet(ctx); errors.Is(err, io.EOF) {
					return fmt.Errorf("nothing result sets")
				}
				if !res.NextRow() {
					return fmt.Errorf("nothing result rows")
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
				res, err = tx.Execute(
					ctx,
					render(
						queryUpsert,
						templateConfig{
							TablePathPrefix: path.Join(db.Name(), folder),
						},
					),
					table.NewQueryParameters(
						table.ValueParam("$seriesID", types.Uint64Value(1)),
						table.ValueParam("$seasonID", types.Uint64Value(1)),
						table.ValueParam("$episodeID", types.Uint64Value(1)),
						table.ValueParam("$views", types.Uint64Value(views+1)), // increment views
					),
					options.WithQueryCachePolicy(
						options.WithQueryCachePolicyKeepInCache(),
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
			table.WithTxSettings(
				table.TxSettings(
					table.WithSerializableReadWrite(),
				),
			),
		); err != nil {
			t.Fatalf("tx failed: %v\n", err)
		}
	})
	t.Run("select upserted data", func(t *testing.T) {
		if err := db.Table().Do(
			ctx,
			func(ctx context.Context, s table.Session) (err error) {
				var (
					res   result.Result
					views uint64
				)
				// select current value of `views`
				_, res, err = s.Execute(
					ctx,
					table.TxControl(
						table.BeginTx(
							table.WithOnlineReadOnly(),
						),
						table.CommitTx(),
					),
					render(
						querySelect,
						templateConfig{
							TablePathPrefix: path.Join(db.Name(), folder),
						},
					),
					table.NewQueryParameters(
						table.ValueParam("$seriesID", types.Uint64Value(1)),
						table.ValueParam("$seasonID", types.Uint64Value(1)),
						table.ValueParam("$episodeID", types.Uint64Value(1)),
					),
					options.WithQueryCachePolicy(
						options.WithQueryCachePolicyKeepInCache(),
					),
				)
				if err != nil {
					return err
				}
				if err = res.NextResultSet(ctx, "views"); errors.Is(err, io.EOF) {
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
		); err != nil {
			t.Fatalf("tx failed: %v\n", err)
		}
	})
	t.Run("select concurrently", func(t *testing.T) {
		wg := sync.WaitGroup{}
		for i := 0; i < limit; i++ {
			wg.Add(3)
			go func() {
				defer wg.Done()
				for {
					select {
					case <-ctx.Done():
						return
					default:
						selectExecuteDataQuery(ctx, t, db.Table(), path.Join(db.Name(), folder))
					}
				}
			}()
			go func() {
				defer wg.Done()
				for {
					select {
					case <-ctx.Done():
						return
					default:
						selectExecuteScanQuery(ctx, t, db.Table(), path.Join(db.Name(), folder))
					}
				}
			}()
			go func() {
				defer wg.Done()
				for {
					select {
					case <-ctx.Done():
						return
					default:
						selectReadTable(ctx, t, db.Table(), path.Join(db.Name(), folder, "series"))
					}
				}
			}()
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				default:
					s.check(t)
				}
			}
		}()
		wg.Wait()
	})
}

func selectReadTable(ctx context.Context, t *testing.T, c table.Client, tableAbsPath string) {
	err := c.Do(
		ctx,
		func(ctx context.Context, s table.Session) (err error) {
			var (
				res   result.StreamResult
				id    *uint64
				title *string
				date  *time.Time
			)
			res, err = s.StreamReadTable(ctx, tableAbsPath,
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
			log.Printf("> read_table:\n")
			for ; err == nil; err = res.NextResultSet(ctx, "series_id", "title", "release_date") {
				for res.NextRow() {
					err = res.Scan(&id, &title, &date)
					if err != nil {
						return err
					}
					log.Printf("  > %d %s %s", *id, *title, date.String())
				}
			}
			if err := res.Err(); err != nil {
				return err
			}
			stats := res.Stats()
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
			return nil
		},
	)
	if err != nil && !ydb.IsTimeoutError(err) {
		t.Fatalf("read table error: %+v", err)
	}
}

func selectExecuteDataQuery(ctx context.Context, t *testing.T, c table.Client, folderAbsPath string) {
	var (
		query = render(
			template.Must(template.New("").Parse(`
			PRAGMA TablePathPrefix("{{ .TablePathPrefix }}");
			DECLARE $seriesID AS Uint64;
			SELECT
				series_id,
				title,
				release_date
			FROM
				series
			WHERE
				series_id = $seriesID;
		`)),
			templateConfig{
				TablePathPrefix: folderAbsPath,
			},
		)
		readTx = table.TxControl(
			table.BeginTx(
				table.WithOnlineReadOnly(),
			),
			table.CommitTx(),
		)
	)
	err := c.Do(
		ctx,
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
				options.WithQueryCachePolicy(
					options.WithQueryCachePolicyKeepInCache(),
				),
				options.WithCollectStatsModeBasic(),
			)
			if err != nil {
				return err
			}
			defer func() {
				_ = res.Close()
			}()
			log.Printf("> select_simple_transaction:\n")
			for ; err == nil; err = res.NextResultSet(ctx) {
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
						"  > %d %s %s\n",
						*id, *title, *date,
					)
				}
			}
			return res.Err()
		},
	)
	if err != nil && !ydb.IsTimeoutError(err) {
		t.Fatalf("select simple error: %+v", err)
	}
}

func selectExecuteScanQuery(ctx context.Context, t *testing.T, c table.Client, folderAbsPath string) {
	query := render(
		template.Must(template.New("").Parse(`
				PRAGMA TablePathPrefix("{{ .TablePathPrefix }}");
	
				DECLARE $series AS List<UInt64>;
	
				SELECT series_id, season_id, title, first_aired
				FROM seasons
				WHERE series_id IN $series
			`)),
		templateConfig{
			TablePathPrefix: folderAbsPath,
		},
	)
	err := c.Do(
		ctx,
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
			log.Printf("> scan_query_select:\n")
			for ; err == nil; err = res.NextResultSet(ctx) {
				for res.NextRow() {
					err = res.ScanWithDefaults(&seriesID, &seasonID, &title, &date)
					if err != nil {
						return err
					}
					log.Printf("  > SeriesId: %d, SeasonId: %d, Title: %s, Air date: %s", seriesID, seasonID, title, date)
				}
			}
			return res.Err()
		},
	)
	if err != nil && !ydb.IsTimeoutError(err) {
		t.Fatalf("scan query error: %+v", err)
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
		types.StructFieldValue("series_id", types.Uint64Value(id)),
		types.StructFieldValue("release_date", types.DateValueFromTime(released)),
		types.StructFieldValue("title", types.UTF8Value(title)),
		types.StructFieldValue("series_info", types.UTF8Value(info)),
		types.StructFieldValue("comment", commentv),
	)
}

func seasonData(seriesID, seasonID uint64, title string, first, last time.Time) types.Value {
	return types.StructValue(
		types.StructFieldValue("series_id", types.Uint64Value(seriesID)),
		types.StructFieldValue("season_id", types.Uint64Value(seasonID)),
		types.StructFieldValue("title", types.UTF8Value(title)),
		types.StructFieldValue("first_aired", types.DateValueFromTime(first)),
		types.StructFieldValue("last_aired", types.DateValueFromTime(last)),
	)
}

func episodeData(seriesID, seasonID, episodeID uint64, title string, date time.Time) types.Value {
	return types.StructValue(
		types.StructFieldValue("series_id", types.Uint64Value(seriesID)),
		types.StructFieldValue("season_id", types.Uint64Value(seasonID)),
		types.StructFieldValue("episode_id", types.Uint64Value(episodeID)),
		types.StructFieldValue("title", types.UTF8Value(title)),
		types.StructFieldValue("air_date", types.DateValueFromTime(date)),
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

func init() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
}

type templateConfig struct {
	TablePathPrefix string
}

var (
	fillQuery = template.Must(template.New("fillQuery database").Parse(`
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

func describeTableOptions(ctx context.Context, c table.Client) error {
	var desc options.TableOptionsDescription
	err := c.Do(
		ctx,
		func(ctx context.Context, s table.Session) (err error) {
			desc, err = s.DescribeTableOptions(ctx)
			return
		},
	)
	if err != nil {
		return err
	}
	log.Println("> describe_table_options:")

	for i, p := range desc.TableProfilePresets {
		log.Printf("  > TableProfilePresets: %d/%d: %+v", i+1, len(desc.TableProfilePresets), p)
	}
	for i, p := range desc.StoragePolicyPresets {
		log.Printf("  > StoragePolicyPresets: %d/%d: %+v", i+1, len(desc.StoragePolicyPresets), p)
	}
	for i, p := range desc.CompactionPolicyPresets {
		log.Printf("  > CompactionPolicyPresets: %d/%d: %+v", i+1, len(desc.CompactionPolicyPresets), p)
	}
	for i, p := range desc.PartitioningPolicyPresets {
		log.Printf("  > PartitioningPolicyPresets: %d/%d: %+v", i+1, len(desc.PartitioningPolicyPresets), p)
	}
	for i, p := range desc.ExecutionPolicyPresets {
		log.Printf("  > ExecutionPolicyPresets: %d/%d: %+v", i+1, len(desc.ExecutionPolicyPresets), p)
	}
	for i, p := range desc.ReplicationPolicyPresets {
		log.Printf("  > ReplicationPolicyPresets: %d/%d: %+v", i+1, len(desc.ReplicationPolicyPresets), p)
	}
	for i, p := range desc.CachingPolicyPresets {
		log.Printf("  > CachingPolicyPresets: %d/%d: %+v", i+1, len(desc.CachingPolicyPresets), p)
	}

	return nil
}

func fill(ctx context.Context, db ydb.Connection, folder string) error {
	// prepare write transaction.
	writeTx := table.TxControl(
		table.BeginTx(
			table.WithSerializableReadWrite(),
		),
		table.CommitTx(),
	)
	return db.Table().Do(
		ctx,
		func(ctx context.Context, s table.Session) (err error) {
			stmt, err := s.Prepare(ctx, render(fillQuery, templateConfig{
				TablePathPrefix: path.Join(db.Name(), folder),
			}))
			if err != nil {
				return
			}
			_, _, err = stmt.Execute(ctx, writeTx, table.NewQueryParameters(
				table.ValueParam("$seriesData", getSeriesData()),
				table.ValueParam("$seasonsData", getSeasonsData()),
				table.ValueParam("$episodesData", getEpisodesData()),
			))
			return
		},
	)
}

func createTables(ctx context.Context, c table.Client, folder string) error {
	err := c.Do(
		ctx,
		func(ctx context.Context, s table.Session) (err error) {
			return s.CreateTable(ctx, path.Join(folder, "series"),
				options.WithColumn("series_id", types.Optional(types.TypeUint64)),
				options.WithColumn("title", types.Optional(types.TypeUTF8)),
				options.WithColumn("series_info", types.Optional(types.TypeUTF8)),
				options.WithColumn("release_date", types.Optional(types.TypeDate)),
				options.WithColumn("comment", types.Optional(types.TypeUTF8)),
				options.WithPrimaryKeyColumn("series_id"),
			)
		},
	)
	if err != nil {
		return err
	}

	err = c.Do(
		ctx,
		func(ctx context.Context, s table.Session) (err error) {
			return s.CreateTable(ctx, path.Join(folder, "seasons"),
				options.WithColumn("series_id", types.Optional(types.TypeUint64)),
				options.WithColumn("season_id", types.Optional(types.TypeUint64)),
				options.WithColumn("title", types.Optional(types.TypeUTF8)),
				options.WithColumn("first_aired", types.Optional(types.TypeDate)),
				options.WithColumn("last_aired", types.Optional(types.TypeDate)),
				options.WithPrimaryKeyColumn("series_id", "season_id"),
			)
		},
	)
	if err != nil {
		return err
	}

	err = c.Do(
		ctx,
		func(ctx context.Context, s table.Session) (err error) {
			return s.CreateTable(ctx, path.Join(folder, "episodes"),
				options.WithColumn("series_id", types.Optional(types.TypeUint64)),
				options.WithColumn("season_id", types.Optional(types.TypeUint64)),
				options.WithColumn("episode_id", types.Optional(types.TypeUint64)),
				options.WithColumn("title", types.Optional(types.TypeUTF8)),
				options.WithColumn("air_date", types.Optional(types.TypeDate)),
				options.WithColumn("views", types.Optional(types.TypeUint64)),
				options.WithPrimaryKeyColumn("series_id", "season_id", "episode_id"),
			)
		},
	)
	return err
}

func describeTable(ctx context.Context, c table.Client, path string) (err error) {
	err = c.Do(
		ctx,
		func(ctx context.Context, s table.Session) (err error) {
			desc, err := s.DescribeTable(ctx, path)
			if err != nil {
				return
			}
			log.Printf("> describe table: %s\n", path)
			for _, c := range desc.Columns {
				log.Printf("  > column, name: %s, %s\n", c.Type, c.Name)
			}
			for i, keyRange := range desc.KeyRanges {
				log.Printf("  > key range %d: %s\n", i, keyRange.String())
			}
			return
		},
	)
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
