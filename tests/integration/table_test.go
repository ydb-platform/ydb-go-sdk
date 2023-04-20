//go:build !fast
// +build !fast

package integration

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"net/http"
	"os"
	"path"
	"runtime/debug"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"text/template"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	grpcCodes "google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/balancers"
	"github.com/ydb-platform/ydb-go-sdk/v3/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xsync"
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

type tableTestScope struct {
	folder string
	db     *ydb.Driver
}

type stats struct {
	xsync.Mutex

	inFlightSessions map[string]struct{}
	openSessions     map[string]struct{}
	inPoolSessions   map[string]struct{}
	limit            int
}

func (s *stats) print(t testing.TB) {
	s.Lock()
	defer s.Unlock()
	t.Log("stats:")
	t.Log(" - limit            :", s.limit)
	t.Log(" - open             :", len(s.openSessions))
	t.Log(" - in-pool          :", len(s.inPoolSessions))
	t.Log(" - in-flight        :", len(s.inFlightSessions))
}

func (s *stats) check(t testing.TB) {
	s.Lock()
	defer s.Unlock()
	if s.limit < 0 {
		t.Fatalf("negative limit: %d", s.limit)
	}
	if len(s.inFlightSessions) > len(s.inPoolSessions) {
		t.Fatalf("len(in_flight) > len(pool) (%d > %d)", len(s.inFlightSessions), len(s.inPoolSessions))
	}
	if len(s.inPoolSessions) > s.limit {
		t.Fatalf("len(pool) > limit (%d > %d)", len(s.inPoolSessions), s.limit)
	}
}

func (s *stats) max() int {
	s.Lock()
	defer s.Unlock()
	return s.limit
}

func (s *stats) addToOpen(t testing.TB, id string) {
	defer s.check(t)

	s.Lock()
	defer s.Unlock()

	if _, ok := s.openSessions[id]; ok {
		t.Fatalf("session '%s' add to open sessions twice", id)
	}

	s.openSessions[id] = struct{}{}

	t.Logf("session '%s' added to open sessions", id)
}

func (s *stats) removeFromOpen(t testing.TB, id string) {
	defer s.check(t)

	s.Lock()
	defer s.Unlock()

	if _, ok := s.openSessions[id]; !ok {
		t.Fatalf("session '%s' already removed from open sessions", id)
	}

	delete(s.openSessions, id)

	t.Logf("session '%s' removed from open sessions", id)
}

func (s *stats) addToPool(t testing.TB, id string) {
	defer s.check(t)

	s.Lock()
	defer s.Unlock()

	if _, ok := s.inPoolSessions[id]; ok {
		t.Fatalf("session '%s' add to pool twice", id)
	}

	s.inPoolSessions[id] = struct{}{}

	t.Logf("session '%s' added to pool", id)
}

func (s *stats) removeFromPool(t testing.TB, id string) {
	defer s.check(t)

	s.Lock()
	defer s.Unlock()

	if _, ok := s.inPoolSessions[id]; !ok {
		t.Fatalf("session '%s' already removed from pool", id)
	}

	delete(s.inPoolSessions, id)

	t.Logf("session '%s' removed from pool", id)
}

func (s *stats) addToInFlight(t testing.TB, id string) {
	defer s.check(t)

	s.Lock()
	defer s.Unlock()

	if _, ok := s.inFlightSessions[id]; ok {
		t.Fatalf("session '%s' add to in-flight twice", id)
	}

	s.inFlightSessions[id] = struct{}{}

	t.Logf("session '%s' added to in-flight", id)
}

func (s *stats) removeFromInFlight(t testing.TB, id string) {
	defer s.check(t)

	s.Lock()
	defer s.Unlock()

	if _, ok := s.inFlightSessions[id]; !ok {
		return
	}

	delete(s.inFlightSessions, id)

	t.Logf("session '%s' removed from in-flight", id)
}

func TestTable(t *testing.T) { //nolint:gocyclo
	scope := tableTestScope{
		folder: t.Name(),
	}

	testDuration := 55 * time.Second
	if v, ok := os.LookupEnv("TEST_DURATION"); ok {
		vv, err := time.ParseDuration(v)
		if err != nil {
			t.Errorf("wrong value of TEST_DURATION: '%s'", v)
		} else {
			testDuration = vv
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), testDuration)
	defer cancel()

	var totalConsumedUnits uint64
	defer func() {
		t.Logf("total consumed units: %d", atomic.LoadUint64(&totalConsumedUnits))
	}()

	ctx = meta.WithTrailerCallback(ctx, func(md metadata.MD) {
		atomic.AddUint64(&totalConsumedUnits, meta.ConsumedUnits(md))
	})

	s := &stats{
		limit:            math.MaxInt32,
		openSessions:     make(map[string]struct{}),
		inPoolSessions:   make(map[string]struct{}),
		inFlightSessions: make(map[string]struct{}),
	}
	defer func() {
		s.Lock()
		defer s.Unlock()
		if len(s.inFlightSessions) != 0 {
			t.Errorf("'in-flight' not a zero after closing table client: %v", s.inFlightSessions)
		}
		if len(s.openSessions) != 0 {
			t.Errorf("'openSessions' not a zero after closing table client: %v", s.openSessions)
		}
		if len(s.inPoolSessions) != 0 {
			t.Errorf("'inPoolSessions' not a zero after closing table client: %v", s.inPoolSessions)
		}
	}()

	var (
		limit = 50

		sessionsMtx sync.Mutex
		sessions    = make(map[string]struct{}, limit)

		shutdowned = uint32(0)

		shutdownTrace = trace.Table{
			OnPoolSessionAdd: func(info trace.TablePoolSessionAddInfo) {
				sessionsMtx.Lock()
				defer sessionsMtx.Unlock()
				sessions[info.Session.ID()] = struct{}{}
			},
			OnPoolGet: func(
				info trace.TablePoolGetStartInfo,
			) func(
				trace.TablePoolGetDoneInfo,
			) {
				return func(info trace.TablePoolGetDoneInfo) {
					if info.Session == nil {
						return
					}
					if atomic.LoadUint32(&shutdowned) == 0 {
						return
					}
					if info.Session.Status() != table.SessionClosing {
						return
					}
					sessionsMtx.Lock()
					defer sessionsMtx.Unlock()
					if _, has := sessions[info.Session.ID()]; !has {
						return
					}
					t.Fatalf("old session returned from pool after shutdown")
				}
			},
		}
	)

	var err error
	scope.db, err = ydb.Open(
		ctx,
		os.Getenv("YDB_CONNECTION_STRING"),
		ydb.WithAccessTokenCredentials(os.Getenv("YDB_ACCESS_TOKEN_CREDENTIALS")),
		ydb.WithUserAgent("table/e2e"),
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
		ydb.WithDiscoveryInterval(5*time.Second),
		ydb.WithLogger(
			trace.MatchDetails(`ydb\.(driver|table|discovery|retry|scheme).*`),
			ydb.WithNamespace("ydb"),
			ydb.WithMinLevel(log.TRACE),
		),
		ydb.WithPanicCallback(func(e interface{}) {
			_, _ = fmt.Fprintf(os.Stderr, "panic recovered:%v:\n%s", e, debug.Stack())
			os.Exit(1)
		}),
		ydb.WithTraceTable(
			shutdownTrace.Compose(
				trace.Table{
					OnInit: func(
						info trace.TableInitStartInfo,
					) func(
						trace.TableInitDoneInfo,
					) {
						return func(info trace.TableInitDoneInfo) {
							s.WithLock(func() {
								s.limit = info.Limit
							})
						}
					},
					OnSessionNew: func(
						info trace.TableSessionNewStartInfo,
					) func(
						trace.TableSessionNewDoneInfo,
					) {
						return func(info trace.TableSessionNewDoneInfo) {
							if info.Error == nil {
								s.addToOpen(t, info.Session.ID())
							}
						}
					},
					OnSessionDelete: func(
						info trace.TableSessionDeleteStartInfo,
					) func(
						trace.TableSessionDeleteDoneInfo,
					) {
						s.removeFromOpen(t, info.Session.ID())
						return nil
					},
					OnPoolSessionAdd: func(info trace.TablePoolSessionAddInfo) {
						s.addToPool(t, info.Session.ID())
					},
					OnPoolSessionRemove: func(info trace.TablePoolSessionRemoveInfo) {
						s.removeFromPool(t, info.Session.ID())
					},
					OnPoolGet: func(
						info trace.TablePoolGetStartInfo,
					) func(
						trace.TablePoolGetDoneInfo,
					) {
						return func(info trace.TablePoolGetDoneInfo) {
							if info.Error == nil {
								s.addToInFlight(t, info.Session.ID())
							}
						}
					},
					OnPoolPut: func(
						info trace.TablePoolPutStartInfo,
					) func(
						trace.TablePoolPutDoneInfo,
					) {
						s.removeFromInFlight(t, info.Session.ID())
						return nil
					},
				},
			),
		),
	)
	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		// cleanup
		_ = scope.db.Close(ctx)
	}()

	if err = scope.db.Table().Do(ctx, func(ctx context.Context, _ table.Session) error {
		// hack for wait pool initializing
		return nil
	}); err != nil {
		t.Fatalf("pool not initialized: %+v", err)
	} else if s.max() != limit {
		t.Fatalf("pool size not applied: %+v", s)
	}

	// prepare scheme
	err = sugar.RemoveRecursive(ctx, scope.db, scope.folder)
	if err != nil {
		t.Fatal(err)
	}
	err = sugar.MakeRecursive(ctx, scope.db, scope.folder)
	if err != nil {
		t.Fatal(err)
	}
	t.Run("describe", func(t *testing.T) {
		t.Run("table", func(t *testing.T) {
			t.Run("options", func(t *testing.T) {
				var desc options.TableOptionsDescription
				err = scope.db.Table().Do(ctx,
					func(ctx context.Context, s table.Session) (err error) {
						desc, err = s.DescribeTableOptions(ctx)
						return err
					},
					table.WithIdempotent(),
				)
				require.NoError(t, err)

				t.Log("> describe_options:")
				for i, p := range desc.TableProfilePresets {
					t.Logf("  > TableProfilePresets: %d/%d: %+v\n", i+1, len(desc.TableProfilePresets), p)
				}
				for i, p := range desc.StoragePolicyPresets {
					t.Logf("  > StoragePolicyPresets: %d/%d: %+v\n", i+1, len(desc.StoragePolicyPresets), p)
				}
				for i, p := range desc.CompactionPolicyPresets {
					t.Logf("  > CompactionPolicyPresets: %d/%d: %+v\n", i+1, len(desc.CompactionPolicyPresets), p)
				}
				for i, p := range desc.PartitioningPolicyPresets {
					t.Logf("  > PartitioningPolicyPresets: %d/%d: %+v\n", i+1, len(desc.PartitioningPolicyPresets), p)
				}
				for i, p := range desc.ExecutionPolicyPresets {
					t.Logf("  > ExecutionPolicyPresets: %d/%d: %+v\n", i+1, len(desc.ExecutionPolicyPresets), p)
				}
				for i, p := range desc.ReplicationPolicyPresets {
					t.Logf("  > ReplicationPolicyPresets: %d/%d: %+v\n", i+1, len(desc.ReplicationPolicyPresets), p)
				}
				for i, p := range desc.CachingPolicyPresets {
					t.Logf("  > CachingPolicyPresets: %d/%d: %+v\n", i+1, len(desc.CachingPolicyPresets), p)
				}
			})
		})
	})

	t.Run("create", func(t *testing.T) {
		t.Run("tables", func(t *testing.T) {
			err = scope.createTables(ctx)
			require.NoError(t, err)
		})
	})

	t.Run("check", func(t *testing.T) {
		t.Run("table", func(t *testing.T) {
			t.Run("exists", func(t *testing.T) {
				t.Run("series", func(t *testing.T) {
					err = scope.describeTable(ctx, "series")
					require.NoError(t, err)
				})
				t.Run("seasons", func(t *testing.T) {
					err = scope.describeTable(ctx, "seasons")
					require.NoError(t, err)
				})
				t.Run("episodes", func(t *testing.T) {
					err = scope.describeTable(ctx, "episodes")
					require.NoError(t, err)
				})
			})
		})
	})

	t.Run("data", func(t *testing.T) {
		t.Run("fill", func(t *testing.T) {
			err = scope.fill(ctx)
			require.NoError(t, err)
		})
	})

	t.Run("increment", func(t *testing.T) {
		t.Run("views", func(t *testing.T) {
			err = scope.db.Table().DoTx(ctx,
				func(ctx context.Context, tx table.TransactionActor) (err error) {
					var (
						res   result.Result
						views uint64
					)
					// select current value of `views`
					res, err = tx.Execute(
						ctx,
						scope.render(
							template.Must(template.New("").Parse(`
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
					`)),
							struct {
								TablePathPrefix string
							}{
								TablePathPrefix: path.Join(scope.db.Name(), scope.folder),
							},
						),
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
					res, err = tx.Execute(
						ctx,
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
								TablePathPrefix: path.Join(scope.db.Name(), scope.folder),
							},
						),
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

	// select upserted data
	if err = scope.db.Table().Do(ctx,
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
				scope.render(
					template.Must(template.New("").Parse(`
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
					`)),
					struct {
						TablePathPrefix string
					}{
						TablePathPrefix: path.Join(scope.db.Name(), scope.folder),
					},
				),
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
	); err != nil {
		t.Fatalf("tx failed: %v\n", err)
	}

	// shutdown existing sessions
	urls := os.Getenv("YDB_SESSIONS_SHUTDOWN_URLS")
	if len(urls) > 0 {
		for _, url := range strings.Split(urls, ",") {
			//nolint:gosec
			_, err = http.Get(url)
			if err != nil {
				t.Fatalf("failed to send request: %v", err)
			}
		}
		atomic.StoreUint32(&shutdowned, 1)
	}

	// select concurrently
	wg := sync.WaitGroup{}

	for i := 0; i < limit; i++ {
		wg.Add(3)
		// ExecuteDataQuery
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				default:
					scope.executeDataQuery(ctx, t, scope.db.Table(), path.Join(scope.db.Name(), scope.folder))
				}
			}
		}()
		// ExecuteScanQuery
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				default:
					scope.executeScanQuery(ctx, t, scope.db.Table(), path.Join(scope.db.Name(), scope.folder))
				}
			}
		}()
		// StreamReadTable
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				default:
					scope.streamReadTable(ctx, t, scope.db.Table(), path.Join(scope.db.Name(), scope.folder, "series"))
				}
			}
		}()
	}
	wg.Wait()
}

func (s *tableTestScope) streamReadTable(ctx context.Context, t testing.TB, c table.Client, tableAbsPath string) {
	err := c.Do(ctx,
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
			for res.NextResultSet(ctx, "series_id", "title", "release_date") {
				for res.NextRow() {
					err = res.Scan(&id, &title, &date)
					if err != nil {
						return err
					}
					t.Logf("  > %d %s %s\n", *id, *title, date.String())
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
	if err != nil && !ydb.IsTimeoutError(err) {
		t.Fatalf("read table error: %+v", err)
	}
}

func (s *tableTestScope) executeDataQuery(ctx context.Context, t testing.TB, c table.Client, folderAbsPath string) {
	var (
		query = s.render(
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
			struct {
				TablePathPrefix string
			}{
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
	err := c.Do(ctx,
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
		t.Fatalf("select simple error: %+v", err)
	}
}

func (s *tableTestScope) executeScanQuery(ctx context.Context, t testing.TB, c table.Client, folderAbsPath string) {
	query := s.render(
		template.Must(template.New("").Parse(`
			PRAGMA TablePathPrefix("{{ .TablePathPrefix }}");

			DECLARE $series AS List<UInt64>;

			SELECT series_id, season_id, title, first_aired
			FROM seasons
			WHERE series_id IN $series
		`)),
		struct {
			TablePathPrefix string
		}{
			TablePathPrefix: folderAbsPath,
		},
	)
	err := c.Do(ctx,
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
	if err != nil && !ydb.IsTimeoutError(err) {
		t.Fatalf("scan query error: %+v", err)
	}
}

func (s *tableTestScope) seriesData(id uint64, released time.Time, title, info, comment string) types.Value {
	var commentv types.Value
	if comment == "" {
		commentv = types.NullValue(types.TypeText)
	} else {
		commentv = types.OptionalValue(types.TextValue(comment))
	}
	return types.StructValue(
		types.StructFieldValue("series_id", types.Uint64Value(id)),
		types.StructFieldValue("release_date", types.DateValueFromTime(released)),
		types.StructFieldValue("title", types.TextValue(title)),
		types.StructFieldValue("series_info", types.TextValue(info)),
		types.StructFieldValue("comment", commentv),
	)
}

func (s *tableTestScope) seasonData(seriesID, seasonID uint64, title string, first, last time.Time) types.Value {
	return types.StructValue(
		types.StructFieldValue("series_id", types.Uint64Value(seriesID)),
		types.StructFieldValue("season_id", types.Uint64Value(seasonID)),
		types.StructFieldValue("title", types.TextValue(title)),
		types.StructFieldValue("first_aired", types.DateValueFromTime(first)),
		types.StructFieldValue("last_aired", types.DateValueFromTime(last)),
	)
}

func (s *tableTestScope) episodeData(
	seriesID, seasonID, episodeID uint64, title string, date time.Time,
) types.Value {
	return types.StructValue(
		types.StructFieldValue("series_id", types.Uint64Value(seriesID)),
		types.StructFieldValue("season_id", types.Uint64Value(seasonID)),
		types.StructFieldValue("episode_id", types.Uint64Value(episodeID)),
		types.StructFieldValue("title", types.TextValue(title)),
		types.StructFieldValue("air_date", types.DateValueFromTime(date)),
	)
}

func (s *tableTestScope) getSeriesData() types.Value {
	return types.ListValue(
		s.seriesData(
			1, s.days("2006-02-03"), "IT Crowd", ""+
				"The IT Crowd is a British sitcom produced by Channel 4, written by Graham Linehan, produced by "+
				"Ash Atalla and starring Chris O'Dowd, Richard Ayoade, Katherine Parkinson, and Matt Berry.",
			"", // NULL comment.
		),
		s.seriesData(
			2, s.days("2014-04-06"), "Silicon Valley", ""+
				"Silicon Valley is an American comedy television series openSessions by Mike Judge, John Altschuler and "+
				"Dave Krinsky. The series focuses on five young men who founded a startup company in Silicon Valley.",
			"Some comment here",
		),
	)
}

func (s *tableTestScope) getSeasonsData() types.Value {
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

func (s *tableTestScope) getEpisodesData() types.Value {
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

func (s *tableTestScope) days(date string) time.Time {
	const dateISO8601 = "2006-01-02"
	t, err := time.Parse(dateISO8601, date)
	if err != nil {
		panic(err)
	}
	return t
}

func (s *tableTestScope) fill(ctx context.Context) error {
	// prepare write transaction.
	writeTx := table.TxControl(
		table.BeginTx(
			table.WithSerializableReadWrite(),
		),
		table.CommitTx(),
	)
	return s.db.Table().Do(ctx,
		func(ctx context.Context, session table.Session) (err error) {
			stmt, err := session.Prepare(ctx, s.render(template.Must(template.New("fillQuery database").Parse(`
				PRAGMA TablePathPrefix("{{ .TablePathPrefix }}");

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
				FROM AS_TABLE($episodesData);
			`)), struct {
				TablePathPrefix string
			}{
				TablePathPrefix: path.Join(s.db.Name(), s.folder),
			}))
			if err != nil {
				return
			}
			_, _, err = stmt.Execute(ctx, writeTx, table.NewQueryParameters(
				table.ValueParam("$seriesData", s.getSeriesData()),
				table.ValueParam("$seasonsData", s.getSeasonsData()),
				table.ValueParam("$episodesData", s.getEpisodesData()),
			))
			return
		},
		table.WithIdempotent(),
	)
}

func (s *tableTestScope) createTables(ctx context.Context) error {
	err := s.db.Table().Do(ctx,
		func(ctx context.Context, session table.Session) (err error) {
			if _, err = session.DescribeTable(ctx, path.Join(s.db.Name(), s.folder, "series")); err == nil {
				_ = session.DropTable(ctx, path.Join(s.db.Name(), s.folder, "series"))
			}
			return session.CreateTable(ctx, path.Join(s.db.Name(), s.folder, "series"),
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
	if err != nil {
		return fmt.Errorf("create table series failed: %w", err)
	}

	err = s.db.Table().Do(ctx,
		func(ctx context.Context, session table.Session) (err error) {
			if _, err = session.DescribeTable(ctx, path.Join(s.db.Name(), s.folder, "seasons")); err == nil {
				_ = session.DropTable(ctx, path.Join(s.db.Name(), s.folder, "seasons"))
			}
			return session.CreateTable(ctx, path.Join(s.db.Name(), s.folder, "seasons"),
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
	if err != nil {
		return fmt.Errorf("create table seasons failed: %w", err)
	}

	err = s.db.Table().Do(ctx,
		func(ctx context.Context, session table.Session) (err error) {
			if _, err = session.DescribeTable(ctx, path.Join(s.db.Name(), s.folder, "episodes")); err == nil {
				_ = session.DropTable(ctx, path.Join(s.db.Name(), s.folder, "episodes"))
			}
			return session.CreateTable(ctx, path.Join(s.db.Name(), s.folder, "episodes"),
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
	if err != nil {
		return fmt.Errorf("create table episodes failed: %w", err)
	}

	return nil
}

func (s *tableTestScope) describeTable(ctx context.Context, tableName string) (err error) {
	err = s.db.Table().Do(ctx,
		func(ctx context.Context, session table.Session) (err error) {
			_, err = session.DescribeTable(ctx, path.Join(s.db.Name(), s.folder, tableName))
			if err != nil {
				return
			}
			return err
		},
		table.WithIdempotent(),
	)
	if err != nil {
		return fmt.Errorf("describe table %q failed: %w", path.Join(s.db.Name(), s.folder, tableName), err)
	}
	return nil
}

func (s *tableTestScope) render(t *template.Template, data interface{}) string {
	var buf bytes.Buffer
	err := t.Execute(&buf, data)
	if err != nil {
		panic(err)
	}
	return buf.String()
}
