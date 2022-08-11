//go:build !fast
// +build !fast

package table_test

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"net/http"
	"os"
	"path"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"text/template"
	"time"

	"google.golang.org/grpc"
	grpcCodes "google.golang.org/grpc/codes"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/balancers"
	"github.com/ydb-platform/ydb-go-sdk/v3/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xsync"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xtest"
	"github.com/ydb-platform/ydb-go-sdk/v3/log"
	"github.com/ydb-platform/ydb-go-sdk/v3/sugar"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result/named"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

const (
	folder = "table_test"
)

type stats struct {
	xsync.Mutex

	keepAliveMinSize int
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
	t.Log(" - keepAliveMinSize :", s.keepAliveMinSize)
	t.Log(" - open             :", len(s.openSessions))
	t.Log(" - in-pool          :", len(s.inPoolSessions))
	t.Log(" - in-flight        :", len(s.inFlightSessions))
}

func (s *stats) check(t testing.TB) {
	s.Lock()
	defer s.Unlock()
	if s.keepAliveMinSize < 0 {
		t.Fatalf("negative keepAliveMinSize: %d", s.keepAliveMinSize)
	}
	if s.limit < 0 {
		t.Fatalf("negative limit: %d", s.limit)
	}
	if s.keepAliveMinSize > len(s.inFlightSessions) {
		t.Fatalf("keepAliveMinSize > len(in-flight) (%d > %d)", s.keepAliveMinSize, len(s.inFlightSessions))
	}
	if len(s.inFlightSessions) > len(s.inPoolSessions) {
		t.Fatalf("len(in_flight) > len(pool) (%d > %d)", len(s.inFlightSessions), len(s.inPoolSessions))
	}
	if len(s.inPoolSessions) > s.limit {
		t.Fatalf("len(pool) > limit (%d > %d)", len(s.inPoolSessions), s.limit)
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

type balancerStats struct {
	endpoints map[string]int
	mu        sync.RWMutex
}

func (s *balancerStats) add(address string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.endpoints[address]++
}

func (s *balancerStats) printStats() {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var (
		rootEndpointCount = math.MaxInt
		rootEndpoint      string
	)
	for endpoint, count := range s.endpoints {
		if count < rootEndpointCount {
			rootEndpointCount = count
			rootEndpoint = endpoint
		}
	}
	var sum int
	for _, count := range s.endpoints {
		sum += count
	}
	sum -= rootEndpointCount
	exp := sum / (len(s.endpoints) - 1)
	fmt.Println("balancer stats:")
	for endpoint, count := range s.endpoints {
		if endpoint != rootEndpoint {
			fmt.Printf(" > %s: %d (delta = %f)\n", endpoint, count, math.Abs(float64(exp-count))/float64(exp))
		}
	}
}

func TestTableMultiple(t *testing.T) {
	if _, ok := os.LookupEnv("LOCAL_TESTING"); !ok {
		t.Skip("only for local testing")
	}
	xtest.TestManyTimes(t, func(t testing.TB) {
		testTable(t)
	}, xtest.StopAfter(time.Hour))
}

func TestTable(t *testing.T) {
	testTable(t)
}

//nolint:gocyclo
func testTable(t testing.TB) {
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

	s := &stats{
		keepAliveMinSize: math.MinInt32,
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

	bs := &balancerStats{endpoints: make(map[string]int)}
	defer func() {
		bs.printStats()
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
					if info.Session.Status() != options.SessionClosing.String() {
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

	db, err := ydb.Open(
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
					if method == "/Ydb.Table.V1.TableService/CreateSession" {
						bs.add(cc.Target())
					}
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
		ydb.WithBalancer(balancers.RoundRobin()),
		ydb.WithDialTimeout(5*time.Second),
		ydb.WithSessionPoolIdleThreshold(time.Nanosecond*1),
		ydb.WithSessionPoolSizeLimit(limit),
		ydb.WithSessionPoolKeepAliveMinSize(-1),
		ydb.WithConnectionTTL(5*time.Second),
		ydb.WithDiscoveryInterval(5*time.Second),
		ydb.WithLogger(
			trace.MatchDetails(`ydb\.(driver|table|discovery|retry|scheme).*`),
			ydb.WithNamespace("ydb"),
			ydb.WithOutWriter(os.Stdout),
			ydb.WithErrWriter(os.Stdout),
			ydb.WithMinLevel(log.WARN),
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
								s.keepAliveMinSize = info.KeepAliveMinSize
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
		_ = db.Close(ctx)
	}()

	if err = db.Table().Do(ctx, func(ctx context.Context, _ table.Session) error {
		// hack for wait pool initializing
		return nil
	}); err != nil {
		t.Fatalf("pool not initialized: %+v", err)
	} else if s.min() < 0 || s.max() != limit {
		t.Fatalf("pool sizes not applied: %+v", s)
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

	// fill data
	if err = fill(ctx, db, folder); err != nil {
		t.Fatalf("fillQuery failed: %v\n", err)
	}

	// upsert with transaction
	if err = db.Table().DoTx(
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
	); err != nil {
		t.Fatalf("tx failed: %v\n", err)
	}
	// select upserted data
	if err = db.Table().Do(
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

	// multiple result sets
	// - create table
	t.Logf("> creating table stream_query...\n")
	if err = db.Table().Do(
		ctx,
		func(ctx context.Context, s table.Session) (err error) {
			_ = s.ExecuteSchemeQuery(
				ctx,
				`DROP TABLE stream_query`,
			)
			return s.ExecuteSchemeQuery(
				ctx,
				`CREATE TABLE stream_query (val Int32, PRIMARY KEY (val))`,
			)
		},
		table.WithIdempotent(),
	); err != nil {
		t.Fatalf("create table failed: %v\n", err)
	}
	fmt.Printf("> table stream_query openSessions\n")
	var (
		upsertRowsCount = 100000
		sum             uint64
	)
	if v, ok := os.LookupEnv("UPSERT_ROWS_COUNT"); ok {
		var vv int
		vv, err = strconv.Atoi(v)
		if err != nil {
			t.Errorf("wrong value of UPSERT_ROWS_COUNT: '%s'", v)
		} else {
			upsertRowsCount = vv
		}
	}

	// - upsert data
	fmt.Printf("> preparing values to upsert...\n")
	values := make([]types.Value, 0, upsertRowsCount)
	for i := 0; i < upsertRowsCount; i++ {
		sum += uint64(i)
		values = append(
			values,
			types.StructValue(
				types.StructFieldValue("val", types.Int32Value(int32(i))),
			),
		)
	}
	fmt.Printf("> values to upsert prepared\n")

	fmt.Printf("> upserting prepared values...\n")
	if err = db.Table().Do(
		ctx,
		func(ctx context.Context, s table.Session) (err error) {
			_, _, err = s.Execute(
				ctx,
				table.TxControl(
					table.BeginTx(
						table.WithSerializableReadWrite(),
					),
					table.CommitTx(),
				), `
					DECLARE $values AS List<Struct<
						val: Int32,
					> >;
					UPSERT INTO stream_query
					SELECT
						val 
					FROM
						AS_TABLE($values);            
				`, table.NewQueryParameters(
					table.ValueParam(
						"$values",
						types.ListValue(values...),
					),
				),
			)
			return err
		},
		table.WithIdempotent(),
	); err != nil {
		t.Fatalf("upsert failed: %v\n", err)
	}
	fmt.Printf("> prepared values upserted\n")

	// - scan select
	fmt.Printf("> scan-selecting values...\n")
	if err = db.Table().Do(
		ctx,
		func(ctx context.Context, s table.Session) (err error) {
			res, err := s.StreamExecuteScanQuery(
				ctx, `SELECT val FROM stream_query;`, table.NewQueryParameters(),
				options.WithExecuteScanQueryStats(options.ExecuteScanQueryStatsTypeFull),
			)
			if err != nil {
				return err
			}
			var (
				resultSetsCount = 0
				rowsCount       = 0
				checkSum        uint64
			)
			for res.NextResultSet(ctx) {
				resultSetsCount++
				for res.NextRow() {
					rowsCount++
					var val *int32
					err = res.Scan(&val)
					if err != nil {
						return err
					}
					checkSum += uint64(*val)
				}
				if stats := res.Stats(); stats != nil {
					fmt.Printf(" --- query stats: compilation: %v, process CPU time: %v, affected shards: %v\n",
						stats.Compilation(),
						stats.ProcessCPUTime(),
						func() (count uint64) {
							for {
								phase, ok := stats.NextPhase()
								if !ok {
									return
								}
								count += phase.AffectedShards()
							}
						}(),
					)
				}
			}
			if rowsCount != upsertRowsCount {
				return fmt.Errorf("wrong rows count: %v, exp: %v", rowsCount, upsertRowsCount)
			}

			if sum != checkSum {
				return fmt.Errorf("wrong checkSum: %v, exp: %v", checkSum, sum)
			}

			if resultSetsCount <= 1 {
				return fmt.Errorf("wrong result sets count: %v", resultSetsCount)
			}

			return res.Err()
		},
		table.WithIdempotent(),
	); err != nil {
		t.Fatalf("scan select failed: %v\n", err)
	}
	fmt.Printf("> values selected\n")
	// shutdown existing sessions
	urls := os.Getenv("YDB_SHUTDOWN_URLS")
	if len(urls) > 0 {
		fmt.Printf("> shutdowning existing sessions...\n")
		for _, url := range strings.Split(urls, ",") {
			//nolint:gosec
			_, err = http.Get(url)
			if err != nil {
				t.Fatalf("failed to send request: %v", err)
			}
		}
		atomic.StoreUint32(&shutdowned, 1)
		fmt.Printf("> existing sessions shutdowned\n")
	}

	// select concurrently
	fmt.Printf("> concurrent quering...\n")
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
					executeDataQuery(ctx, t, db.Table(), path.Join(db.Name(), folder))
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
					executeScanQuery(ctx, t, db.Table(), path.Join(db.Name(), folder))
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
					streamReadTable(ctx, t, db.Table(), path.Join(db.Name(), folder, "series"))
				}
			}
		}()
	}
	wg.Wait()
	fmt.Printf("> concurrent quering done\n")
}

func streamReadTable(ctx context.Context, t testing.TB, c table.Client, tableAbsPath string) {
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
			fmt.Printf("> read_table:\n")
			for res.NextResultSet(ctx, "series_id", "title", "release_date") {
				for res.NextRow() {
					err = res.Scan(&id, &title, &date)
					if err != nil {
						return err
					}
					fmt.Printf("  > %d %s %s\n", *id, *title, date.String())
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
					fmt.Printf(
						"# phase #%d: took %s\n",
						i, phase.Duration(),
					)
					for {
						tbl, ok := phase.NextTableAccess()
						if !ok {
							break
						}
						fmt.Printf(
							"#  accessed %s: read=(%drows, %dbytes)\n",
							tbl.Name, tbl.Reads.Rows, tbl.Reads.Bytes,
						)
					}
				}
			}

			return nil
		},
		table.WithIdempotent(),
	)
	if err != nil && !ydb.IsTimeoutError(err) {
		t.Fatalf("read table error: %+v", err)
	}
}

func executeDataQuery(ctx context.Context, t testing.TB, c table.Client, folderAbsPath string) {
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
				options.WithCollectStatsModeBasic(),
			)
			if err != nil {
				return err
			}
			defer func() {
				_ = res.Close()
			}()
			fmt.Printf("> select_simple_transaction:\n")
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
					fmt.Printf(
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

func executeScanQuery(ctx context.Context, t testing.TB, c table.Client, folderAbsPath string) {
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
			fmt.Printf("> scan_query_select:\n")
			for res.NextResultSet(ctx) {
				for res.NextRow() {
					err = res.ScanWithDefaults(&seriesID, &seasonID, &title, &date)
					if err != nil {
						return err
					}
					fmt.Printf("  > SeriesId: %d, SeasonId: %d, Title: %s, Air date: %s\n", seriesID, seasonID, title, date)
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
				"Silicon Valley is an American comedy television series openSessions by Mike Judge, John Altschuler and "+
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
		table.WithIdempotent(),
	)
	if err != nil {
		return err
	}
	fmt.Println("> describe_options:")

	for i, p := range desc.TableProfilePresets {
		fmt.Printf("  > TableProfilePresets: %d/%d: %+v\n", i+1, len(desc.TableProfilePresets), p)
	}
	for i, p := range desc.StoragePolicyPresets {
		fmt.Printf("  > StoragePolicyPresets: %d/%d: %+v\n", i+1, len(desc.StoragePolicyPresets), p)
	}
	for i, p := range desc.CompactionPolicyPresets {
		fmt.Printf("  > CompactionPolicyPresets: %d/%d: %+v\n", i+1, len(desc.CompactionPolicyPresets), p)
	}
	for i, p := range desc.PartitioningPolicyPresets {
		fmt.Printf("  > PartitioningPolicyPresets: %d/%d: %+v\n", i+1, len(desc.PartitioningPolicyPresets), p)
	}
	for i, p := range desc.ExecutionPolicyPresets {
		fmt.Printf("  > ExecutionPolicyPresets: %d/%d: %+v\n", i+1, len(desc.ExecutionPolicyPresets), p)
	}
	for i, p := range desc.ReplicationPolicyPresets {
		fmt.Printf("  > ReplicationPolicyPresets: %d/%d: %+v\n", i+1, len(desc.ReplicationPolicyPresets), p)
	}
	for i, p := range desc.CachingPolicyPresets {
		fmt.Printf("  > CachingPolicyPresets: %d/%d: %+v\n", i+1, len(desc.CachingPolicyPresets), p)
	}

	return nil
}

func fill(ctx context.Context, db ydb.Connection, folder string) error {
	fmt.Printf("> filling tables\n")
	defer func() {
		fmt.Printf("> filling tables done\n")
	}()
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
			if _, err = s.DescribeTable(ctx, path.Join(folder, "series")); err == nil {
				_ = s.DropTable(ctx, path.Join(folder, "series"))
			}
			return s.CreateTable(ctx, path.Join(folder, "series"),
				options.WithColumn("series_id", types.Optional(types.TypeUint64)),
				options.WithColumn("title", types.Optional(types.TypeUTF8)),
				options.WithColumn("series_info", types.Optional(types.TypeUTF8)),
				options.WithColumn("release_date", types.Optional(types.TypeDate)),
				options.WithColumn("comment", types.Optional(types.TypeUTF8)),
				options.WithPrimaryKeyColumn("series_id"),
			)
		},
		table.WithIdempotent(),
	)
	if err != nil {
		return err
	}

	err = c.Do(
		ctx,
		func(ctx context.Context, s table.Session) (err error) {
			if _, err = s.DescribeTable(ctx, path.Join(folder, "seasons")); err == nil {
				_ = s.DropTable(ctx, path.Join(folder, "seasons"))
			}
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
			if _, err = s.DescribeTable(ctx, path.Join(folder, "episodes")); err == nil {
				_ = s.DropTable(ctx, path.Join(folder, "episodes"))
			}
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
			fmt.Printf("> describe table: %s\n", path)
			for _, c := range desc.Columns {
				fmt.Printf("  > column, name: %s, %s\n", c.Type, c.Name)
			}
			for i, keyRange := range desc.KeyRanges {
				fmt.Printf("  > key range %d: %s\n", i, keyRange.String())
			}
			return
		},
		table.WithIdempotent(),
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

func TestLongStream(t *testing.T) {
	var (
		tableName         = `long_stream_query`
		discoveryInterval = 10 * time.Second
		db                ydb.Connection
		err               error
		upsertRowsCount   = 100000
		batchSize         = 10000
	)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	db, err = ydb.Open(
		ctx,
		os.Getenv("YDB_CONNECTION_STRING"),
		ydb.WithAccessTokenCredentials(
			os.Getenv("YDB_ACCESS_TOKEN_CREDENTIALS"),
		),
		ydb.WithDiscoveryInterval(0), // disable re-discovery on upsert time
	)
	if err != nil {
		t.Fatal(err)
	}
	defer func(db ydb.Connection) {
		// cleanup
		_ = db.Close(ctx)
	}(db)

	t.Run("creating stream table", func(t *testing.T) {
		if err = db.Table().Do(
			ctx,
			func(ctx context.Context, s table.Session) (err error) {
				_, err = s.DescribeTable(ctx, path.Join(db.Name(), tableName))
				if !ydb.IsOperationErrorSchemeError(err) {
					return err
				}
				if err == nil {
					if err = s.DropTable(ctx, path.Join(db.Name(), tableName)); err != nil {
						return err
					}
				}
				return s.ExecuteSchemeQuery(
					ctx,
					`CREATE TABLE `+tableName+` (val Int64, PRIMARY KEY (val))`,
				)
			},
		); err != nil {
			t.Fatalf("create table failed: %v\n", err)
		}
	})

	t.Run("check batch size", func(t *testing.T) {
		if upsertRowsCount%batchSize != 0 {
			t.Fatalf("wrong batch size: (%d mod %d = %d) != 0", upsertRowsCount, batchSize, upsertRowsCount%batchSize)
		}
	})

	t.Run("upserting rows", func(t *testing.T) {
		var upserted uint32
		for i := 0; i < (upsertRowsCount / batchSize); i++ {
			var (
				from = int32(i * batchSize)
				to   = int32((i + 1) * batchSize)
			)
			t.Run(fmt.Sprintf("upserting %d..%d", from, to-1), func(t *testing.T) {
				values := make([]types.Value, 0, batchSize)
				for j := from; j < to; j++ {
					values = append(
						values,
						types.StructValue(
							types.StructFieldValue("val", types.Int32Value(j)),
						),
					)
				}
				if err = db.Table().Do(
					ctx,
					func(ctx context.Context, s table.Session) (err error) {
						_, _, err = s.Execute(
							ctx,
							table.TxControl(
								table.BeginTx(
									table.WithSerializableReadWrite(),
								),
								table.CommitTx(),
							), `
								DECLARE $values AS List<Struct<
									val: Int32,
								>>;
								UPSERT INTO long_stream_query
								SELECT
									val 
								FROM
									AS_TABLE($values);            
							`, table.NewQueryParameters(
								table.ValueParam(
									"$values",
									types.ListValue(values...),
								),
							),
						)
						return err
					},
				); err != nil {
					t.Fatalf("upsert failed: %v\n", err)
				} else {
					upserted += uint32(to - from)
					fmt.Printf("upserted %d rows, total upserted rows: %d\n", uint32(to-from), upserted)
				}
			})
		}
		t.Run("check upserted rows", func(t *testing.T) {
			fmt.Printf("total upserted rows: %d, expected: %d\n", upserted, upsertRowsCount)
			if upserted != uint32(upsertRowsCount) {
				t.Fatalf("wrong rows count: %v, expected: %d", upserted, upsertRowsCount)
			}
		})
	})

	t.Run("make child discovered connection", func(t *testing.T) {
		db, err = db.With(ctx, ydb.WithDiscoveryInterval(discoveryInterval))
		if err != nil {
			t.Fatal(err)
		}
	})

	defer func(db ydb.Connection) {
		// cleanup
		_ = db.Close(ctx)
	}(db)

	t.Run("execute stream query", func(t *testing.T) {
		if err = db.Table().Do(
			ctx,
			func(ctx context.Context, s table.Session) (err error) {
				var (
					start     = time.Now()
					rowsCount = 0
				)
				res, err := s.StreamExecuteScanQuery(ctx, "SELECT val FROM long_stream_query", table.NewQueryParameters())
				if err != nil {
					return err
				}
				defer func() {
					_ = res.Close()
				}()
				for res.NextResultSet(ctx) {
					count := 0
					for res.NextRow() {
						count++
					}
					rowsCount += count
					fmt.Printf("received set with %d rows. total received: %d\n", count, rowsCount)
					time.Sleep(discoveryInterval)
				}
				if err = res.Err(); err != nil {
					return fmt.Errorf("received error: %w (duration: %v)", err, time.Since(start))
				}
				if rowsCount != upsertRowsCount {
					return fmt.Errorf("wrong rows count: %v, expected: %d (duration: %v)",
						rowsCount,
						upsertRowsCount,
						time.Since(start),
					)
				}
				return nil
			},
		); err != nil {
			t.Fatalf("stream query failed: %v\n", err)
		}
	})

	t.Run("stream read table", func(t *testing.T) {
		if err = db.Table().Do(
			ctx,
			func(ctx context.Context, s table.Session) (err error) {
				var (
					start     = time.Now()
					rowsCount = 0
				)
				res, err := s.StreamReadTable(ctx, path.Join(db.Name(), "long_stream_query"), options.ReadColumn("val"))
				if err != nil {
					return err
				}
				defer func() {
					_ = res.Close()
				}()
				for res.NextResultSet(ctx) {
					count := 0
					for res.NextRow() {
						count++
					}
					rowsCount += count
					fmt.Printf("received set with %d rows. total received: %d\n", count, rowsCount)
					time.Sleep(discoveryInterval)
				}
				if err = res.Err(); err != nil {
					return fmt.Errorf("received error: %w (duration: %v)", err, time.Since(start))
				}
				if rowsCount != upsertRowsCount {
					return fmt.Errorf("wrong rows count: %v, expected: %d (duration: %v)",
						rowsCount,
						upsertRowsCount,
						time.Since(start),
					)
				}
				return nil
			},
		); err != nil {
			t.Fatalf("stream query failed: %v\n", err)
		}
	})
}
