// +build integration

package test

import (
	"context"
	"math"
	"os"
	"sync"
	"testing"
	"time"

	ydb "github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

type stats struct {
	sync.Mutex

	min      int
	inFlight int
	balance  int
	max      int

	waited int
}

func (s *stats) print(t *testing.T) {
	s.Lock()
	defer s.Unlock()
	t.Log("stats:")
	t.Log(" - min      :", s.min)
	t.Log(" - in_flight:", s.min)
	t.Log(" - balance  :", s.min)
	t.Log(" - max      :", s.max)
	t.Log(" - waited   :", s.waited)
}

func (s *stats) check(t *testing.T) {
	s.Lock()
	defer s.Unlock()
	if s.min > s.inFlight {
		t.Fatalf("min > in_flight (%d > %d)", s.min, s.inFlight)
	}
	if s.inFlight > s.max {
		t.Fatalf("in_flight > max (%d > %d)", s.inFlight, s.max)
	}
	if s.balance > s.max {
		t.Fatalf("balance > max (%d > %d)", s.balance, s.max)
	}
}

func (s *stats) Min() int {
	s.Lock()
	defer s.Unlock()
	return s.min
}

func (s *stats) Max() int {
	s.Lock()
	defer s.Unlock()
	return s.max
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

func TestPoolHealth(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 55*time.Second)
	defer cancel()

	Quet()

	s := &stats{
		Mutex:    sync.Mutex{},
		min:      math.MinInt32,
		inFlight: 0,
		balance:  0,
		max:      math.MaxInt32,
		waited:   0,
	}

	defer s.print(t)

	db, err := open(
		ctx,
		ydb.WithConnectionString(os.Getenv("YDB_CONNECTION_STRING")),
		ydb.WithAnonymousCredentials(),
		ydb.WithDialTimeout(5*time.Second),
		ydb.WithGrpcConnectionTTL(5*time.Second),
		ydb.WithSessionPoolIdleThreshold(time.Second*5),
		ydb.WithSessionPoolSizeLimit(200),
		ydb.WithSessionPoolKeepAliveMinSize(-1),
		ydb.WithDiscoveryInterval(5*time.Second),
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
					s.min = info.KeepAliveMinSize
					s.max = info.Limit
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
				return func(info trace.PoolPutDoneInfo) {
					s.addInFlight(t, -1)
				}
			},
		}),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = db.Close(ctx)
	}()

	if err = db.Table().Do(ctx, func(ctx context.Context, _ table.Session) error {
		// after initializing pool
		return nil
	}); err != nil {
		t.Fatalf("pool not initialized: %+v", err)
	}

	if s.Min() < 0 || s.Max() != 200 {
		t.Fatalf("pool sizes not applied: %+v", s)
	}

	if err := Prepare(ctx, db); err != nil {
		t.Fatalf("fill failed: %v\n", err)
	}

	if err := Fill(ctx, db); err != nil {
		t.Fatalf("fill failed: %v\n", err)
	}

	concurrency := 200
	wg := sync.WaitGroup{}
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		t.Run("", func(t *testing.T) {
			defer wg.Done()
			for {
				err := Select(ctx, db)
				if err != nil {
					if ctx.Err() != nil {
						return
					}
					t.Fatalf("select error: %v\n", err)
				}
			}
		})
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
}
