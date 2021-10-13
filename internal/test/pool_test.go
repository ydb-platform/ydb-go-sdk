package test

import (
	"context"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3"
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
	if s.inFlight > s.balance {
		t.Fatalf("in_flight > balance (%d > %d)", s.inFlight, s.balance)
	}
	if s.balance > s.max {
		t.Fatalf("balance > max (%d > %d)", s.balance, s.max)
	}
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
	if !CheckEndpointDatabaseEnv() {
		t.Skip("need to be tested with docker")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
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

	db := OpenDB(
		ctx,
		t,
		ydb.WithDialTimeout(5*time.Second),
		ydb.WithGrpcConnectionTTL(5*time.Second),
		ydb.WithSessionPoolIdleThreshold(time.Second*5),
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
			OnSessionKeepAlive: nil,
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
	defer func() { _ = db.Close(ctx) }()

	Prepare(ctx, t, db)

	if err := Fill(ctx, db.Table(), db.Name()); err != nil {
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
