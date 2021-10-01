package test

import (
	"context"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3"
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

func TestPoolStats(t *testing.T) {
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
		t,
		ctx,
		ydb.WithDialTimeout(5*time.Second),
		ydb.WithGrpcConnectionTTL(5*time.Second),
		ydb.WithSessionPoolIdleThreshold(time.Second*5),
		ydb.WithSessionPoolKeepAliveMinSize(-1),
		ydb.WithDiscoveryInterval(5*time.Second),
		ydb.WithTraceTable(trace.Table{
			OnCreateSession: func(info trace.CreateSessionStartInfo) func(trace.CreateSessionDoneInfo) {
				return func(info trace.CreateSessionDoneInfo) {
					if info.Error == nil {
						s.addBalance(t, 1)
					}
				}
			},
			OnKeepAlive: nil,
			OnDeleteSession: func(info trace.DeleteSessionStartInfo) func(trace.DeleteSessionDoneInfo) {
				return func(info trace.DeleteSessionDoneInfo) {
					s.addBalance(t, -1)
				}
			},
			OnPrepareDataQuery:       nil,
			OnExecuteDataQuery:       nil,
			OnStreamExecuteScanQuery: nil,
			OnStreamReadTable:        nil,
			OnBeginTransaction:       nil,
			OnCommitTransaction:      nil,
			OnRollbackTransaction:    nil,
			OnPoolInit: func(info trace.PoolInitStartInfo) func(trace.PoolInitDoneInfo) {
				return func(info trace.PoolInitDoneInfo) {
					s.Lock()
					s.min = info.KeepAliveMinSize
					s.max = info.Limit
					s.Unlock()
				}
			},
			OnPoolCreate: nil,
			OnPoolClose:  nil,
			OnPoolGet: func(info trace.PoolGetStartInfo) func(trace.PoolGetDoneInfo) {
				return func(info trace.PoolGetDoneInfo) {
					if info.Error == nil {
						s.addInFlight(t, 1)
					}
				}
			},
			OnPoolWait: nil,
			OnPoolTake: nil,
			OnPoolPut: func(info trace.PoolPutStartInfo) func(trace.PoolPutDoneInfo) {
				return func(info trace.PoolPutDoneInfo) {
					s.addInFlight(t, -1)
				}
			},
			OnPoolCloseSession: nil,
			OnPoolRetry:        nil,
		}),
	)
	defer func() { _ = db.Close() }()

	Prepare(t, ctx, db)

	Fill(ctx, db.Table(), db.Name())

	concurrency := 200
	wg := sync.WaitGroup{}
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
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
}
