package table

import (
	"context"
	"fmt"
	"path"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/yandex-cloud/ydb-go-sdk/testutil"
	"github.com/yandex-cloud/ydb-go-sdk/timeutil"
	"github.com/yandex-cloud/ydb-go-sdk/timeutil/timetest"
)

func (p *SessionPool) debug() {
	fmt.Printf("head ")
	for el := p.idle.Front(); el != nil; el = el.Next() {
		s := el.Value.(*Session)
		x := p.index[s]
		fmt.Printf("<-> %s(%d) ", s.ID, x.touched.Unix())
	}
	fmt.Printf("<-> tail\n")
}

// TestSessionPoolTouchWaitQueueOverflow tests that Get() call will block
// as many callers as it possible to number of touched sessions while
// sessions being touched by a background routine.
func TestSessionPoolTouchWaitQueueOverflow(t *testing.T) {
	timerCh := make(chan time.Time)
	cleanupTimer := timeutil.StubTestHookNewTimer(func(d time.Duration) (r timeutil.Timer) {
		return timetest.Timer{Ch: timerCh}
	})
	defer cleanupTimer()

	shiftTime, cleanupNow := timeutil.StubTestHookTimeNow(time.Unix(0, 0))
	defer cleanupNow()

	keepalive := make(chan struct{})
	client := &Client{
		Driver: &testutil.Driver{
			OnCall: func(ctx context.Context, m testutil.MethodCode, req, res interface{}) error {
				switch m {
				case testutil.TableCreateSession:
					return nil
				case testutil.TableKeepAlive:
					keepalive <- struct{}{}
				default:
					t.Fatalf("unexpected operation: %s", m)
				}
				return nil
			},
		},
	}
	p := &SessionPool{
		SizeLimit:          2,
		KeepAliveBatchSize: 2,
		IdleThreshold:      time.Minute,
		Builder:            client,
	}

	// Create two sessions.
	s1 := mustGetSession(t, p)
	s2 := mustGetSession(t, p)
	mustPutSession(t, p, s1)
	mustPutSession(t, p, s2)

	shiftTime(time.Hour)
	timerCh <- timeutil.Now()

	var (
		get = make(chan struct{})
		got = make(chan struct{})
		gtx = WithSessionPoolTrace(context.Background(), SessionPoolTrace{
			GetStart: func(SessionPoolGetStartInfo) {
				get <- struct{}{}
			},
		})
	)
	go func() {
		if _, err := p.Get(gtx); err != nil {
			t.Fatal(err)
		}
		got <- struct{}{}
	}()
	go func() {
		if _, err := p.Get(gtx); err != nil {
			t.Fatal(err)
		}
		got <- struct{}{}
	}()

	// Await for both goroutines locked on Get() call.
	<-get
	<-get

	<-keepalive // Release KeepAlive() on first session.
	<-got       // Await first touched session returned by Get().

	{
		// Call Get() with already canceled context. Receiving context.Canceled
		// error means that we are still wait for touched session. Which is
		// unexpected – we can not touch more sessions than we created; that
		// is, we created two sessions and already awaiting for both – third
		// session will not appear, thus we should not await for more touched
		// sessions.
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		_, err := p.Get(ctx)
		if err == context.Canceled {
			t.Fatalf("unexpected await for touched session")
		}
	}

	<-keepalive // Release KeepAlive() on second session.
	<-got       // Await second touched session returned by Get().
}

// TestSessionPoolGetDisconnected tests case when session successfully created,
// but after that connection become broken and cant be reestablished.
func TestSessionPoolGetDisconnected(t *testing.T) {
	timerCh := make(chan time.Time)
	cleanupTimer := timeutil.StubTestHookNewTimer(func(d time.Duration) (r timeutil.Timer) {
		return timetest.Timer{Ch: timerCh}
	})
	defer cleanupTimer()

	shiftTime, cleanupNow := timeutil.StubTestHookTimeNow(time.Unix(0, 0))
	defer cleanupNow()

	var (
		connected = make(chan struct{})
		keepalive = make(chan struct{})
	)
	client := &Client{
		Driver: &testutil.Driver{
			OnCall: func(ctx context.Context, m testutil.MethodCode, req, res interface{}) error {
				switch m {
				case testutil.TableCreateSession:
					return nil
				case testutil.TableKeepAlive:
					keepalive <- struct{}{}
					// Here we emulating blocked connection initialization.
					<-connected
				default:
					t.Fatalf("unexpected operation: %s", m)
				}
				return nil
			},
		},
	}
	p := &SessionPool{
		SizeLimit:     1,
		IdleThreshold: time.Hour,
		Builder:       client,
	}

	touched := p.touchCond()

	s := mustGetSession(t, p)
	mustPutSession(t, p, s)

	// Trigger next KeepAlive iteration.
	shiftTime(p.IdleThreshold)
	timerCh <- timeutil.Now()
	<-keepalive

	// Here we are in touching state. That is, there are no session in the pool
	// – it is removed for keepalive operation.
	//
	// We expect that Get() method will fail on ctx cancelation.
	ctx1, cancel := context.WithCancel(context.Background())
	time.AfterFunc(500*time.Millisecond, cancel)

	x, err := p.Get(ctx1)
	if err != context.Canceled {
		t.Fatalf("unexpected error: %v", err)
	}
	if x != nil {
		t.Fatalf("obtained unexpected session")
	}

	// Release session s – connection established.
	connected <- struct{}{}
	<-touched // Wait until first keep alive loop finished.

	mustTakeSession(t, p, s)
	mustPutSession(t, p, s)

	// Trigger next KeepAlive iteration.
	shiftTime(p.IdleThreshold)
	timerCh <- timeutil.Now()
	<-keepalive

	ctx2, cancel := context.WithCancel(context.Background())
	time.AfterFunc(500*time.Millisecond, cancel)

	took, err := p.Take(ctx2, s)
	if err != context.Canceled {
		t.Fatalf("unexpected error: %v", err)
	}
	if took {
		t.Fatalf("unexpected take over session")
	}
}

func TestSessionPoolGetPut(t *testing.T) {
	var (
		created int
		deleted int
	)
	client := &Client{
		Driver: &testutil.Driver{
			OnCall: func(ctx context.Context, m testutil.MethodCode, req, res interface{}) error {
				switch m {
				case testutil.TableCreateSession:
					created++
				case testutil.TableDeleteSession:
					deleted++
				default:
					t.Errorf("unexpected operation: %s", m)
				}
				return nil
			},
		},
	}
	p := &SessionPool{
		SizeLimit: 1,
		Builder:   client,
	}

	s1 := mustGetSession(t, p)
	s2 := mustGetSession(t, p)
	s3 := mustGetSession(t, p)
	if act, exp := created, 3; act != exp {
		t.Errorf(
			"unexpected created sessions: %v; want %v",
			act, exp,
		)
	}

	mustPutSession(t, p, s1)
	if deleted > 0 {
		t.Errorf("unexpected deletion")
	}
	mustPutSession(t, p, s2)
	mustPutSession(t, p, s3)
	if act, exp := deleted, 2; act != exp {
		t.Errorf(
			"unexpected deleted sessions: %v; want %v",
			act, exp,
		)
	}

	mustGetSession(t, p)
	if created > 3 {
		t.Errorf("unexpected creation")
	}
}

func TestSessionPoolDisableKeepAlive(t *testing.T) {
	client := &Client{
		Driver: &testutil.Driver{
			OnCall: func(ctx context.Context, m testutil.MethodCode, req, res interface{}) error {
				switch m {
				case testutil.TableCreateSession:
					// OK
				default:
					t.Errorf("unexpected operation: %s", m)
				}
				return nil
			},
		},
	}
	p := &SessionPool{
		SizeLimit: 1,
		Builder:   client,
	}

	s := mustGetSession(t, p)
	mustPutSession(t, p, s)
	time.Sleep(time.Second * 1)
}

func TestSessionPoolKeepAlive(t *testing.T) {
	var (
		keepAlive uint32
		create    uint32
	)
	client := &Client{
		Driver: &testutil.Driver{
			OnCall: func(ctx context.Context, m testutil.MethodCode, req, res interface{}) error {
				switch m {
				case testutil.TableCreateSession:
					i := atomic.AddUint32(&create, 1)

					r := testutil.TableCreateSessionResult{res}
					r.SetSessionID(fmt.Sprintf(
						"session-%d", i,
					))
				case testutil.TableKeepAlive:
					atomic.AddUint32(&keepAlive, 1)
				default:
					t.Errorf("unexpected operation: %s", m)
				}
				return nil
			},
		},
	}

	idleThreshold := 4 * time.Second

	var (
		timerC       = make(chan time.Time)
		timerReset   = make(chan time.Duration, 1)
		newTimerDone = make(chan struct{})

		timerOnce sync.Once
		interval  time.Duration
	)
	cleanup := timeutil.StubTestHookNewTimer(func(d time.Duration) (r timeutil.Timer) {
		timerOnce.Do(func() {
			r = timetest.Timer{
				Ch: timerC,
				OnReset: func(d time.Duration) bool {
					select {
					case timerReset <- d:
					default:
						t.Fatal("timer.Reset() blocked")
					}
					return true
				},
			}
			interval = d
			close(newTimerDone)
		})
		if r == nil {
			t.Fatal("NewTimer() called twice")
		}
		return r
	})
	defer cleanup()

	shiftTime, cleanup := timeutil.StubTestHookTimeNow(time.Unix(0, 0))
	defer cleanup()

	p := &SessionPool{
		SizeLimit:          -1,
		KeepAliveBatchSize: -1,
		Builder:            client,
		IdleThreshold:      idleThreshold,
	}

	s1 := mustGetSession(t, p)
	s2 := mustGetSession(t, p)

	// Put both session at the absolutely same time.
	// That is, both sessions must be keepalived by a single tick (due to
	// KeepAliveBatchSize equal for -1, that is batch any number of sessions
	// per tick).
	mustPutSession(t, p, s1)
	mustPutSession(t, p, s2)

	<-newTimerDone

	if interval != idleThreshold {
		t.Fatalf(
			"unexpected ticker duration: %s; want %s",
			interval, idleThreshold,
		)
	}

	// Emulate first simple tick event. We expect two sessions be keepalived.
	shiftTime(idleThreshold)
	timerC <- timeutil.Now()
	mustResetTimer(t, timerReset, idleThreshold)
	if !atomic.CompareAndSwapUint32(&keepAlive, 2, 0) {
		t.Fatal("unexpected number of keepalives")
	}

	// Now get first session and "spent" some time working within it.
	x := mustGetSession(t, p)
	shiftTime(idleThreshold / 2)

	// Now put that session back and emulate keepalive moment.
	mustPutSession(t, p, x)
	shiftTime(idleThreshold / 2)
	// We expect here next tick to be registered after half of a idleThreshold.
	// That is, x was touched half of idleThreshold ago, so we need to wait for
	// the second half until we must touch it.
	timerC <- timeutil.Now()
	mustResetTimer(t, timerReset, idleThreshold/2)
}

func TestSessionPoolKeepAliveOrdering(t *testing.T) {
	var (
		keepalive = make(chan chan<- struct{})

		id uint32
	)
	client := &Client{
		Driver: &testutil.Driver{
			OnCall: func(ctx context.Context, m testutil.MethodCode, req, res interface{}) error {
				switch m {
				case testutil.TableCreateSession:
					i := atomic.AddUint32(&id, 1)
					r := testutil.TableCreateSessionResult{res}
					r.SetSessionID(fmt.Sprintf(
						"session-%d", i,
					))
				case testutil.TableKeepAlive:
					done := make(chan struct{})
					keepalive <- done
					<-done

				default:
					t.Errorf("unexpected operation: %s", m)
				}
				return nil
			},
		},
	}

	var (
		timerC = make(chan time.Time)
	)
	cleanup := timeutil.StubTestHookNewTimer(func(d time.Duration) (r timeutil.Timer) {
		return timetest.Timer{Ch: timerC}
	})
	defer cleanup()

	shiftTime, cleanup := timeutil.StubTestHookTimeNow(time.Unix(0, 0))
	defer cleanup()

	idleThreshold := 4 * time.Second
	p := &SessionPool{
		SizeLimit:          -1,
		KeepAliveBatchSize: -1,
		Builder:            client,
		IdleThreshold:      idleThreshold,
	}

	s1 := mustGetSession(t, p)
	s2 := mustGetSession(t, p)

	// Put s1 to a pull. Shift time that pool need to keepalive s1.
	mustPutSession(t, p, s1)
	shiftTime(idleThreshold)
	timerC <- timeutil.Now()

	// Await for keepalive request came in.
	var releaseKeepAlive chan<- struct{}
	select {
	case releaseKeepAlive = <-keepalive:
	case <-time.After(time.Millisecond * 100):
		t.Fatal("no keepalive request")
	}

	touchDone := p.touchCond()

	// Now keeper routine sticked on awaiting result of keep alive request.
	// That is perfect time to emulate race condition of pushing s2 back to the
	// pool with time, that is greater than `now` of s1 being touched.
	shiftTime(idleThreshold / 2)
	mustPutSession(t, p, s2)

	// Now release keepalive request, leading keeper to push s1 to the list
	// with touch time lower, than list's back element (s2).
	close(releaseKeepAlive)
	// Wait for touching routine exits.
	<-touchDone

	if x1 := mustGetSession(t, p); x1 != s1 {
		t.Errorf("reordering of sessions did not occur")
	}
}

func TestSessionPoolDoublePut(t *testing.T) {
	client := &Client{
		Driver: &testutil.Driver{
			OnCall: func(ctx context.Context, m testutil.MethodCode, req, res interface{}) error {
				return nil
			},
		},
	}
	p := &SessionPool{
		SizeLimit: 2,
		Builder:   client,
	}
	ctx := context.Background()
	s, err := p.Get(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if thePanic := recover(); thePanic == nil {
			t.Fatalf("no panic")
		}
	}()
	if ok, err := p.Put(ctx, s); !ok || err != nil {
		t.Fatalf("unexpected Put() fail: %t %v", ok, err)
	}
	if ok, err := p.Put(ctx, s); ok && err == nil {
		// Must panic before this line.
		t.Fatalf("unexpected Put() success")
	}
}

func mustResetTimer(t *testing.T, ch <-chan time.Duration, exp time.Duration) {
	select {
	case act := <-ch:
		if act != exp {
			t.Errorf(
				"unexpected timer reset: %s; want %s",
				act, exp,
			)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatalf("%s: no timer reset", caller())
	}
}

func mustGetSession(t *testing.T, p *SessionPool) *Session {
	s, err := p.Get(context.Background())
	if err != nil {
		t.Fatalf("%s: %v", caller(), err)
	}
	return s
}

func mustPutSession(t *testing.T, p *SessionPool, s *Session) {
	_, err := p.Put(context.Background(), s)
	if err != nil {
		t.Fatalf("%s: %v", caller(), err)
	}
}

func mustTakeSession(t *testing.T, p *SessionPool, s *Session) {
	took, err := p.Take(context.Background(), s)
	if !took {
		t.Fatalf("%s: can not take session (%v)", caller(), err)
	}
}

func caller() string {
	_, file, line, _ := runtime.Caller(2)
	return fmt.Sprintf("%s:%d", path.Base(file), line)
}
