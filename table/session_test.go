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

func TestSessionPoolKeeperWake(t *testing.T) {
	var (
		timerOnce    sync.Once
		timerCh      = make(chan time.Time)
		timerCreated = make(chan struct{})
		timerReset   = make(chan time.Duration)
	)
	cleanupTimer := timeutil.StubTestHookNewTimer(func(d time.Duration) (r timeutil.Timer) {
		var success bool
		timerOnce.Do(func() {
			success = true
		})
		if !success {
			t.Fatalf("timeutil.NewTimer() is called more than once")
			return nil
		}
		close(timerCreated)
		return timetest.Timer{
			Ch: timerCh,
			OnReset: func(d time.Duration) bool {
				timerReset <- d
				return true
			},
		}
	})
	defer cleanupTimer()

	shiftTime, cleanupNow := timeutil.StubTestHookTimeNow(time.Unix(0, 0))
	defer cleanupNow()

	var (
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

	s := mustGetSession(t, p)

	// Wait for keeper goroutine become initialized.
	<-timerCreated

	// Trigger keepalive timer event.
	// NOTE: code below would blocked if KeepAlive() call will happen for this
	// event.
	done := p.touchCond()
	shiftTime(p.IdleThreshold)
	timerCh <- timeutil.Now()
	<-done

	// Return session to wake up the keeper.
	mustPutSession(t, p, s)
	<-timerReset

	// Trigger timer event and expect keepalive to be prepared.
	shiftTime(p.IdleThreshold)
	timerCh <- timeutil.Now()
	<-keepalive
}

func TestSessionPoolCloseWhenWaiting(t *testing.T) {
	for _, test := range []struct {
		name string
		racy bool
	}{
		{
			name: "normal",
			racy: false,
		},
		{
			name: "racy",
			racy: true,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			p := &SessionPool{
				SizeLimit: 1,
				Builder: &StubBuilder{
					T:     t,
					Limit: 1,
				},
			}

			mustGetSession(t, p)

			var (
				get  = make(chan struct{})
				wait = make(chan struct{})
				got  = make(chan error)
			)
			go func() {
				_, err := p.Get(WithSessionPoolTrace(context.Background(), SessionPoolTrace{
					GetStart: func(SessionPoolGetStartInfo) {
						get <- struct{}{}
					},
					WaitStart: func(SessionPoolWaitStartInfo) {
						wait <- struct{}{}
					},
				}))
				got <- err
			}()

			regwait := whenWantWaitCh()
			<-get     // Await for getter blocked on awaiting session.
			<-regwait // Let the getter register itself in the wait queue.

			if test.racy {
				// We testing the case, when session consumer registered
				// himself in the wait queue, but not ready to receive the
				// session when session arrives (that is, stucked between
				// pushing channel in the list and reading from the channel).
				p.Close(context.Background())
				<-wait
			} else {
				// We testing the normal case, when session consumer registered
				// himself in the wait queue and successfully blocked on
				// reading from signaling channel.
				<-wait
				// Let the waiting goroutine to block on reading from channel.
				runtime.Gosched()
				p.Close(context.Background())
			}

			const timeout = time.Second
			select {
			case err := <-got:
				if err != ErrSessionPoolClosed {
					t.Fatalf(
						"unexpected error: %v; want %v",
						err, ErrSessionPoolClosed,
					)
				}
			case <-time.After(timeout):
				t.Fatalf("no result after %s", timeout)
			}
		})
	}

}

func TestSessionPoolClose(t *testing.T) {
	p := &SessionPool{
		SizeLimit: 2,
		Builder: &StubBuilder{
			T:     t,
			Limit: 2,
		},
	}

	s1 := mustGetSession(t, p)
	s2 := mustGetSession(t, p)

	var (
		closed1 bool
		closed2 bool
	)
	s1.OnClose(func() { closed1 = true })
	s2.OnClose(func() { closed2 = true })

	mustPutSession(t, p, s1)

	p.Close(context.Background())

	if !closed1 {
		t.Fatalf("session was not closed")
	}
	if closed2 {
		t.Fatalf("unexpected session close")
	}

	err := p.Put(context.Background(), s2)
	if err != ErrSessionPoolClosed {
		t.Errorf(
			"unexpected Put() error: %v; want %v",
			err, ErrSessionPoolClosed,
		)
	}
	if !closed2 {
		t.Fatalf("session was not closed")
	}
}

func TestSessionPoolDeleteReleaseWait(t *testing.T) {
	for _, test := range []struct {
		name string
		racy bool
	}{
		{
			name: "normal",
			racy: false,
		},
		{
			name: "racy",
			racy: true,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			p := &SessionPool{
				SizeLimit: 1,
				Builder: &StubBuilder{
					T:     t,
					Limit: 2,
				},
			}
			s := mustGetSession(t, p)
			var (
				get  = make(chan struct{})
				wait = make(chan struct{})
				got  = make(chan struct{})
			)
			go func() {
				defer func() {
					close(got)
				}()
				p.Get(WithSessionPoolTrace(context.Background(), SessionPoolTrace{
					GetStart: func(SessionPoolGetStartInfo) {
						get <- struct{}{}
					},
					WaitStart: func(SessionPoolWaitStartInfo) {
						wait <- struct{}{}
					},
				}))
			}()

			regwait := whenWantWaitCh()
			<-get     // Await for getter blocked on awaiting session.
			<-regwait // Let the getter register itself in the wait queue.

			if test.racy {
				// We testing the case, when session consumer registered
				// himself in the wait queue, but not ready to receive the
				// session when session arrives (that is, stucked between
				// pushing channel in the list and reading from the channel).
				s.Close(context.Background())
				<-wait
			} else {
				// We testing the normal case, when session consumer registered
				// himself in the wait queue and successfully blocked on
				// reading from signaling channel.
				<-wait
				// Let the waiting goroutine to block on reading from channel.
				runtime.Gosched()
				s.Close(context.Background())
			}

			const timeout = time.Second
			select {
			case <-got:
			case <-time.After(timeout):
				t.Fatalf("no get after %s", timeout)
			}
		})
	}
}

func TestSessionPoolRacyGet(t *testing.T) {
	type createReq struct {
		release chan struct{}
		session *Session
	}
	create := make(chan createReq)
	p := &SessionPool{
		SizeLimit: 1,
		Builder: &StubBuilder{
			Limit: 2,
			OnCreateSession: func(ctx context.Context) (*Session, error) {
				req := createReq{
					release: make(chan struct{}),
					session: simpleSession(),
				}
				create <- req
				<-req.release
				return req.session, nil
			},
		},
	}
	var (
		expSession *Session
		done       = make(chan struct{}, 2)
	)
	for i := 0; i < 2; i++ {
		go func() {
			defer func() {
				done <- struct{}{}
			}()
			s, err := p.Get(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if s != expSession {
				t.Fatalf("unexpected session: %v; want %v", s, expSession)
			}
			mustPutSession(t, p, s)
		}()
	}

	// Wait for both requests are created.
	r1 := <-create
	r2 := <-create

	// Release the first create session request.
	// Created session must be stored in the pool.
	expSession = r1.session
	expSession.OnClose(func() {
		t.Fatalf("unexpected first session close")
	})
	close(r1.release)

	// Wait for r1's session will be stored in the pool.
	<-done

	// Ensure that session is in the pool.
	s := mustGetSession(t, p)
	mustPutSession(t, p, s)

	// Release the second create session request.
	// Created session must deleted immediately because there is no more space
	// in the pool.
	deleted := make(chan struct{})
	r2.session.OnClose(func() {
		close(deleted)
	})
	close(r2.release)

	const timeout = time.Second
	select {
	case <-deleted:
	case <-time.After(timeout):
		t.Fatalf("no session delete after %s", timeout)
	}
}

func TestSessionPoolPutInFull(t *testing.T) {
	p := &SessionPool{
		SizeLimit: 1,
	}
	p.Put(context.Background(), simpleSession())

	defer func() {
		if thePanic := recover(); thePanic == nil {
			t.Fatalf("no panic")
		}
	}()
	p.Put(context.Background(), simpleSession())
}

func TestSessionPoolSizeLimitOverflow(t *testing.T) {
	type sessionAndError struct {
		session *Session
		err     error
	}
	for _, test := range []struct {
		name string
		racy bool
	}{
		{
			name: "normal",
			racy: false,
		},
		{
			name: "racy",
			racy: true,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			p := &SessionPool{
				SizeLimit: 1,
				Builder: &StubBuilder{
					T:     t,
					Limit: 1,
				},
			}
			s := mustGetSession(t, p)
			{
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				if _, err := p.Get(ctx); err != context.Canceled {
					t.Fatalf(
						"unexpected error: %v; want %v",
						err, context.Canceled,
					)
				}
			}
			var (
				get  = make(chan struct{})
				wait = make(chan struct{})
				got  = make(chan sessionAndError)
			)
			go func() {
				ctx := WithSessionPoolTrace(context.Background(), SessionPoolTrace{
					GetStart: func(SessionPoolGetStartInfo) {
						get <- struct{}{}
					},
					WaitStart: func(SessionPoolWaitStartInfo) {
						wait <- struct{}{}
					},
				})
				s, err := p.Get(ctx)
				got <- sessionAndError{s, err}
			}()

			regwait := whenWantWaitCh()
			<-get     // Await for getter blocked on awaiting session.
			<-regwait // Let the getter register itself in the wait queue.

			if test.racy {
				// We testing the case, when session consumer registered
				// himself in the wait queue, but not ready to receive the
				// session when session arrives (that is, stucked between
				// pushing channel in the list and reading from the channel).
				p.Put(context.Background(), s)
				<-wait
			} else {
				// We testing the normal case, when session consumer registered
				// himself in the wait queue and successfully blocked on
				// reading from signaling channel.
				<-wait
				// Let the waiting goroutine to block on reading from channel.
				runtime.Gosched()
				p.Put(context.Background(), s)
			}

			const timeout = time.Second
			select {
			case se := <-got:
				if se.err != nil {
					t.Fatal(se.err)
				}
				if se.session != s {
					t.Fatalf("unexpected session")
				}
			case <-time.After(timeout):
				t.Fatalf("no session after %s", timeout)
			}
		})
	}
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
	assertCreated := func(exp int) {
		if act := created; act != exp {
			t.Errorf(
				"unexpected number of created sessions: %v; want %v",
				act, exp,
			)
		}
	}
	assertDeleted := func(exp int) {
		if act := deleted; act != exp {
			t.Errorf(
				"unexpected number of deleted sessions: %v; want %v",
				act, exp,
			)
		}
	}
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

	s := mustGetSession(t, p)
	assertCreated(1)

	mustPutSession(t, p, s)
	assertDeleted(0)

	mustGetSession(t, p)
	assertCreated(1)

	s.Close(context.Background())
	assertDeleted(1)

	mustGetSession(t, p)
	assertCreated(2)
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
		SizeLimit:     2,
		Builder:       client,
		IdleThreshold: idleThreshold,
	}

	s1 := mustGetSession(t, p)
	s2 := mustGetSession(t, p)

	// Put both session at the absolutely same time.
	// That is, both sessions must be keepalived by a single tick (due to
	// KeepAliveBatchSize is unlimited).
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
		SizeLimit:     2,
		Builder:       client,
		IdleThreshold: idleThreshold,
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
	if err := p.Put(ctx, s); err != nil {
		t.Fatalf("unexpected Put() error: %v", err)
	}
	if err := p.Put(ctx, s); err == nil {
		// Must panic before this line.
		t.Fatalf("unexpected Put() success")
	}
}

func TestSessionPoolReuseWaitChannel(t *testing.T) {
	ch1 := getWaitCh()
	putWaitCh(ch1)
	ch2 := getWaitCh()
	if ch1 != ch2 {
		t.Errorf("unexpected reused channel")
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
	if err := p.Put(context.Background(), s); err != nil {
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

func simpleSession() *Session {
	return &Session{
		c: Client{
			Driver: &testutil.Driver{
				OnCall: func(ctx context.Context, m testutil.MethodCode, req, res interface{}) error {
					return nil
				},
			},
		},
	}
}

type StubBuilder struct {
	OnCreateSession func(ctx context.Context) (*Session, error)
	Limit           int
	T               *testing.T

	mu     sync.Mutex
	actual int
}

func (s *StubBuilder) CreateSession(ctx context.Context) (*Session, error) {
	s.mu.Lock()
	if s.Limit > 0 && s.actual == s.Limit {
		if s.T != nil {
			s.T.Errorf("create session limit overflow")
		}
		return nil, fmt.Errorf("stub builder: limit overflow")
	}
	s.actual++
	s.mu.Unlock()

	if f := s.OnCreateSession; f != nil {
		return f(ctx)
	}

	return simpleSession(), nil
}

func (p *SessionPool) debug() {
	fmt.Printf("head ")
	for el := p.idle.Front(); el != nil; el = el.Next() {
		s := el.Value.(*Session)
		x := p.index[s]
		fmt.Printf("<-> %s(%d) ", s.ID, x.touched.Unix())
	}
	fmt.Printf("<-> tail\n")
}

func whenWantWaitCh() <-chan struct{} {
	var (
		prev = testHookGetWaitCh
		ch   = make(chan struct{})
	)
	testHookGetWaitCh = func() {
		testHookGetWaitCh = prev
		close(ch)
	}
	return ch
}
