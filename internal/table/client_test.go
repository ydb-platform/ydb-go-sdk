package table

import (
	"context"
	"fmt"
	"math/rand"
	"path"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Table"

	"github.com/ydb-platform/ydb-go-sdk/v3/cluster"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/errors"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/testutil"
	"github.com/ydb-platform/ydb-go-sdk/v3/testutil/timeutil"
	"github.com/ydb-platform/ydb-go-sdk/v3/testutil/timeutil/timetest"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func TestSessionPoolCreateAbnormalResult(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping testing in non-short mode")
	}

	p := newClientWithStubBuilder(
		t,
		testutil.NewCluster(
			testutil.WithInvokeHandlers(
				testutil.InvokeHandlers{
					//nolint: unparam
					testutil.TableCreateSession: func(interface{}) (proto.Message, error) {
						return &Ydb_Table.CreateSessionResult{
							SessionId: testutil.SessionID(),
						}, nil
					},
					testutil.TableDeleteSession: okHandler,
				},
			),
		),
		1000,
		config.WithSizeLimit(1000),
	)
	defer func() {
		_ = p.Close(context.Background())
	}()
	for i := 0; i < 10000; i++ {
		t.Run("", func(t *testing.T) {
			ctx, cancel := context.WithTimeout(
				context.Background(),
				time.Duration(rand.Float32()+float32(time.Second)),
			)
			defer cancel()
			s, err := p.createSession(ctx)

			if s == nil && err == nil {
				t.Fatalf("unexpected result: <%v, %v>", s, err)
			}
		})
	}
}

func TestSessionPoolKeeperWake(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping testing in non-short mode")
	}

	timer := timetest.StubSingleTimer(t)
	defer timer.Cleanup()

	shiftTime, cleanupNow := timeutil.StubTestHookTimeNow(time.Unix(0, 0))
	defer cleanupNow()

	var (
		keepalive = make(chan struct{})
	)

	p := newClientWithStubBuilder(
		t,
		testutil.NewCluster(
			testutil.WithInvokeHandlers(
				testutil.InvokeHandlers{
					// nolint:unparam
					testutil.TableCreateSession: func(interface{}) (proto.Message, error) {
						return &Ydb_Table.CreateSessionResult{
							SessionId: testutil.SessionID(),
						}, nil
					},
					// nolint:unparam
					testutil.TableKeepAlive: func(interface{}) (proto.Message, error) {
						keepalive <- struct{}{}
						return nil, nil
					},
					testutil.TableDeleteSession: okHandler,
				},
			),
		),
		1,
		config.WithSizeLimit(1),
		config.WithIdleThreshold(time.Hour),
	)

	defer func() {
		err := p.Close(context.Background())
		if err != nil {
			t.Errorf("unexpected error on close: %v", err)
		}
	}()

	s := mustGetSession(t, p)

	// Wait for keeper goroutine become initialized.
	<-timer.Created

	// Trigger keepalive timer event.
	// NOTE: code below would be blocked if KeepAlive() call will happen for this
	// event.
	done := p.touchCond()
	shiftTime(p.config.IdleThreshold())
	timer.C <- timeutil.Now()
	<-done

	// Return build to wake up the keeper.
	mustPutSession(t, p, s)
	<-timer.Reset

	// Trigger timer event and expect keepalive to be prepared.
	shiftTime(p.config.IdleThreshold())
	timer.C <- timeutil.Now()
	<-keepalive
	<-timer.Reset
}

func TestSessionPoolCloseWhenWaiting(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping testing in non-short mode")
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
			var (
				get  = make(chan struct{})
				wait = make(chan struct{})
				got  = make(chan error)
			)
			p := newClientWithStubBuilder(
				t,
				testutil.NewCluster(testutil.WithInvokeHandlers(testutil.InvokeHandlers{
					//nolint: unparam
					testutil.TableCreateSession: func(interface{}) (proto.Message, error) {
						return &Ydb_Table.CreateSessionResult{
							SessionId: testutil.SessionID(),
						}, nil
					}},
				)),
				1,
				config.WithSizeLimit(1),
				config.WithTrace(
					trace.Table{
						OnPoolWait: func(trace.PoolWaitStartInfo) func(trace.PoolWaitDoneInfo) {
							wait <- struct{}{}
							return nil
						},
					},
				),
			)
			defer func() {
				_ = p.Close(context.Background())
			}()

			mustGetSession(t, p)

			go func() {
				_, err := p.Get(
					trace.WithTable(
						context.Background(),
						trace.Table{
							OnPoolGet: func(trace.PoolGetStartInfo) func(trace.PoolGetDoneInfo) {
								get <- struct{}{}
								return nil
							},
						},
					),
				)
				got <- err
			}()

			regWait := whenWantWaitCh(p)
			<-get     // Await for getter blocked on awaiting build.
			<-regWait // Let the getter register itself in the wait queue.

			if test.racy {
				// We are testing the case, when build consumer registered
				// himself in the wait queue, but not ready to receive the
				// build when build arrives (that is, stuck between
				// pushing channel in the list and reading from the channel).
				_ = p.Close(context.Background())
				<-wait
			} else {
				// We are testing the normal case, when build consumer registered
				// himself in the wait queue and successfully blocked on
				// reading from signaling channel.
				<-wait
				// Let the waiting goroutine to block on reading from channel.
				_ = p.Close(context.Background())
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
	if !testing.Short() {
		t.Skip("skipping testing in non-short mode")
	}

	wg := sync.WaitGroup{}
	p := newClientWithStubBuilder(
		t,
		testutil.NewCluster(testutil.WithInvokeHandlers(testutil.InvokeHandlers{
			//nolint: unparam
			testutil.TableCreateSession: func(interface{}) (proto.Message, error) {
				return &Ydb_Table.CreateSessionResult{
					SessionId: testutil.SessionID(),
				}, nil
			}},
		)),
		3,
		config.WithSizeLimit(3),
		config.WithIdleThreshold(time.Hour),
		config.WithTrace(
			trace.Table{
				OnPoolPut: func(info trace.PoolPutStartInfo) func(trace.PoolPutDoneInfo) {
					wg.Add(1)
					return func(info trace.PoolPutDoneInfo) {
						wg.Done()
					}
				},
				OnPoolSessionClose: func(info trace.PoolSessionCloseStartInfo) func(doneInfo trace.PoolSessionCloseDoneInfo) {
					wg.Add(1)
					return func(info trace.PoolSessionCloseDoneInfo) {
						wg.Done()
					}
				},
			},
		),
	)
	defer func() {
		_ = p.Close(context.Background())
	}()

	var (
		s1      = mustGetSession(t, p)
		s2      = mustGetSession(t, p)
		s3      = mustGetSession(t, p)
		closed1 = false
		closed2 = false
		closed3 = false
	)

	s1.OnClose(func(context.Context) { closed1 = true })
	s2.OnClose(func(context.Context) { closed2 = true })
	s3.OnClose(func(context.Context) { closed3 = true })

	mustPutSession(t, p, s1)
	mustPutSession(t, p, s2)
	mustClose(t, p)

	if !closed1 {
		t.Fatalf("session1 was not closed")
	}
	if !closed2 {
		t.Fatalf("session2 was not closed")
	}
	if closed3 {
		t.Fatalf("unexpected build close")
	}

	if err := p.Put(context.Background(), s3); err != ErrSessionPoolClosed {
		t.Errorf(
			"unexpected Put() error: %v; want %v",
			err, ErrSessionPoolClosed,
		)
	}
	wg.Wait()

	if !closed3 {
		t.Fatalf("build was not closed")
	}
}

func TestSessionPoolDeleteReleaseWait(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping testing in non-short mode")
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
			var (
				get  = make(chan struct{}, 1)
				wait = make(chan struct{})
				got  = make(chan struct{}, 1)
			)
			p := newClientWithStubBuilder(
				t,
				testutil.NewCluster(testutil.WithInvokeHandlers(testutil.InvokeHandlers{
					//nolint: unparam
					testutil.TableCreateSession: func(interface{}) (proto.Message, error) {
						return &Ydb_Table.CreateSessionResult{
							SessionId: testutil.SessionID(),
						}, nil
					}},
				)),
				2,
				config.WithSizeLimit(1),
				config.WithIdleThreshold(time.Hour),
				config.WithTrace(
					trace.Table{
						OnPoolGet: func(trace.PoolGetStartInfo) func(trace.PoolGetDoneInfo) {
							get <- struct{}{}
							return nil
						},
						OnPoolWait: func(trace.PoolWaitStartInfo) func(trace.PoolWaitDoneInfo) {
							wait <- struct{}{}
							return nil
						},
					},
				),
			)
			defer func() {
				_ = p.Close(context.Background())
			}()
			s := mustGetSession(t, p)
			go func() {
				defer func() {
					close(got)
				}()
				_, _ = p.Get(context.Background())
			}()

			regWait := whenWantWaitCh(p)
			<-get     // Await for getter blocked on awaiting build.
			<-regWait // Let the getter register itself in the wait queue.

			if test.racy {
				// We are testing the case, when build consumer registered
				// himself in the wait queue, but not ready to receive the
				// build when build arrives (that is, it was stucked between
				// pushing channel in the list and reading from the channel).
				_ = s.Close(context.Background())
				<-wait
			} else {
				// We are testing the normal case, when build consumer registered
				// himself in the wait queue and successfully blocked on
				// reading from signaling channel.
				<-wait
				// Let the waiting goroutine to block on reading from channel.
				runtime.Gosched()
				_ = s.Close(context.Background())
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
	if !testing.Short() {
		t.Skip("skipping testing in non-short mode")
	}

	type createReq struct {
		release chan struct{}
		session Session
	}
	create := make(chan createReq)
	p := newClient(
		context.Background(),
		nil,
		(&StubBuilder{
			Limit: 1,
			OnCreateSession: func(ctx context.Context) (Session, error) {
				req := createReq{
					release: make(chan struct{}),
					session: simpleSession(t),
				}
				create <- req
				<-req.release
				return req.session, nil
			},
		}).createSession,
		config.New(
			config.WithSizeLimit(1),
			config.WithIdleThreshold(-1),
		),
	)
	var (
		expSession Session
		done       = make(chan struct{}, 2)
	)
	var err error
	for i := 0; i < 2; i++ {
		go func() {
			defer func() {
				done <- struct{}{}
			}()
			s, e := p.Get(context.Background())
			if e != nil {
				err = e
				return
			}
			if s != expSession {
				err = fmt.Errorf("unexpected build: %v; want %v", s, expSession)
				return
			}
			mustPutSession(t, p, s)
		}()
	}
	if err != nil {
		t.Fatal(err)
	}
	// Wait for both requests are created.
	r1 := <-create
	select {
	case <-create:
		t.Fatalf("build 2 on race created while client size 1")
	case <-time.After(time.Millisecond * 5):
		// ok
	}

	// Release the first create build request.
	// Created build must be stored in the client.
	expSession = r1.session
	expSession.OnClose(func(context.Context) {
		t.Fatalf("unexpected first build close")
	})
	close(r1.release)

	// Wait for r1's build will be stored in the client.
	<-done

	// Ensure that build is in the client.
	s := mustGetSession(t, p)
	mustPutSession(t, p, s)
}

func TestSessionPoolPutInFull(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping testing in non-short mode")
	}

	p := newClientWithStubBuilder(
		t,
		testutil.NewCluster(testutil.WithInvokeHandlers(testutil.InvokeHandlers{
			//nolint: unparam
			testutil.TableCreateSession: func(interface{}) (proto.Message, error) {
				return &Ydb_Table.CreateSessionResult{
					SessionId: testutil.SessionID(),
				}, nil
			}},
		)),
		1,
		config.WithSizeLimit(1),
		config.WithIdleThreshold(-1),
	)
	s := mustGetSession(t, p)
	if err := p.Put(context.Background(), s); err != nil {
		t.Fatalf("unexpected error on put build into non-full client: %v, wand: %v", err, nil)
	}

	if err := p.Put(context.Background(), simpleSession(t)); err != ErrSessionPoolOverflow {
		t.Fatalf("unexpected error on put build into full client: %v, wand: %v", err, ErrSessionPoolOverflow)
	}
}

func TestSessionPoolSizeLimitOverflow(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping testing in non-short mode")
	}

	type sessionAndError struct {
		session Session
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
			var (
				get  = make(chan struct{})
				wait = make(chan struct{})
				got  = make(chan sessionAndError)
			)
			p := newClientWithStubBuilder(
				t,
				testutil.NewCluster(testutil.WithInvokeHandlers(testutil.InvokeHandlers{
					//nolint: unparam
					testutil.TableCreateSession: func(interface{}) (result proto.Message, _ error) {
						return &Ydb_Table.CreateSessionResult{
							SessionId: testutil.SessionID(),
						}, nil
					}},
				)),
				1,
				config.WithSizeLimit(1),
			)
			defer func() {
				_ = p.Close(context.Background())
			}()
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
			go func() {
				session, err := p.Get(
					trace.WithTable(
						context.Background(),
						trace.Table{
							OnPoolGet: func(trace.PoolGetStartInfo) func(trace.PoolGetDoneInfo) {
								get <- struct{}{}
								return nil
							},
							OnPoolWait: func(trace.PoolWaitStartInfo) func(trace.PoolWaitDoneInfo) {
								wait <- struct{}{}
								return nil
							},
						},
					),
				)
				got <- sessionAndError{session, err}
			}()

			regWait := whenWantWaitCh(p)
			<-get     // Await for getter blocked on awaiting build.
			<-regWait // Let the getter register itself in the wait queue.

			if test.racy {
				// We are testing the case, when build consumer registered
				// himself in the wait queue, but not ready to receive the
				// build when build arrives (that is, it was stucked between
				// pushing channel in the list and reading from the channel).
				_ = p.Put(context.Background(), s)
				<-wait
			} else {
				// We are testing the normal case, when build consumer registered
				// himself in the wait queue and successfully blocked on
				// reading from signaling channel.
				<-wait
				// Let the waiting goroutine to block on reading from channel.
				_ = p.Put(context.Background(), s)
			}

			const timeout = time.Second
			select {
			case se := <-got:
				if se.err != nil {
					t.Fatal(se.err)
				}
				if se.session != s {
					t.Fatalf("unexpected build")
				}
			case <-time.After(timeout):
				t.Fatalf("no build after %s", timeout)
			}
		})
	}
}

// TestSessionPoolGetDisconnected tests case when build successfully created,
// but after that connection become broken and cannot be reestablished.
func TestSessionPoolGetDisconnected(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping testing in non-short mode")
	}

	timer := timetest.StubSingleTimer(t)
	defer timer.Cleanup()

	shiftTime, cleanupNow := timeutil.StubTestHookTimeNow(time.Unix(0, 0))
	defer cleanupNow()

	var (
		release   = make(chan struct{})
		keepalive = make(chan struct{})
	)

	p := newClientWithStubBuilder(
		t,
		testutil.NewCluster(
			testutil.WithInvokeHandlers(
				testutil.InvokeHandlers{
					// nolint:unparam
					testutil.TableCreateSession: func(interface{}) (proto.Message, error) {
						return &Ydb_Table.CreateSessionResult{
							SessionId: testutil.SessionID(),
						}, nil
					},
					// nolint:unparam
					testutil.TableKeepAlive: func(interface{}) (proto.Message, error) {
						keepalive <- struct{}{}
						// Here we are emulating blocked connection initialization.
						<-release
						return nil, nil
					},
					testutil.TableDeleteSession: okHandler,
				},
			),
		),
		1,
		config.WithSizeLimit(1),
		config.WithIdleThreshold(time.Hour),
	)
	defer func() {
		_ = p.Close(context.Background())
	}()

	touched := p.touchCond()

	s := mustGetSession(t, p)
	mustPutSession(t, p, s)

	<-timer.Created

	// Trigger next KeepAlive iteration.
	shiftTime(p.config.IdleThreshold())
	timer.C <- timeutil.Now()
	<-keepalive

	// Here we are in touching state. That is, there are no build in the client
	// – it is removed for keepalive operation.
	//
	// We expect that Get() method will fail on ctx cancellation.
	ctx1, cancel := context.WithCancel(context.Background())
	time.AfterFunc(500*time.Millisecond, cancel)

	x, err := p.Get(ctx1)
	if err != context.Canceled {
		t.Fatalf("unexpected error: %v", err)
	}
	if x != nil {
		t.Fatalf("obtained unexpected build")
	}

	// Release build s – connection established.
	release <- struct{}{}
	<-timer.Reset
	<-touched // Wait until first keep alive loop finished.

	mustTakeSession(t, p, s)
	mustPutSession(t, p, s)

	// Trigger next KeepAlive iteration.
	shiftTime(p.config.IdleThreshold())
	timer.C <- timeutil.Now()
	<-keepalive

	ctx2, cancel := context.WithCancel(context.Background())
	time.AfterFunc(500*time.Millisecond, cancel)

	took, err := p.Take(ctx2, s)
	if err != context.Canceled {
		t.Fatalf("unexpected error: %v", err)
	}
	if took {
		t.Fatalf("unexpected take over build")
	}

	// Release build s to not block on defer p.Close().
	release <- struct{}{}
	<-timer.Reset
}

func TestSessionPoolGetPut(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping testing in non-short mode")
	}

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
	p := newClientWithStubBuilder(
		t,
		testutil.NewCluster(
			testutil.WithInvokeHandlers(
				testutil.InvokeHandlers{
					// nolint:unparam
					testutil.TableCreateSession: func(interface{}) (proto.Message, error) {
						created++
						return &Ydb_Table.CreateSessionResult{
							SessionId: testutil.SessionID(),
						}, nil
					},
					// nolint:unparam
					testutil.TableDeleteSession: func(interface{}) (proto.Message, error) {
						deleted++
						return nil, nil
					},
				},
			),
		),
		0,
		config.WithSizeLimit(1),
	)
	defer func() {
		_ = p.Close(context.Background())
	}()

	s := mustGetSession(t, p)
	assertCreated(1)

	mustPutSession(t, p, s)
	assertDeleted(0)

	mustGetSession(t, p)
	assertCreated(1)

	_ = s.Close(context.Background())
	assertDeleted(1)

	mustGetSession(t, p)
	assertCreated(2)
}

func TestSessionPoolDisableBackgroundGoroutines(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping testing in non-short mode")
	}

	timer := timetest.StubSingleTimer(t)
	defer timer.Cleanup()

	p := newClientWithStubBuilder(
		t,
		testutil.NewCluster(testutil.WithInvokeHandlers(testutil.InvokeHandlers{
			//nolint: unparam
			testutil.TableCreateSession: func(interface{}) (result proto.Message, _ error) {
				return &Ydb_Table.CreateSessionResult{
					SessionId: testutil.SessionID(),
				}, nil
			}},
		)),
		1,
		config.WithSizeLimit(1),
		config.WithIdleThreshold(-1),
	)

	s := mustGetSession(t, p)
	mustPutSession(t, p, s)

	const timeout = time.Second
	select {
	case <-timer.Created:
		t.Fatalf("unexpected created timer")
	case <-time.After(timeout):
	}
}

func TestSessionPoolKeepAlive(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping testing in non-short mode")
	}

	timer := timetest.StubSingleTimer(t)
	defer timer.Cleanup()

	shiftTime, cleanup := timeutil.StubTestHookTimeNow(time.Unix(0, 0))
	defer cleanup()

	var (
		idleThreshold = 4 * time.Second

		keepAliveCount uint32
	)
	p := newClientWithStubBuilder(
		t,
		testutil.NewCluster(
			testutil.WithInvokeHandlers(
				testutil.InvokeHandlers{
					// nolint:unparam
					testutil.TableKeepAlive: func(interface{}) (proto.Message, error) {
						atomic.AddUint32(&keepAliveCount, 1)
						return &Ydb_Table.KeepAliveResult{}, nil
					},
					testutil.TableDeleteSession: okHandler,
					// nolint:unparam
					testutil.TableCreateSession: func(interface{}) (proto.Message, error) {
						return &Ydb_Table.CreateSessionResult{
							SessionId: testutil.SessionID(),
						}, nil
					},
				},
			),
		),
		2,
		config.WithSizeLimit(2),
		config.WithIdleThreshold(idleThreshold),
	)
	defer func() {
		_ = p.Close(context.Background())
	}()

	s1 := mustGetSession(t, p)
	s2 := mustGetSession(t, p)

	// Put both sessions at the absolutely same time.
	// That is, both sessions must be keepalived by a single tick.
	mustPutSession(t, p, s1)
	mustPutSession(t, p, s2)

	interval := <-timer.Created
	if interval != idleThreshold {
		t.Fatalf(
			"unexpected ticker duration: %s; want %s",
			interval, idleThreshold,
		)
	}

	// Emulate first simple tick event. We expect two sessions be keepalived.
	shiftTime(idleThreshold)
	timer.C <- timeutil.Now()
	mustResetTimer(t, timer.Reset, idleThreshold)
	if !atomic.CompareAndSwapUint32(&keepAliveCount, 2, 0) {
		t.Fatal("unexpected number of keepalives")
	}

	// Now get first build and "spent" some time working within it.
	x := mustGetSession(t, p)
	shiftTime(idleThreshold / 2)

	// Now put that build back and emulate keepalive moment.
	mustPutSession(t, p, x)
	shiftTime(idleThreshold / 2)
	// We expect here next tick to be registered after half of a idleThreshold.
	// That is, x was touched half of idleThreshold ago, so we need to wait for
	// the second half until we must touch it.
	timer.C <- timeutil.Now()
	mustResetTimer(t, timer.Reset, idleThreshold/2)
}

func TestSessionPoolKeepAliveOrdering(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping testing in non-short mode")
	}

	timer := timetest.StubSingleTimer(t)
	defer timer.Cleanup()

	shiftTime, cleanup := timeutil.StubTestHookTimeNow(time.Unix(0, 0))
	defer cleanup()

	var (
		idleThreshold = 4 * time.Second
		keepalive     = make(chan chan<- struct{})
	)
	p := newClientWithStubBuilder(
		t,
		testutil.NewCluster(
			testutil.WithInvokeHandlers(
				testutil.InvokeHandlers{
					// nolint:unparam
					testutil.TableCreateSession: func(interface{}) (proto.Message, error) {
						return &Ydb_Table.CreateSessionResult{
							SessionId: testutil.SessionID(),
						}, nil
					},
					// nolint:unparam
					testutil.TableKeepAlive: func(interface{}) (proto.Message, error) {
						done := make(chan struct{})
						keepalive <- done
						<-done
						return nil, nil
					},
					testutil.TableDeleteSession: okHandler,
				},
			),
		),
		2,
		config.WithSizeLimit(2),
		config.WithIdleThreshold(idleThreshold),
	)
	defer func() {
		_ = p.Close(context.Background())
	}()

	s1 := mustGetSession(t, p)
	s2 := mustGetSession(t, p)

	<-timer.Created

	// Put s1 to a pull. Shift time that client need to keepalive s1.
	mustPutSession(t, p, s1)
	shiftTime(idleThreshold)
	timer.C <- timeutil.Now()

	// Await for keepalive request came in.
	var releaseKeepAlive chan<- struct{}
	select {
	case releaseKeepAlive = <-keepalive:
	case <-time.After(time.Millisecond * 100):
		t.Fatal("no keepalive request")
	}

	touchDone := p.touchCond()

	// Now keeper routine must be sticked on awaiting result of keep alive request.
	// That is perfect time to emulate race condition of pushing s2 back to the
	// client with time, that is greater than `now` of s1 being touched.
	shiftTime(idleThreshold / 2)
	mustPutSession(t, p, s2)

	// Now release keepalive request, leading keeper to push s1 to the list
	// with touch time lower, than list's back element (s2).
	close(releaseKeepAlive)
	// Wait for touching routine exits.
	<-timer.Reset
	<-touchDone

	if x1 := mustGetSession(t, p); x1 != s1 {
		t.Errorf("reordering of sessions did not occur")
	}
}

func TestSessionPoolDoublePut(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping testing in non-short mode")
	}

	p := newClientWithStubBuilder(
		t,
		testutil.NewCluster(testutil.WithInvokeHandlers(testutil.InvokeHandlers{
			//nolint: unparam
			testutil.TableCreateSession: func(interface{}) (proto.Message, error) {
				return &Ydb_Table.CreateSessionResult{
					SessionId: testutil.SessionID(),
				}, nil
			}},
		)),
		1,
		config.WithSizeLimit(1),
		config.WithIdleThreshold(-1),
	)

	s := mustGetSession(t, p)
	mustPutSession(t, p, s)

	defer func() {
		if thePanic := recover(); thePanic == nil {
			t.Fatalf("no panic")
		}
	}()
	_ = p.Put(context.Background(), s)
}

func TestSessionPoolKeepAliveCondFairness(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping testing in non-short mode")
	}

	timer := timetest.StubSingleTimer(t)
	defer timer.Cleanup()

	shiftTime, cleanupNow := timeutil.StubTestHookTimeNow(time.Unix(0, 0))
	defer cleanupNow()

	var (
		keepalive           = make(chan interface{})
		keepaliveResult     = make(chan error)
		deleteSession       = make(chan interface{})
		deleteSessionResult = make(chan error)
	)
	p := newClientWithStubBuilder(
		t,
		testutil.NewCluster(
			testutil.WithInvokeHandlers(
				testutil.InvokeHandlers{
					// nolint:unparam
					testutil.TableCreateSession: func(interface{}) (proto.Message, error) {
						return &Ydb_Table.CreateSessionResult{
							SessionId: testutil.SessionID(),
						}, nil
					},
					// nolint:unparam
					testutil.TableKeepAlive: func(request interface{}) (result proto.Message, err error) {
						keepalive <- request
						return nil, <-keepaliveResult
					},
					// nolint:unparam
					testutil.TableDeleteSession: func(request interface{}) (result proto.Message, err error) {
						deleteSession <- request
						return nil, <-deleteSessionResult
					},
				},
			),
		),
		1,
		config.WithSizeLimit(1),
		config.WithIdleThreshold(time.Second),
	)

	// First Get&Put to initialize client's timers.
	mustPutSession(t, p, mustGetSession(t, p))
	<-timer.Created

	// Now the most interesting and delicate part: we want to emulate a race
	// condition between awaiting the build by touchCond() call and build
	// deletion after failed Keepalive().
	//
	// So first step is to force keepalive. Note that we do not send keepalive
	// result here making Keepalive() being blocked.
	shiftTime(5 * time.Second)
	timer.C <- timeutil.Now()
	<-keepalive

	cond := p.touchCond()
	assertFilled := func(want bool) {
		const timeout = time.Millisecond
		select {
		case <-cond:
			if !want {
				t.Fatalf("unexpected cond event")
			}
		case <-time.After(timeout):
			if want {
				t.Fatalf("no cond event after %s", timeout)
			}
		}
	}

	// Now fail the Keepalive() call from above.
	keepaliveResult <- &errors.OpError{
		Reason: errors.StatusBadSession,
	}

	// Block the keeper()'s deletion routine.
	// While delete is not finished cond must not be fulfilled.
	<-deleteSession
	assertFilled(false)

	// Complete the build deletion routine. After that cond must become
	// fulfilled.
	deleteSessionResult <- nil
	assertFilled(true)
}

func TestSessionPoolKeepAliveMinSize(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping testing in non-short mode")
	}

	timer := timetest.StubSingleTimer(t)
	defer timer.Cleanup()

	shiftTime, cleanupNow := timeutil.StubTestHookTimeNow(time.Unix(0, 0))
	defer cleanupNow()
	idleThreshold := 5 * time.Second
	p := newClientWithStubBuilder(
		t,
		testutil.NewCluster(
			testutil.WithInvokeHandlers(
				testutil.InvokeHandlers{
					//nolint: unparam
					testutil.TableCreateSession: func(interface{}) (result proto.Message, _ error) {
						return &Ydb_Table.CreateSessionResult{
							SessionId: testutil.SessionID(),
						}, nil
					},
					//nolint: unparam
					testutil.TableKeepAlive: func(interface{}) (result proto.Message, _ error) {
						return &Ydb_Table.KeepAliveResult{}, nil
					},
					testutil.TableDeleteSession: okHandler,
				},
			),
		),
		4,
		config.WithSizeLimit(3),
		config.WithKeepAliveMinSize(1),
		config.WithIdleKeepAliveThreshold(2),
		config.WithIdleThreshold(idleThreshold),
	)
	defer func() {
		_ = p.Close(context.Background())
	}()
	sessionBuilder := func(t *testing.T, pool *client) (Session, chan bool) {
		s := mustCreateSession(t, pool)
		closed := make(chan bool)
		s.OnClose(func(context.Context) {
			close(closed)
		})
		return s, closed
	}
	s1, c1 := sessionBuilder(t, p)
	<-timer.Created
	s2, c2 := sessionBuilder(t, p)
	s3, c3 := sessionBuilder(t, p)
	mustPutSession(t, p, s1)
	mustPutSession(t, p, s2)
	mustPutSession(t, p, s3)
	shiftTime(idleThreshold)
	timer.C <- timeutil.Now()
	<-timer.Reset
	shiftTime(idleThreshold)
	timer.C <- timeutil.Now()
	<-timer.Reset
	shiftTime(idleThreshold)
	timer.C <- timeutil.Now()
	<-timer.Reset
	<-c1
	<-c2

	if s3.IsClosed() {
		t.Fatalf("lower bound for sessions in the client is not equal KeepAliveMinSize")
	}

	s := mustGetSession(t, p)
	if s != s3 {
		t.Fatalf("build is not reused")
	}
	_ = s.Close(context.Background())
	<-c3
}

func TestSessionPoolKeepAliveWithBadSession(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping testing in non-short mode")
	}

	p := newClientWithStubBuilder(
		t,
		testutil.NewCluster(
			testutil.WithInvokeHandlers(
				testutil.InvokeHandlers{
					// nolint:unparam
					testutil.TableCreateSession: func(interface{}) (proto.Message, error) {
						return &Ydb_Table.CreateSessionResult{
							SessionId: testutil.SessionID(),
						}, nil
					},
					// nolint:unparam
					testutil.TableKeepAlive: func(interface{}) (proto.Message, error) {
						return nil, &errors.OpError{
							Reason: errors.StatusBadSession,
						}
					},
					testutil.TableDeleteSession: okHandler,
				},
			),
		),
		4,
		config.WithSizeLimit(3),
		config.WithIdleThreshold(2*time.Second),
	)
	defer func() {
		_ = p.Close(context.Background())
	}()
	s := mustCreateSession(t, p)
	closed := make(chan bool)
	s.OnClose(func(context.Context) {
		close(closed)
	})
	mustPutSession(t, p, s)
	<-closed
}

func TestSessionPoolKeeperRetry(t *testing.T) {
	if !testing.Short() {
		t.Skip("skipping testing in non-short mode")
	}

	timer := timetest.StubSingleTimer(t)
	defer timer.Cleanup()
	shiftTime, cleanupNow := timeutil.StubTestHookTimeNow(time.Unix(0, 0))
	defer cleanupNow()

	retry := true
	p := newClientWithStubBuilder(
		t,
		testutil.NewCluster(
			testutil.WithInvokeHandlers(
				testutil.InvokeHandlers{
					// nolint:unparam
					testutil.TableCreateSession: func(interface{}) (proto.Message, error) {
						return &Ydb_Table.CreateSessionResult{
							SessionId: testutil.SessionID(),
						}, nil
					},
					// nolint:unparam
					testutil.TableKeepAlive: func(interface{}) (proto.Message, error) {
						if retry {
							retry = false
							return nil, context.DeadlineExceeded
						}
						return nil, nil
					},
					testutil.TableDeleteSession: okHandler,
				},
			),
		),
		2,
		config.WithSizeLimit(3),
		config.WithIdleKeepAliveThreshold(-1),
		config.WithIdleThreshold(3*time.Second),
	)
	defer func() {
		_ = p.Close(context.Background())
	}()
	s := mustCreateSession(t, p)
	<-timer.Created
	s2 := mustCreateSession(t, p)
	mustPutSession(t, p, s)
	mustPutSession(t, p, s2)
	//retry
	shiftTime(p.config.IdleThreshold())
	timer.C <- timeutil.Now()
	<-timer.Reset
	//get first build
	s1 := mustGetSession(t, p)
	if s2 == s1 {
		t.Fatalf("retry build is not returned")
	}
	mustPutSession(t, p, s1)
	//keepalive success
	shiftTime(p.config.IdleThreshold())
	timer.C <- timeutil.Now()
	<-timer.Reset

	// get retry build
	s1 = mustGetSession(t, p)
	if s == s1 {
		t.Fatalf("second build is not returned")
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

func mustCreateSession(t *testing.T, p *client) Session {
	wg := sync.WaitGroup{}
	defer wg.Wait()
	s, err := p.Create(trace.WithTable(
		context.Background(),
		trace.Table{
			OnPoolSessionNew: func(info trace.PoolSessionNewStartInfo) func(trace.PoolSessionNewDoneInfo) {
				wg.Add(1)
				return func(info trace.PoolSessionNewDoneInfo) {
					wg.Done()
				}
			},
		},
	))
	if err != nil {
		t.Helper()
		t.Fatalf("%s: %v", caller(), err)
	}
	return s
}

func mustGetSession(t *testing.T, p *client) Session {
	wg := sync.WaitGroup{}
	defer wg.Wait()
	s, err := p.Get(context.Background())
	if err != nil {
		t.Helper()
		t.Fatalf("%s: %v", caller(), err)
	}
	return s
}

func mustPutSession(t *testing.T, p *client, s Session) {
	wg := sync.WaitGroup{}
	defer wg.Wait()
	if err := p.Put(
		trace.WithTable(
			context.Background(),
			trace.Table{
				OnPoolPut: func(info trace.PoolPutStartInfo) func(trace.PoolPutDoneInfo) {
					wg.Add(1)
					return func(info trace.PoolPutDoneInfo) {
						wg.Done()
					}
				},
				OnPoolSessionClose: func(info trace.PoolSessionCloseStartInfo) func(doneInfo trace.PoolSessionCloseDoneInfo) {
					wg.Add(1)
					return func(info trace.PoolSessionCloseDoneInfo) {
						wg.Done()
					}
				},
			},
		),
		s,
	); err != nil {
		t.Helper()
		t.Fatalf("%s: %v", caller(), err)
	}
}

func mustTakeSession(t *testing.T, p *client, s Session) {
	wg := sync.WaitGroup{}
	defer wg.Wait()
	took, err := p.Take(
		trace.WithTable(
			context.Background(),
			trace.Table{
				OnPoolTake: func(info trace.PoolTakeStartInfo) func(trace.PoolTakeWaitInfo) func(trace.PoolTakeDoneInfo) {
					wg.Add(1)
					return func(trace.PoolTakeWaitInfo) func(trace.PoolTakeDoneInfo) {
						return func(trace.PoolTakeDoneInfo) {
							wg.Done()
						}
					}
				},
			},
		),
		s,
	)
	if !took {
		t.Helper()
		t.Fatalf("%s: can not take build (%v)", caller(), err)
	}
}

func mustClose(t *testing.T, p *client) {
	wg := sync.WaitGroup{}
	defer wg.Wait()
	if err := p.Close(
		trace.WithTable(
			context.Background(),
			trace.Table{
				OnPoolSessionClose: func(info trace.PoolSessionCloseStartInfo) func(doneInfo trace.PoolSessionCloseDoneInfo) {
					wg.Add(1)
					return func(info trace.PoolSessionCloseDoneInfo) {
						wg.Done()
					}
				},
			},
		),
	); err != nil {
		t.Helper()
		t.Fatalf("%s: %v", caller(), err)
	}
}

func caller() string {
	_, file, line, _ := runtime.Caller(2)
	return fmt.Sprintf("%s:%d", path.Base(file), line)
}

var okHandler = func(interface{}) (proto.Message, error) {
	return nil, nil
}

var simpleCluster = testutil.NewCluster(
	testutil.WithInvokeHandlers(
		testutil.InvokeHandlers{
			// nolint:unparam
			testutil.TableExecuteDataQuery: func(interface{}) (proto.Message, error) {
				return &Ydb_Table.ExecuteQueryResult{
					TxMeta: &Ydb_Table.TransactionMeta{
						Id: "",
					},
				}, nil
			},
			// nolint:unparam
			testutil.TableBeginTransaction: func(interface{}) (proto.Message, error) {
				return &Ydb_Table.BeginTransactionResult{
					TxMeta: &Ydb_Table.TransactionMeta{
						Id: "",
					},
				}, nil
			},
			// nolint:unparam
			testutil.TableExplainDataQuery: func(interface{}) (proto.Message, error) {
				return &Ydb_Table.ExecuteQueryResult{}, nil
			},
			// nolint:unparam
			testutil.TablePrepareDataQuery: func(interface{}) (proto.Message, error) {
				return &Ydb_Table.PrepareQueryResult{}, nil
			},
			// nolint:unparam
			testutil.TableCreateSession: func(interface{}) (proto.Message, error) {
				return &Ydb_Table.CreateSessionResult{
					SessionId: testutil.SessionID(),
				}, nil
			},
			// nolint:unparam
			testutil.TableDeleteSession: func(interface{}) (proto.Message, error) {
				return &Ydb_Table.DeleteSessionResponse{}, nil
			},
			// nolint:unparam
			testutil.TableCommitTransaction: func(interface{}) (proto.Message, error) {
				return &Ydb_Table.CommitTransactionResponse{}, nil
			},
			// nolint:unparam
			testutil.TableRollbackTransaction: func(interface{}) (proto.Message, error) {
				return &Ydb_Table.RollbackTransactionResponse{}, nil
			},
			// nolint:unparam
			testutil.TableKeepAlive: func(interface{}) (proto.Message, error) {
				return &Ydb_Table.KeepAliveResult{}, nil
			},
		},
	),
)

func simpleSession(t *testing.T) Session {
	return _newSession(t, simpleCluster)
}

type StubBuilder struct {
	OnCreateSession func(ctx context.Context) (Session, error)

	Cluster cluster.Cluster
	Limit   int
	T       *testing.T

	mu     sync.Mutex
	actual int
}

func newClientWithStubBuilder(t *testing.T, cluster cluster.Cluster, stubLimit int, options ...config.Option) *client {
	return newClient(
		context.Background(),
		cluster,
		(&StubBuilder{
			T:       t,
			Limit:   stubLimit,
			Cluster: cluster,
		}).createSession,
		config.New(options...),
	)
}

func (s *StubBuilder) createSession(ctx context.Context) (session Session, err error) {
	defer func() {
		s.mu.Lock()
		if session != nil {
			s.actual++
		}
		s.mu.Unlock()
	}()
	s.mu.Lock()
	if s.Limit > 0 && s.actual == s.Limit {
		s.mu.Unlock()
		return nil, fmt.Errorf("stub build: limit overflow")
	}
	s.mu.Unlock()

	if f := s.OnCreateSession; f != nil {
		return f(ctx)
	}

	return newSession(ctx, s.Cluster, trace.ContextTable(ctx))
}

func (c *client) debug() {
	fmt.Printf("head ")
	for el := c.idle.Front(); el != nil; el = el.Next() {
		s := el.Value.(Session)
		x := c.index[s]
		fmt.Printf("<-> %s(%d) ", s.ID(), x.touched.Unix())
	}
	fmt.Printf("<-> tail\n")
}

func whenWantWaitCh(p *client) <-chan struct{} {
	var (
		prev = p.testHookGetWaitCh
		ch   = make(chan struct{})
	)
	p.testHookGetWaitCh = func() {
		p.testHookGetWaitCh = prev
		close(ch)
	}
	return ch
}
