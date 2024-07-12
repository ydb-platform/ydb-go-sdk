package table

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"path"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Table"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/closer"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/table/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xcontext"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xrand"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xsync"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xtest"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/testutil"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func TestSessionPoolCreateAbnormalResult(t *testing.T) {
	xtest.TestManyTimes(t, func(t testing.TB) {
		limit := 100
		ctx, cancel := xcontext.WithTimeout(
			context.Background(),
			55*time.Second,
		)
		defer cancel()
		p := newClientWithStubBuilder(
			t,
			testutil.NewBalancer(
				testutil.WithInvokeHandlers(
					testutil.InvokeHandlers{
						testutil.TableCreateSession: func(interface{}) (proto.Message, error) {
							return &Ydb_Table.CreateSessionResult{
								SessionId: testutil.SessionID(),
							}, nil
						},
						testutil.TableDeleteSession: okHandler,
					},
				),
			),
			limit,
			config.WithSizeLimit(limit),
		)
		defer func() {
			_ = p.Close(context.Background())
		}()
		r := xrand.New(xrand.WithLock())
		errCh := make(chan error, limit*10)
		fn := func(wg *sync.WaitGroup) {
			defer wg.Done()
			childCtx, childCancel := xcontext.WithTimeout(
				ctx,
				time.Duration(r.Int64(int64(time.Minute))),
			)
			defer childCancel()
			s, err := p.internalPoolCreateSession(childCtx)
			if s == nil && err == nil {
				errCh <- fmt.Errorf("unexpected result: <%v, %w>", s, err)
			}
		}
		wg := &sync.WaitGroup{}
		wg.Add(limit * 10)
		for i := 0; i < limit*10; i++ {
			go fn(wg)
		}
		go func() {
			wg.Wait()
			close(errCh)
		}()
		for e := range errCh {
			t.Fatal(e)
		}
	}, xtest.StopAfter(17*time.Second))
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
			var (
				get  = make(chan struct{})
				wait = make(chan struct{})
				got  = make(chan error)
			)
			p := newClientWithStubBuilder(
				t,
				testutil.NewBalancer(testutil.WithInvokeHandlers(testutil.InvokeHandlers{
					testutil.TableCreateSession: func(interface{}) (proto.Message, error) {
						return &Ydb_Table.CreateSessionResult{
							SessionId: testutil.SessionID(),
						}, nil
					},
				})),
				1,
				config.WithSizeLimit(1),
				config.WithTrace(
					&trace.Table{
						OnPoolWait: func(trace.TablePoolWaitStartInfo) func(trace.TablePoolWaitDoneInfo) {
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
				_, err := p.internalPoolGet(
					context.Background(),
					withTrace(&trace.Table{
						OnPoolGet: func(trace.TablePoolGetStartInfo) func(trace.TablePoolGetDoneInfo) {
							get <- struct{}{}

							return nil
						},
					}),
				)
				got <- err
			}()

			regWait := whenWantWaitCh(p)
			<-get     // Await for getter blocked on awaiting session.
			<-regWait // Let the getter register itself in the wait queue.

			if test.racy {
				// We are testing the case, when session consumer registered
				// himself in the wait queue, but not ready to receive the
				// session when session arrives (that is, stuck between
				// pushing channel in the list and reading from the channel).
				_ = p.Close(context.Background())
				<-wait
			} else {
				// We are testing the normal case, when session consumer registered
				// himself in the wait queue and successfully blocked on
				// reading from signaling channel.
				<-wait
				// Let the waiting goroutine to block on reading from channel.
				_ = p.Close(context.Background())
			}

			const timeout = time.Second
			select {
			case err := <-got:
				if !xerrors.Is(err, errClosedClient) {
					t.Fatalf(
						"unexpected error: %v; want %v",
						err, errClosedClient,
					)
				}
			case <-p.clock.After(timeout):
				t.Fatalf("no result after %s", timeout)
			}
		})
	}
}

func TestSessionPoolClose(t *testing.T) {
	counter := 0
	xtest.TestManyTimes(t, func(t testing.TB) {
		counter++
		defer func() {
			if counter%1000 == 0 {
				t.Logf("%d times test passed", counter)
			}
		}()

		p := newClientWithStubBuilder(
			t,
			testutil.NewBalancer(testutil.WithInvokeHandlers(testutil.InvokeHandlers{
				testutil.TableCreateSession: func(interface{}) (proto.Message, error) {
					return &Ydb_Table.CreateSessionResult{
						SessionId: testutil.SessionID(),
					}, nil
				},
				testutil.TableDeleteSession: func(interface{}) (proto.Message, error) {
					return &Ydb_Table.DeleteSessionResponse{}, nil
				},
			})),
			3,
			config.WithSizeLimit(3),
			config.WithIdleThreshold(time.Hour),
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

		s1.onClose = append(s1.onClose, func(s *session) { closed1 = true })
		s2.onClose = append(s2.onClose, func(s *session) { closed2 = true })
		s3.onClose = append(s3.onClose, func(s *session) { closed3 = true })

		mustPutSession(t, p, s1)
		mustPutSession(t, p, s2)
		mustClose(t, p)

		if !closed1 {
			t.Errorf("session1 was not closed")
		}
		if !closed2 {
			t.Errorf("session2 was not closed")
		}
		if closed3 {
			t.Fatalf("unexpected session close")
		}

		if err := p.Put(context.Background(), s3); !xerrors.Is(err, errClosedClient) {
			t.Errorf(
				"unexpected Put() error: %v; want %v",
				err, errClosedClient,
			)
		}
		if !closed3 {
			t.Fatalf("session was not closed")
		}
	}, xtest.StopAfter(17*time.Second))
}

func TestRaceWgClosed(t *testing.T) {
	defer func() {
		if e := recover(); e != nil {
			t.Fatal(e)
		}
	}()

	var (
		limit   = 100
		start   = time.Now()
		counter int
	)

	xtest.TestManyTimes(t, func(t testing.TB) {
		counter++
		defer func() {
			if counter%1000 == 0 {
				t.Logf("%0.1fs: %d times test passed", time.Since(start).Seconds(), counter)
			}
		}()
		ctx, cancel := xcontext.WithTimeout(context.Background(),
			//nolint:gosec
			time.Duration(rand.Int31n(int32(100*time.Millisecond))),
		)
		defer cancel()

		wg := sync.WaitGroup{}
		p := newClientWithStubBuilder(t,
			testutil.NewBalancer(testutil.WithInvokeHandlers(testutil.InvokeHandlers{
				testutil.TableCreateSession: func(interface{}) (proto.Message, error) {
					return &Ydb_Table.CreateSessionResult{
						SessionId: testutil.SessionID(),
					}, nil
				},
			})),
			limit,
			config.WithSizeLimit(limit),
		)
		for j := 0; j < limit*10; j++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for {
					err := p.Do(ctx,
						func(ctx context.Context, s table.Session) error {
							return nil
						},
					)
					if err != nil && xerrors.Is(err, errClosedClient) {
						return
					}
				}
			}()
		}
		_ = p.Close(context.Background())
		wg.Wait()
	}, xtest.StopAfter(27*time.Second))
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
			var (
				get  = make(chan struct{}, 1)
				wait = make(chan struct{})
				got  = make(chan struct{}, 1)
			)
			p := newClientWithStubBuilder(
				t,
				testutil.NewBalancer(testutil.WithInvokeHandlers(testutil.InvokeHandlers{
					testutil.TableCreateSession: func(interface{}) (proto.Message, error) {
						return &Ydb_Table.CreateSessionResult{
							SessionId: testutil.SessionID(),
						}, nil
					},
				})),
				2,
				config.WithSizeLimit(1),
				config.WithIdleThreshold(time.Hour),
				config.WithTrace(
					&trace.Table{
						OnPoolGet: func(trace.TablePoolGetStartInfo) func(trace.TablePoolGetDoneInfo) {
							get <- struct{}{}

							return nil
						},
						OnPoolWait: func(trace.TablePoolWaitStartInfo) func(trace.TablePoolWaitDoneInfo) {
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
			<-get     // Await for getter blocked on awaiting session.
			<-regWait // Let the getter register itself in the wait queue.

			if test.racy {
				// We are testing the case, when session consumer registered
				// himself in the wait queue, but not ready to receive the
				// session when session arrives (that is, it was stucked between
				// pushing channel in the list and reading from the channel).
				_ = s.Close(context.Background())
				<-wait
			} else {
				// We are testing the normal case, when session consumer registered
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
			case <-p.clock.After(timeout):
				t.Fatalf("no internalPoolGet after %s", timeout)
			}
		})
	}
}

func TestSessionPoolRacyGet(t *testing.T) {
	type createReq struct {
		release chan struct{}
		session *session
	}
	create := make(chan createReq)
	p := newClient(
		context.Background(),
		nil,
		(&StubBuilder{
			Limit: 1,
			OnCreateSession: func(ctx context.Context) (*session, error) {
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
		expSession *session
		done       = make(chan struct{}, 2)
		err        error
	)
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
				err = fmt.Errorf("unexpected session: %v; want %v", s, expSession)

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
		t.Fatalf("session 2 on race created while client size 1")
	case <-p.clock.After(time.Millisecond * 5):
		// ok
	}

	// Release the first create session request.
	// Created session must be stored in the Client.
	expSession = r1.session
	expSession.onClose = append(expSession.onClose, func(s *session) {
		t.Fatalf("unexpected first session close")
	})
	close(r1.release)

	// Wait for r1's session will be stored in the Client.
	<-done

	// Ensure that session is in the Client.
	s := mustGetSession(t, p)
	mustPutSession(t, p, s)
}

func TestSessionPoolPutInFull(t *testing.T) {
	p := newClientWithStubBuilder(
		t,
		testutil.NewBalancer(testutil.WithInvokeHandlers(testutil.InvokeHandlers{
			testutil.TableCreateSession: func(interface{}) (proto.Message, error) {
				return &Ydb_Table.CreateSessionResult{
					SessionId: testutil.SessionID(),
				}, nil
			},
		})),
		1,
		config.WithSizeLimit(1),
		config.WithIdleThreshold(-1),
	)
	s := mustGetSession(t, p)
	if err := p.Put(context.Background(), s); err != nil {
		t.Fatalf("unexpected error on put session into non-full client: %v, wand: %v", err, nil)
	}

	if err := p.Put(context.Background(), simpleSession(t)); !xerrors.Is(err, errSessionPoolOverflow) {
		t.Fatalf("unexpected error on put session into full client: %v, wand: %v", err, errSessionPoolOverflow)
	}
}

func TestSessionPoolSizeLimitOverflow(t *testing.T) {
	type sessionAndError struct {
		session *session
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
				testutil.NewBalancer(testutil.WithInvokeHandlers(testutil.InvokeHandlers{
					testutil.TableCreateSession: func(interface{}) (result proto.Message, _ error) {
						return &Ydb_Table.CreateSessionResult{
							SessionId: testutil.SessionID(),
						}, nil
					},
				})),
				1,
				config.WithSizeLimit(1),
			)
			defer func() {
				_ = p.Close(context.Background())
			}()
			s := mustGetSession(t, p)
			{
				ctx, cancel := xcontext.WithCancel(context.Background())
				cancel()
				if _, err := p.Get(ctx); !xerrors.Is(err, context.Canceled) {
					t.Fatalf(
						"unexpected error: %v; want %v",
						err, context.Canceled,
					)
				}
			}
			go func() {
				session, err := p.internalPoolGet(
					context.Background(),
					withTrace(&trace.Table{
						OnPoolGet: func(trace.TablePoolGetStartInfo) func(trace.TablePoolGetDoneInfo) {
							get <- struct{}{}

							return nil
						},
						OnPoolWait: func(trace.TablePoolWaitStartInfo) func(trace.TablePoolWaitDoneInfo) {
							wait <- struct{}{}

							return nil
						},
					}),
				)
				got <- sessionAndError{session, err}
			}()

			regWait := whenWantWaitCh(p)
			<-get     // Await for getter blocked on awaiting session.
			<-regWait // Let the getter register itself in the wait queue.

			if test.racy {
				// We are testing the case, when session consumer registered
				// himself in the wait queue, but not ready to receive the
				// session when session arrives (that is, it was stucked between
				// pushing channel in the list and reading from the channel).
				_ = p.Put(context.Background(), s)
				<-wait
			} else {
				// We are testing the normal case, when session consumer registered
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
					t.Fatalf("unexpected session")
				}
			case <-p.clock.After(timeout):
				t.Fatalf("no session after %s", timeout)
			}
		})
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
	p := newClientWithStubBuilder(
		t,
		testutil.NewBalancer(
			testutil.WithInvokeHandlers(
				testutil.InvokeHandlers{
					testutil.TableCreateSession: func(interface{}) (proto.Message, error) {
						created++

						return &Ydb_Table.CreateSessionResult{
							SessionId: testutil.SessionID(),
						}, nil
					},
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

func TestSessionPoolCloseIdleSessions(t *testing.T) {
	xtest.TestManyTimes(t, func(t testing.TB) {
		var (
			idleThreshold = 4 * time.Second
			closedCount   atomic.Int64
			fakeClock     = clockwork.NewFakeClock()
		)
		p := newClientWithStubBuilder(
			t,
			testutil.NewBalancer(
				testutil.WithInvokeHandlers(
					testutil.InvokeHandlers{
						testutil.TableDeleteSession: okHandler,
						testutil.TableCreateSession: func(interface{}) (proto.Message, error) {
							closedCount.Add(1)

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
			config.WithClock(fakeClock),
		)

		s1 := mustGetSession(t, p)
		s2 := mustGetSession(t, p)

		// Put both sessions at the absolutely same time.
		// That is, both sessions must be keepalived by a single tick.
		mustPutSession(t, p, s1)
		mustPutSession(t, p, s2)

		// Emulate first simple tick event. We expect two sessions be keepalived.
		fakeClock.Advance(idleThreshold / 2)
		if !closedCount.CompareAndSwap(2, 0) {
			t.Fatal("unexpected number of keepalives")
		}

		// Now internalPoolGet first session and "spent" some time working within it.
		x := mustGetSession(t, p)

		// Move time to idleThreshold / 2
		fakeClock.Advance(idleThreshold / 2)

		// Now put that session back and emulate keepalive moment.
		mustPutSession(t, p, x)

		// Move time to idleThreshold / 2
		fakeClock.Advance(idleThreshold / 2)
		// We expect here next tick to be registered after half of a idleThreshold.
		// That is, x was touched half of idleThreshold ago, so we need to wait for
		// the second half until we must touch it.

		_ = p.Close(context.Background())
	}, xtest.StopAfter(12*time.Second))
}

func TestSessionPoolDoublePut(t *testing.T) {
	p := newClientWithStubBuilder(
		t,
		testutil.NewBalancer(testutil.WithInvokeHandlers(testutil.InvokeHandlers{
			testutil.TableCreateSession: func(interface{}) (proto.Message, error) {
				return &Ydb_Table.CreateSessionResult{
					SessionId: testutil.SessionID(),
				}, nil
			},
		})),
		1,
		config.WithSizeLimit(2),
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

func mustGetSession(t testing.TB, p *Client) *session {
	wg := sync.WaitGroup{}
	defer wg.Wait()
	s, err := p.Get(context.Background())
	if err != nil {
		t.Helper()
		t.Fatalf("%s: %v", caller(), err)
	}

	return s
}

func mustPutSession(t testing.TB, p *Client, s *session) {
	wg := sync.WaitGroup{}
	defer wg.Wait()
	if err := p.Put(context.Background(), s); err != nil {
		t.Helper()
		t.Fatalf("%s: %v", caller(), err)
	}
}

func mustClose(t testing.TB, p closer.Closer) {
	wg := sync.WaitGroup{}
	defer wg.Wait()
	if err := p.Close(context.Background()); err != nil {
		t.Helper()
		t.Fatalf("%s: %v", caller(), err)
	}
}

func caller() string {
	_, file, line, _ := runtime.Caller(2)

	return fmt.Sprintf("%s:%d", path.Base(file), line)
}

var okHandler = func(interface{}) (proto.Message, error) {
	return &emptypb.Empty{}, nil
}

var simpleCluster = testutil.NewBalancer(
	testutil.WithInvokeHandlers(
		testutil.InvokeHandlers{
			testutil.TableExecuteDataQuery: func(interface{}) (proto.Message, error) {
				return &Ydb_Table.ExecuteQueryResult{
					TxMeta: &Ydb_Table.TransactionMeta{
						Id: "",
					},
				}, nil
			},
			testutil.TableBeginTransaction: func(interface{}) (proto.Message, error) {
				return &Ydb_Table.BeginTransactionResult{
					TxMeta: &Ydb_Table.TransactionMeta{
						Id: "",
					},
				}, nil
			},
			testutil.TableExplainDataQuery: func(interface{}) (proto.Message, error) {
				return &Ydb_Table.ExecuteQueryResult{}, nil
			},
			testutil.TablePrepareDataQuery: func(interface{}) (proto.Message, error) {
				return &Ydb_Table.PrepareQueryResult{}, nil
			},
			testutil.TableCreateSession: func(interface{}) (proto.Message, error) {
				return &Ydb_Table.CreateSessionResult{
					SessionId: testutil.SessionID(),
				}, nil
			},
			testutil.TableDeleteSession: func(interface{}) (proto.Message, error) {
				return &Ydb_Table.DeleteSessionResponse{}, nil
			},
			testutil.TableCommitTransaction: func(interface{}) (proto.Message, error) {
				return &Ydb_Table.CommitTransactionResponse{}, nil
			},
			testutil.TableRollbackTransaction: func(interface{}) (proto.Message, error) {
				return &Ydb_Table.RollbackTransactionResponse{}, nil
			},
			testutil.TableKeepAlive: func(interface{}) (proto.Message, error) {
				return &Ydb_Table.KeepAliveResult{}, nil
			},
		},
	),
)

func simpleSession(t *testing.T) *session {
	s, err := newSession(context.Background(), simpleCluster, config.New())
	if err != nil {
		t.Fatalf("newSession unexpected error: %v", err)
	}

	return s
}

type StubBuilder struct {
	OnCreateSession func(ctx context.Context) (*session, error)

	cc    grpc.ClientConnInterface
	Limit int
	T     testing.TB

	mu     xsync.Mutex
	actual int
}

func newClientWithStubBuilder(
	t testing.TB,
	cc grpc.ClientConnInterface,
	stubLimit int,
	options ...config.Option,
) *Client {
	c := newClient(
		context.Background(),
		cc,
		(&StubBuilder{
			T:     t,
			Limit: stubLimit,
			cc:    cc,
		}).createSession,
		config.New(options...),
	)

	return c
}

func (s *StubBuilder) createSession(ctx context.Context) (session *session, err error) {
	defer s.mu.WithLock(func() {
		if session != nil {
			s.actual++
		}
	})

	s.mu.WithLock(func() {
		if s.Limit > 0 && s.actual == s.Limit {
			err = fmt.Errorf("stub session: limit overflow")
		}
	})
	if err != nil {
		return nil, err
	}

	if f := s.OnCreateSession; f != nil {
		return f(ctx)
	}

	return newSession(ctx, s.cc, config.New())
}

func (c *Client) debug() {
	fmt.Print("head ")
	for el := c.idle.Front(); el != nil; el = el.Next() {
		s := el.Value
		x := c.index[s]
		fmt.Printf("<-> %s(%d) ", s.ID(), x.touched.Unix())
	}
	fmt.Print("<-> tail\n")
}

func whenWantWaitCh(p *Client) <-chan struct{} {
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

func TestDeadlockOnUpdateNodes(t *testing.T) {
	xtest.TestManyTimes(t, func(t testing.TB) {
		ctx, cancel := xcontext.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()
		var (
			nodes         = make([]uint32, 0, 3)
			nodeIDCounter = uint32(0)
		)
		balancer := testutil.NewBalancer(testutil.WithInvokeHandlers(testutil.InvokeHandlers{
			testutil.TableCreateSession: func(interface{}) (proto.Message, error) {
				sessionID := testutil.SessionID(testutil.WithNodeID(nodeIDCounter))
				nodeIDCounter++
				nodeID, err := nodeID(sessionID)
				if err != nil {
					return nil, err
				}
				nodes = append(nodes, nodeID)

				return &Ydb_Table.CreateSessionResult{
					SessionId: sessionID,
				}, nil
			},
		}))
		c := newClientWithStubBuilder(t, balancer, 3)
		defer func() {
			_ = c.Close(ctx)
		}()
		s1, err := c.Get(ctx)
		require.NoError(t, err)
		s2, err := c.Get(ctx)
		require.NoError(t, err)
		s3, err := c.Get(ctx)
		require.NoError(t, err)
		require.Len(t, nodes, 3)
		err = c.Put(ctx, s1)
		require.NoError(t, err)
		err = c.Put(ctx, s2)
		require.NoError(t, err)
		err = c.Put(ctx, s3)
		require.NoError(t, err)
	}, xtest.StopAfter(12*time.Second))
}

func TestDeadlockOnInternalPoolGCTick(t *testing.T) {
	xtest.TestManyTimes(t, func(t testing.TB) {
		ctx, cancel := xcontext.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()
		var (
			nodes         = make([]uint32, 0, 3)
			nodeIDCounter = uint32(0)
		)
		balancer := testutil.NewBalancer(testutil.WithInvokeHandlers(testutil.InvokeHandlers{
			testutil.TableCreateSession: func(interface{}) (proto.Message, error) {
				sessionID := testutil.SessionID(testutil.WithNodeID(nodeIDCounter))
				nodeIDCounter++
				nodeID, err := nodeID(sessionID)
				if err != nil {
					return nil, err
				}
				nodes = append(nodes, nodeID)

				return &Ydb_Table.CreateSessionResult{
					SessionId: sessionID,
				}, nil
			},
		}))
		c := newClientWithStubBuilder(t, balancer, 3)
		defer func() {
			_ = c.Close(ctx)
		}()
		s1, err := c.Get(ctx)
		if err != nil && errors.Is(err, context.DeadlineExceeded) {
			return
		}
		require.NoError(t, err)
		s2, err := c.Get(ctx)
		if err != nil && errors.Is(err, context.DeadlineExceeded) {
			return
		}
		require.NoError(t, err)
		s3, err := c.Get(ctx)
		if err != nil && errors.Is(err, context.DeadlineExceeded) {
			return
		}
		require.NoError(t, err)
		require.Len(t, nodes, 3)
		err = c.Put(ctx, s1)
		if err != nil && errors.Is(err, context.DeadlineExceeded) {
			return
		}
		require.NoError(t, err)
		err = c.Put(ctx, s2)
		if err != nil && errors.Is(err, context.DeadlineExceeded) {
			return
		}
		require.NoError(t, err)
		err = c.Put(ctx, s3)
		if err != nil && errors.Is(err, context.DeadlineExceeded) {
			return
		}
		require.NoError(t, err)
		c.internalPoolGCTick(ctx, 0)
	}, xtest.StopAfter(12*time.Second))
}
