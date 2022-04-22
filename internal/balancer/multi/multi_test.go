package multi

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/balancer"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/balancer/mock"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/conn"
)

func tSleep() {
	time.Sleep(time.Millisecond * 10)
}

func TestCreate(t *testing.T) {
	t.Run("Empty", func(t *testing.T) {
		b := Balancer().(*multi)
		if blen := len(b.balancers); blen != 0 {
			t.Errorf("bad balancers len: %v", blen)
		}
		if flen := len(b.balancers); flen != 0 {
			t.Errorf("bal filters len: %v", flen)
		}

		b2 := b.Create(nil).(*multi)
		if blen := len(b2.balancers); blen != 0 {
			t.Errorf("bad balancers len: %v", blen)
		}
		if flen := len(b2.balancers); flen != 0 {
			t.Errorf("bal filters len: %v", flen)
		}
	})

	t.Run("Filled", func(t *testing.T) {
		var conns0, conns1 []conn.Conn
		m0 := mock.Balancer()
		m0.OnCreate = func(conns []conn.Conn) balancer.Balancer {
			conns0 = conns
			return mock.Balancer()
		}

		m1 := mock.Balancer()
		m1.OnCreate = func(conns []conn.Conn) balancer.Balancer {
			conns1 = conns
			newMock := mock.Balancer()
			newMock.OnCreate = func(conns []conn.Conn) balancer.Balancer {
				panic("must not called")
			}
			return newMock
		}

		b := Balancer(
			WithBalancer(m0, func(cc conn.Conn) bool {
				num, _ := strconv.Atoi(cc.Endpoint().Address())
				return num%2 == 0
			}),
			WithBalancer(m1, func(cc conn.Conn) bool {
				num, _ := strconv.Atoi(cc.Endpoint().Address())
				return num%2 == 1
			}),
		)

		connCount := 6
		var conns []conn.Conn
		for i := 0; i < connCount; i++ {
			conns = append(conns, &mock.ConnMock{Address: strconv.Itoa(i)})
		}

		b2 := b.Create(conns).(*multi)
		if blen := len(b2.balancers); blen != 2 {
			t.Errorf("bad balancers len: %v", blen)
		}
		if flen := len(b2.balancers); flen != 2 {
			t.Errorf("bal filters len: %v", flen)
		}

		// zero mock return balancer with nil oncreate
		if b2.balancers[0].(*mock.BalancerMock).OnCreate != nil {
			t.Errorf("failed to create balancer 0")
		}

		// first mock return balancer with non nil oncreate
		if b2.balancers[1].(*mock.BalancerMock).OnCreate == nil {
			t.Errorf("failed to create balancer 1")
		}

		if len(conns0) != connCount/2 {
			t.Errorf("bad len conns0")
		}
		for i := 0; i < len(conns0); i++ {
			if strconv.Itoa(i*2) != conns0[i].Endpoint().Address() {
				t.Errorf("bad endpoint address conns0[%v]: %v", i, conns0[i].Endpoint().Address())
			}
		}

		if len(conns1) != connCount/2 {
			t.Errorf("bad len conns1")
		}
		for i := 1; i < len(conns1); i++ {
			if strconv.Itoa(i*2+1) != conns1[i].Endpoint().Address() {
				t.Errorf("bad endpoint address conns1[%v]: %v", i, conns0[i].Endpoint().Address())
			}
		}

		if flen := len(b2.filters); flen != 2 {
			t.Errorf("failed flen: %v", flen)
		}

		if !b2.filters[0](conns[0]) {
			t.Errorf("failed filter 0-0")
		}
		if b2.filters[0](conns[1]) {
			t.Errorf("failed filter 0-1")
		}

		if b2.filters[1](conns[0]) {
			t.Errorf("failed filter 1-0")
		}
		if !b2.filters[1](conns[1]) {
			t.Errorf("failed filter 1-1")
		}
	})
}

func TestNeedRefresh(t *testing.T) {
	t.Run("Empty", func(t *testing.T) {
		ctx, ctxCancel := context.WithCancel(context.Background())
		b := Balancer()

		callResult := make(chan bool)
		go func() {
			callResult <- b.NeedRefresh(ctx)
		}()

		ctxCancel()
		res := <-callResult
		if res {
			t.Errorf("empty multi balancer must not need request")
		}
	})

	t.Run("Filled", func(t *testing.T) {
		bTimeoutAnswer := mock.Balancer()
		bTimeoutAnswer.OnNeedRefresh = func(ctx context.Context) bool {
			<-ctx.Done()
			return false
		}

		ansBalancerCreate := func() (*mock.BalancerMock, chan bool) {
			ch := make(chan bool, 1)
			b := mock.Balancer()
			b.OnNeedRefresh = func(ctx context.Context) bool {
				select {
				case <-ctx.Done():
					return false
				case ans := <-ch:
					return ans
				}
			}
			return b, ch
		}

		t.Run("Timeout", func(t *testing.T) {
			ctx, ctxCancel := context.WithCancel(context.Background())

			m2, _ := ansBalancerCreate()

			b := Balancer(WithBalancer(bTimeoutAnswer, nil), WithBalancer(m2, nil))
			callResult := make(chan bool, 1)
			go func() {
				callResult <- b.NeedRefresh(ctx)
			}()

			tSleep()

			select {
			case ans := <-callResult:
				t.Errorf("Unexpected answer: %v", ans)
			default:
				// pass
			}

			ctxCancel()
			ans := <-callResult
			if ans {
				t.Errorf("timeout answer must be false")
			}
		})

		t.Run("Answer-true", func(t *testing.T) {
			checkAnswer := func(t *testing.T, needPause bool) {
				ctx, ctxCancel := context.WithCancel(context.Background())
				defer ctxCancel()

				m2, m2ans := ansBalancerCreate()

				b := Balancer(WithBalancer(bTimeoutAnswer, nil), WithBalancer(m2, nil))
				callResult := make(chan bool, 1)

				go func() {
					callResult <- b.NeedRefresh(ctx)
				}()

				if needPause {
					tSleep()
				}

				m2ans <- true

				ans := <-callResult
				if !ans {
					t.Errorf("Bad answer")
				}
			}

			for _, needPause := range []bool{true, false} {
				t.Run(fmt.Sprintf("NeedPause_%v", needPause), func(t *testing.T) {
					checkAnswer(t, needPause)
				})
			}
		})

		t.Run("Answer-false", func(t *testing.T) {
			checkAnswer := func(t *testing.T, needPause bool) {
				ctx, ctxCancel := context.WithCancel(context.Background())
				defer ctxCancel()

				m1, m1ans := ansBalancerCreate()
				m2, m2ans := ansBalancerCreate()

				b := Balancer(WithBalancer(m1, nil), WithBalancer(m2, nil))
				callResult := make(chan bool, 1)

				go func() {
					callResult <- b.NeedRefresh(ctx)
				}()

				if needPause {
					tSleep()
				}

				m1ans <- false

				if needPause {
					tSleep()
				}

				select {
				case ans := <-callResult:
					t.Errorf("unexpected result: %v", ans)
				default:
					// no block
				}

				m2ans <- false

				ans := <-callResult
				if ans {
					t.Errorf("Bad answer")
				}
			}

			for _, needPause := range []bool{true, false} {
				t.Run(fmt.Sprintf("NeedPause_%v", needPause), func(t *testing.T) {
					checkAnswer(t, needPause)
				})
			}
		})
	})
}

func TestNext(t *testing.T) {
	t.Run("Empty", func(t *testing.T) {
		b := Balancer()
		res := b.Next(nil, false)
		if res != nil {
			t.Errorf("must be nil")
		}
	})

	t.Run("SelectFirstNonNilAnswer", func(t *testing.T) {
		answer := mock.Balancer()
		answer.OnNext = func(ctx context.Context, allowBanned bool) conn.Conn {
			return &mock.ConnMock{Address: "ok"}
		}
		noanswer := mock.Balancer()
		noanswer.OnNext = func(ctx context.Context, allowBanned bool) conn.Conn {
			return nil
		}

		t.Run("First", func(t *testing.T) {
			b := Balancer(WithBalancer(answer, nil), WithBalancer(noanswer, nil))
			res := b.Next(context.Background(), false)
			if res.Endpoint().Address() != "ok" {
				t.Errorf("Bad answer")
			}
		})

		t.Run("Second", func(t *testing.T) {
			b := Balancer(WithBalancer(noanswer, nil), WithBalancer(answer, nil))
			res := b.Next(context.Background(), false)
			if res.Endpoint().Address() != "ok" {
				t.Errorf("Bad answer")
			}
		})

		t.Run("None", func(t *testing.T) {
			b := Balancer(WithBalancer(noanswer, nil), WithBalancer(noanswer, nil))
			res := b.Next(context.Background(), false)
			if res != nil {
				t.Errorf("Bad answer")
			}
		})
	})

	t.Run("ProxySameParams", func(t *testing.T) {
		createCheckParams := func(t *testing.T, needContext context.Context, needAllowBanned bool) balancer.Balancer {
			b := mock.Balancer()
			b.OnNext = func(ctx context.Context, allowBanned bool) conn.Conn {
				if ctx != needContext {
					t.Errorf("bad context")
				}
				if allowBanned != needAllowBanned {
					t.Errorf("bad allowBanned")
				}
				return nil
			}
			return b
		}

		ctx := context.WithValue(context.Background(), "test", "test")
		for _, filter := range []bool{true, false} {
			t.Run(fmt.Sprint(filter), func(t *testing.T) {
				b := Balancer(
					WithBalancer(createCheckParams(t, ctx, filter), nil),
					WithBalancer(createCheckParams(t, ctx, filter), nil),
				)
				_ = b.Next(ctx, filter)
			})
		}
	})
}
