package conn

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xtest"
)

func TestInUseGuard(t *testing.T) {
	xtest.TestManyTimes(t, func(t testing.TB) {
		g := &inUseGuard{}
		ch := make(chan struct{})
		unlockFuncs := make([]func(), 10)
		for i := range unlockFuncs {
			locked, unlock := g.TryLock()
			require.True(t, locked)
			require.NotNil(t, unlock)
			unlockFuncs[i] = func() {
				<-ch
				unlock()
			}
		}
		waitStop := make(chan struct{})
		go func() {
			defer func() {
				close(waitStop)
			}()
			g.Stop()
		}()
		for i := range unlockFuncs {
			go func(i int) {
				unlockFuncs[i]()
			}(i)
		}
		for range unlockFuncs {
			select {
			case <-waitStop:
				require.Fail(t, "unexpected stop signal")
			case ch <- struct{}{}:
			}
		}
		close(ch)
		select {
		case <-waitStop:
		case <-time.After(time.Second):
			require.Fail(t, "not stopped after 1 second")
		}
	})
}
