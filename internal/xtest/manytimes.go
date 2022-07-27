package xtest

import (
	"sync"
	"testing"
	"time"
)

func TestManyTimes(t testing.TB, test TestFunc) {
	t.Helper()

	const testTimeout = time.Second

	start := time.Now()
	for {
		// run test, then check timeout for guarantee run test least once
		runTest(t, test)

		if time.Since(start) > testTimeout {
			return
		}
	}
}

func TestManyTimesWithName(t *testing.T, name string, test TestFunc) {
	t.Helper()

	t.Run(name, func(t *testing.T) {
		t.Helper()
		TestManyTimes(t, test)
	})
}

type TestFunc func(t testing.TB)

func runTest(t testing.TB, test TestFunc) {
	t.Helper()

	tw := &testWrapper{
		TB: t,
	}

	defer tw.doCleanup()

	test(tw)
}

type testWrapper struct {
	testing.TB

	m       sync.Mutex
	cleanup []func()
}

func (tw *testWrapper) Cleanup(f func()) {
	tw.Helper()

	tw.m.Lock()
	defer tw.m.Unlock()

	tw.cleanup = append(tw.cleanup, f)
}

func (tw *testWrapper) doCleanup() {
	tw.Helper()

	for len(tw.cleanup) > 0 {
		last := tw.cleanup[len(tw.cleanup)-1]
		tw.cleanup = tw.cleanup[:len(tw.cleanup)-1]

		last()
	}
}
